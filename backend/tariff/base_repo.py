import httpx
import xmltodict
import asyncio
import pandas as pd
from fastapi import HTTPException
from typing import Dict, Any, List, Optional, Tuple
from abc import ABC, abstractmethod
from ..vector_store.vector_store import VectorStore
from ..system_store.system_store import SystemStore, SystemStoreConfig
from langchain_community.embeddings import HuggingFaceEmbeddings

class BaseVectorRepo(ABC):
    """
    Base repository class that handles common functionality for repositories that need
    vector storage and system configuration.
    """
    def __init__(self, config: Any, system_config: SystemStoreConfig = None):
        self.config = config
        self.system_config = system_config if system_config is not None else SystemStoreConfig()
        self.table_name = self.config.postgres_table
        self.embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
        self.metadata_columns = None
        self.vector_store = None
        self.system_store = None
        self.metadata_key = self._get_metadata_key()

    @abstractmethod
    def _get_metadata_key(self) -> str:
        """Return the key used to store metadata in the system store."""
        pass

    @abstractmethod
    def _get_api_url(self) -> str:
        """Return the API URL to fetch data."""
        pass

    @abstractmethod
    def _process_api_response(self, response_text: str) -> pd.DataFrame:
        """Process the API response and return a DataFrame."""
        pass

    @abstractmethod
    def _get_content_column(self) -> str:
        """Return the name of the column to use for content embedding."""
        pass

    async def _ensure_initialized(self):
        """
        Ensure the vector store and metadata columns are initialized.
        This should be called at the beginning of each async operation.
        """
        if self.system_store is None:
            self.system_store = SystemStore(self.system_config)
            await self.system_store.connect()

        if self.metadata_columns is None:
            self.metadata_columns = await self._load_metadata_columns()
            if self.metadata_columns is None:
                raise Exception(f"Failed to load metadata columns from system store. Please fetch data first.")
            
        if self.vector_store is None:
            self.vector_store = VectorStore(
                embeddings=self.embeddings,
                table_name=self.table_name,
                content_column=self._get_content_column(),
                metadata_columns=self.metadata_columns
            )
            await self.vector_store.connect()

    async def drop(self):
        """
        Drop the table from the vectorStore if exists and remove metadata from system_store
        """
        # Ensure everything is initialized before attempting to drop
        await self._ensure_initialized()
            
        # Drop vector store and clean up
        await self.vector_store.drop()
        await self.vector_store.close()
        self.vector_store = None
        
        # Remove metadata from system store and clean up
        await self.system_store.delete_item(self.metadata_key)
        self.metadata_columns = None

    async def find_items(self, query: str, top_k: int = 1, metadata: dict = None) -> List[Dict[str, Any]]:
        """
        Connect to the vectorstore and query the items.
        """
        await self._ensure_initialized()
        results = await self.vector_store.find_content(query, top_k=top_k, filter=metadata)
        return results

    async def _load_metadata_columns(self) -> List[Dict[str, Any]]:
        """
        Load metadata columns from system store and cache them in the instance.
        If no metadata columns are found, returns an empty list.
        """
        # Make sure we have a system store connection
        if self.system_store is None:
            raise Exception("System store is not initialized.")
        
        # Get metadata columns from the store
        self.metadata_columns = await self.system_store.get_item(self.metadata_key)
        
        # If no metadata columns are found, use an empty list to prevent errors
        if self.metadata_columns is None:
            self.metadata_columns = []
            
        return self.metadata_columns

    async def _save_metadata_columns(self, metadata_columns: List[Dict[str, Any]]):
        """
        Save metadata columns to the system store and update the instance attribute.
        """
        if self.system_store is None:
            raise Exception("System store is not initialized.")
        
        self.metadata_columns = metadata_columns
        await self.system_store.upsert_item(self.metadata_key, metadata_columns)

    async def close(self):
        """
        Close all open resources and connections
        """
        if self.vector_store is not None:
            await self.vector_store.close()
            self.vector_store = None
            
        if self.system_store is not None:
            await self.system_store.close()
            self.system_store = None

    async def fetch_data(self):
        """
        Call API to get all data and store it in vector store.
        """
        url = self._get_api_url()
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url)
                response.raise_for_status()

                df = self._process_api_response(response.text)
                self.metadata_columns = [{"name": col, "data_type": "text", "nullable": True} for col in df.columns]   
                
                # Initialize system store only, not vector store
                await self._ensure_initialized()
                await self.vector_store.truncate_store()
                await self.vector_store.add_dataframe(df)
                
                # Save metadata using the initialized system store
                await self._save_metadata_columns(self.metadata_columns)
                return True
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
