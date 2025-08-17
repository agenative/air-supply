from .config import VectorStoreConfig
import pandas as pd
import uuid
from typing import List, Dict, Any
from langchain_postgres import PGEngine, PGVectorStore, ColumnDict
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

class VectorStore:
    def __init__(self, embeddings: Embeddings, table_name: str, content_column: str, metadata_columns: List[ColumnDict] = None):
        self.config = VectorStoreConfig()
        self.connection_str = (
            f"postgresql+asyncpg://{self.config.postgres_user}:{self.config.postgres_password}@{self.config.postgres_host}"
            f":{self.config.postgres_port}/{self.config.postgres_database}"
        )
        self.pg_engine = PGEngine.from_connection_string(url=self.connection_str)
        self.embeddings = embeddings
        # Remove any metadata columns that have the same name as the content column
        if metadata_columns:
            self.metadata_columns = [col for col in metadata_columns if col.get('name') != content_column]
        else:
            self.metadata_columns = metadata_columns
        self.table_name = table_name
        self.content_column = content_column

    async def connect(self):
        # Create table if it does not exist, skip if already exists
        try:
            await self.pg_engine.ainit_vectorstore_table(
                table_name=self.table_name,
                vector_size=self.config.vector_size,
                metadata_columns=self.metadata_columns
            )
        except Exception as e:
            if "already exists" in str(e) or "duplicate key value" in str(e):
                pass  # Table already exists, skip
            else:
                raise
        self.store = await PGVectorStore.create(
            engine=self.pg_engine,
            table_name=self.table_name,
            embedding_service=self.embeddings,
            metadata_columns=[col['name'] for col in self.metadata_columns] if self.metadata_columns else None
        )

    async def truncate_store(self):
        """
        Truncate the vector store table - clears all data while preserving the table structure.
        """
        # Drop the existing table
        await self.pg_engine.adrop_table(self.table_name)
        
        # Re-create the table with the same structure
        await self.pg_engine.ainit_vectorstore_table(
            table_name=self.table_name,
            vector_size=self.config.vector_size,
            metadata_columns=self.metadata_columns
        )
        
        # Re-create the store
        self.store = await PGVectorStore.create(
            engine=self.pg_engine,
            table_name=self.table_name,
            embedding_service=self.embeddings,
            metadata_columns=[col['name'] for col in self.metadata_columns] if self.metadata_columns else None
        )

    async def add_dataframe(self, df: pd.DataFrame):
        # Use the content_column that was set during initialization
        for _, row in df.iterrows():
            doc = Document(
                id=str(uuid.uuid4()),
                page_content=row[self.content_column],
                metadata={col['name']: row[col['name']] for col in self.metadata_columns if col['name'] in row} if self.metadata_columns else {}
            )
            await self.store.aadd_documents([doc])

    async def find_content(self, content: str, top_k: int, filter: dict = None) -> List[Dict[str, Any]]:
        # Query the database for similar vectors
        docs = await self.store.asimilarity_search(content, k=top_k, filter=filter)

        return [{"content": doc.page_content, "metadata": doc.metadata} for doc in docs]


    async def drop(self):
        """
        Drop the table from the vector store.
        """
        await self.pg_engine.adrop_table(table_name=self.table_name)
        self.store = None
        
    async def close(self):
        """
        Close any open connections.
        Currently this is a no-op as the async context is managed by the engine.
        This method is provided for interface compatibility with other components.
        """
        pass

