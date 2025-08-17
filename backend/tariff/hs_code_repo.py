import httpx
import xmltodict
import asyncio
import pandas as pd
from fastapi import HTTPException
from typing import Dict, Any, List
from .config import HSCodeRepoConfig
from ..system_store.system_store import SystemStoreConfig
from .base_repo import BaseVectorRepo

class HSCodeRepo(BaseVectorRepo):
    def __init__(self, config: HSCodeRepoConfig = None, system_config: SystemStoreConfig = None):
        config = config if config is not None else HSCodeRepoConfig()
        super().__init__(config, system_config)

    def _get_metadata_key(self) -> str:
        """Return the key used to store metadata in the system store."""
        return "hs_code_metadata_columns"
    
    def _get_api_url(self) -> str:
        """Return the API URL to fetch data."""
        return "https://wits.worldbank.org/API/V1/wits/datasource/trn/product/all"
    
    def _get_content_column(self) -> str:
        """Return the name of the column to use for content embedding."""
        return "productdescription"
    
    def _process_api_response(self, response_text: str) -> pd.DataFrame:
        """Process the API response and return a DataFrame."""
        doc = xmltodict.parse(response_text)
        products = doc.get('wits:datasource', {}).get('wits:products', {}).get('wits:product', [])
        
        df = pd.json_normalize(products)
        df.columns = [col.replace('@', '').replace('wits:', '') for col in df.columns]
        
        return df
    
    async def find_hs_codes(self, name: str, top_k: int = 1, metadata: dict = None) -> List[Dict[str, Any]]:
        """
        Query the HS codes using the initialized vector store.
        """
        return await self.find_items(name, top_k=top_k, metadata=metadata)
    
    async def fetch_hs_codes(self):
        """
        Call WITS REST API to get all HS codes.
        """
        return await self.fetch_data()


    async def fetch_hs_codes(self):
        """
        Call WITS REST API to get all HS codes.
        """

        
        url = "https://wits.worldbank.org/API/V1/wits/datasource/trn/product/all"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url)
                response.raise_for_status()

                doc = xmltodict.parse(response.text)
                products = doc.get('wits:datasource', {}).get('wits:products', {}).get('wits:product', [])

                df = pd.json_normalize(products)
                df.columns = [col.replace('@', '').replace('wits:', '') for col in df.columns]
                self.metadata_columns = [{"name": col, "data_type": "text", "nullable": True} for col in df.columns]   
                
                # Initialize system store only, not vector store
                await self._ensure_initialized()
                await self.vector_store.truncate_store()
                await self.vector_store.add_dataframe(df)
                
                # Save metadata using the initialized system store
                await self._save_metadata_columns(self.metadata_columns)
                return True
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error fetching HS codes: {str(e)}")

# Add a main method
if __name__ == "__main__":
    async def main():
        try:
            config = HSCodeRepoConfig()
            repo = HSCodeRepo(config=config)
            await repo.fetch_hs_codes()
            print("HS codes fetched successfully.")
        except Exception as e:
            print(f"Error: {str(e)}")
            raise
        finally:
            if repo:
                await repo.close()
    
    asyncio.run(main())
