from .config import SystemStoreConfig

import asyncpg
from typing import Optional, Dict, Any
import json

class SystemStore:
    def __init__(self, config: Optional[SystemStoreConfig] = None):
        self.config = config if config is not None else SystemStoreConfig()
        self.pool: Optional[asyncpg.Pool] = None
        self.dsn = f"postgresql://{self.config.postgres_user}:{self.config.postgres_password}@{self.config.postgres_host}:{self.config.postgres_port}/{self.config.postgres_database}"
        self.store_table = self.config.postgres_table
        self.pool_min = self.config.pool_min_size
        self.pool_max = self.config.pool_max_size


    async def connect(self):
        """Initialize the connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=self.pool_min,
                max_size=self.pool_max
            )
            print("Database pool connected successfully")

            # Create table if not exists
            await self._execute(f"""
                CREATE TABLE IF NOT EXISTS {self.store_table} (
                    id SERIAL PRIMARY KEY,
                    key TEXT UNIQUE NOT NULL,
                    value JSONB NOT NULL
                )
            """)
        except Exception as e:
            print(f"Failed to create pool: {e}")
            raise

    async def drop(self):
        """Drop the table from the database."""
        try:
            await self._execute(f"DROP TABLE IF EXISTS {self.store_table}")
            print(f"Table '{self.store_table}' dropped successfully")
        except Exception as e:
            print(f"Failed to drop table: {e}")
            raise

    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            print("Database pool closed")

    async def add_item(self, key: str, value: Dict[str, Any]):
        """Add an item to the database."""
        query = f"INSERT INTO {self.store_table} (key, value) VALUES ($1, $2)"
        value_json = json.dumps(value)
        await self._execute(query, key, value_json)

    async def upsert_item(self, key: str, value: Dict[str, Any]):
        """Upsert an item in the database."""
        query = f"""
            INSERT INTO {self.store_table} (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = $2
        """
        value_json = json.dumps(value)
        await self._execute(query, key, value_json)

    async def get_item(self, key: str) -> Optional[Dict[str, Any]]:
        query = f"SELECT value FROM {self.store_table} WHERE key = $1"
        result = await self._fetch(query, key)
        if result:
            return json.loads(result[0]["value"])
        return None

    async def delete_item(self, key: str):
        query = f"DELETE FROM {self.store_table} WHERE key = $1"
        await self._execute(query, key)

    async def _execute(self, query: str, *args):
        """Execute a query using the pool."""
        if not self.pool:
            raise Exception("Pool not initialized")
        
        async with self.pool.acquire() as connection:
            try:
                return await connection.execute(query, *args)
            except Exception as e:
                print(f"Query execution failed: {e}")
                raise

    async def _fetch(self, query: str, *args):
        """Fetch results from a query using the pool."""
        if not self.pool:
            raise Exception("Pool not initialized")
        
        async with self.pool.acquire() as connection:
            try:
                return await connection.fetch(query, *args)
            except Exception as e:
                print(f"Query fetch failed: {e}")
                raise
