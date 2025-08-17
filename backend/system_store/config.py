from pydantic_settings import BaseSettings
from pydantic import Field

class SystemStoreConfig(BaseSettings):
    """
    Configuration for PostgreSQL connection.
    """
    postgres_host: str = Field("localhost", description="Database host")
    postgres_port: int = Field(5432, description="Database port")
    postgres_user: str = Field("air_supply_user", description="Database user")
    postgres_password: str = Field("air_supply_password", description="Database password")
    postgres_database: str = Field("air_supply_db", description="Database name")
    postgres_table: str = Field("air_supply_system_table", description="Database table for system settings")
    pool_min_size: int = Field(1, description="Minimum connection pool size")
    pool_max_size: int = Field(10, description="Maximum connection pool size")

    class Config:
        case_sensitive = False
        