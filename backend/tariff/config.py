# pydantic model for the tariff configuration
from pydantic_settings import BaseSettings
from pydantic import Field
from ..vector_store.config import VectorStoreConfig


class TariffConfig(BaseSettings):
    wto_api_key: str = Field(..., description="WTO API key")

    class Config:
        case_sensitive = False

class CountryCodeRepoConfig(VectorStoreConfig):
    """
    Configuration for tariff data fetching.
    """
    country_code_file: str = Field(
        default="data/country_code.csv",
        description="country code file"
    )
    postgres_table: str = Field(
        default="country_code_table",
        description="Vectorstore for country codes"
    )

    class Config:
        case_sensitive = False


class HSCodeRepoConfig(VectorStoreConfig):
    """
    Configuration for HS code fetching.
    """
    hs_code_file: str = Field(
        default="data/hs_code.csv",
        description="HS code file"
    )
    postgres_table: str = Field(
        default="hs_code_table",
        description="Vectorstore for HS codes"
    )

    class Config:
        case_sensitive = False

        description="HS code file"
