import pytest
import pandas as pd
import os
from unittest.mock import AsyncMock, MagicMock, patch
import httpx
import asyncio
from backend.tariff.base_repo import BaseVectorRepo
from backend.system_store.system_store import SystemStore, SystemStoreConfig
from backend.vector_store.vector_store import VectorStore, VectorStoreConfig
from backend.vector_store.config import VectorStoreConfig
from langchain_community.embeddings import HuggingFaceEmbeddings

# Test implementation of BaseRepo abstract class
class TestRepo(BaseVectorRepo):
    def _get_metadata_key(self) -> str:
        return "test_integration_metadata_key"
    
    def _get_api_url(self) -> str:
        return "https://test.api/data"
    
    def _get_content_column(self) -> str:
        return "content"
    
    def _process_api_response(self, response_text: str) -> pd.DataFrame:
        # Process the XML response
        import xmltodict
        
        # Parse XML string to dictionary
        data_dict = xmltodict.parse(response_text)
        
        # Extract items from the XML structure
        items = data_dict.get('root', {}).get('items', {}).get('item', [])
        if not isinstance(items, list):
            items = [items]  # Ensure items is a list
        
        # Extract content and metadata from items
        contents = []
        categories = []
        ids = []
        
        for item in items:
            contents.append(item.get('@description', ''))
            categories.append(item.get('@category', ''))
            ids.append(item.get('@id', ''))
        
        # Create DataFrame
        data = {
            "content": contents,
            "category": categories,
            "id": ids
        }
        
        return pd.DataFrame(data)

class TestConfig:
    postgres_table = "test_integration_table"

# Create unique test database configuration
@pytest.fixture
def test_system_config():
    return SystemStoreConfig(
        postgres_table="test_integration_system_table"
    )

@pytest.fixture
def test_repo():
    # Create a TestRepo instance with real dependencies
    repo = TestRepo(TestConfig())
    yield repo
    
    # Clean up after tests
    asyncio.run(repo.drop())
    asyncio.run(repo.close())

@pytest.fixture
def mock_http_response():
    """Mock HTTP response with test XML data"""
    xml_data = """
    <root>
        <items>
            <item id="1" category="electronics" description="Smartphone with 5G capability"/>
            <item id="2" category="electronics" description="Laptop with 16GB RAM"/>
            <item id="3" category="books" description="Python Programming Guide"/>
            <item id="4" category="furniture" description="Ergonomic Office Chair"/>
        </items>
    </root>
    """
    
    mock_response = MagicMock()
    mock_response.text = xml_data
    mock_response.raise_for_status = MagicMock()
    
    return mock_response

@pytest.mark.asyncio
async def test_initialization(test_repo):
    """Test if the repository can be properly initialized with real dependencies"""
    # Initialize the repo
    await test_repo._ensure_initialized()
    
    # Verify the connections were made
    assert test_repo.system_store is not None
    assert test_repo.vector_store is not None
    
    # Clean up
    await test_repo.close()

@pytest.mark.asyncio
async def test_fetch_data(test_repo, mock_http_response):
    """Test fetching data with mocked HTTP response but real storage backends"""
    # Mock the HTTP client
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.return_value = mock_http_response
    
    # Apply the mock only for the HTTP client
    with patch("backend.tariff.base_repo.httpx.AsyncClient", return_value=mock_client):
        result = await test_repo.fetch_data()
    
    assert result is True
    
    # Verify that data has been stored correctly
    await test_repo._ensure_initialized()  # Make sure connections are initialized
    
    # Check if metadata was saved in system store
    metadata = await test_repo.system_store.get_item(test_repo._get_metadata_key())
    assert metadata is not None
    assert len(metadata) > 0
    
    # Verify columns exist in metadata
    column_names = [col["name"] for col in metadata]
    assert "content" in column_names
    assert "category" in column_names
    assert "id" in column_names

@pytest.mark.asyncio
async def test_find_items(test_repo, mock_http_response):
    """Test searching for items after data has been fetched"""
    # Mock HTTP client for fetch_data
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.return_value = mock_http_response
    
    # First, fetch data to populate the vector store
    with patch("backend.tariff.base_repo.httpx.AsyncClient", return_value=mock_client):
        await test_repo.fetch_data()
    
    # Now search with real vector store
    results = await test_repo.find_items("laptop", top_k=1)
    
    # Check that we get results back
    assert len(results) > 0
    assert isinstance(results[0], dict)
    assert "content" in results[0]
    assert "metadata" in results[0]
    
    # Verify metadata filtering works
    electronics_results = await test_repo.find_items(
        "electronics", 
        top_k=5,
        metadata={"category": "electronics"}
    )
    
    # Should only return electronics items
    for result in electronics_results:
        assert result["metadata"]["category"] == "electronics"

@pytest.mark.asyncio
async def test_drop(test_repo, mock_http_response):
    """Test dropping the repository data"""
    # First load some data
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.return_value = mock_http_response
    
    with patch("backend.tariff.base_repo.httpx.AsyncClient", return_value=mock_client):
        await test_repo.fetch_data()
    
    # Now drop everything
    await test_repo.drop()
    
    # Check that metadata is gone from system store
    await test_repo._ensure_initialized()  # Reinitialize
    metadata = await test_repo.system_store.get_item(test_repo._get_metadata_key())
    assert metadata is None  # Should be empty after dropping

@pytest.mark.asyncio
async def test_close(test_repo):
    """Test properly closing all connections"""
    # Initialize connections
    await test_repo._ensure_initialized()
    
    # Verify we have active connections
    assert test_repo.system_store is not None
    assert test_repo.vector_store is not None
    
    # Close all connections
    await test_repo.close()
    
    # Verify connections are closed (set to None)
    assert test_repo.system_store is None
    assert test_repo.vector_store is None
