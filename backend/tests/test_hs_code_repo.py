import pytest
from backend.tariff.config import HSCodeRepoConfig
from backend.tariff.hs_code_repo import HSCodeRepo
from backend.vector_store.vector_store import VectorStore
from backend.system_store.system_store import SystemStore, SystemStoreConfig
from langchain_community.embeddings import HuggingFaceEmbeddings


@pytest.mark.asyncio
async def test_store_and_found_hs_code() -> str:
    config = HSCodeRepoConfig(postgres_table="test_hs_code_table")
    system_config = SystemStoreConfig(postgres_table="test_system_table")
    repo = HSCodeRepo(config, system_config)
    result = await repo.fetch_hs_codes()
    results = await repo.find_hs_codes("wireless earbuds")
    assert results is not None
    assert isinstance(results, list)
    assert len(results) > 0
    assert "metadata" in results[0]
    metadata = results[0]["metadata"]
    assert metadata["productcode"] == '851830'
    # productdescription should now be in content field, not in metadata
    assert 'earphones' in results[0]['content'].lower()
    # Verify that productdescription is not in metadata since it's the content column
    assert 'productdescription' not in metadata

    # Clean up test data
    await repo.drop()
    system_store = SystemStore(system_config)
    await system_store.connect()
    await system_store.drop()
    await system_store.close()


@pytest.mark.asyncio
async def test_drop_hs_code_repo():
    # Create a test configuration with a unique table name
    test_table_name = "test_drop_hs_code_table"
    test_system_table_name = "test_drop_system_table"
    system_config = SystemStoreConfig(postgres_table=test_system_table_name)
    config = HSCodeRepoConfig(postgres_table=test_table_name)
    repo = HSCodeRepo(config, system_config)

    # First, create some test data in the vector store
    meta_columns = [{"name": "test_column", "data_type": "text", "nullable": True}]
    vector_store = VectorStore(
        embeddings=HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2"),
        table_name=test_table_name,
        content_column="productdescription",
        metadata_columns=meta_columns
    )
    
    # Connect and create the table
    await vector_store.connect()
    
    # Create test data in system store
    system_store = SystemStore(system_config)
    await system_store.connect()
    await system_store.upsert_item("hs_code_metadata_columns", meta_columns)
    
    # Verify the data exists
    metadata_from_system = await system_store.get_item("hs_code_metadata_columns")
    assert metadata_from_system is not None
    assert len(metadata_from_system) > 0
    
    # Now call the drop function
    await repo.drop()
    
    # Verify that the system store entry is gone
    # Reconnect to the system store (since drop function closes it)
    system_store = SystemStore(system_config)
    await system_store.connect()
    metadata_after_drop = await system_store.get_item("hs_code_metadata_columns")
    assert metadata_after_drop is None
    # Drop the test system store table to clean because repo.drop() 
    # do not drop the whole system table
    await system_store.drop()
    await system_store.close()
    
    # Verify that the vector store table is gone by trying to query it
    vector_store = VectorStore(
        embeddings=HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2"),
        table_name=test_table_name,
        content_column="productdescription",
        metadata_columns=meta_columns
    )
    await vector_store.connect()
    
    # Try to query the table - it should be empty since it was just created
    results = await vector_store.find_content("test", top_k=1)
    assert len(results) == 0
    
    # Clean up by dropping the newly created table
    await vector_store.drop()
    await vector_store.close()
