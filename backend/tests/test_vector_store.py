import pytest
import pandas as pd
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from backend.vector_store.vector_store import VectorStore
from langchain_postgres import Column

@pytest.mark.asyncio
async def test_add_documents_to_vector_store():
    metadata_columns = [
        {"name": "metadata1", "data_type": "text", "nullable": True},
        {"name": "metadata2", "data_type": "text", "nullable": True}
    ]

    vector_store = VectorStore(
        embeddings=HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2"), 
        table_name="test_table",
        content_column="content",
        metadata_columns=metadata_columns
    )
    await vector_store.connect()

    df = pd.DataFrame({
        "content": ["doc1", "doc2", "doc3"],
        "metadata1": ["meta11", "meta12", "meta13"],
        "metadata2": ["meta21", "meta22", "meta23"]
    })

    await vector_store.add_dataframe(df)

    # Verify find without filter
    results = await vector_store.find_content("doc1", top_k=1)
    assert len(results) == 1
    assert results[0]["content"] == "doc1"
    assert results[0]["metadata"] == {"metadata1": "meta11", "metadata2": "meta21"}

    # Verify find with filter
    results = await vector_store.find_content("doc1", top_k=1, filter={"metadata1": {"$eq": "meta11"}})
    assert len(results) == 1
    assert results[0]["content"] == "doc1"
    assert results[0]["metadata"] == {"metadata1": "meta11", "metadata2": "meta21"}

    results = await vector_store.find_content("doc1", top_k=1, filter={"metadata1": {"$eq": "not_exist"}})
    assert len(results) == 0

    # Drop the table after test
    await vector_store.drop()
    assert vector_store.store is None


@pytest.mark.asyncio
async def test_truncate_store():
    metadata_columns = [
        {"name": "metadata1", "data_type": "text", "nullable": True},
        {"name": "metadata2", "data_type": "text", "nullable": True}
    ]

    vector_store = VectorStore(
        embeddings=HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2"), 
        table_name="truncate_test_table",
        content_column="content",
        metadata_columns=metadata_columns
    )
    await vector_store.connect()

    # Add some test data
    df = pd.DataFrame({
        "content": ["test1", "test2", "test3"],
        "metadata1": ["meta11", "meta12", "meta13"],
        "metadata2": ["meta21", "meta22", "meta23"]
    })
    await vector_store.add_dataframe(df)

    # Verify data exists
    results = await vector_store.find_content("test1", top_k=3)
    assert len(results) == 3  # Should find all documents

    # Truncate the store
    await vector_store.truncate_store()
    
    # Reconnect to verify table is empty
    await vector_store.connect()
    
    # Try to find data after truncate
    results = await vector_store.find_content("test1", top_k=3)
    assert len(results) == 0  # Should be empty after truncate

    # Clean up
    await vector_store.drop()


@pytest.mark.asyncio
async def test_content_column_not_in_metadata():
    # Create metadata columns including one with the same name as the content column
    metadata_columns = [
        {"name": "content", "data_type": "text", "nullable": True},  # Same name as content_column
        {"name": "metadata1", "data_type": "text", "nullable": True},
        {"name": "metadata2", "data_type": "text", "nullable": True}
    ]

    vector_store = VectorStore(
        embeddings=HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2"), 
        table_name="content_column_test_table",
        content_column="content",
        metadata_columns=metadata_columns
    )
    
    # Verify that the content column has been filtered out from metadata_columns
    assert len(metadata_columns) == 3  # Original list should be unchanged
    assert len(vector_store.metadata_columns) == 2  # Should have filtered out "content"
    
    # Verify the names of the remaining columns
    metadata_column_names = [col['name'] for col in vector_store.metadata_columns]
    assert "content" not in metadata_column_names
    assert "metadata1" in metadata_column_names
    assert "metadata2" in metadata_column_names
    
    await vector_store.connect()
    
    # Test with a dataframe to make sure everything works correctly
    df = pd.DataFrame({
        "content": ["test_content", "test_content2"],
        "metadata1": ["meta11", "meta12"],
        "metadata2": ["meta21", "meta22"]
    })
    
    await vector_store.add_dataframe(df)
    
    # Verify the content is stored correctly
    results = await vector_store.find_content("test_content", top_k=1)
    assert len(results) == 1
    assert results[0]["content"] == "test_content"
    # The content field should not be duplicated in metadata
    assert "content" not in results[0]["metadata"]
    assert results[0]["metadata"] == {"metadata1": "meta11", "metadata2": "meta21"}
    
    # Clean up
    await vector_store.drop()
