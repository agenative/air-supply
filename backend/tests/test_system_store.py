import pytest
import asyncio
import json
from typing import Dict, Any

import pytest_asyncio

from backend.system_store.system_store import SystemStore
from backend.system_store.config import SystemStoreConfig


class TestSystemStoreConfig(SystemStoreConfig):
    """Custom config for testing that uses a dedicated test table."""
    postgres_table: str = "test_items_table"
    

@pytest_asyncio.fixture
async def system_store(monkeypatch):
    """
    Fixture for SystemStore instance that connects to the actual PostgreSQL database
    and uses a dedicated test table.
    
    This is an integration test fixture that creates a temporary test table
    and drops it after the tests are complete.
    """
    # Create a test config instance
    test_config = TestSystemStoreConfig()
    
    # Create a store with our test configuration
    store = SystemStore(test_config)
    
    # Connect to the database
    await store.connect()
    
    # The table should be created by the connect method
    yield store
    
    try:
        # After tests complete, drop the test table
        if store.pool:
            async with store.pool.acquire() as conn:
                await conn.execute(f"DROP TABLE IF EXISTS {store.store_table}")
                print(f"Test table {store.store_table} dropped successfully")
    except Exception as e:
        print(f"Error dropping test table: {e}")
    finally:
        # Close the database connection
        await store.close()


@pytest.mark.asyncio
async def test_table_created(system_store):
    """
    Test that verifies the test table was created properly.
    """
    async with system_store.pool.acquire() as conn:
        # Check if the test table exists
        table_exists = await conn.fetchval(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{system_store.store_table}'
            )
        """)
        assert table_exists is True, f"Test table {system_store.store_table} was not created"


@pytest.mark.asyncio
async def test_add_and_get_item(system_store):
    """
    Integration test for adding and retrieving an item from the database.
    """
    # Test data
    test_key = "test_integration_key"
    test_value = {"name": "Test Item", "value": 42, "active": True}

    # Add the item to the database
    await system_store.add_item(test_key, test_value)
    
    # Get the item from the database
    retrieved_value = await system_store.get_item(test_key)
    
    # Check that the retrieved value matches the original
    assert retrieved_value == test_value


@pytest.mark.asyncio
async def test_get_nonexistent_item(system_store):
    """
    Integration test for getting an item that doesn't exist.
    """
    # Try to get an item that doesn't exist
    result = await system_store.get_item("test_nonexistent_key")
    
    # Check that the result is None
    assert result is None


@pytest.mark.asyncio
async def test_add_duplicate_key(system_store):
    """
    Integration test for adding an item with a duplicate key.
    """
    # Test data
    test_key = "test_duplicate_key"
    test_value_1 = {"version": 1, "data": "original"}
    test_value_2 = {"version": 2, "data": "duplicate"}
    
    # Add the first item
    await system_store.add_item(test_key, test_value_1)
    
    # Try to add another item with the same key, should raise an exception
    with pytest.raises(Exception) as excinfo:
        await system_store.add_item(test_key, test_value_2)
    
    # Check that the exception message contains something about uniqueness or duplicate
    error_message = str(excinfo.value).lower()
    assert any(word in error_message for word in ["unique", "duplicate", "already exists"])
    
    # Verify the original item is still there unchanged
    retrieved_value = await system_store.get_item(test_key)
    assert retrieved_value == test_value_1


@pytest.mark.asyncio
async def test_add_and_get_complex_item(system_store):
    """
    Integration test for adding and retrieving a complex nested item.
    """
    # Test data with nested structure
    test_key = "test_complex_key"
    test_value = {
        "metadata": {
            "created": "2025-08-13T12:34:56Z",
            "creator": "integration_test",
            "version": 1
        },
        "settings": {
            "enabled": True,
            "limits": {
                "max": 100,
                "min": 0,
                "thresholds": [10, 20, 50, 80]
            }
        },
        "data": [
            {"id": 1, "name": "Item 1", "tags": ["test", "integration"]},
            {"id": 2, "name": "Item 2", "tags": ["sample"]}
        ]
    }
    
    # Add the complex item
    await system_store.add_item(test_key, test_value)
    
    # Get the item
    retrieved_value = await system_store.get_item(test_key)
    
    # Check that the retrieved value matches the original complex structure
    assert retrieved_value == test_value
    
    # Check specific nested elements to be sure
    assert retrieved_value["metadata"]["creator"] == "integration_test"
    assert retrieved_value["settings"]["limits"]["thresholds"] == [10, 20, 50, 80]
    assert retrieved_value["data"][0]["tags"] == ["test", "integration"]


@pytest.mark.asyncio
async def test_multiple_items(system_store):
    """
    Integration test for adding and retrieving multiple items.
    """
    # Test data - multiple items
    items = {
        "test_multi_1": {"id": 1, "name": "First"},
        "test_multi_2": {"id": 2, "name": "Second"},
        "test_multi_3": {"id": 3, "name": "Third"}
    }
    
    # Add all items
    for key, value in items.items():
        await system_store.add_item(key, value)
    
    # Retrieve and verify each item
    for key, expected_value in items.items():
        retrieved_value = await system_store.get_item(key)
        assert retrieved_value == expected_value


@pytest.mark.asyncio
async def test_direct_sql_verification(system_store):
    """
    Integration test that directly verifies the database state using SQL.
    """
    # Test data
    test_key = "test_direct_sql"
    test_value = {"verified": True, "method": "direct_sql"}
    
    # Add the item using the SystemStore API
    await system_store.add_item(test_key, test_value)
    
    # Verify directly with SQL query
    async with system_store.pool.acquire() as conn:
        # Query the database directly
        result = await conn.fetchrow(f"SELECT key, value FROM {system_store.store_table} WHERE key = $1", test_key)
        
        # Check that the result exists
        assert result is not None
        
        # Check the key
        assert result["key"] == test_key
        
        # Check the value (PostgreSQL returns JSONB as a string)
        db_value = json.loads(result["value"])
        assert db_value == test_value


@pytest.mark.asyncio
async def test_database_structure(system_store):
    """
    Integration test to verify the database structure created by the connect method.
    """
    # Verify the test table exists and has the expected structure
    async with system_store.pool.acquire() as conn:
        # Check if table exists
        table_exists = await conn.fetchval(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{system_store.store_table}'
            )
        """)
        assert table_exists is True
        
        # Check columns and their data types
        columns = await conn.fetch(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{system_store.store_table}'
            ORDER BY ordinal_position
        """)
        
        # Convert to a more testable format
        column_info = {row['column_name']: {
            'data_type': row['data_type'],
            'is_nullable': row['is_nullable']
        } for row in columns}
        
        # Verify the expected columns exist with correct types
        assert 'id' in column_info
        assert column_info['id']['data_type'] == 'integer'
        assert column_info['id']['is_nullable'] == 'NO'
        
        assert 'key' in column_info
        assert column_info['key']['data_type'] == 'text'
        assert column_info['key']['is_nullable'] == 'NO'
        
        assert 'value' in column_info
        assert column_info['value']['data_type'] == 'jsonb'
        assert column_info['value']['is_nullable'] == 'NO'
        
        # Check primary key constraint
        pk_constraint = await conn.fetchval(f"""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' 
            AND table_name = '{system_store.store_table}'
            AND constraint_type = 'PRIMARY KEY'
        """)
        assert pk_constraint is not None
        
        # Check unique constraint on the key column
        unique_constraint = await conn.fetchval(f"""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public' 
            AND table_name = '{system_store.store_table}'
            AND constraint_type = 'UNIQUE'
        """)
        assert unique_constraint is not None


@pytest.mark.asyncio
async def test_upsert_item(system_store):
    """
    Integration test for upsert_item method.
    Tests both inserting new items and updating existing items.
    """
    # Test data
    test_key = "test_upsert_key"
    initial_value = {"status": "initial", "count": 1, "tags": ["new"]}
    updated_value = {"status": "updated", "count": 2, "tags": ["updated"]}
    
    # 1. First, insert a new item
    await system_store.upsert_item(test_key, initial_value)
    
    # Verify the item was inserted correctly
    retrieved_value = await system_store.get_item(test_key)
    assert retrieved_value == initial_value
    
    # 2. Now update the existing item
    await system_store.upsert_item(test_key, updated_value)
    
    # Verify the item was updated
    retrieved_value = await system_store.get_item(test_key)
    assert retrieved_value == updated_value
    assert retrieved_value != initial_value
    
    # 3. Directly verify using SQL query
    async with system_store.pool.acquire() as conn:
        # Query the database directly
        result = await conn.fetchrow(f"SELECT key, value FROM {system_store.store_table} WHERE key = $1", test_key)
        
        # Check that the result exists
        assert result is not None
        
        # Check the key
        assert result["key"] == test_key
        
        # Check the value (PostgreSQL returns JSONB as a string)
        db_value = json.loads(result["value"])
        assert db_value == updated_value
        
        # Verify only one row exists for this key (no duplicates)
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {system_store.store_table} WHERE key = $1", test_key)
        assert count == 1


@pytest.mark.asyncio
async def test_concurrent_operations(system_store):
    """
    Integration test for concurrent operations to verify connection pool handling.
    """
    # Number of concurrent operations
    num_operations = 10
    
    # Generate unique keys and values
    test_items = {
        f"test_concurrent_{i}": {"index": i, "timestamp": f"2025-08-13T{i:02d}:00:00Z"}
        for i in range(num_operations)
    }
    
    # Define an async function to add an item and then retrieve it
    async def add_and_get(key, value):
        # Add the item
        await system_store.add_item(key, value)
        # Small delay to simulate workload
        await asyncio.sleep(0.01)
        # Retrieve the item
        retrieved = await system_store.get_item(key)
        return key, retrieved
    
    # Run all operations concurrently
    tasks = [add_and_get(key, value) for key, value in test_items.items()]
    results = await asyncio.gather(*tasks)
    
    # Verify all operations were successful
    for key, retrieved in results:
        assert retrieved == test_items[key]
    
    # Verify the count in the database
    async with system_store.pool.acquire() as conn:
        count = await conn.fetchval(f"""
            SELECT COUNT(*) FROM {system_store.store_table}
            WHERE key LIKE 'test_concurrent_%'
        """)
        assert count == num_operations


@pytest.mark.asyncio
async def test_delete_item(system_store):
    """
    Integration test for delete_item method.
    Tests deleting an existing item and handling the deletion of a non-existent item.
    """
    # Test data
    test_key = "test_delete_key"
    test_value = {"status": "active", "data": "to be deleted"}
    
    # 1. First, add an item to delete
    await system_store.add_item(test_key, test_value)
    
    # Verify the item was added correctly
    retrieved_value = await system_store.get_item(test_key)
    assert retrieved_value == test_value
    
    # 2. Delete the item
    await system_store.delete_item(test_key)
    
    # 3. Verify the item was deleted
    retrieved_after_delete = await system_store.get_item(test_key)
    assert retrieved_after_delete is None
    
    # 4. Directly verify using SQL query that the item no longer exists
    async with system_store.pool.acquire() as conn:
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {system_store.store_table} WHERE key = $1", test_key)
        assert count == 0
    
    # 5. Test deleting a non-existent item (should not raise an exception)
    try:
        await system_store.delete_item("non_existent_key")
        # If we reach here, no exception was raised
        passed = True
    except Exception as e:
        passed = False
    assert passed, "Deleting a non-existent key should not raise an exception"
    
    # 6. Add multiple items and delete one
    keys = ["multi_delete_1", "multi_delete_2", "multi_delete_3"]
    for i, key in enumerate(keys):
        await system_store.add_item(key, {"index": i})
    
    # Delete the middle item
    await system_store.delete_item(keys[1])
    
    # Verify only that item was deleted
    assert await system_store.get_item(keys[0]) is not None
    assert await system_store.get_item(keys[1]) is None
    assert await system_store.get_item(keys[2]) is not None


@pytest.mark.asyncio
async def test_drop_table(system_store):
    """
    Integration test for the drop function that removes the table.
    """
    # Create the table if it doesn't exist
    await system_store._execute(f"""
        CREATE TABLE IF NOT EXISTS {system_store.store_table} (
            id SERIAL PRIMARY KEY,
            key TEXT UNIQUE NOT NULL,
            value JSONB NOT NULL
        )
    """)
    
    # Call the drop function
    await system_store.drop()
    
    # Verify the table no longer exists
    async with system_store.pool.acquire() as conn:
        table_exists_after = await conn.fetchval(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{system_store.store_table}'
            )
        """)
        assert table_exists_after is False, f"Test table {system_store.store_table} should not exist after drop"

