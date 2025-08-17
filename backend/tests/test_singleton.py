import pytest
from unittest.mock import MagicMock
import sys
import os

# Add the parent directory to sys.path to make imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from backend.utils.singleton import singleton_cache


def test_function_called_once_with_non_none_result():
    """Test that the function is called only once when it returns a non-None result."""
    mock_func = MagicMock(return_value="test_result")
    decorated_func = singleton_cache(mock_func)

    # Call the decorated function multiple times
    result1 = decorated_func()
    result2 = decorated_func()
    result3 = decorated_func()

    # Assert that the original function was called only once
    assert mock_func.call_count == 1
    
    # Assert that all calls return the same result
    assert result1 == "test_result"
    assert result2 == "test_result"
    assert result3 == "test_result"


def test_function_called_multiple_times_with_none_result():
    """Test that the function is called multiple times when it returns None."""
    mock_func = MagicMock(return_value=None)
    decorated_func = singleton_cache(mock_func)

    # Call the decorated function multiple times
    result1 = decorated_func()
    result2 = decorated_func()
    result3 = decorated_func()

    # Assert that the original function was called multiple times
    assert mock_func.call_count == 3
    
    # Assert that all calls return None
    assert result1 is None
    assert result2 is None
    assert result3 is None


def test_function_called_until_non_none_result():
    """Test that the function is called until it returns a non-None result."""
    # Create a mock function that returns None twice, then a value
    mock_func = MagicMock(side_effect=[None, None, "final_result", "should_not_be_called"])
    decorated_func = singleton_cache(mock_func)

    # Call the decorated function multiple times
    result1 = decorated_func()  # Returns None, not cached
    result2 = decorated_func()  # Returns None, not cached
    result3 = decorated_func()  # Returns "final_result", cached
    result4 = decorated_func()  # Should return cached "final_result"

    # Assert the correct call count
    assert mock_func.call_count == 3
    
    # Assert the correct results
    assert result1 is None
    assert result2 is None
    assert result3 == "final_result"
    assert result4 == "final_result"


def test_function_preserves_metadata():
    """Test that the decorator preserves function metadata like __name__ and __doc__."""
    
    @singleton_cache
    def test_function():
        """Test docstring."""
        return "result"
        
    assert test_function.__name__ == "test_function"
    assert test_function.__doc__ == "Test docstring."


def test_multiple_decorated_functions():
    """Test that multiple decorated functions don't interfere with each other."""
    
    @singleton_cache
    def func_a():
        return "result_a"
        
    @singleton_cache
    def func_b():
        return "result_b"
        
    # Call each function
    result_a = func_a()
    result_b = func_b()
    
    # Assert correct results
    assert result_a == "result_a"
    assert result_b == "result_b"
    
    # Call again to ensure they use their own caches
    assert func_a() == "result_a"
    assert func_b() == "result_b"


def test_with_args_and_kwargs():
    """Test that the decorator works with functions that take arguments."""
    mock_func = MagicMock(return_value="result_with_args")
    decorated_func = singleton_cache(mock_func)
    
    # Call with arguments
    result = decorated_func(1, 2, key="value")
    
    # Assert function was called with correct arguments
    mock_func.assert_called_once_with(1, 2, key="value")
    assert result == "result_with_args"
    
    # Call again with different arguments
    result2 = decorated_func(3, 4, key="different")
    
    # Function should not be called again regardless of different args
    assert mock_func.call_count == 1
    assert result2 == "result_with_args"
