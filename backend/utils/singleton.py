from functools import wraps
from typing import Callable, TypeVar, Any, Optional, Dict
import uuid

T = TypeVar('T')

def singleton_cache(func: Callable[..., Optional[T]]) -> Callable[..., Optional[T]]:
    """
    A decorator that caches the non-None return value of the function.
    Subsequent calls to the function will return the cached value directly.
    
    Args:
        func: The function to be decorated
        
    Returns:
        A wrapped function that caches the first non-None return value
    """
    # Use a shared dictionary for all cached values with function name as key
    _cache: Dict[str, Any] = {}
    
    # Try to get the function name, fallback to a unique id for mock objects
    try:
        cache_key = func.__name__
    except (AttributeError, TypeError):
        # For objects without __name__ (like MagicMock in tests)
        cache_key = f"func_{str(uuid.uuid4())}"
    
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
        if cache_key not in _cache:
            result = func(*args, **kwargs)
            if result is not None:
                _cache[cache_key] = result
            return result
        return _cache[cache_key]
    
    return wrapper
