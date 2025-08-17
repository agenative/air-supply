import os
import pytest
import sys
from dotenv import load_dotenv

# Auto-load .env file at the beginning of test session
def pytest_configure(config):
    """
    Load environment variables from .env file before running tests
    """
    # Find .env file in the backend folder
    backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    env_path = os.path.join(backend_dir, ".env")
    
    if os.path.exists(env_path):
        print(f"Loading environment variables from {env_path}")
        load_dotenv(env_path)
    else:
        print(f"ERROR: .env file not found at {env_path}", file=sys.stderr)
        print(f"Tests require a properly configured .env file in the backend folder. Please create one before running tests.", file=sys.stderr)
        sys.exit(1)  # Exit with error code 1
