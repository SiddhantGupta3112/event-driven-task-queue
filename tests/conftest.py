import os
import pytest
from dotenv import load_dotenv

# This hook runs BEFORE any test files or application modules are imported
def pytest_configure(config):
    # Only load the test env if we are explicitly running integration or benchmark tests
    # This prevents overriding your setup if you are just running local unit tests
    if os.path.exists("tests/.env.test"):
        load_dotenv(dotenv_path="tests/.env.test", override=True)
    else:
        raise FileNotFoundError(
            "CRITICAL CONFIGURATION ERROR: Your '.env.test' file is missing! "
            "Pytest execution halted to protect development infrastructure data."
        )