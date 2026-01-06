"""Test configuration and fixtures for CNPJ data pipeline tests."""

import pytest
import os
from pathlib import Path


@pytest.fixture(scope="session")
def test_data_dir():
    """Provide path to test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session") 
def temp_dir(tmp_path_factory):
    """Provide temporary directory for tests."""
    return tmp_path_factory.mktemp("cnpj_tests")


# Configure test environment
def pytest_configure(config):
    """Configure pytest environment."""
    # Ensure we don't accidentally use production settings
    os.environ.setdefault("TESTING", "1")