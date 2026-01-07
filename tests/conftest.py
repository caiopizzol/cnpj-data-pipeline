"""Test configuration and fixtures for CNPJ data pipeline tests."""

import os


# Configure test environment
def pytest_configure(config):
    """Configure pytest environment."""
    # Ensure we don't accidentally use production settings
    os.environ.setdefault("TESTING", "1")
