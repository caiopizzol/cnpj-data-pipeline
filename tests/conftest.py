"""Test configuration and fixtures for CNPJ data pipeline tests."""

import os


# Configure test environment
def pytest_configure(config):
    """Configure pytest environment."""
    # Test marker only; application configuration does not read TESTING.
    os.environ.setdefault("TESTING", "1")
