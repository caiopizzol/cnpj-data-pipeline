"""Test configuration and fixtures for CNPJ data pipeline tests."""

import os

import pytest


# Configure test environment
def pytest_configure(config: pytest.Config):
    """Configure pytest environment."""
    # Test marker only; application configuration does not read TESTING.
    os.environ.setdefault("TESTING", "1")
