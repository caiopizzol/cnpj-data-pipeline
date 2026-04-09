"""Tests for config module."""

from unittest.mock import patch

import pytest

from config import Config


class TestFromEnv:
    """Test Config.from_env() environment variable parsing."""

    def test_defaults_when_no_env_vars(self):
        """All fields should have sensible defaults when env vars are absent."""
        with patch.dict("os.environ", {}, clear=True):
            cfg = Config.from_env()

        assert cfg.database_url == ""
        assert cfg.batch_size == 500000
        assert cfg.temp_dir == "./temp"
        assert cfg.download_workers == 4
        assert cfg.process_workers == 1
        assert cfg.retry_attempts == 3
        assert cfg.retry_delay == 5
        assert cfg.connect_timeout == 30
        assert cfg.read_timeout == 300
        assert cfg.keep_files is False
        assert cfg.loading_strategy == "upsert"
        assert cfg.output_format == "postgres"
        assert cfg.parquet_output_dir == "./parquet"
        assert cfg.post_file_command == ""

    def test_env_vars_override_defaults(self):
        """Environment variables should override default values."""
        env = {
            "DATABASE_URL": "postgres://custom:5432/db",
            "BATCH_SIZE": "100000",
            "DOWNLOAD_WORKERS": "8",
            "PROCESS_WORKERS": "4",
            "RETRY_ATTEMPTS": "5",
        }
        with patch.dict("os.environ", env, clear=True):
            cfg = Config.from_env()

        assert cfg.database_url == "postgres://custom:5432/db"
        assert cfg.batch_size == 100000
        assert cfg.download_workers == 8
        assert cfg.process_workers == 4
        assert cfg.retry_attempts == 5

    def test_int_coercion(self):
        """Integer env vars should be correctly coerced."""
        with patch.dict("os.environ", {"BATCH_SIZE": "1000", "CONNECT_TIMEOUT": "60"}, clear=True):
            cfg = Config.from_env()

        assert cfg.batch_size == 1000
        assert cfg.connect_timeout == 60

    def test_invalid_int_raises(self):
        """Non-numeric integer env vars should raise ValueError."""
        with patch.dict("os.environ", {"BATCH_SIZE": "abc"}, clear=True):
            with pytest.raises(ValueError):
                Config.from_env()

    def test_keep_files_boolean_parsing(self):
        """KEEP_DOWNLOADED_FILES should parse 'true' (case-insensitive) as True."""
        with patch.dict("os.environ", {"KEEP_DOWNLOADED_FILES": "true"}, clear=True):
            assert Config.from_env().keep_files is True

        with patch.dict("os.environ", {"KEEP_DOWNLOADED_FILES": "True"}, clear=True):
            assert Config.from_env().keep_files is True

        with patch.dict("os.environ", {"KEEP_DOWNLOADED_FILES": "TRUE"}, clear=True):
            assert Config.from_env().keep_files is True

        with patch.dict("os.environ", {"KEEP_DOWNLOADED_FILES": "false"}, clear=True):
            assert Config.from_env().keep_files is False

        with patch.dict("os.environ", {"KEEP_DOWNLOADED_FILES": "yes"}, clear=True):
            assert Config.from_env().keep_files is False  # only "true" is truthy

    def test_string_lowering(self):
        """LOADING_STRATEGY and OUTPUT_FORMAT should be lowercased."""
        env = {"LOADING_STRATEGY": "REPLACE", "OUTPUT_FORMAT": "PARQUET"}
        with patch.dict("os.environ", env, clear=True):
            cfg = Config.from_env()

        assert cfg.loading_strategy == "replace"
        assert cfg.output_format == "parquet"

    def test_base_url_and_share_token_override(self):
        """BASE_URL and SHARE_TOKEN should be overridable via env."""
        env = {"BASE_URL": "https://custom.server/webdav", "SHARE_TOKEN": "custom_token"}
        with patch.dict("os.environ", env, clear=True):
            cfg = Config.from_env()

        assert cfg.base_url == "https://custom.server/webdav"
        assert cfg.share_token == "custom_token"
