"""Tests for main module."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from main import get_file_priority, main


class TestGetFilePriority:
    """Test file processing priority (controls FK dependency order)."""

    def test_reference_tables_first(self):
        """Reference tables should have lowest priority values (processed first)."""
        assert get_file_priority("CNAECSV.D51213") < get_file_priority("EMPRECSV.D51213")
        assert get_file_priority("PAISCSV.D51213") < get_file_priority("ESTABELE.D51213")

    def test_fk_dependency_order(self):
        """Tables must load in FK dependency order: references → empresas → estabelecimentos → socios."""
        empresas = get_file_priority("EMPRECSV.D51213")
        estabelecimentos = get_file_priority("ESTABELE.D51213")
        socios = get_file_priority("SOCIOCSV.D51213")

        assert empresas < estabelecimentos
        assert estabelecimentos < socios

    def test_unknown_file_sorts_last(self):
        """Unknown file types should sort after all known types."""
        assert get_file_priority("UNKNOWN.csv") == 999
        assert get_file_priority("UNKNOWN.csv") > get_file_priority("SIMPLESCSV.D51213")


class TestMain:
    """Test main pipeline orchestration."""

    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_list_mode_never_touches_database(self, mock_args, mock_downloader_cls):
        """--list should print months and never create a Database."""
        mock_args.return_value = MagicMock(list=True, month=None, force=False)
        mock_downloader = MagicMock()
        mock_downloader.get_available_directories.return_value = ["2024-01", "2024-02"]
        mock_downloader_cls.return_value = mock_downloader

        with patch("main.Database") as mock_db_cls:
            main()

            mock_db_cls.assert_not_called()

    @patch("main.config")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_missing_database_url_exits(self, mock_args, mock_downloader_cls, mock_config):
        """Missing DATABASE_URL should exit with code 1."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        mock_config.database_url = ""

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1

    @patch("main.config")
    @patch("main.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_invalid_month_exits(self, mock_args, mock_downloader_cls, mock_db_cls, mock_config):
        """Invalid --month should exit with code 1."""
        mock_args.return_value = MagicMock(list=False, month="2099-01", force=False)
        mock_config.database_url = "postgresql://test"
        mock_downloader = MagicMock()
        mock_downloader.get_available_directories.return_value = ["2024-01", "2024-02"]
        mock_downloader_cls.return_value = mock_downloader

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1

    @patch("main.config")
    @patch("main.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_force_clears_processed_files(self, mock_args, mock_downloader_cls, mock_db_cls, mock_config):
        """--force should call clear_processed_files before processing."""
        mock_args.return_value = MagicMock(list=False, month=None, force=True)
        mock_config.database_url = "postgresql://test"
        mock_config.batch_size = 500000
        mock_config.keep_files = False

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader.download_files.return_value = iter([])
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db.get_processed_files.return_value = {"Cnaes.zip"}
        mock_db_cls.return_value = mock_db

        main()

        mock_db.clear_processed_files.assert_called_once_with("2024-01")

    @patch("main.config")
    @patch("main.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_no_pending_files_returns_early(self, mock_args, mock_downloader_cls, mock_db_cls, mock_config):
        """When all files are processed, should return without downloading."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        mock_config.database_url = "postgresql://test"

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db.get_processed_files.return_value = {"Cnaes.zip"}
        mock_db_cls.return_value = mock_db

        main()

        mock_downloader.download_files.assert_not_called()

    @patch("main.process_file")
    @patch("main.config")
    @patch("main.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_file_only_deleted_after_mark_processed(
        self, mock_args, mock_downloader_cls, mock_db_cls, mock_config, mock_process_file, tmp_path
    ):
        """CSV file should only be deleted after mark_processed succeeds."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        mock_config.database_url = "postgresql://test"
        mock_config.batch_size = 500000
        mock_config.keep_files = False

        csv_file = tmp_path / "CNAECSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader.download_files.return_value = iter([(csv_file, "Cnaes.zip")])
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db.get_processed_files.return_value = set()
        mock_db.mark_processed.side_effect = Exception("DB write failed")
        mock_db_cls.return_value = mock_db

        mock_process_file.return_value = iter(
            [
                (pl.DataFrame({"codigo": ["001"]}), "cnaes", ["codigo"]),
            ]
        )

        main()

        # File should NOT be deleted because mark_processed failed
        assert csv_file.exists()

    @patch("main.config")
    @patch("main.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_always_disconnects_and_cleans_up(self, mock_args, mock_downloader_cls, mock_db_cls, mock_config):
        """disconnect and cleanup should always be called, even on error."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        mock_config.database_url = "postgresql://test"

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.side_effect = Exception("network error")
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db

        with pytest.raises(SystemExit):
            main()

        mock_db.disconnect.assert_called_once()
        mock_downloader.cleanup.assert_called_once()
