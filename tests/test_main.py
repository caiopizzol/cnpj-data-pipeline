"""Tests for main module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from config import Config
from main import get_file_priority, group_files_by_dependency, main, parquet_worker, parse_args, pg_worker


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    return Config(
        database_url="postgresql://test", temp_dir=str(tmp_path), parquet_output_dir=str(tmp_path / "parquet")
    )


class TestGetFilePriority:
    """Test sorting priority within the configured logical loading order."""

    def test_reference_tables_first(self) -> None:
        """Reference tables should have lowest priority values (processed first)."""
        assert get_file_priority("CNAECSV.D51213") < get_file_priority("EMPRECSV.D51213")
        assert get_file_priority("PAISCSV.D51213") < get_file_priority("ESTABELE.D51213")

    def test_sort_priority_within_loading_order(self) -> None:
        """Sorting ranks empresas before estabelecimentos before socios; the latter two share a worker group."""
        empresas = get_file_priority("EMPRECSV.D51213")
        estabelecimentos = get_file_priority("ESTABELE.D51213")
        socios = get_file_priority("SOCIOCSV.D51213")

        assert empresas < estabelecimentos
        assert estabelecimentos < socios

    def test_unknown_file_sorts_last(self) -> None:
        """Unknown file types should sort after all known types."""
        assert get_file_priority("UNKNOWN.csv") == 999
        assert get_file_priority("UNKNOWN.csv") > get_file_priority("SIMPLESCSV.D51213")


class TestGroupFilesByDependency:
    """Test dependency grouping for parallel processing."""

    def test_groups_reference_files(self) -> None:
        """Reference files should all be in group 0."""
        files = ["Cnaes.zip", "Motivos.zip", "Paises.zip"]
        groups = group_files_by_dependency(files)
        assert len(groups[0]) == 3
        assert len(groups[1]) == 0
        assert len(groups[2]) == 0

    def test_groups_by_dependency_level(self) -> None:
        """Files should be grouped by their logical loading level."""
        files = ["Cnaes.zip", "Empresas0.zip", "Estabele0.zip", "Socios0.zip"]
        groups = group_files_by_dependency(files)
        assert groups[0] == ["Cnaes.zip"]
        assert groups[1] == ["Empresas0.zip"]
        assert sorted(groups[2]) == sorted(["Estabele0.zip", "Socios0.zip"])

    def test_multiple_files_same_type(self) -> None:
        """Multiple files of the same type should be in the same group."""
        files = ["Empresas0.zip", "Empresas1.zip", "Empresas2.zip"]
        groups = group_files_by_dependency(files)
        assert len(groups[1]) == 3

    def test_unknown_files_excluded(self) -> None:
        """Unknown file types should not appear in any group."""
        files = ["Unknown.zip", "Cnaes.zip"]
        groups = group_files_by_dependency(files)
        assert len(groups[0]) == 1
        assert all(len(g) == 0 for g in groups[1:])


class TestParseArgs:
    """Test the public command-line arguments."""

    @patch("sys.argv", ["main.py", "--month", "2024-11", "--force"])
    def test_parses_month_and_force(self) -> None:
        args = parse_args()

        assert args.list is False
        assert args.month == "2024-11"
        assert args.force is True


class TestMain:
    """Test main pipeline orchestration."""

    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_list_mode_never_touches_database(self, mock_args: MagicMock, mock_downloader_cls: MagicMock) -> None:
        """--list should print months and never create a Database."""
        mock_args.return_value = MagicMock(list=True, month=None, force=False)
        mock_downloader = MagicMock()
        mock_downloader.get_available_directories.return_value = ["2024-01", "2024-02"]
        mock_downloader_cls.return_value = mock_downloader

        with patch("database.Database") as mock_db_cls:
            main()

            mock_db_cls.assert_not_called()

    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_missing_database_url_exits(
        self, mock_args: MagicMock, mock_downloader_cls: MagicMock, cfg: Config
    ) -> None:
        """Missing DATABASE_URL should exit with code 1."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "postgres"
        cfg.database_url = ""

        with pytest.raises(SystemExit) as exc_info:
            main(cfg=cfg)

        assert exc_info.value.code == 1

    @patch("database.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_invalid_month_exits(
        self, mock_args: MagicMock, mock_downloader_cls: MagicMock, mock_db_cls: MagicMock, cfg: Config
    ) -> None:
        """Invalid --month should exit with code 1."""
        mock_args.return_value = MagicMock(list=False, month="2099-01", force=False)
        cfg.output_format = "postgres"
        cfg.database_url = "postgresql://test"
        mock_downloader = MagicMock()
        mock_downloader.get_available_directories.return_value = ["2024-01", "2024-02"]
        mock_downloader_cls.return_value = mock_downloader

        with pytest.raises(SystemExit) as exc_info:
            main(cfg=cfg)

        assert exc_info.value.code == 1

    @patch("database.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_force_clears_processed_files(
        self, mock_args: MagicMock, mock_downloader_cls: MagicMock, mock_db_cls: MagicMock, cfg: Config
    ) -> None:
        """--force should call clear_processed_files before processing."""
        mock_args.return_value = MagicMock(list=False, month=None, force=True)
        cfg.output_format = "postgres"
        cfg.database_url = "postgresql://test"
        cfg.batch_size = 500000
        cfg.keep_files = False

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader.download_files.return_value = iter([])
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db.get_processed_files.return_value = {"Cnaes.zip"}
        mock_db_cls.return_value = mock_db

        main(cfg=cfg)

        mock_db.clear_processed_files.assert_called_once_with("2024-01")

    @patch("database.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_no_pending_files_returns_early(
        self, mock_args: MagicMock, mock_downloader_cls: MagicMock, mock_db_cls: MagicMock, cfg: Config
    ) -> None:
        """When all files are processed, should return without downloading."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "postgres"
        cfg.database_url = "postgresql://test"

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db.get_processed_files.return_value = {"Cnaes.zip"}
        mock_db_cls.return_value = mock_db

        main(cfg=cfg)

        mock_downloader.download_files.assert_not_called()

    @patch("main.process_file")
    @patch("database.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_mark_processed_failure_skips_explicit_csv_unlink(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_db_cls: MagicMock,
        mock_process_file: MagicMock,
        tmp_path: Path,
        cfg: Config,
    ) -> None:
        """A failed mark_processed skips the explicit unlink; cleanup is mocked here."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "postgres"
        cfg.database_url = "postgresql://test"
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.process_workers = 1
        cfg.loading_strategy = "upsert"

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

        with pytest.raises(SystemExit):
            main(cfg=cfg)

        # The explicit unlink was skipped; real downloader cleanup still removes the file.
        assert csv_file.exists()

    @patch("database.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_always_disconnects_and_cleans_up(
        self, mock_args: MagicMock, mock_downloader_cls: MagicMock, mock_db_cls: MagicMock, cfg: Config
    ) -> None:
        """disconnect and cleanup should always be called, even on error."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "postgres"
        cfg.database_url = "postgresql://test"

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.side_effect = Exception("network error")
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db

        with pytest.raises(SystemExit):
            main(cfg=cfg)

        mock_db.disconnect.assert_called_once()
        mock_downloader.cleanup.assert_called_once()


class TestParquetOutput:
    """Test parquet output format path."""

    @patch("main.process_file")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_writes_parquet_and_manifest(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_process_file: MagicMock,
        tmp_path: Path,
        cfg: Config,
    ) -> None:
        """Parquet mode should write parquet files and manifest without touching database."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "parquet"
        cfg.post_file_command = ""
        cfg.parquet_output_dir = str(tmp_path / "parquet")
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.process_workers = 1

        csv_file = tmp_path / "CNAECSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader.download_files.return_value = iter([(csv_file, "Cnaes.zip")])
        mock_downloader_cls.return_value = mock_downloader

        mock_process_file.return_value = iter(
            [(pl.DataFrame({"codigo": ["001"], "descricao": ["Test"]}), "cnaes", ["codigo", "descricao"])]
        )

        main(cfg=cfg)

        parquet_dir = tmp_path / "parquet"
        assert (parquet_dir / "cnaes.parquet").exists()
        assert (parquet_dir / "manifest.json").exists()
        assert not csv_file.exists()

    @patch("main.process_file")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_does_not_require_database_url(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_process_file: MagicMock,
        tmp_path: Path,
        cfg: Config,
    ) -> None:
        """Parquet mode should work without DATABASE_URL."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "parquet"
        cfg.post_file_command = ""
        cfg.database_url = ""
        cfg.parquet_output_dir = str(tmp_path / "parquet")
        cfg.batch_size = 500000
        cfg.keep_files = True
        cfg.process_workers = 1

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader.download_files.return_value = iter([(tmp_path / "CNAECSV.D51213", "Cnaes.zip")])
        mock_downloader_cls.return_value = mock_downloader

        mock_process_file.return_value = iter([(pl.DataFrame({"codigo": ["001"]}), "cnaes", ["codigo"])])

        main(cfg=cfg)  # Should not raise SystemExit

    @patch("main.process_file")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_estabelecimentos_single_file(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_process_file: MagicMock,
        tmp_path: Path,
        cfg: Config,
    ) -> None:
        """Estabelecimentos should be written to a single file."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "parquet"
        cfg.post_file_command = ""
        cfg.parquet_output_dir = str(tmp_path / "parquet")
        cfg.batch_size = 500000
        cfg.keep_files = True
        cfg.process_workers = 1

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Estabele.zip"]
        mock_downloader.download_files.return_value = iter([(tmp_path / "ESTABELE.D51213", "Estabele.zip")])
        mock_downloader_cls.return_value = mock_downloader

        mock_process_file.return_value = iter(
            [
                (
                    pl.DataFrame({"cnpj_basico": ["00000000", "11111111"], "uf": ["SP", "RJ"]}),
                    "estabelecimentos",
                    ["cnpj_basico", "uf"],
                )
            ]
        )

        main(cfg=cfg)

        assert (tmp_path / "parquet" / "estabelecimentos.parquet").exists()

    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_cleans_up_on_error(
        self, mock_args: MagicMock, mock_downloader_cls: MagicMock, tmp_path: Path, cfg: Config
    ) -> None:
        """Parquet mode should always call downloader.cleanup on error."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "parquet"
        cfg.post_file_command = ""
        cfg.parquet_output_dir = str(tmp_path / "parquet")

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.side_effect = Exception("network error")
        mock_downloader_cls.return_value = mock_downloader

        with pytest.raises(SystemExit):
            main(cfg=cfg)

        mock_downloader.cleanup.assert_called_once()

    @patch("main.subprocess")
    @patch("main.process_file")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_post_file_command_runs_per_table(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_process_file: MagicMock,
        mock_subprocess: MagicMock,
        tmp_path: Path,
        cfg: Config,
    ) -> None:
        """POST_FILE_COMMAND should run once per table after all its files are processed."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "parquet"
        cfg.post_file_command = "echo"
        cfg.parquet_output_dir = str(tmp_path / "parquet")
        cfg.batch_size = 500000
        cfg.keep_files = True
        cfg.process_workers = 1

        csv_file = tmp_path / "CNAECSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader.download_files.return_value = iter([(csv_file, "Cnaes.zip")])
        mock_downloader_cls.return_value = mock_downloader

        mock_process_file.return_value = iter(
            [(pl.DataFrame({"codigo": ["001"], "descricao": ["Test"]}), "cnaes", ["codigo", "descricao"])]
        )

        main(cfg=cfg)

        mock_subprocess.run.assert_called_once()
        call_args = mock_subprocess.run.call_args[0][0]
        assert call_args[0] == "echo"
        assert "cnaes.parquet" in call_args[-1]


class TestPgWorker:
    """Test pg_worker function."""

    @patch("database.Database")
    @patch("main.process_file")
    def test_downloads_processes_and_marks_file(
        self, mock_process_file: MagicMock, mock_db_cls: MagicMock, tmp_path: Path
    ) -> None:
        """pg_worker should download, process, mark processed, and delete CSV."""
        csv_file = tmp_path / "CNAECSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.download_file.return_value = [csv_file]

        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db

        cfg = Config(database_url="postgresql://test")
        cfg.database_url = "postgresql://test"
        cfg.loading_strategy = "upsert"
        cfg.batch_size = 500000
        cfg.keep_files = False

        mock_process_file.return_value = iter([(pl.DataFrame({"codigo": ["001"]}), "cnaes", ["codigo"])])

        pg_worker("Cnaes.zip", "2024-01", mock_downloader, cfg)

        mock_db.bulk_upsert.assert_called_once()
        mock_db.mark_processed.assert_called_once_with("2024-01", "Cnaes.zip")
        mock_db.disconnect.assert_called_once()
        assert not csv_file.exists()

    @patch("database.Database")
    @patch("main.process_file")
    def test_uses_bulk_insert_for_replace(
        self, mock_process_file: MagicMock, mock_db_cls: MagicMock, tmp_path: Path
    ) -> None:
        """pg_worker should use bulk_insert when loading_strategy is replace."""
        csv_file = tmp_path / "CNAECSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.download_file.return_value = [csv_file]

        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db

        cfg = Config(database_url="postgresql://test")
        cfg.database_url = "postgresql://test"
        cfg.loading_strategy = "replace"
        cfg.batch_size = 500000
        cfg.keep_files = False

        mock_process_file.return_value = iter([(pl.DataFrame({"codigo": ["001"]}), "cnaes", ["codigo"])])

        pg_worker("Cnaes.zip", "2024-01", mock_downloader, cfg)

        mock_db.bulk_insert.assert_called_once()
        mock_db.bulk_upsert.assert_not_called()

    @patch("database.Database")
    @patch("main.process_file")
    def test_passes_pre_truncated_to_database(
        self, mock_process_file: MagicMock, mock_db_cls: MagicMock, tmp_path: Path
    ) -> None:
        """pg_worker should pass pre_truncated to Database constructor."""
        csv_file = tmp_path / "CNAECSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.download_file.return_value = [csv_file]

        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db

        cfg = Config(database_url="postgresql://test")
        cfg.database_url = "postgresql://test"
        cfg.loading_strategy = "replace"
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.retry_attempts = 3
        cfg.retry_delay = 5

        mock_process_file.return_value = iter([(pl.DataFrame({"codigo": ["001"]}), "cnaes", ["codigo"])])

        pg_worker("Cnaes.zip", "2024-01", mock_downloader, cfg, pre_truncated={"cnaes"})

        mock_db_cls.assert_called_once_with("postgresql://test", pre_truncated={"cnaes"}, retry_attempts=3)

    @patch("database.Database")
    def test_disconnects_on_error(self, mock_db_cls: MagicMock) -> None:
        """pg_worker should always call db.disconnect(), even on error."""
        mock_downloader = MagicMock()
        mock_downloader.download_file.side_effect = Exception("download failed")

        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db

        cfg = Config(database_url="postgresql://test")
        cfg.database_url = "postgresql://test"

        with pytest.raises(Exception, match="download failed"):
            pg_worker("Cnaes.zip", "2024-01", mock_downloader, cfg)

        mock_db.disconnect.assert_called_once()


class TestParquetWorker:
    """Test parquet_worker function."""

    @patch("main.process_file")
    def test_downloads_and_writes_batches(self, mock_process_file: MagicMock, tmp_path: Path) -> None:
        """parquet_worker should download, process, and write to parquet."""
        csv_file = tmp_path / "CNAECSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.download_file.return_value = [csv_file]
        mock_parquet = MagicMock()

        cfg = Config(database_url="postgresql://test")
        cfg.batch_size = 500000
        cfg.keep_files = False

        mock_process_file.return_value = iter([(pl.DataFrame({"codigo": ["001"]}), "cnaes", ["codigo"])])

        parquet_worker("Cnaes.zip", "2024-01", mock_downloader, mock_parquet, cfg)

        mock_parquet.write_batch.assert_called_once()
        assert not csv_file.exists()

    @patch("main.process_file")
    def test_keeps_file_when_configured(self, mock_process_file: MagicMock, tmp_path: Path) -> None:
        """parquet_worker should not delete CSV when keep_files is True."""
        csv_file = tmp_path / "CNAECSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.download_file.return_value = [csv_file]
        mock_parquet = MagicMock()

        cfg = Config(database_url="postgresql://test")
        cfg.batch_size = 500000
        cfg.keep_files = True

        mock_process_file.return_value = iter([(pl.DataFrame({"codigo": ["001"]}), "cnaes", ["codigo"])])

        parquet_worker("Cnaes.zip", "2024-01", mock_downloader, mock_parquet, cfg)

        assert csv_file.exists()


class TestParallelProcessing:
    """Test workers > 1 code paths."""

    @patch("main.process_file")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_parquet_parallel_produces_output(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_process_file: MagicMock,
        tmp_path: Path,
        cfg: Config,
    ) -> None:
        """With workers > 1 in parquet mode, files should still be processed correctly."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "parquet"
        cfg.post_file_command = ""
        cfg.parquet_output_dir = str(tmp_path / "parquet")
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.process_workers = 2

        csv_file = tmp_path / "CNAECSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader.download_file.return_value = [csv_file]
        mock_downloader_cls.return_value = mock_downloader

        mock_process_file.return_value = iter(
            [(pl.DataFrame({"codigo": ["001"], "descricao": ["Test"]}), "cnaes", ["codigo", "descricao"])]
        )

        main(cfg=cfg)

        assert (tmp_path / "parquet" / "cnaes.parquet").exists()
        mock_downloader.download_file.assert_called()

    @patch("main.pg_worker")
    @patch("database.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_postgres_parallel_submits_workers(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_db_cls: MagicMock,
        mock_pg_worker: MagicMock,
        cfg: Config,
    ) -> None:
        """With workers > 1 in postgres mode, pg_worker should be submitted to executor."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "postgres"
        cfg.database_url = "postgresql://test"
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.process_workers = 2
        cfg.loading_strategy = "upsert"

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip", "Motivos.zip"]
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db.get_processed_files.return_value = set()
        mock_db_cls.return_value = mock_db

        mock_pg_worker.return_value = None

        main(cfg=cfg)

        assert mock_pg_worker.call_count == 2

    @patch("main.pg_worker")
    @patch("database.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_parallel_worker_failure_aborts_pipeline(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_db_cls: MagicMock,
        mock_pg_worker: MagicMock,
        cfg: Config,
    ) -> None:
        """A failing worker aborts the pipeline after the current group finishes."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "postgres"
        cfg.database_url = "postgresql://test"
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.process_workers = 2
        cfg.loading_strategy = "upsert"

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip", "Motivos.zip"]
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db.get_processed_files.return_value = set()
        mock_db_cls.return_value = mock_db

        mock_pg_worker.side_effect = [Exception("worker crashed"), None]

        with pytest.raises(SystemExit):
            main(cfg=cfg)


class TestPreTruncation:
    """Test pre-truncation logic for parallel replace strategy."""

    @patch("main.pg_worker")
    @patch("database.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_replace_strategy_pre_truncates(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_db_cls: MagicMock,
        mock_pg_worker: MagicMock,
        cfg: Config,
    ) -> None:
        """With workers > 1 and replace strategy, tables should be truncated before workers."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "postgres"
        cfg.database_url = "postgresql://test"
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.process_workers = 2
        cfg.loading_strategy = "replace"

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Empresas0.zip", "Empresas1.zip"]
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db.get_processed_files.return_value = set()
        mock_db_cls.return_value = mock_db
        mock_pg_worker.return_value = None

        main(cfg=cfg)

        mock_db.truncate_table.assert_called_with("empresas")

        # Workers should receive pre_truncated set
        for call in mock_pg_worker.call_args_list:
            pre_truncated_arg = call[0][4]
            assert "empresas" in pre_truncated_arg

    @patch("main.pg_worker")
    @patch("database.Database")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_upsert_strategy_does_not_truncate(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_db_cls: MagicMock,
        mock_pg_worker: MagicMock,
        cfg: Config,
    ) -> None:
        """With workers > 1 and upsert strategy, tables should NOT be truncated."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "postgres"
        cfg.database_url = "postgresql://test"
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.process_workers = 2
        cfg.loading_strategy = "upsert"

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader_cls.return_value = mock_downloader

        mock_db = MagicMock()
        mock_db.get_processed_files.return_value = set()
        mock_db_cls.return_value = mock_db
        mock_pg_worker.return_value = None

        main(cfg=cfg)

        mock_db.truncate_table.assert_not_called()


class TestParquetResume:
    """Test parquet resume/skip logic for already-exported tables."""

    @patch("main.process_file")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_skips_already_exported_table(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_process_file: MagicMock,
        tmp_path: Path,
        cfg: Config,
    ) -> None:
        """Tables with existing parquet files should be skipped entirely."""
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()
        # Pre-create cnaes.parquet to simulate prior export
        (parquet_dir / "cnaes.parquet").write_bytes(b"existing")

        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "parquet"
        cfg.post_file_command = ""
        cfg.parquet_output_dir = str(parquet_dir)
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.process_workers = 1

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip"]
        mock_downloader_cls.return_value = mock_downloader

        main(cfg=cfg)

        # download_files should never be called — table was skipped
        mock_downloader.download_files.assert_not_called()
        mock_process_file.assert_not_called()

    @patch("main.process_file")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_processes_only_missing_tables(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_process_file: MagicMock,
        tmp_path: Path,
        cfg: Config,
    ) -> None:
        """Only tables without existing parquet files should be processed."""
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()
        # cnaes already exported, motivos not
        (parquet_dir / "cnaes.parquet").write_bytes(b"existing")

        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "parquet"
        cfg.post_file_command = ""
        cfg.parquet_output_dir = str(parquet_dir)
        cfg.batch_size = 500000
        cfg.keep_files = True
        cfg.process_workers = 1

        csv_file = tmp_path / "MOTICSV.D51213"
        csv_file.write_text("data")

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip", "Motivos.zip"]
        mock_downloader.download_files.return_value = iter([(csv_file, "Motivos.zip")])
        mock_downloader_cls.return_value = mock_downloader

        mock_process_file.return_value = iter(
            [(pl.DataFrame({"codigo": ["001"], "descricao": ["Test"]}), "motivos", ["codigo", "descricao"])]
        )

        main(cfg=cfg)

        # Only motivos should have been processed
        mock_process_file.assert_called_once()
        assert (parquet_dir / "motivos.parquet").exists()

    @patch("main.parquet_worker")
    @patch("main.Downloader")
    @patch("main.parse_args")
    def test_parallel_parquet_worker_failure_aborts(
        self,
        mock_args: MagicMock,
        mock_downloader_cls: MagicMock,
        mock_parquet_worker: MagicMock,
        tmp_path: Path,
        cfg: Config,
    ) -> None:
        """Parallel parquet worker failure should abort the pipeline."""
        mock_args.return_value = MagicMock(list=False, month=None, force=False)
        cfg.output_format = "parquet"
        cfg.post_file_command = ""
        cfg.parquet_output_dir = str(tmp_path / "parquet")
        cfg.batch_size = 500000
        cfg.keep_files = False
        cfg.process_workers = 2

        mock_downloader = MagicMock()
        mock_downloader.get_latest_directory.return_value = "2024-01"
        mock_downloader.get_directory_files.return_value = ["Cnaes.zip", "Motivos.zip"]
        mock_downloader_cls.return_value = mock_downloader

        mock_parquet_worker.side_effect = [Exception("worker crashed"), None]

        with pytest.raises(SystemExit):
            main(cfg=cfg)
