"""A ZIP is complete only after every member loads successfully."""

from pathlib import Path
from threading import Event

import polars as pl
import pytest

from config import Config
from database import Database
from main import main


@pytest.fixture
def source_files() -> dict[str, dict[str, str]]:
    return {
        "Empresas0.zip": {
            "0.EMPRECSV.csv": '"00000001";"First company";"2062";"49";"10,00";"01";""\n',
            "1.EMPRECSV.csv": '"00000002";"Second company";"2062";"49";"20,00";"01";""\n',
        },
    }


@pytest.mark.parametrize("keep_files", [False, True])
@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize("strategy", ["upsert", "replace"])
def test_retry_replays_zip_after_second_member_fails(
    workers: int,
    keep_files: bool,
    strategy: str,
    source_server: tuple[str, Event, list[str]],
    empty_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_url, _, downloads = source_server
    monkeypatch.setattr("sys.argv", ["cnpj-pipeline", "--month", "2024-01"])
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    cfg = Config(
        database_url=empty_db.database_url,
        base_url=base_url,
        share_token="test",
        temp_dir=str(tmp_path / "downloads"),
        loading_strategy=strategy,
        process_workers=workers,
        download_workers=1,
        retry_attempts=1,
        keep_files=keep_files,
    )
    method = "bulk_insert" if strategy == "replace" else "bulk_upsert"
    original = getattr(Database, method)

    def fail_second_member(db: Database, frame: pl.DataFrame, table: str, columns: list[str]) -> None:
        if frame["cnpj_basico"][0] == "00000002":
            raise OSError("Second CSV failed")
        original(db, frame, table, columns)

    with monkeypatch.context() as fault:
        fault.setattr(Database, method, fail_second_member)
        with pytest.raises(SystemExit) as error:
            main(cfg)
        assert error.value.code == 1

    with empty_db.connect().cursor() as cur:
        cur.execute("SELECT cnpj_basico FROM empresas")
        assert cur.fetchall() == [("00000001",)]
    assert empty_db.get_processed_files("2024-01") == set()
    empty_db.connect().commit()

    downloads.clear()
    main(cfg)
    assert downloads == ([] if keep_files else ["/2024-01/Empresas0.zip"])
    with empty_db.connect().cursor() as cur:
        cur.execute("SELECT cnpj_basico, razao_social FROM empresas ORDER BY cnpj_basico")
        assert cur.fetchall() == [("00000001", "First company"), ("00000002", "Second company")]
    assert empty_db.get_processed_files("2024-01") == {"Empresas0.zip"}
    empty_db.connect().commit()

    downloads.clear()
    main(cfg)
    assert downloads == []
