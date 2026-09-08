"""Exercise PostgreSQL loading and file tracking through the real pipeline."""

from pathlib import Path
from threading import Event

import pytest

from config import Config
from database import Database
from main import main


@pytest.mark.parametrize("strategy", ["upsert", "replace"])
@pytest.mark.parametrize("workers", [1, 2])
def test_postgres_retry_loads_only_pending_files(
    strategy: str,
    workers: int,
    source_server: tuple[str, Event, list[str]],
    empty_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_url, fail_second_shard, downloads = source_server
    monkeypatch.setattr("sys.argv", ["cnpj-pipeline", "--month", "2024-01"])
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    cfg = Config(
        database_url=empty_db.database_url,
        base_url=base_url,
        share_token="test",
        temp_dir=str(tmp_path / "downloads"),
        download_workers=1,
        process_workers=workers,
        loading_strategy=strategy,
        retry_attempts=1,
        retry_delay=0,
        connect_timeout=2,
        read_timeout=2,
        stall_timeout=2,
    )
    empresas = [
        ("00000001", "Café Brasil", "2062", "49", 1234.56, "01", None),
        ("00000002", "Ação Comércio", "2062", "49", 50.0, "03", None),
    ]

    def assert_database_state(company_count: int, completed: list[str]) -> None:
        with empty_db.connect().cursor() as cur:
            cur.execute("SELECT codigo, descricao FROM cnaes")
            assert cur.fetchall() == [("0111301", "Cultivo de café")]
            cur.execute(
                "SELECT cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, "
                "capital_social, porte, ente_federativo_responsavel FROM empresas ORDER BY cnpj_basico"
            )
            assert cur.fetchall() == empresas[:company_count]
            cur.execute("SELECT directory, filename FROM processed_files ORDER BY filename")
            assert cur.fetchall() == [("2024-01", filename) for filename in completed]
        empty_db.connect().commit()

    fail_second_shard.set()
    with pytest.raises(SystemExit) as error:
        main(cfg)
    assert error.value.code == 1
    assert sorted(downloads) == ["/2024-01/Cnaes.zip", "/2024-01/Empresas0.zip", "/2024-01/Empresas1.zip"]
    assert_database_state(1, ["Cnaes.zip", "Empresas0.zip"])

    downloads.clear()
    fail_second_shard.clear()
    main(cfg)
    assert downloads == ["/2024-01/Empresas1.zip"]
    assert_database_state(2, ["Cnaes.zip", "Empresas0.zip", "Empresas1.zip"])

    downloads.clear()
    main(cfg)
    assert downloads == []
    assert_database_state(2, ["Cnaes.zip", "Empresas0.zip", "Empresas1.zip"])


@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize("force", [False, True])
def test_replace_starts_fresh_for_new_month_or_force(
    workers: int,
    force: bool,
    source_server: tuple[str, Event, list[str]],
    empty_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_url, _, _ = source_server
    with empty_db.connect().cursor() as cur:
        cur.execute("INSERT INTO empresas (cnpj_basico, razao_social) VALUES ('99999999', 'Old company')")
    empty_db.mark_processed("2024-01" if force else "2023-12", "Empresas0.zip")
    monkeypatch.setattr("sys.argv", ["cnpj-pipeline", "--month", "2024-01", *(["--force"] if force else [])])
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    cfg = Config(
        database_url=empty_db.database_url,
        base_url=base_url,
        share_token="test",
        temp_dir=str(tmp_path / "downloads"),
        loading_strategy="replace",
        process_workers=workers,
        download_workers=1,
    )

    main(cfg)

    with empty_db.connect().cursor() as cur:
        cur.execute("SELECT cnpj_basico FROM empresas ORDER BY cnpj_basico")
        assert cur.fetchall() == [("00000001",), ("00000002",)]
    assert empty_db.get_processed_files("2024-01") == {"Cnaes.zip", "Empresas0.zip", "Empresas1.zip"}
