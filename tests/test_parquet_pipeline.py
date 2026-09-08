"""Exercise the Parquet pipeline against a local WebDAV source."""

import json
from dataclasses import replace
from pathlib import Path
from threading import Event

import polars as pl
import pyarrow.parquet as pq
import pytest
from polars.testing import assert_frame_equal

from config import Config
from main import main


@pytest.mark.parametrize("workers", [1, 2])
def test_parquet_download_and_retry_match_clean_export(
    source_server: tuple[str, Event, list[str]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    base_url, fail_second_shard, downloads = source_server
    monkeypatch.setattr("sys.argv", ["cnpj-pipeline", "--month", "2024-01"])
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    clean = tmp_path / "clean"
    retried = tmp_path / "retried"
    cfg = Config(
        database_url="",
        base_url=base_url,
        share_token="test",
        temp_dir=str(tmp_path / "clean-downloads"),
        output_format="parquet",
        parquet_output_dir=str(clean),
        parquet_typed_output=True,
        process_workers=workers,
        retry_attempts=1,
        retry_delay=0,
        connect_timeout=2,
        read_timeout=2,
        stall_timeout=2,
    )

    main(cfg)
    assert sorted(downloads) == [
        "/2024-01/Cnaes.zip",
        "/2024-01/Empresas0.zip",
        "/2024-01/Empresas1.zip",
    ]
    cnaes = pl.DataFrame({"codigo": ["0111301"], "descricao": ["Cultivo de café"]})
    empresas = pl.DataFrame(
        {
            "cnpj_basico": ["00000001", "00000002"],
            "razao_social": ["Café Brasil", "Ação Comércio"],
            "natureza_juridica": ["2062", "2062"],
            "qualificacao_responsavel": ["49", "49"],
            "capital_social": [1234.56, 50.0],
            "porte": ["01", "03"],
            "ente_federativo_responsavel": [None, None],
        },
        schema_overrides={"ente_federativo_responsavel": pl.String},
    )

    cfg = replace(cfg, temp_dir=str(tmp_path / "retry-downloads"), parquet_output_dir=str(retried))
    fail_second_shard.set()
    with pytest.raises(SystemExit) as error:
        main(cfg)
    assert error.value.code == 1
    assert not (retried / "empresas.parquet").exists()
    assert not (retried / "manifest.json").exists()
    assert_frame_equal(pl.read_parquet(retried / "empresas.parquet.partial"), empresas.head(1))
    reference_bytes = (retried / "cnaes.parquet").read_bytes()

    downloads.clear()
    fail_second_shard.clear()
    main(cfg)
    assert sorted(downloads) == ["/2024-01/Empresas0.zip", "/2024-01/Empresas1.zip"]
    assert (retried / "cnaes.parquet").read_bytes() == reference_bytes

    for output in (clean, retried):
        assert sorted(path.name for path in output.iterdir()) == [
            "cnaes.parquet",
            "empresas.parquet",
            "manifest.json",
        ]
        manifest = json.loads((output / "manifest.json").read_text())
        assert manifest["sourceMonth"] == "2024-01"
        assert manifest["schemaVersion"] == "2"
        for table, expected in (("cnaes", cnaes), ("empresas", empresas)):
            path = output / f"{table}.parquet"
            assert_frame_equal(pl.read_parquet(path).sort(expected.columns[0]), expected)
            with pq.ParquetFile(path) as parquet:
                metadata = parquet.metadata
            assert metadata.num_rows == expected.height
            assert metadata.metadata is not None
            assert metadata.metadata[b"cnpj.sourceMonth"] == b"2024-01"
            assert metadata.metadata[b"cnpj.schemaVersion"] == b"2"
            assert metadata.metadata[b"cnpj.typed"] == b"true"
            assert manifest["tables"][table] == {
                "rows": expected.height,
                "sizeBytes": path.stat().st_size,
                "file": path.name,
            }
        assert set(manifest["tables"]) == {"cnaes", "empresas"}
        assert manifest["totals"] == {
            "rows": 3,
            "files": 2,
            "sizeBytes": sum(path.stat().st_size for path in output.glob("*.parquet")),
        }


@pytest.mark.parametrize("workers", [1, 2])
def test_retry_runs_failed_post_file_command_without_reloading_table(
    source_server: tuple[str, Event, list[str]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    import subprocess
    from unittest.mock import patch

    base_url, _failure, downloads = source_server
    monkeypatch.setattr("sys.argv", ["cnpj-pipeline", "--month", "2024-01"])
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    output = tmp_path / "output"
    cfg = Config(
        database_url="",
        base_url=base_url,
        share_token="test",
        temp_dir=str(tmp_path / "downloads"),
        output_format="parquet",
        parquet_output_dir=str(output),
        process_workers=workers,
        post_file_command='publish --label "CNPJ sample"',
        retry_delay=0,
    )
    cnaes = output / "cnaes.parquet"
    with patch("main.subprocess.run", side_effect=subprocess.CalledProcessError(1, "publish")):
        with pytest.raises(SystemExit) as error:
            main(cfg)
        assert error.value.code == 1
    original = cnaes.read_bytes()
    assert not (output / "manifest.json").exists()
    downloads.clear()
    with patch("main.subprocess.run") as publish:
        main(cfg)
    calls = [call.args[0] for call in publish.call_args_list]
    assert calls.count(["publish", "--label", "CNPJ sample", str(cnaes)]) == 1
    assert ["publish", "--label", "CNPJ sample", str(output / "empresas.parquet")] in calls
    assert cnaes.read_bytes() == original
    assert "/2024-01/Cnaes.zip" not in downloads
    assert (output / "manifest.json").exists()

    previous_output = {path.name: path.read_bytes() for path in output.iterdir()}
    downloads.clear()
    with patch("main.subprocess.run") as publish:
        main(cfg)
    calls = [call.args[0] for call in publish.call_args_list]
    assert sorted(calls) == sorted(
        ["publish", "--label", "CNPJ sample", str(output / f"{table}.parquet")] for table in ("cnaes", "empresas")
    )
    assert downloads == []
    assert {path.name: path.read_bytes() for path in output.iterdir() if path.suffix == ".parquet"} == {
        name: content for name, content in previous_output.items() if name.endswith(".parquet")
    }
    assert (
        json.loads((output / "manifest.json").read_text())["tables"]
        == json.loads(previous_output["manifest.json"])["tables"]
    )
