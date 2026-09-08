"""Check benchmark provenance, output verification, and failure reporting."""

import json
import runpy
import subprocess
import sys
from pathlib import Path
from threading import Event
from unittest.mock import patch

import polars as pl
import pytest
import requests

from config import Config
from scripts.benchmark_pipeline import logical_sha256, main, measure, prepare, sha256


def test_prepare_and_measure_real_http_sample(
    source_server: tuple[str, Event, list[str]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base_url, _failure, downloads = source_server
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    work = tmp_path / "source"
    monkeypatch.setattr(
        "sys.argv", ["benchmark", "prepare", "2024-01", "Cnaes.zip", str(work), "--base-url", base_url, "--rows", "1"]
    )
    main()
    preparation = json.loads((work / "sample.json").read_text())
    source = Path(preparation["sample"])
    assert preparation["source_month"] == "2024-01"
    assert preparation["member"] == "CNAECSV.csv"
    assert preparation["archive_sha256"] == sha256(work / "2024-01.Cnaes.zip")
    assert preparation["sample_sha256"] == sha256(source)
    assert preparation["sample_rows"] == 1
    assert preparation["resumed_preparation"] is False
    assert downloads == ["/2024-01/Cnaes.zip"]
    assert source.read_text(encoding="iso-8859-1") == '"0111301";"Cultivo de café"\n'

    output = tmp_path / "output"
    monkeypatch.setattr("sys.argv", ["benchmark", "run", str(source), str(output), "--typed"])
    runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/benchmark_pipeline.py"), run_name="__main__")
    report = json.loads((output / "measurement.json").read_text())
    assert report["rows"] == 1
    assert report["typed"] is True
    assert report["input_sha256"] == preparation["sample_sha256"]
    assert report["processing_seconds"] > 0
    assert report["writing_seconds"] > 0
    assert report["total_seconds"] >= report["processing_seconds"] + report["writing_seconds"]
    assert report["peak_process_rss_bytes_before_verification"] > 0
    assert report["output_logical_sha256"] == logical_sha256(output / "cnaes.parquet")
    assert pl.read_parquet(output / "cnaes.parquet").rows() == [("0111301", "Cultivo de café")]
    with pytest.raises(FileExistsError):
        measure(source, output, 500_000, True)
    with pytest.raises(ValueError, match="unfinished preparation"):
        prepare(Config(database_url="", temp_dir=str(work), base_url=base_url), "2024-01", "Cnaes.zip", 1)


def test_prepare_retries_saved_partial_for_same_request(
    source_server: tuple[str, Event, list[str]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base_url, _failure, _downloads = source_server
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    work = tmp_path / "sample"
    config = Config(database_url="", temp_dir=str(work), base_url=base_url, keep_files=True)
    partial = work / "Cnaes.zip.2024-01.part"

    def interrupted_download(_month: str, _archive: str) -> list[Path]:
        partial.write_bytes(b"partial ZIP")
        raise requests.exceptions.ConnectionError("interrupted")

    with patch("scripts.benchmark_pipeline.TimedDownloader.download_file", side_effect=interrupted_download):
        with pytest.raises(requests.exceptions.ConnectionError):
            prepare(config, "2024-01", "Cnaes.zip", 1)
    assert partial.read_bytes() == b"partial ZIP"
    with pytest.raises(ValueError, match="unfinished preparation"):
        prepare(config, "2024-02", "Cnaes.zip", 1)
    assert partial.read_bytes() == b"partial ZIP"

    (work / "2023-12.Cnaes.zip").write_bytes(b"not the requested archive")
    with patch("requests.get", wraps=requests.get) as get:
        result = prepare(config, "2024-01", "Cnaes.zip", 1)
    assert get.call_args.kwargs["headers"]["Range"] == "bytes=11-"
    assert result["resumed_preparation"] is True
    assert result["sample_rows"] == 1
    assert result["archive_sha256"] == sha256(work / "2024-01.Cnaes.zip")
    assert not partial.exists()


def test_prepare_rejects_unowned_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unfinished preparation"):
        prepare(Config(database_url="", temp_dir=str(tmp_path)), "2024-01", "Cnaes.zip", 1)


@pytest.mark.parametrize("keep_files", [False, True])
def test_prepare_retries_cached_zip_after_sampling_failure(
    source_server: tuple[str, Event, list[str]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, keep_files: bool
) -> None:
    base_url, _failure, downloads = source_server
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    config = Config(database_url="", temp_dir=str(tmp_path / "sample"), base_url=base_url, keep_files=keep_files)
    with patch("scripts.benchmark_pipeline.csv.writer", side_effect=OSError("disk full")):
        with pytest.raises(OSError, match="disk full"):
            prepare(config, "2024-01", "Cnaes.zip", 1)

    result = prepare(config, "2024-01", "Cnaes.zip", 1)
    assert result["resumed_preparation"] is True
    assert result["sample_rows"] == 1
    assert downloads == ["/2024-01/Cnaes.zip"]
    report = json.loads((tmp_path / "sample/sample.json").read_text())
    assert report["download_and_crc_seconds"] > 0
    assert config.keep_files is keep_files


def test_prepare_retries_interrupted_manifest_write(
    source_server: tuple[str, Event, list[str]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base_url, _failure, downloads = source_server
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    work = tmp_path / "sample"
    config = Config(database_url="", temp_dir=str(work), base_url=base_url, keep_files=True)
    write_text = Path.write_text

    def interrupted_write(path: Path, data: str) -> int:
        if path.name in ("sample.json", "sample.json.tmp"):
            write_text(path, '{"unfinished":')
            raise OSError("interrupted manifest")
        return write_text(path, data)

    with patch.object(Path, "write_text", autospec=True, side_effect=interrupted_write):
        with pytest.raises(OSError, match="interrupted manifest"):
            prepare(config, "2024-01", "Cnaes.zip", 1)
    assert not (work / "sample.json").exists()
    result = prepare(config, "2024-01", "Cnaes.zip", 1)
    assert json.loads((work / "sample.json").read_text()) == result
    assert downloads == ["/2024-01/Cnaes.zip"]


def test_prepare_excludes_other_process_and_releases_lock_after_failure(tmp_path: Path) -> None:
    work = tmp_path / "sample"
    config = Config(database_url="", temp_dir=str(work))
    contender = """
import sys
from unittest.mock import patch
from config import Config
from scripts.benchmark_pipeline import prepare
with patch("scripts.benchmark_pipeline.TimedDownloader.download_file", side_effect=RuntimeError("concurrent download")):
    prepare(Config(database_url="", temp_dir=sys.argv[1]), "2024-01", "Cnaes.zip", 1)
"""

    def download(_month: str, _archive: str) -> list[Path]:
        process = subprocess.run(
            [sys.executable, "-c", contender, str(work)], capture_output=True, text=True, timeout=15
        )
        assert process.returncode != 0
        assert "BlockingIOError" in process.stderr
        assert "concurrent download" not in process.stderr
        raise RuntimeError("first process interrupted")

    with patch("scripts.benchmark_pipeline.TimedDownloader.download_file", side_effect=download):
        with pytest.raises(RuntimeError, match="first process interrupted"):
            prepare(config, "2024-01", "Cnaes.zip", 1)
    with patch("scripts.benchmark_pipeline.TimedDownloader.download_file", side_effect=RuntimeError("lock released")):
        with pytest.raises(RuntimeError, match="lock released"):
            prepare(config, "2024-01", "Cnaes.zip", 1)


def test_checksum_ignores_row_groups_but_detects_data_schema_and_order_changes(tmp_path: Path) -> None:
    frame = pl.DataFrame({"codigo": ["01", "02"], "descricao": ["Café", None]})
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    frame.write_parquet(a, row_group_size=1)
    frame.write_parquet(b, row_group_size=2)
    assert logical_sha256(a) == logical_sha256(b)
    for changed in (
        frame.reverse(),
        frame.with_columns(pl.lit("different").alias("descricao")),
        frame.rename({"codigo": "id"}),
    ):
        changed.write_parquet(b)
        assert logical_sha256(a) != logical_sha256(b)


def test_prepare_rejects_nonpositive_rows(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="positive"):
        prepare(Config(database_url="", temp_dir=str(tmp_path / "sample")), "2024-01", "Cnaes.zip", 0)


@pytest.mark.parametrize("members", [[], ["one.CNAECSV", "two.CNAECSV"], ["empty.CNAECSV"]])
def test_prepare_does_not_report_ambiguous_or_empty_sources(tmp_path: Path, members: list[str]) -> None:
    sources = [tmp_path / name for name in members]
    for source in sources:
        source.touch()
    work = tmp_path / "sample"
    with patch("scripts.benchmark_pipeline.TimedDownloader.download_file", return_value=sources):
        with pytest.raises(ValueError, match="exactly one|empty"):
            prepare(Config(database_url="", temp_dir=str(work)), "2024-01", "Cnaes.zip", 1)
    assert not (work / "sample.json").exists()


def test_measure_does_not_report_empty_or_truncated_output(tmp_path: Path) -> None:
    source = tmp_path / "CNAECSV.csv"
    source.write_text("")
    with pytest.raises(ValueError, match="positive"):
        measure(source, tmp_path / "invalid", 0, False)
    with pytest.raises(ValueError, match="no rows"):
        measure(source, tmp_path / "empty", 1, False)
    assert not (tmp_path / "empty" / "measurement.json").exists()

    source.write_text("01;One\n02;Two\n")
    output = tmp_path / "truncated"
    batch = pl.DataFrame({"codigo": ["01"], "descricao": ["One"]})
    with patch("scripts.benchmark_pipeline.process_file", return_value=iter_batches(batch)):
        with pytest.raises(ValueError, match="2 rows, processed 1"):
            measure(source, output, 1, False)
    assert not (output / "measurement.json").exists()
    assert not (output / "cnaes.parquet").exists()


def iter_batches(batch: pl.DataFrame):
    yield batch, "cnaes", batch.columns
