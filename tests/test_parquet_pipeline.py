"""Exercise the Parquet pipeline against a local WebDAV source."""

import json
from collections.abc import Iterator
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from pathlib import Path
from threading import Event, Thread
from zipfile import ZIP_DEFLATED, ZipFile

import polars as pl
import pyarrow.parquet as pq
import pytest
from polars.testing import assert_frame_equal

from config import Config
from main import main


@pytest.fixture
def source_server() -> Iterator[tuple[str, Event, list[str]]]:
    sources = {
        "Cnaes.zip": ("CNAECSV.csv", '"0111301";"Cultivo de café"\n'),
        "Empresas0.zip": ("0.EMPRECSV.csv", '"00000001";"Café Brasil";"2062";"49";"1.234,56";"01";""\n'),
        "Empresas1.zip": ("1.EMPRECSV.csv", '"00000002";"Ação Comércio";"2062";"49";"50,00";"03";""\n'),
    }
    archives: dict[str, bytes] = {}
    for filename, (member, csv) in sources.items():
        buffer = BytesIO()
        with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
            archive.writestr(member, csv.encode("iso-8859-1"))
        archives[f"/2024-01/{filename}"] = buffer.getvalue()

    fail_second_shard = Event()
    downloads: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def respond(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_PROPFIND(self) -> None:
            if self.path == "/":
                paths = ["/2024-01/"]
            elif self.path == "/2024-01/":
                paths = list(archives)
            else:
                self.send_error(404)
                return
            responses = "".join(f"<d:response><d:href>{path}</d:href></d:response>" for path in paths)
            body = f'<d:multistatus xmlns:d="DAV:">{responses}</d:multistatus>'.encode()
            self.respond(207, body, "application/xml")

        def do_GET(self) -> None:
            downloads.append(self.path)
            if self.path == "/2024-01/Empresas1.zip" and fail_second_shard.is_set():
                self.send_error(503, "Second shard unavailable")
            elif self.path in archives:
                self.respond(200, archives[self.path], "application/zip")
            else:
                self.send_error(404)

        def log_message(self, format: str, *args: object) -> None:
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", fail_second_shard, downloads
        finally:
            server.shutdown()
            thread.join(timeout=5)


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
