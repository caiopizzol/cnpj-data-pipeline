"""Tiny local source shared by end-to-end pipeline tests."""

from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from threading import Event, Thread
from zipfile import ZIP_DEFLATED, ZipFile

import pytest


@pytest.fixture
def source_files() -> dict[str, dict[str, str]]:
    return {
        "Cnaes.zip": {"CNAECSV.csv": '"0111301";"Cultivo de café"\n'},
        "Empresas0.zip": {"0.EMPRECSV.csv": '"00000001";"Café Brasil";"2062";"49";"1.234,56";"01";""\n'},
        "Empresas1.zip": {"1.EMPRECSV.csv": '"00000002";"Ação Comércio";"2062";"49";"50,00";"03";""\n'},
    }


@pytest.fixture
def source_server(source_files: dict[str, dict[str, str]]) -> Iterator[tuple[str, Event, list[str]]]:
    archives = {f"/2024-01/{filename}": members for filename, members in source_files.items()}

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
            else:
                assert self.path == "/2024-01/"
                paths = list(archives)
            responses = "".join(f"<d:response><d:href>{path}</d:href></d:response>" for path in paths)
            body = f'<d:multistatus xmlns:d="DAV:">{responses}</d:multistatus>'.encode()
            self.respond(207, body, "application/xml")

        def do_GET(self) -> None:
            downloads.append(self.path)
            if self.path == "/2024-01/Empresas1.zip" and fail_second_shard.is_set():
                self.send_error(503, "Second shard unavailable")
            else:
                buffer = BytesIO()
                with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
                    for member, csv in archives[self.path].items():
                        archive.writestr(member, csv.encode("iso-8859-1"))
                self.respond(200, buffer.getvalue(), "application/zip")

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
