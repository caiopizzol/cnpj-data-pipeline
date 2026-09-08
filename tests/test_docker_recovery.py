"""Exercise the Linux image's entrypoint without replacing its dependencies."""

import os
import subprocess
from threading import Event
from uuid import uuid4

import pytest

from tests.integration.conftest import database

IMAGE = os.environ.get("CNPJ_TEST_IMAGE")
pytestmark = pytest.mark.skipif(not IMAGE, reason="Set CNPJ_TEST_IMAGE to run the Docker recovery check")
FIRST = '"00000001";"Café Brasil";"2062";"49";"10,00";"01";""\n'
SECOND = '"00000002";"Ação Comércio";"2062";"49";"20,00";"01";""\n'


@pytest.fixture
def source_files() -> dict[str, dict[str, str]]:
    return {"Empresas0.zip": {"0.EMPRECSV.csv": FIRST * 2, "1.EMPRECSV.csv": "broken;row\n"}}


def test_docker_recovers_incomplete_zip(
    source_files: dict[str, dict[str, str]],
    source_server: tuple[str, Event, list[str]],
) -> None:
    image = IMAGE
    assert image
    base_url, _, downloads = source_server
    # No availability skip: a requested Docker check must fail if PostgreSQL is missing.
    with database(f"cnpj_docker_{uuid4().hex}") as db:

        def run() -> subprocess.CompletedProcess[str]:
            name = f"cnpj-recovery-{uuid4().hex}"
            try:
                return subprocess.run(
                    [
                        "docker",
                        "run",
                        "--rm",
                        "--name",
                        name,
                        "--network",
                        "host",
                        "-e",
                        f"DATABASE_URL={db.database_url}",
                        "-e",
                        f"BASE_URL={base_url}",
                        "-e",
                        "SHARE_TOKEN=test",
                        "-e",
                        "OUTPUT_FORMAT=postgres",
                        "-e",
                        "LOADING_STRATEGY=replace",
                        "-e",
                        "PROCESS_WORKERS=1",
                        "-e",
                        "DOWNLOAD_WORKERS=1",
                        image,
                        "--month",
                        "2024-01",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
            finally:
                subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)

        failed = run()
        assert failed.returncode != 0, failed.stdout + failed.stderr
        assert "Layout drift" in failed.stderr, failed.stdout + failed.stderr
        with db.connect().cursor() as cur:
            cur.execute("SELECT cnpj_basico, razao_social FROM empresas")
            assert cur.fetchall() == [("00000001", "Café Brasil")]
        assert db.get_processed_files("2024-01") == set()
        db.connect().commit()

        source_files["Empresas0.zip"]["1.EMPRECSV.csv"] = SECOND
        downloads.clear()
        retry = run()
        assert retry.returncode == 0, retry.stdout + retry.stderr
        assert downloads == ["/2024-01/Empresas0.zip"]
        with db.connect().cursor() as cur:
            cur.execute("SELECT cnpj_basico, razao_social, capital_social FROM empresas ORDER BY cnpj_basico")
            expected = [("00000001", "Café Brasil", 10.0), ("00000002", "Ação Comércio", 20.0)]
            assert cur.fetchall() == expected
        assert db.get_processed_files("2024-01") == {"Empresas0.zip"}
        db.connect().commit()

        downloads.clear()
        noop = run()
        assert noop.returncode == 0, noop.stdout + noop.stderr
        assert downloads == []
        with db.connect().cursor() as cur:
            cur.execute("SELECT cnpj_basico, razao_social, capital_social FROM empresas ORDER BY cnpj_basico")
            assert cur.fetchall() == expected
