"""A killed pipeline must replay its unfinished shard without losing rows."""

import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from threading import Event

import pytest

from database import Database
from tests.integration.support import FIXTURES_DIR

LOADER = """
import json
import os
import signal
import sys
import time
from pathlib import Path
import main
from config import Config
from database import Database

config, interrupt = sys.argv[1:]
cfg = Config(**json.loads(config))
sys.argv = ['cnpj-pipeline', '--month', '2024-01']
original = main.process_file

def interrupted(path, batch_size):
    for batch in original(path, batch_size):
        yield batch
        if interrupt == 'yes' and path.name == '1.SOCIOCSV.csv':
            # The yielded batch has committed. Let the other worker finish shard 0.
            db = Database(cfg.database_url)
            try:
                deadline = time.monotonic() + 20
                while 'Socios0.zip' not in db.get_processed_files('2024-01'):
                    if time.monotonic() > deadline:
                        raise TimeoutError('First shard did not complete')
                    time.sleep(0.01)
            finally:
                db.disconnect()
            os.kill(os.getpid(), signal.SIGKILL)

main.process_file = interrupted
main.main(cfg)
"""


@pytest.fixture
def source_files() -> dict[str, dict[str, str]]:
    rows = (FIXTURES_DIR / "SOCIOCSV.csv").read_text(encoding="iso-8859-1").splitlines(keepends=True)
    assert len(rows) == 2000
    return {
        "Socios0.zip": {"0.SOCIOCSV.csv": "".join(rows[:1000])},
        # Include a shared key to exercise cross-shard duplicate handling on replay.
        "Socios1.zip": {"1.SOCIOCSV.csv": "".join(rows[1000:]) + rows[999]},
    }


@pytest.mark.skipif(os.name != "posix", reason="Requires SIGKILL")
@pytest.mark.parametrize("strategy", ["upsert", "replace"])
@pytest.mark.parametrize("workers", [1, 2])
def test_killed_pipeline_replays_like_uninterrupted_load(
    empty_db: Database,
    strategy: str,
    workers: int,
    source_server: tuple[str, Event, list[str]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_url, _, downloads = source_server
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    cfg = dict(
        database_url=empty_db.database_url,
        base_url=base_url,
        share_token="test",
        temp_dir=str(tmp_path / "downloads"),
        loading_strategy=strategy,
        process_workers=workers,
        download_workers=1,
        batch_size=1,
        retry_attempts=1,
    )
    command = [sys.executable, "-c", LOADER, json.dumps(cfg)]

    def snapshot() -> list[str]:
        with empty_db.connect().cursor() as cur:
            cur.execute(
                "SELECT (to_jsonb(s) - 'data_criacao' - 'data_atualizacao')::text FROM socios s ORDER BY socio_id"
            )
            result = [json.dumps(json.loads(row[0]), sort_keys=True) for row in cur.fetchall()]
        empty_db.connect().commit()
        return result

    subprocess.run([*command, "no"], check=True, capture_output=True, timeout=60)
    expected = snapshot()
    assert len(expected) == 2000
    assert empty_db.get_processed_files("2024-01") == {"Socios0.zip", "Socios1.zip"}
    empty_db.truncate_table("socios")
    empty_db.clear_processed_files("2024-01")

    killed = subprocess.run([*command, "yes"], capture_output=True, timeout=60)
    assert killed.returncode == -signal.SIGKILL, killed.stderr.decode()
    assert 1000 < len(snapshot()) < len(expected)
    assert empty_db.get_processed_files("2024-01") == {"Socios0.zip"}
    empty_db.connect().commit()

    downloads.clear()
    subprocess.run([*command, "no"], check=True, capture_output=True, timeout=60)
    assert snapshot() == expected
    assert empty_db.get_processed_files("2024-01") == {"Socios0.zip", "Socios1.zip"}
    assert "/2024-01/Socios0.zip" not in downloads
    empty_db.connect().commit()

    downloads.clear()
    subprocess.run([*command, "no"], check=True, capture_output=True, timeout=60)
    assert downloads == []
    assert snapshot() == expected
