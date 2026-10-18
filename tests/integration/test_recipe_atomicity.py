"""A recipe that fails must leave the previous build of its tables in place."""

import os
import subprocess
from pathlib import Path

import pytest

from database import Database
from tests.integration.support import RECIPES_DIR

RECIPES = sorted(RECIPES_DIR.glob("*.sql"))


@pytest.mark.parametrize("path", RECIPES, ids=[p.stem for p in RECIPES])
def test_recipe_runs_in_one_transaction(path: Path) -> None:
    code = [
        line.strip() for line in path.read_text().splitlines() if line.strip() and not line.lstrip().startswith("--")
    ]
    assert code[0] == "BEGIN;", f"{path.name} must start with BEGIN;"
    assert code[-1] == "COMMIT;", f"{path.name} must end with COMMIT;"


@pytest.mark.usefixtures("empresa_detalhe")
def test_failed_recipe_keeps_previous_table(test_db: Database) -> None:
    """Run the recipe the way recipes/README.md documents, with a source table missing."""
    with test_db.connect().cursor() as cur:
        cur.execute("SELECT count(*) FROM empresa_detalhe")
        before = cur.fetchone()
        cur.execute("ALTER TABLE empresas RENAME TO empresas_hidden")
    test_db.connect().commit()

    params = test_db.connect().get_dsn_parameters()
    result = subprocess.run(
        ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", params["host"], "-p", params["port"], "-U", params["user"]]
        + ["-d", params["dbname"], "-f", str(RECIPES_DIR / "empresa_detalhe.sql")],
        env={**os.environ, "PGPASSWORD": "postgres"},
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode != 0
    assert 'relation "empresas" does not exist' in result.stderr
    with test_db.connect().cursor() as cur:
        cur.execute("SELECT count(*) FROM empresa_detalhe")
        assert cur.fetchone() == before
