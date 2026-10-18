"""The built package, installed outside the repository, can bootstrap an empty database."""

import subprocess
import sys
from pathlib import Path

from database import Database
from tests.integration.support import ROOT


def test_installed_package_creates_schema(empty_db: Database, tmp_path: Path) -> None:
    with empty_db.connect().cursor() as cur:
        cur.execute("DROP TABLE processed_files")
    empty_db.connect().commit()
    dist, site = tmp_path / "dist", tmp_path / "site"
    subprocess.run(["uv", "build", "--wheel", "--out-dir", str(dist), str(ROOT)], check=True, capture_output=True)
    wheel = next(dist.glob("*.whl"))
    subprocess.run(
        ["uv", "pip", "install", "--no-deps", "--target", str(site), str(wheel)], check=True, capture_output=True
    )

    # -I keeps the repository off sys.path, so only the installed modules are importable.
    params = empty_db.connect().get_dsn_parameters()
    url = f"postgresql://{params['user']}:postgres@{params['host']}:{params['port']}/{params['dbname']}"
    code = f"import sys; sys.path.insert(0, {str(site)!r}); import database; database.Database({url!r}).ensure_schema()"
    result = subprocess.run([sys.executable, "-I", "-c", code], cwd=tmp_path, capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    with empty_db.connect().cursor() as cur:
        cur.execute("SELECT to_regclass('processed_files')")
        assert cur.fetchone() == ("processed_files",)
