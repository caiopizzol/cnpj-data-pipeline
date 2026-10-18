"""Replace loads tolerate duplicate keys and retry rolled-back first batches."""

import polars as pl
import psycopg2
import pytest

from database import Database


def test_first_replace_batch_deduplicates_and_replaces(empty_db: Database) -> None:
    columns = ["codigo", "descricao"]
    empty_db.bulk_upsert(pl.DataFrame({"codigo": ["old"], "descricao": ["Old"]}), "cnaes", columns)
    batch = pl.DataFrame({"codigo": ["001", "001", "002"], "descricao": ["First", "First", "Second"]})

    empty_db.bulk_insert(batch, "cnaes", columns)
    with empty_db.connect().cursor() as cur:
        cur.execute("SELECT codigo, descricao FROM cnaes ORDER BY codigo")
        assert cur.fetchall() == [("001", "First"), ("002", "Second")]

    empty_db.bulk_insert(pl.DataFrame({"codigo": ["001"], "descricao": ["Updated"]}), "cnaes", columns)
    with empty_db.connect().cursor() as cur:
        cur.execute("SELECT codigo, descricao FROM cnaes ORDER BY codigo")
        assert cur.fetchall() == [("001", "Updated"), ("002", "Second")]


def test_failed_first_batch_keeps_old_rows_and_retry_replaces(empty_db: Database) -> None:
    columns = ["codigo", "descricao"]
    empty_db.bulk_upsert(pl.DataFrame({"codigo": ["old"], "descricao": ["Old"]}), "cnaes", columns)

    with pytest.raises(psycopg2.errors.NotNullViolation):
        empty_db.bulk_insert(pl.DataFrame({"codigo": [None], "descricao": ["Invalid"]}), "cnaes", columns)
    with empty_db.connect().cursor() as cur:
        cur.execute("SELECT codigo FROM cnaes")
        assert cur.fetchall() == [("old",)]

    empty_db.bulk_insert(pl.DataFrame({"codigo": ["001"], "descricao": ["New"]}), "cnaes", columns)
    with empty_db.connect().cursor() as cur:
        cur.execute("SELECT codigo FROM cnaes")
        assert cur.fetchall() == [("001",)]


@pytest.mark.parametrize("first_load", ["truncate_table", "bulk_insert"])
def test_replace_never_empties_tables_that_reference_pipeline_tables(empty_db: Database, first_load: str) -> None:
    """A user table with a foreign key to a pipeline table must block the truncate, not be emptied by it."""
    columns = ["codigo", "descricao"]
    empty_db.bulk_upsert(pl.DataFrame({"codigo": ["001"], "descricao": ["Old"]}), "cnaes", columns)
    with empty_db.connect().cursor() as cur:
        cur.execute("CREATE TABLE favoritos (cnae TEXT REFERENCES cnaes (codigo))")
        cur.execute("INSERT INTO favoritos VALUES ('001')")
    empty_db.connect().commit()

    with pytest.raises(psycopg2.errors.FeatureNotSupported):
        if first_load == "truncate_table":
            empty_db.truncate_table("cnaes")
        else:
            empty_db.bulk_insert(pl.DataFrame({"codigo": ["002"], "descricao": ["New"]}), "cnaes", columns)
    empty_db.connect().rollback()

    with empty_db.connect().cursor() as cur:
        cur.execute("SELECT cnae FROM favoritos")
        assert cur.fetchall() == [("001",)]
