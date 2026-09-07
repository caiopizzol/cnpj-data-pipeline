"""Shared source fixtures and SQL helpers for integration tests."""

from pathlib import Path

from psycopg2.extensions import cursor

from database import Database
from processor import process_file

ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = ROOT / "tests" / "fixtures"
RECIPES_DIR = ROOT / "recipes" / "postgres"

# Fixture loading order follows file_types.py; the schema has no foreign-key constraints.
PROCESSING_ORDER = [
    "CNAECSV.csv",
    "MOTICSV.csv",
    "MUNICCSV.csv",
    "NATJUCSV.csv",
    "PAISCSV.csv",
    "QUALSCSV.csv",
    "EMPRECSV.csv",
    "ESTABELE.csv",
    "SOCIOCSV.csv",
    "SIMPLESCSV.csv",
]

EXPECTED_COUNTS = {
    "cnaes": 1359,
    "motivos": 63,
    "municipios": 5572,
    "naturezas_juridicas": 91,
    "paises": 255,
    "qualificacoes_socios": 68,
    "empresas": 2006,
    "estabelecimentos": 2006,
    "socios": 2000,
    "dados_simples": 2000,
}

# Crafted fixture rows (cnpj_basico 99000001-99000004) exercise the enriched
# reference-domain recipe: motivo 32 (supplemental), pais 150/994 (supplemental),
# pais 008 (unresolved orphan), qualificacao_responsavel 36 (legacy supplemental).
# Row 99000005 exercises the static domain-label recipe's NULL path: it carries
# an unknown porte (07) and unknown situacao_cadastral (07), neither of which is
# in the label tables, so empresa_detalhe's LEFT JOINs must keep the row with a
# NULL descricao. The ingest validator only warns on out-of-domain porte/situacao
# codes; it does not drop or nullify them, so the codes survive ingest.
# Row 99000006 carries situacao_cadastral 05 (Ativa Não Regular), a code in the
# SERPRO domain but outside the open-data layout regex (^(01|02|03|04|08)$). The
# validator only warns, so it survives ingest and must resolve end to end through
# empresa_detalhe's LEFT JOIN, not silently become NULL.
ENRICHED_SUPPLEMENTAL_MOTIVO_32 = "Inexistente De Fato – Ade/Cosar"


def fetch_row(cur: cursor):
    row = cur.fetchone()
    assert row is not None
    return row


def count_rows(db: Database, table: str) -> int:
    with db.connect().cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return fetch_row(cur)[0]


def load_source_fixtures(db: Database) -> None:
    for name in PROCESSING_ORDER:
        for batch, table, columns in process_file(FIXTURES_DIR / name, batch_size=500000):
            db.bulk_upsert(batch, table, columns)


def apply_recipe(db: Database, name: str) -> None:
    with db.connect().cursor() as cur:
        cur.execute((RECIPES_DIR / f"{name}.sql").read_text())
    db.connect().commit()
