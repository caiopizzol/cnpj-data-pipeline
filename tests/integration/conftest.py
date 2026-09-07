"""Fresh PostgreSQL state and explicit recipe prerequisites for each test."""

from collections.abc import Generator, Iterator
from contextlib import closing, contextmanager

import psycopg2
import pytest
from psycopg2 import sql

from database import Database
from tests.integration.support import ROOT, apply_recipe, load_source_fixtures


@pytest.fixture(scope="session")
def postgres_available() -> None:
    try:
        with closing(
            psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="postgres")
        ):
            pass
    except Exception:
        pytest.skip("PostgreSQL not available")


@contextmanager
def database(name: str, template: str | None = None) -> Generator[Database]:
    with closing(
        psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="postgres")
    ) as admin:
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))
            query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
            if template is not None:
                query += sql.SQL(" TEMPLATE {}").format(sql.Identifier(template))
            cur.execute(query)
        db = Database(f"postgresql://postgres:postgres@localhost:5435/{name}")
        try:
            if template is None:
                with db.connect().cursor() as cur:
                    cur.execute((ROOT / "initial.sql").read_text())
                db.connect().commit()
            yield db
        finally:
            db.disconnect()
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))


@pytest.fixture
def empty_db(postgres_available: None) -> Iterator[Database]:
    with database("cnpj_test_empty") as db:
        yield db


@pytest.fixture(scope="session")
def source_template(postgres_available: None) -> Iterator[str]:
    # Close the source connection before PostgreSQL clones it for each test.
    name = "cnpj_test_source"
    with database(name) as db:
        load_source_fixtures(db)
        db.disconnect()
        yield name


@pytest.fixture
def test_db(source_template: str) -> Iterator[Database]:
    with database("cnpj_test", template=source_template) as db:
        yield db


@pytest.fixture
def reference_domains_enriched(test_db: Database) -> None:
    apply_recipe(test_db, "reference_domains_enriched")


@pytest.fixture
def reference_domain_labels(test_db: Database) -> None:
    apply_recipe(test_db, "reference_domain_labels")


@pytest.fixture
def empresa_detalhe(test_db: Database, reference_domains_enriched: None, reference_domain_labels: None) -> None:
    apply_recipe(test_db, "empresa_detalhe")


@pytest.fixture
def data_quality_flags(test_db: Database, reference_domains_enriched: None) -> None:
    apply_recipe(test_db, "data_quality_flags")


@pytest.fixture
def estabelecimentos_clean(test_db: Database, data_quality_flags: None) -> None:
    apply_recipe(test_db, "estabelecimentos_clean")


@pytest.fixture
def cnae_secundaria_exploded(test_db: Database) -> None:
    apply_recipe(test_db, "cnae_secundaria_exploded")


@pytest.fixture
def socios_quality_flags(test_db: Database, reference_domains_enriched: None) -> None:
    apply_recipe(test_db, "socios_quality_flags")


@pytest.fixture
def socios_clean(test_db: Database, socios_quality_flags: None) -> None:
    apply_recipe(test_db, "socios_clean")


@pytest.fixture
def socios_detalhe(test_db: Database, reference_domains_enriched: None, reference_domain_labels: None) -> None:
    apply_recipe(test_db, "socios_detalhe")


@pytest.fixture
def empresas_busca_nome(test_db: Database) -> None:
    apply_recipe(test_db, "empresas_busca_nome")


@pytest.fixture
def empresas_busca_nome_counts(test_db: Database, empresas_busca_nome: None) -> None:
    apply_recipe(test_db, "empresas_busca_nome_counts")


@pytest.fixture
def cnaes_hierarquia(test_db: Database) -> None:
    apply_recipe(test_db, "cnaes_hierarquia")
