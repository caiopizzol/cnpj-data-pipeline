import pytest

from database import Database
from tests.integration.support import (
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("reference_domain_labels")
class TestRecipeDomainLabels:
    """Verify reference domain labels against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "reference_domain_labels.sql"

    LABEL_TABLES = (
        "portes_empresa",
        "situacoes_cadastrais",
        "indicadores_matriz_filial",
        "identificadores_socio",
        "faixas_etarias",
    )

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute, creating all three tables."""

        for table in self.LABEL_TABLES:
            with test_db.connect().cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                    (table,),
                )
                assert cur.fetchone() is not None, f"{table} not created"

    def test_porte_labels_resolve_verbatim(self, test_db: Database) -> None:
        """porte codes resolve to their verbatim SERPRO labels."""
        expected = {
            "01": "Microempresa",
            "03": "Empresa de Pequeno Porte",
            "05": "Demais",
        }
        with test_db.connect().cursor() as cur:
            for codigo, descricao in expected.items():
                cur.execute("SELECT descricao FROM portes_empresa WHERE codigo = %s", (codigo,))
                row = fetch_row(cur)
                assert row is not None, f"porte {codigo} missing from portes_empresa"
                assert row[0] == descricao, f"porte {codigo}: {row[0]!r}"

    def test_situacao_labels_resolve_verbatim(self, test_db: Database) -> None:
        """situacao_cadastral codes resolve to their verbatim SERPRO labels."""
        expected = {
            "02": "Ativa",
            "05": "Ativa Não Regular",
            "08": "Baixada",
        }
        with test_db.connect().cursor() as cur:
            for codigo, descricao in expected.items():
                cur.execute("SELECT descricao FROM situacoes_cadastrais WHERE codigo = %s", (codigo,))
                row = fetch_row(cur)
                assert row is not None, f"situacao {codigo} missing from situacoes_cadastrais"
                assert row[0] == descricao, f"situacao {codigo}: {row[0]!r}"

    def test_matriz_filial_labels_resolve_verbatim(self, test_db: Database) -> None:
        """The matriz/filial indicator resolves both integer codes verbatim."""
        expected = {1: "Matriz", 2: "Filial"}
        with test_db.connect().cursor() as cur:
            for codigo, descricao in expected.items():
                cur.execute("SELECT descricao FROM indicadores_matriz_filial WHERE codigo = %s", (codigo,))
                row = fetch_row(cur)
                assert row is not None, f"matriz/filial {codigo} missing"
                assert row[0] == descricao, f"matriz/filial {codigo}: {row[0]!r}"

    def test_identificador_socio_labels_resolve(self, test_db: Database) -> None:
        """identificador_de_socio codes resolve to their layout-derived labels
        (readable title case, not byte-verbatim - the layout gives these in prose)."""
        expected = {
            "1": "Pessoa Jurídica",
            "2": "Pessoa Física",
            "3": "Estrangeiro",
        }
        with test_db.connect().cursor() as cur:
            for codigo, descricao in expected.items():
                cur.execute("SELECT descricao FROM identificadores_socio WHERE codigo = %s", (codigo,))
                row = fetch_row(cur)
                assert row is not None, f"identificador {codigo} missing from identificadores_socio"
                assert row[0] == descricao, f"identificador {codigo}: {row[0]!r}"

    def test_faixa_etaria_labels_resolve(self, test_db: Database) -> None:
        """faixa_etaria codes resolve to their layout-derived labels (readable
        title case, not byte-verbatim - the layout gives the age bands in prose),
        including the documented '0' (Não se aplica), which is a real value here -
        nulling it is socios_clean's job, not this lookup's."""
        expected = {
            "0": "Não se aplica",
            "1": "0 a 12 anos",
            "6": "51 a 60 anos",
            "9": "Maiores de 80 anos",
        }
        with test_db.connect().cursor() as cur:
            for codigo, descricao in expected.items():
                cur.execute("SELECT descricao FROM faixas_etarias WHERE codigo = %s", (codigo,))
                row = fetch_row(cur)
                assert row is not None, f"faixa {codigo} missing from faixas_etarias"
                assert row[0] == descricao, f"faixa {codigo}: {row[0]!r}"

    def test_socio_label_provenance_is_receita_layout(self, test_db: Database) -> None:
        """The sócio enums have no SERPRO domain CSV, so every row is sourced
        from the Receita CNPJ layout PDF."""
        for table in ("identificadores_socio", "faixas_etarias"):
            with test_db.connect().cursor() as cur:
                cur.execute(
                    f"SELECT count(*) FROM {table} "
                    "WHERE source_kind <> 'receita_layout' "
                    "OR source_url <> 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'"
                )
                assert fetch_row(cur)[0] == 0, f"{table} has non-receita_layout provenance"

    def test_codigo_is_unique_primary_key(self, test_db: Database) -> None:
        """codigo is the PK of every label table, so no code may appear twice."""
        for table in self.LABEL_TABLES:
            with test_db.connect().cursor() as cur:
                cur.execute(f"SELECT codigo, COUNT(*) FROM {table} GROUP BY codigo HAVING COUNT(*) > 1")
                assert cur.fetchall() == [], f"{table} has duplicate codigo"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe drops+recreates without error, same counts."""
        sql = self.RECIPE_PATH.read_text()
        counts_before = {t: count_rows(test_db, t) for t in self.LABEL_TABLES}
        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()
        for table, before in counts_before.items():
            assert count_rows(test_db, table) == before, f"{table} row count changed on re-run"
