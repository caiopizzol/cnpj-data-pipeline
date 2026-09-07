import pytest

from database import Database
from tests.integration.support import (
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("socios_detalhe")
class TestRecipeSociosDetalhe:
    """Verify socios detalhe against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "socios_detalhe.sql"

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute. It depends on the enriched
        lookups and the static label tables, so apply those first."""

        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'socios_detalhe'")
            assert cur.fetchone() is not None, "socios_detalhe table not created"

    def test_row_count_matches_socios(self, test_db: Database) -> None:
        """Every lookup is 1:1 on its codigo key, so the five LEFT JOINs neither
        drop nor multiply rows: one socios_detalhe row per socios row."""
        socios = count_rows(test_db, "socios")
        detalhe = count_rows(test_db, "socios_detalhe")
        assert detalhe > 0, "socios_detalhe is empty"
        assert detalhe == socios, f"socios_detalhe ({detalhe}) != socios ({socios})"

    def test_socio_id_is_unique(self, test_db: Database) -> None:
        """socio_id is the grain; it must be unique (the old triple is not)."""
        with test_db.connect().cursor() as cur:
            cur.execute("SELECT socio_id, COUNT(*) FROM socios_detalhe GROUP BY socio_id HAVING COUNT(*) > 1")
            assert cur.fetchall() == [], "socios_detalhe has duplicate socio_id"

    def test_identificador_descriptions_resolve(self, test_db: Database) -> None:
        """The sócio-type code resolves to its label for every fixture value."""
        expected = {"1": "Pessoa Jurídica", "2": "Pessoa Física", "3": "Estrangeiro"}
        with test_db.connect().cursor() as cur:
            for codigo, descricao in expected.items():
                cur.execute(
                    "SELECT DISTINCT identificador_de_socio_descricao FROM socios_detalhe "
                    "WHERE identificador_de_socio = %s",
                    (codigo,),
                )
                rows = [r[0] for r in cur.fetchall()]
                assert rows == [descricao], f"identificador {codigo} resolved to {rows!r}"

    def test_faixa_etaria_descriptions_resolve(self, test_db: Database) -> None:
        """Age-band codes resolve to their labels, including '0' (Não se aplica),
        which this recipe keeps as a real value (no nulling - that's socios_clean)."""
        expected = {"0": "Não se aplica", "6": "51 a 60 anos", "9": "Maiores de 80 anos"}
        with test_db.connect().cursor() as cur:
            for codigo, descricao in expected.items():
                cur.execute(
                    "SELECT DISTINCT faixa_etaria_descricao FROM socios_detalhe WHERE faixa_etaria = %s",
                    (codigo,),
                )
                rows = [r[0] for r in cur.fetchall()]
                assert rows == [descricao], f"faixa {codigo} resolved to {rows!r}"

    def test_descriptions_consistent_with_lookups(self, test_db: Database) -> None:
        """Every description column equals the LEFT JOIN of its raw code against
        the source lookup - resolving where the code matches and NULL where it
        does not. One invariant covers correct resolution and unknown -> NULL for
        all five joins at once."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM socios_detalhe sd
                WHERE sd.identificador_de_socio_descricao IS DISTINCT FROM
                        (SELECT descricao FROM identificadores_socio l WHERE l.codigo = sd.identificador_de_socio)
                   OR sd.qualificacao_do_socio_descricao IS DISTINCT FROM
                        (SELECT descricao FROM qualificacoes_socios_enriched l
                         WHERE l.codigo = sd.qualificacao_do_socio)
                   OR sd.pais_descricao IS DISTINCT FROM
                        (SELECT descricao FROM paises_enriched l WHERE l.codigo = sd.pais)
                   OR sd.qualificacao_do_representante_legal_descricao IS DISTINCT FROM
                        (SELECT descricao FROM qualificacoes_socios_enriched l
                         WHERE l.codigo = sd.qualificacao_do_representante_legal)
                   OR sd.faixa_etaria_descricao IS DISTINCT FROM
                        (SELECT descricao FROM faixas_etarias l WHERE l.codigo = sd.faixa_etaria)
            """)
            assert fetch_row(cur)[0] == 0, "a description column diverged from its source lookup"

    def test_unknown_code_keeps_row_with_null_descricao(self, test_db: Database) -> None:
        """An unresolved code keeps the row (LEFT JOIN) with a NULL descricao.
        The fixtures exercise this through sócios whose pais is empty/unmatched."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE NOT EXISTS (SELECT 1 FROM paises_enriched pe WHERE pe.codigo = sd.pais)
                    ) AS unmatched,
                    COUNT(*) FILTER (
                        WHERE NOT EXISTS (SELECT 1 FROM paises_enriched pe WHERE pe.codigo = sd.pais)
                          AND sd.pais_descricao IS NOT NULL
                    ) AS unmatched_with_descricao
                FROM socios_detalhe sd
            """)
            unmatched, unmatched_with_descricao = fetch_row(cur)
        assert unmatched > 0, "fixtures no longer exercise an unmatched pais code"
        assert unmatched_with_descricao == 0, "an unmatched pais resolved to a non-NULL descricao"

    def test_no_value_mutation_of_raw_columns(self, test_db: Database) -> None:
        """Raw source columns pass through verbatim. In particular the
        representante placeholders (qualificacao '00', representante_legal
        '***000000**') stay raw - nulling them is socios_clean's job, not this
        recipe's."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM socios_detalhe sd JOIN socios s USING (socio_id)
                WHERE sd.cnpj_basico IS DISTINCT FROM s.cnpj_basico
                   OR sd.identificador_de_socio IS DISTINCT FROM s.identificador_de_socio
                   OR sd.nome_socio IS DISTINCT FROM s.nome_socio
                   OR sd.cnpj_cpf_do_socio IS DISTINCT FROM s.cnpj_cpf_do_socio
                   OR sd.qualificacao_do_socio IS DISTINCT FROM s.qualificacao_do_socio
                   OR sd.data_entrada_sociedade IS DISTINCT FROM s.data_entrada_sociedade
                   OR sd.pais IS DISTINCT FROM s.pais
                   OR sd.representante_legal IS DISTINCT FROM s.representante_legal
                   OR sd.nome_do_representante IS DISTINCT FROM s.nome_do_representante
                   OR sd.qualificacao_do_representante_legal IS DISTINCT FROM s.qualificacao_do_representante_legal
                   OR sd.faixa_etaria IS DISTINCT FROM s.faixa_etaria
            """)
            assert fetch_row(cur)[0] == 0, "a raw column was mutated"

            # The placeholder qualificacao '00' is present and kept raw (a real
            # description may or may not exist; the point is the code is not nulled).
            cur.execute("SELECT COUNT(*) FROM socios_detalhe WHERE qualificacao_do_representante_legal = '00'")
            assert fetch_row(cur)[0] > 0, "fixtures no longer exercise the representante placeholder '00'"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe drops+recreates without error, same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = count_rows(test_db, "socios_detalhe")
        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()
        count_after = count_rows(test_db, "socios_detalhe")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"
