"""Integration tests using real data fixtures against PostgreSQL.

Requires a running PostgreSQL instance (docker compose up -d postgres).
Skipped automatically in CI if DATABASE_URL is not set.
"""

from pathlib import Path

import psycopg2
import pytest

from database import Database
from processor import process_file

FIXTURES_DIR = Path(__file__).parent / "fixtures"
DATABASE_URL = "postgresql://postgres:postgres@localhost:5435/cnpj_test"

# Processing order (same as main.py — respects FK dependencies)
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
    "empresas": 2004,
    "estabelecimentos": 2004,
    "socios": 2000,
    "dados_simples": 2000,
}

# Crafted fixture rows (cnpj_basico 99000001-99000004) exercise the enriched
# reference-domain recipe: motivo 32 (supplemental), pais 150/994 (supplemental),
# pais 008 (unresolved orphan), qualificacao_responsavel 36 (legacy supplemental).
ENRICHED_SUPPLEMENTAL_MOTIVO_32 = "Inexistente De Fato – Ade/Cosar"


def _pg_available() -> bool:
    """Check if PostgreSQL is reachable."""
    try:
        conn = psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="postgres")
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="PostgreSQL not available")


@pytest.fixture(scope="module")
def test_db():
    """Create a test database, run schema, yield Database, then drop it."""
    # Connect to default db to create test db
    conn = psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS cnpj_test")
        cur.execute("CREATE DATABASE cnpj_test")
    conn.close()

    # Run schema
    conn = psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="cnpj_test")
    conn.autocommit = True
    schema = (Path(__file__).parent.parent / "initial.sql").read_text()
    with conn.cursor() as cur:
        cur.execute(schema)
    conn.close()

    db = Database(DATABASE_URL)

    yield db

    db.disconnect()

    # Drop test db
    conn = psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS cnpj_test")
    conn.close()


def _count_rows(db: Database, table: str) -> int:
    """Count rows in a table."""
    with db.conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


class TestFullPipeline:
    """Test processing all fixture files into PostgreSQL."""

    def test_load_all_fixtures(self, test_db):
        """Process all fixtures in order and verify row counts."""
        for fixture_name in PROCESSING_ORDER:
            fixture_path = FIXTURES_DIR / fixture_name
            assert fixture_path.exists(), f"Missing fixture: {fixture_name}"

            for batch, table_name, columns in process_file(fixture_path, batch_size=500000):
                test_db.bulk_upsert(batch, table_name, columns)

        # Verify row counts (may be less than fixture lines due to PK dedup)
        for table, expected in EXPECTED_COUNTS.items():
            actual = _count_rows(test_db, table)
            assert actual > 0, f"{table} is empty"
            assert actual <= expected, f"{table} has more rows ({actual}) than fixture ({expected})"

    def test_upsert_idempotency(self, test_db):
        """Loading the same data twice should not create duplicates."""
        # Get counts after first load
        counts_before = {table: _count_rows(test_db, table) for table in EXPECTED_COUNTS}

        # Load again
        for fixture_name in PROCESSING_ORDER:
            fixture_path = FIXTURES_DIR / fixture_name
            for batch, table_name, columns in process_file(fixture_path, batch_size=500000):
                test_db.bulk_upsert(batch, table_name, columns)

        # Counts should be identical
        for table, before in counts_before.items():
            after = _count_rows(test_db, table)
            assert after == before, f"{table}: {before} rows before, {after} after (duplicates created)"

    def test_data_integrity(self, test_db):
        """Verify data was loaded correctly — spot check key fields."""
        with test_db.conn.cursor() as cur:
            # CNAE codes should be 7 chars
            cur.execute("SELECT codigo FROM cnaes LIMIT 1")
            codigo = cur.fetchone()[0]
            assert len(codigo) == 7, f"CNAE code wrong length: {codigo}"

            # Country codes should be 3 chars (padded)
            cur.execute("SELECT DISTINCT pais FROM estabelecimentos WHERE pais IS NOT NULL LIMIT 5")
            for (pais,) in cur.fetchall():
                assert len(pais) == 3, f"Country code not padded: {pais}"

            # Capital social should be numeric (not Brazilian format)
            cur.execute("SELECT capital_social FROM empresas WHERE capital_social IS NOT NULL LIMIT 1")
            capital = cur.fetchone()[0]
            assert isinstance(capital, float), f"Capital social not float: {capital}"

            # No '0' or '00000000' dates should exist
            cur.execute("""
                SELECT count(*) FROM estabelecimentos
                WHERE data_situacao_cadastral::text IN ('0', '00000000')
            """)
            assert cur.fetchone()[0] == 0, "Found invalid dates in estabelecimentos"

    def test_replace_strategy(self, test_db):
        """Loading with bulk_insert should truncate and reload cleanly."""
        # Load with replace strategy
        test_db._truncated_tables.clear()
        for fixture_name in PROCESSING_ORDER:
            fixture_path = FIXTURES_DIR / fixture_name
            for batch, table_name, columns in process_file(fixture_path, batch_size=500000):
                test_db.bulk_insert(batch, table_name, columns)

        # Verify data is still there (not empty after truncate)
        for table, expected in EXPECTED_COUNTS.items():
            actual = _count_rows(test_db, table)
            assert actual > 0, f"{table} is empty after replace"
            assert actual <= expected, f"{table} has more rows ({actual}) than fixture ({expected})"

    def test_replace_handles_cross_batch_pk_overlap(self, test_db):
        """bulk_insert must handle PK overlap across batches of the same table.

        RFB occasionally ships the same (cnpj_basico, cnpj_ordem, cnpj_dv)
        across two sharded ZIPs of the same source table. Before the fix,
        the second batch's direct COPY into the target crashed on the PK
        constraint. The fix routes subsequent batches through a temp table
        + ON CONFLICT path so the load completes.

        Simulating cross-batch overlap by calling bulk_insert TWICE on the
        same estabelecimentos fixture - second call must not raise.
        """
        test_db._truncated_tables.clear()
        fixture_path = FIXTURES_DIR / "ESTABELE.csv"
        all_batches = list(process_file(fixture_path, batch_size=500000))
        assert len(all_batches) > 0, "fixture produced no batches"

        # First pass: truncate + load (fast path)
        for batch, table_name, columns in all_batches:
            test_db.bulk_insert(batch, table_name, columns)
        count_after_first = _count_rows(test_db, "estabelecimentos")
        assert count_after_first > 0

        # Second pass: same data, should hit the overlap path. The fix
        # makes this complete without raising; the row count stays stable
        # because every (basico, ordem, dv) already exists.
        for batch, table_name, columns in all_batches:
            test_db.bulk_insert(batch, table_name, columns)
        count_after_second = _count_rows(test_db, "estabelecimentos")
        assert count_after_second == count_after_first, (
            f"cross-batch overlap path changed row count: {count_after_first} -> {count_after_second}"
        )


class TestRecipeReferenceDomainsEnriched:
    """Apply recipes/postgres/reference_domains_enriched.sql against the
    fixture-loaded database and verify the enriched lookups add only verified
    supplemental rows, preserve monthly rows, and carry provenance.

    Runs before TestRecipeEmpresaDetalhe so the enriched tables exist when the
    denormalization recipe LEFT JOINs them (pytest preserves file order).
    """

    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "reference_domains_enriched.sql"

    ENRICHED_TABLES = ("motivos_enriched", "paises_enriched", "qualificacoes_socios_enriched")
    EXPECTED_COLUMNS = {
        "codigo",
        "descricao",
        "source_kind",
        "source_url",
        "is_supplemental",
        "confidence",
        "notes",
    }

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute, creating all three tables."""
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        for table in self.ENRICHED_TABLES:
            with test_db.conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                    (table,),
                )
                assert cur.fetchone() is not None, f"{table} not created"

    def test_provenance_schema(self, test_db):
        """Every enriched table exposes the same provenance columns."""
        for table in self.ENRICHED_TABLES:
            with test_db.conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (table,),
                )
                cols = {row[0] for row in cur.fetchall()}
            assert cols == self.EXPECTED_COLUMNS, f"{table} columns: {cols}"

    def test_monthly_rows_preserved_and_win(self, test_db):
        """Each enriched table is a superset of its monthly lookup: every
        monthly (codigo, descricao) pair is present as a non-supplemental row,
        and supplemental rows never override a monthly codigo."""
        for monthly, enriched in (
            ("motivos", "motivos_enriched"),
            ("paises", "paises_enriched"),
            ("qualificacoes_socios", "qualificacoes_socios_enriched"),
        ):
            with test_db.conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM {monthly} mo
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {enriched} en
                        WHERE en.codigo = mo.codigo
                          AND en.descricao IS NOT DISTINCT FROM mo.descricao
                          AND en.is_supplemental = false
                          AND en.source_kind = 'receita_monthly'
                    )
                    """
                )
                missing = cur.fetchone()[0]
                assert missing == 0, f"{enriched} dropped/altered {missing} monthly rows"

                # No codigo appears more than once (anti-join + PK guarantee).
                cur.execute(f"SELECT codigo, COUNT(*) FROM {enriched} GROUP BY codigo HAVING COUNT(*) > 1")
                assert cur.fetchall() == [], f"{enriched} has duplicate codigo"

                # Supplemental rows must not collide with a monthly codigo.
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM {enriched} en
                    WHERE en.is_supplemental
                      AND EXISTS (SELECT 1 FROM {monthly} mo WHERE mo.codigo = en.codigo)
                    """
                )
                assert cur.fetchone()[0] == 0, f"{enriched} supplemental row overrides a monthly codigo"

    def test_motivo_32_supplemental(self, test_db):
        """motivo 32 is absent from the monthly Motivos delivery but resolves via
        the SERPRO supplemental row, verbatim and flagged supplemental."""
        with test_db.conn.cursor() as cur:
            cur.execute(
                "SELECT descricao, source_kind, is_supplemental, source_url, confidence "
                "FROM motivos_enriched WHERE codigo = '32'"
            )
            row = cur.fetchone()
        assert row is not None, "motivo 32 missing from motivos_enriched"
        descricao, source_kind, is_supplemental, source_url, confidence = row
        assert descricao == ENRICHED_SUPPLEMENTAL_MOTIVO_32, repr(descricao)
        assert source_kind == "serpro_dominio"
        assert is_supplemental is True
        assert source_url and source_url.startswith("https://bcadastros.serpro.gov.br/")
        assert confidence == "high"

    def test_pais_supplemental_codes(self, test_db):
        """The SERPRO-confirmed orphan country codes resolve via supplemental
        rows with their official labels; codes absent from every official table
        stay unresolved (no row)."""
        expected = {
            # 015/042 exercise the zero-padding match: SERPRO stores them as
            # "15"/"42", the pipeline pads pais to "015"/"042".
            "015": "ALAND, ILHAS",
            "042": "ANTÁRTICA",
            "150": "JERSEY, ILHA DO CANAL",
            "151": "CANÁRIAS, ILHAS",
            "200": "CURACAO",
            "321": "GUERNSEY",
            "359": "MAN, ILHA DE",
            "367": "INGLATERRA",
            "393": "JERSEY",
            "449": "MACEDÔNIA, ANT.REP.IUGOSLAVA",
            "498": "MONTENEGRO",
            "578": "PALESTINA",
            "678": "SAINT KITTS E NEVIS",
            "693": "SAO BARTOLOMEU",
            "699": "SÃO MARTINHO, ILHA DE (PARTE HOLANDESA)",
            "737": "SERVIA",
            "755": "SVALBARD E JAN MAYEN",
            "994": "A DESIGNAR",
        }
        with test_db.conn.cursor() as cur:
            for codigo, descricao in expected.items():
                cur.execute(
                    "SELECT descricao, is_supplemental, source_kind FROM paises_enriched WHERE codigo = %s",
                    (codigo,),
                )
                row = cur.fetchone()
                assert row is not None, f"pais {codigo} missing from paises_enriched"
                assert row[0] == descricao, f"pais {codigo}: {row[0]!r}"
                assert row[1] is True and row[2] == "serpro_dominio"

            # Absent from both supplemental sources -> intentionally unresolved.
            for codigo in ("008", "009", "452"):
                cur.execute("SELECT 1 FROM paises_enriched WHERE codigo = %s", (codigo,))
                assert cur.fetchone() is None, f"pais {codigo} should stay unresolved"

    def test_qualificacao_36_legacy_supplement(self, test_db):
        """Code 36 (Gerente-Delegado) is a legacy qualification - documented in
        Receita's open-data table but no longer collected, so it is absent from
        the monthly delivery and resolved via a receita_ods supplemental row. It
        is the only supplemental qualification (nothing else is invented)."""
        with test_db.conn.cursor() as cur:
            cur.execute(
                "SELECT descricao, is_supplemental, source_kind, confidence "
                "FROM qualificacoes_socios_enriched WHERE codigo = '36'"
            )
            row = cur.fetchone()
            assert row is not None, "qualificacao 36 missing from qualificacoes_socios_enriched"
            assert row[0] == "Gerente-Delegado", repr(row[0])
            assert row[1] is True and row[2] == "receita_ods" and row[3] == "high"

            cur.execute("SELECT codigo FROM qualificacoes_socios_enriched WHERE is_supplemental ORDER BY codigo")
            assert [r[0] for r in cur.fetchall()] == ["36"], "only code 36 may be supplemented"

    def test_idempotent(self, test_db):
        """Re-running the recipe drops+recreates without error, same counts."""
        sql = self.RECIPE_PATH.read_text()
        counts_before = {t: _count_rows(test_db, t) for t in self.ENRICHED_TABLES}
        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()
        for table, before in counts_before.items():
            assert _count_rows(test_db, table) == before, f"{table} row count changed on re-run"


class TestRecipeEmpresaDetalhe:
    """Apply recipes/postgres/empresa_detalhe.sql against the fixture-loaded
    database and verify the derived table has the expected shape.

    Depends on test_db being populated by TestFullPipeline; pytest runs
    classes in file order so the fixture state from earlier tests is
    available here. We don't reload fixtures - the database fixture is
    module-scoped and TestFullPipeline.test_load_all_fixtures already
    populated it.
    """

    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "empresa_detalhe.sql"
    ENRICHED_RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "reference_domains_enriched.sql"

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute without errors. It depends on
        the enriched lookups, so apply those first."""
        enriched_sql = self.ENRICHED_RECIPE_PATH.read_text()
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(enriched_sql)
            cur.execute(sql)
        test_db.conn.commit()

        # Table exists
        with test_db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'empresa_detalhe'")
            assert cur.fetchone() is not None, "empresa_detalhe table not created"

    def test_row_count_matches_empresa_estabelecimento_join(self, test_db):
        """The recipe INNER-JOINs empresas with estabelecimentos and then
        LEFT-JOINs reference tables + dados_simples. So the output row count
        must equal the cardinality of empresas JOIN estabelecimentos USING
        (cnpj_basico) - no rows lost to the LEFT JOINs, no rows gained.

        In real prod data this is also = COUNT(*) FROM estabelecimentos
        because every estabelecimento has a parent empresa. The test
        fixtures sample independently so overlap is partial, which is why
        we assert against the JOIN cardinality, not the raw count."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas e
                JOIN estabelecimentos s USING (cnpj_basico)
            """)
            expected = cur.fetchone()[0]
        ed = _count_rows(test_db, "empresa_detalhe")
        assert ed > 0, "empresa_detalhe is empty"
        assert ed == expected, (
            f"empresa_detalhe ({ed}) != empresas⋈estabelecimentos ({expected}) - "
            f"LEFT JOINs on reference tables or dados_simples must be preserving "
            f"all rows from the base inner join"
        )

    def test_cnpj_column_is_concatenation(self, test_db):
        """cnpj column = cnpj_basico || cnpj_ordem || cnpj_dv."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT cnpj, cnpj_basico, cnpj_ordem, cnpj_dv
                FROM empresa_detalhe
                LIMIT 10
            """)
            for cnpj, basico, ordem, dv in cur.fetchall():
                assert cnpj == basico + ordem + dv, f"{cnpj} != {basico}+{ordem}+{dv}"
                assert len(cnpj) == 14, f"cnpj wrong length: {cnpj}"

    def test_reference_descriptions_joined(self, test_db):
        """When a code has a matching reference-table row, the description
        column should be populated."""
        with test_db.conn.cursor() as cur:
            # At least some rows should have all reference descriptions
            cur.execute("""
                SELECT COUNT(*) FROM empresa_detalhe
                WHERE cnae_fiscal_principal_descricao IS NOT NULL
                  AND municipio_nome IS NOT NULL
                  AND natureza_juridica_descricao IS NOT NULL
            """)
            count = cur.fetchone()[0]
            assert count > 0, "No rows have all reference descriptions joined"

    def test_enriched_motivo_description_resolves(self, test_db):
        """The crafted motivo-32 estabelecimento (cnpj_basico 99000001) gets its
        description from the enriched supplemental row, not NULL."""
        with test_db.conn.cursor() as cur:
            cur.execute(
                "SELECT motivo_situacao_cadastral, motivo_situacao_cadastral_descricao "
                "FROM empresa_detalhe WHERE cnpj_basico = '99000001'"
            )
            row = cur.fetchone()
        assert row is not None, "crafted motivo-32 estabelecimento missing"
        assert row[0] == "32"
        assert row[1] == ENRICHED_SUPPLEMENTAL_MOTIVO_32, repr(row[1])

    def test_enriched_pais_resolved_and_unresolved(self, test_db):
        """pais 150/994 resolve via enriched supplements; the spurious 008 code
        stays NULL so the gap stays visible."""
        with test_db.conn.cursor() as cur:
            cur.execute("SELECT pais, pais_descricao FROM empresa_detalhe WHERE cnpj_basico = '99000002'")
            assert cur.fetchone() == ("150", "JERSEY, ILHA DO CANAL")
            cur.execute("SELECT pais, pais_descricao FROM empresa_detalhe WHERE cnpj_basico = '99000004'")
            assert cur.fetchone() == ("994", "A DESIGNAR")
            cur.execute("SELECT pais, pais_descricao FROM empresa_detalhe WHERE cnpj_basico = '99000003'")
            pais, descricao = cur.fetchone()
            assert pais == "008" and descricao is None, "unresolved pais 008 must stay NULL"

    def test_qualificacao_responsavel_descricao(self, test_db):
        """The new qualificacao_responsavel_descricao column resolves codes via
        the enriched lookup, including the legacy code 36 (Gerente-Delegado)."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'empresa_detalhe'
                  AND column_name = 'qualificacao_responsavel_descricao'
            """)
            assert cur.fetchone() is not None, "qualificacao_responsavel_descricao column missing"

            # code 36 is a legacy code, resolved via the receita_ods supplement.
            cur.execute(
                "SELECT qualificacao_responsavel, qualificacao_responsavel_descricao "
                "FROM empresa_detalhe WHERE cnpj_basico = '99000004'"
            )
            assert cur.fetchone() == ("36", "Gerente-Delegado"), "legacy code 36 should resolve"

            # at least some rows resolve to a non-null description.
            cur.execute("SELECT COUNT(*) FROM empresa_detalhe WHERE qualificacao_responsavel_descricao IS NOT NULL")
            assert cur.fetchone()[0] > 0, "no qualificacao_responsavel descriptions resolved"

    def test_dados_simples_columns_present(self, test_db):
        """dados_simples LEFT JOIN should expose raw columns. Some rows may
        have NULL Simples (no record), but the columns must exist."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'empresa_detalhe'
                  AND column_name IN (
                    'opcao_pelo_simples', 'data_opcao_pelo_simples',
                    'data_exclusao_do_simples', 'opcao_pelo_mei',
                    'data_opcao_pelo_mei', 'data_exclusao_do_mei'
                  )
            """)
            cols = {row[0] for row in cur.fetchall()}
            assert cols == {
                "opcao_pelo_simples",
                "data_opcao_pelo_simples",
                "data_exclusao_do_simples",
                "opcao_pelo_mei",
                "data_opcao_pelo_mei",
                "data_exclusao_do_mei",
            }, f"Missing dados_simples columns: {cols}"

    def test_no_derived_columns_leaked(self, test_db):
        """The recipe should NOT add opinionated columns like is_ativa,
        is_matriz, or label-substituted enums (situacao_cadastral stays
        as the source code, not 'Ativa'). Sanity check against scope creep."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'empresa_detalhe'
            """)
            cols = {row[0] for row in cur.fetchall()}
            forbidden = {
                "is_ativa",
                "is_matriz",
                "is_optante_simples",
                "is_mei",
                "situacao_cadastral_descricao",
                "porte_descricao",
                "cnpj_formatado",
                "endereco_completo",
            }
            leaked = cols & forbidden
            assert not leaked, f"Recipe leaked opinionated columns: {leaked}"

    def test_idempotent(self, test_db):
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = _count_rows(test_db, "empresa_detalhe")

        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        count_after = _count_rows(test_db, "empresa_detalhe")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"


class TestRecipeDataQualityFlags:
    """Apply recipes/postgres/data_quality_flags.sql against the fixture-loaded
    database and verify the flag table stays narrow and predicate-driven.
    """

    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "data_quality_flags.sql"
    ENRICHED_RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "reference_domains_enriched.sql"

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute without errors. The enriched
        flags depend on the enriched lookups, so apply those first."""
        enriched_sql = self.ENRICHED_RECIPE_PATH.read_text()
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(enriched_sql)
            cur.execute(sql)
        test_db.conn.commit()

        with test_db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'data_quality_flags'")
            assert cur.fetchone() is not None, "data_quality_flags table not created"

    def test_row_count_matches_empresa_estabelecimento_join(self, test_db):
        """The recipe joins estabelecimentos with empresas, so the output row
        count must match that base join. Reference-table checks happen through
        NOT EXISTS predicates and must not add or drop rows."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas emp
                JOIN estabelecimentos e USING (cnpj_basico)
            """)
            expected = cur.fetchone()[0]

        actual = _count_rows(test_db, "data_quality_flags")
        assert actual > 0, "data_quality_flags is empty"
        assert actual == expected, (
            f"data_quality_flags ({actual}) != empresas⋈estabelecimentos ({expected}) - "
            "flags must preserve the base join cardinality"
        )

    def test_schema_is_narrow_flags_only(self, test_db):
        """The recipe should not duplicate raw source columns. It carries the
        estabelecimento key, cnpj concat, and quality signals only."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'data_quality_flags'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "cnpj",
            "cep_status",
            "is_exterior",
            "pais_lookup_missing",
            "motivo_lookup_missing",
            "pais_enriched_lookup_missing",
            "motivo_enriched_lookup_missing",
            "capital_social_is_suspicious_sentinel",
        }

    def test_cnpj_column_is_concatenation(self, test_db):
        """cnpj column = cnpj_basico || cnpj_ordem || cnpj_dv."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT cnpj, cnpj_basico, cnpj_ordem, cnpj_dv
                FROM data_quality_flags
                LIMIT 10
            """)
            for cnpj, basico, ordem, dv in cur.fetchall():
                assert cnpj == basico + ordem + dv, f"{cnpj} != {basico}+{ordem}+{dv}"
                assert len(cnpj) == 14, f"cnpj wrong length: {cnpj}"

    def test_cep_status_matches_source_predicate(self, test_db):
        """cep_status should be a direct predicate over estabelecimentos.cep."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE dq.cep_status IS DISTINCT FROM (
                    CASE
                        WHEN e.cep IS NULL THEN 'missing'
                        WHEN e.cep = '00000000' THEN 'zero_sentinel'
                        WHEN e.cep ~ '^\\d{8}$' THEN 'valid_shape'
                        ELSE 'malformed'
                    END
                )
            """)
            mismatches = cur.fetchone()[0]
        assert mismatches == 0, f"cep_status mismatches source predicate for {mismatches} rows"

    def test_lookup_flags_match_reference_predicates(self, test_db):
        """Lookup flags should mirror missing-reference predicates."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE dq.pais_lookup_missing IS DISTINCT FROM (
                    e.pais IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = e.pais)
                )
            """)
            pais_mismatches = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE dq.motivo_lookup_missing IS DISTINCT FROM (
                    e.motivo_situacao_cadastral IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM motivos m
                        WHERE m.codigo = e.motivo_situacao_cadastral
                    )
                )
            """)
            motivo_mismatches = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE dq.pais_enriched_lookup_missing IS DISTINCT FROM (
                    e.pais IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM paises_enriched p WHERE p.codigo = e.pais)
                )
                   OR dq.motivo_enriched_lookup_missing IS DISTINCT FROM (
                    e.motivo_situacao_cadastral IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM motivos_enriched m
                        WHERE m.codigo = e.motivo_situacao_cadastral
                    )
                )
            """)
            enriched_mismatches = cur.fetchone()[0]

        assert pais_mismatches == 0, f"pais_lookup_missing mismatches for {pais_mismatches} rows"
        assert motivo_mismatches == 0, f"motivo_lookup_missing mismatches for {motivo_mismatches} rows"
        assert enriched_mismatches == 0, f"enriched lookup-missing flags mismatch for {enriched_mismatches} rows"

    def test_monthly_and_enriched_flags_diverge_on_supplemental(self, test_db):
        """The whole point: a supplemental code (motivo 32, pais 150/994) is
        monthly-missing but enriched-resolved; a spurious code (pais 008) is
        missing under both."""
        with test_db.conn.cursor() as cur:
            # crafted motivo-32 estabelecimento: monthly-missing, enriched-resolved
            cur.execute(
                "SELECT motivo_lookup_missing, motivo_enriched_lookup_missing "
                "FROM data_quality_flags WHERE cnpj_basico = '99000001'"
            )
            assert cur.fetchone() == (True, False), "motivo 32 should be monthly-missing, enriched-resolved"

            # pais 150 and 994: monthly-missing, enriched-resolved
            cur.execute(
                "SELECT pais_lookup_missing, pais_enriched_lookup_missing "
                "FROM data_quality_flags WHERE cnpj_basico IN ('99000002', '99000004') "
                "ORDER BY cnpj_basico"
            )
            assert cur.fetchall() == [(True, False), (True, False)]

            # pais 008: missing under both (spurious, intentionally unresolved)
            cur.execute(
                "SELECT pais_lookup_missing, pais_enriched_lookup_missing "
                "FROM data_quality_flags WHERE cnpj_basico = '99000003'"
            )
            assert cur.fetchone() == (True, True), "spurious pais 008 must stay missing under both"

    def test_company_level_flag_matches_empresas_predicate(self, test_db):
        """capital_social flag should be derived from empresas, not guessed
        from estabelecimento-level columns."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN empresas emp USING (cnpj_basico)
                WHERE dq.capital_social_is_suspicious_sentinel
                    IS DISTINCT FROM (emp.capital_social = 999999999999)
            """)
            mismatches = cur.fetchone()[0]

        assert mismatches == 0, f"capital sentinel flag mismatches for {mismatches} rows"

    def test_idempotent(self, test_db):
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = _count_rows(test_db, "data_quality_flags")

        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        count_after = _count_rows(test_db, "data_quality_flags")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"


class TestRecipeEstabelecimentosClean:
    """Apply recipes/postgres/estabelecimentos_clean.sql and verify it uses
    data_quality_flags as the single source of truth for interpretation.
    """

    FLAGS_RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "data_quality_flags.sql"
    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "estabelecimentos_clean.sql"

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute after data_quality_flags."""
        flags_sql = self.FLAGS_RECIPE_PATH.read_text()
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(flags_sql)
            cur.execute(sql)
        test_db.conn.commit()

        with test_db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'estabelecimentos_clean'")
            assert cur.fetchone() is not None, "estabelecimentos_clean table not created"

    def test_row_count_matches_flags(self, test_db):
        """The recipe is one row per data_quality_flags row."""
        flags = _count_rows(test_db, "data_quality_flags")
        clean = _count_rows(test_db, "estabelecimentos_clean")
        assert clean > 0, "estabelecimentos_clean is empty"
        assert clean == flags, f"estabelecimentos_clean ({clean}) != data_quality_flags ({flags})"

    def test_schema_preserves_raw_and_clean_pairs(self, test_db):
        """The table exposes raw + clean values and passes through flags,
        without becoming an empresa_detalhe replacement."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'estabelecimentos_clean'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "cnpj",
            "cep_raw",
            "cep_clean",
            "capital_social_raw",
            "capital_social_clean",
            "cep_status",
            "capital_social_is_suspicious_sentinel",
            "pais_lookup_missing",
            "motivo_lookup_missing",
            "is_exterior",
        }

    def test_flags_are_passed_through_verbatim(self, test_db):
        """Flag columns should be direct copies from data_quality_flags."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM estabelecimentos_clean ec
                JOIN data_quality_flags f USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE ec.cep_status IS DISTINCT FROM f.cep_status
                   OR ec.capital_social_is_suspicious_sentinel IS DISTINCT FROM
                        f.capital_social_is_suspicious_sentinel
                   OR ec.pais_lookup_missing IS DISTINCT FROM f.pais_lookup_missing
                   OR ec.motivo_lookup_missing IS DISTINCT FROM f.motivo_lookup_missing
                   OR ec.is_exterior IS DISTINCT FROM f.is_exterior
            """)
            mismatches = cur.fetchone()[0]

        assert mismatches == 0, f"flag pass-through mismatches for {mismatches} rows"

    def test_clean_values_use_flags_only(self, test_db):
        """Clean columns should use the materialized flags rather than
        duplicating source predicates in a second place."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM estabelecimentos_clean ec
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                JOIN empresas emp USING (cnpj_basico)
                JOIN data_quality_flags f USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE ec.cep_raw IS DISTINCT FROM e.cep
                   OR ec.capital_social_raw IS DISTINCT FROM emp.capital_social
                   OR ec.cep_clean IS DISTINCT FROM (
                        CASE WHEN f.cep_status = 'valid_shape' THEN e.cep ELSE NULL END
                   )
                   OR ec.capital_social_clean IS DISTINCT FROM (
                        CASE
                            WHEN f.capital_social_is_suspicious_sentinel THEN NULL
                            ELSE emp.capital_social
                        END
                   )
            """)
            mismatches = cur.fetchone()[0]

        assert mismatches == 0, f"clean values mismatch flag predicates for {mismatches} rows"

    def test_no_reference_or_label_columns(self, test_db):
        """This recipe should not grow into empresa_detalhe. Reference-table
        descriptions and labels belong in other recipes."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'estabelecimentos_clean'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "razao_social",
            "nome_fantasia",
            "cnae_fiscal_principal_descricao",
            "municipio_nome",
            "situacao_cadastral_descricao",
            "porte_descricao",
            "endereco_completo",
            "is_ativa",
            "is_matriz",
        }
        leaked = cols & forbidden
        assert not leaked, f"estabelecimentos_clean leaked unrelated columns: {leaked}"

    def test_idempotent(self, test_db):
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = _count_rows(test_db, "estabelecimentos_clean")

        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        count_after = _count_rows(test_db, "estabelecimentos_clean")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"


class TestRecipeCnaeSecundariaExploded:
    """Apply recipes/postgres/cnae_secundaria_exploded.sql and verify it
    explodes secondary CNAEs without adding interpretation.
    """

    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "cnae_secundaria_exploded.sql"

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute without errors."""
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        with test_db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'cnae_secundaria_exploded'")
            assert cur.fetchone() is not None, "cnae_secundaria_exploded table not created"

    def test_row_count_matches_string_split_cardinality(self, test_db):
        """Output cardinality should match the trimmed non-empty entries from
        cnae_fiscal_secundaria. No rows are gained through joins."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM estabelecimentos e
                CROSS JOIN LATERAL unnest(string_to_array(e.cnae_fiscal_secundaria, ',')) AS code(raw_code)
                WHERE e.cnae_fiscal_secundaria IS NOT NULL
                  AND trim(code.raw_code) <> ''
            """)
            expected = cur.fetchone()[0]

        actual = _count_rows(test_db, "cnae_secundaria_exploded")
        assert actual > 0, "cnae_secundaria_exploded is empty"
        assert actual == expected, f"cnae_secundaria_exploded ({actual}) != split cardinality ({expected})"

    def test_schema_is_side_table_only(self, test_db):
        """The recipe should stay as key + cnae_codigo. No descriptions,
        validation flags, or position column in v1."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'cnae_secundaria_exploded'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "cnpj",
            "cnae_codigo",
        }

    def test_cnpj_column_is_concatenation(self, test_db):
        """cnpj column = cnpj_basico || cnpj_ordem || cnpj_dv."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT cnpj, cnpj_basico, cnpj_ordem, cnpj_dv
                FROM cnae_secundaria_exploded
                LIMIT 10
            """)
            for cnpj, basico, ordem, dv in cur.fetchall():
                assert cnpj == basico + ordem + dv, f"{cnpj} != {basico}+{ordem}+{dv}"
                assert len(cnpj) == 14, f"cnpj wrong length: {cnpj}"

    def test_codes_are_trimmed_and_non_empty(self, test_db):
        """The only normalization in the recipe is trimming whitespace and
        dropping empty split entries."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM cnae_secundaria_exploded
                WHERE cnae_codigo IS NULL
                   OR cnae_codigo = ''
                   OR cnae_codigo <> trim(cnae_codigo)
            """)
            bad_rows = cur.fetchone()[0]

        assert bad_rows == 0, f"Found {bad_rows} untrimmed or empty CNAE rows"

    def test_no_reference_join_or_description_columns(self, test_db):
        """Consumers LEFT JOIN cnaes themselves when they want descriptions.
        The recipe should preserve codes only."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'cnae_secundaria_exploded'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "descricao",
            "cnae_descricao",
            "cnae_fiscal_secundaria_descricao",
            "cnae_lookup_missing",
            "position",
            "ordinality",
        }
        leaked = cols & forbidden
        assert not leaked, f"cnae_secundaria_exploded leaked interpretation columns: {leaked}"

    def test_idempotent(self, test_db):
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = _count_rows(test_db, "cnae_secundaria_exploded")

        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        count_after = _count_rows(test_db, "cnae_secundaria_exploded")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"


class TestRecipeSociosQualityFlags:
    """Apply recipes/postgres/socios_quality_flags.sql and verify it
    materializes socio-level quality predicates without mutating values.
    """

    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "socios_quality_flags.sql"
    ENRICHED_RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "reference_domains_enriched.sql"

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute without errors. The enriched
        flags depend on the enriched lookups, so apply those first."""
        enriched_sql = self.ENRICHED_RECIPE_PATH.read_text()
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(enriched_sql)
            cur.execute(sql)
        test_db.conn.commit()

        with test_db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'socios_quality_flags'")
            assert cur.fetchone() is not None, "socios_quality_flags table not created"

    def test_row_count_matches_socios(self, test_db):
        """The recipe is one row per socios row."""
        socios = _count_rows(test_db, "socios")
        flags = _count_rows(test_db, "socios_quality_flags")
        assert flags > 0, "socios_quality_flags is empty"
        assert flags == socios, f"socios_quality_flags ({flags}) != socios ({socios})"

    def test_schema_is_narrow_flags_only(self, test_db):
        """The recipe should carry the source key plus quality signals only."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'socios_quality_flags'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "socio_id",
            "cnpj_basico",
            "identificador_de_socio",
            "cnpj_cpf_do_socio",
            "representante_is_placeholder",
            "pais_lookup_missing",
            "qualificacao_socio_lookup_missing",
            "qualificacao_representante_lookup_missing",
            "pais_enriched_lookup_missing",
            "qualificacao_socio_enriched_lookup_missing",
            "qualificacao_representante_enriched_lookup_missing",
            "faixa_etaria_nao_se_aplica",
        }

    def test_flags_match_source_predicates(self, test_db):
        """Flags should mirror direct predicates over socios and reference
        tables. No interpretation should be duplicated elsewhere."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM socios_quality_flags f
                JOIN socios s USING (socio_id)
                WHERE f.representante_is_placeholder IS DISTINCT FROM (
                    s.representante_legal = '***000000**'
                    AND s.qualificacao_do_representante_legal = '00'
                )
                   OR f.pais_lookup_missing IS DISTINCT FROM (
                    s.pais IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = s.pais)
                )
                   OR f.qualificacao_socio_lookup_missing IS DISTINCT FROM (
                    s.qualificacao_do_socio IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM qualificacoes_socios q
                        WHERE q.codigo = s.qualificacao_do_socio
                    )
                )
                   OR f.qualificacao_representante_lookup_missing IS DISTINCT FROM (
                    s.qualificacao_do_representante_legal IS NOT NULL
                    AND s.qualificacao_do_representante_legal <> '00'
                    AND NOT EXISTS (
                        SELECT 1 FROM qualificacoes_socios q
                        WHERE q.codigo = s.qualificacao_do_representante_legal
                    )
                )
                   OR f.faixa_etaria_nao_se_aplica IS DISTINCT FROM (s.faixa_etaria = '0')
            """)
            mismatches = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM socios_quality_flags f
                JOIN socios s USING (socio_id)
                WHERE f.pais_enriched_lookup_missing IS DISTINCT FROM (
                    s.pais IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM paises_enriched p WHERE p.codigo = s.pais)
                )
                   OR f.qualificacao_socio_enriched_lookup_missing IS DISTINCT FROM (
                    s.qualificacao_do_socio IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM qualificacoes_socios_enriched q
                        WHERE q.codigo = s.qualificacao_do_socio
                    )
                )
                   OR f.qualificacao_representante_enriched_lookup_missing IS DISTINCT FROM (
                    s.qualificacao_do_representante_legal IS NOT NULL
                    AND s.qualificacao_do_representante_legal <> '00'
                    AND NOT EXISTS (
                        SELECT 1 FROM qualificacoes_socios_enriched q
                        WHERE q.codigo = s.qualificacao_do_representante_legal
                    )
                )
            """)
            enriched_mismatches = cur.fetchone()[0]

        assert mismatches == 0, f"socios_quality_flags predicate mismatches for {mismatches} rows"
        assert enriched_mismatches == 0, f"socios enriched predicate mismatches for {enriched_mismatches} rows"

    def test_no_raw_or_clean_columns(self, test_db):
        """This is a flags table, not socios_clean or socios_detalhe."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'socios_quality_flags'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "nome_socio",
            "representante_legal",
            "representante_legal_raw",
            "representante_legal_clean",
            "nome_do_representante",
            "qualificacao_do_socio_descricao",
            "qualificacao_do_representante_legal_descricao",
            "pais_descricao",
            "faixa_etaria_descricao",
        }
        leaked = cols & forbidden
        assert not leaked, f"socios_quality_flags leaked non-flag columns: {leaked}"

    def test_idempotent(self, test_db):
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = _count_rows(test_db, "socios_quality_flags")

        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        count_after = _count_rows(test_db, "socios_quality_flags")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"


class TestRecipeSociosClean:
    """Apply recipes/postgres/socios_clean.sql and verify it uses
    socios_quality_flags as the single source of truth for interpretation.
    """

    FLAGS_RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "socios_quality_flags.sql"
    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "socios_clean.sql"

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute after socios_quality_flags."""
        flags_sql = self.FLAGS_RECIPE_PATH.read_text()
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(flags_sql)
            cur.execute(sql)
        test_db.conn.commit()

        with test_db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'socios_clean'")
            assert cur.fetchone() is not None, "socios_clean table not created"

    def test_row_count_matches_flags(self, test_db):
        """The recipe is one row per socios_quality_flags row."""
        flags = _count_rows(test_db, "socios_quality_flags")
        clean = _count_rows(test_db, "socios_clean")
        assert clean > 0, "socios_clean is empty"
        assert clean == flags, f"socios_clean ({clean}) != socios_quality_flags ({flags})"

    def test_schema_preserves_raw_and_clean_pairs(self, test_db):
        """The table exposes raw + clean values and passes through flags,
        without becoming socios_detalhe."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'socios_clean'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "socio_id",
            "cnpj_basico",
            "identificador_de_socio",
            "cnpj_cpf_do_socio",
            "representante_legal_raw",
            "representante_legal_clean",
            "nome_do_representante_raw",
            "nome_do_representante_clean",
            "qualificacao_do_representante_legal_raw",
            "qualificacao_do_representante_legal_clean",
            "faixa_etaria_raw",
            "faixa_etaria_clean",
            "representante_is_placeholder",
            "pais_lookup_missing",
            "qualificacao_socio_lookup_missing",
            "qualificacao_representante_lookup_missing",
            "faixa_etaria_nao_se_aplica",
        }

    def test_flags_are_passed_through_verbatim(self, test_db):
        """Flag columns should be direct copies from socios_quality_flags."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM socios_clean sc
                JOIN socios_quality_flags f USING (socio_id)
                WHERE sc.representante_is_placeholder IS DISTINCT FROM f.representante_is_placeholder
                   OR sc.pais_lookup_missing IS DISTINCT FROM f.pais_lookup_missing
                   OR sc.qualificacao_socio_lookup_missing IS DISTINCT FROM f.qualificacao_socio_lookup_missing
                   OR sc.qualificacao_representante_lookup_missing IS DISTINCT FROM
                        f.qualificacao_representante_lookup_missing
                   OR sc.faixa_etaria_nao_se_aplica IS DISTINCT FROM f.faixa_etaria_nao_se_aplica
            """)
            mismatches = cur.fetchone()[0]

        assert mismatches == 0, f"flag pass-through mismatches for {mismatches} rows"

    def test_clean_values_use_flags_only(self, test_db):
        """Clean columns should use the materialized flags rather than
        duplicating source predicates in a second place."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM socios_clean sc
                JOIN socios s USING (socio_id)
                JOIN socios_quality_flags f USING (socio_id)
                WHERE sc.representante_legal_raw IS DISTINCT FROM s.representante_legal
                   OR sc.nome_do_representante_raw IS DISTINCT FROM s.nome_do_representante
                   OR sc.qualificacao_do_representante_legal_raw IS DISTINCT FROM
                        s.qualificacao_do_representante_legal
                   OR sc.faixa_etaria_raw IS DISTINCT FROM s.faixa_etaria
                   OR sc.representante_legal_clean IS DISTINCT FROM (
                        CASE WHEN f.representante_is_placeholder THEN NULL ELSE s.representante_legal END
                   )
                   OR sc.nome_do_representante_clean IS DISTINCT FROM (
                        CASE WHEN f.representante_is_placeholder THEN NULL ELSE s.nome_do_representante END
                   )
                   OR sc.qualificacao_do_representante_legal_clean IS DISTINCT FROM (
                        CASE
                            WHEN f.representante_is_placeholder THEN NULL
                            ELSE s.qualificacao_do_representante_legal
                        END
                   )
                   OR sc.faixa_etaria_clean IS DISTINCT FROM (
                        CASE WHEN f.faixa_etaria_nao_se_aplica THEN NULL ELSE s.faixa_etaria END
                   )
            """)
            mismatches = cur.fetchone()[0]

        assert mismatches == 0, f"clean values mismatch flag predicates for {mismatches} rows"

    def test_no_reference_or_label_columns(self, test_db):
        """This recipe should not grow into socios_detalhe. Reference-table
        descriptions and labels belong in other recipes."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'socios_clean'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "nome_socio",
            "identificador_de_socio_descricao",
            "qualificacao_do_socio_descricao",
            "qualificacao_do_representante_legal_descricao",
            "pais_descricao",
            "faixa_etaria_descricao",
            "socio_type",
            "is_representante_pj",
        }
        leaked = cols & forbidden
        assert not leaked, f"socios_clean leaked unrelated columns: {leaked}"

    def test_idempotent(self, test_db):
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = _count_rows(test_db, "socios_clean")

        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        count_after = _count_rows(test_db, "socios_clean")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"


class TestRecipeEmpresasBuscaNome:
    """Apply recipes/postgres/empresas_busca_nome.sql against the fixture-loaded
    database and verify the search-serving table has the expected shape.

    Depends on test_db being populated by TestFullPipeline; pytest runs
    classes in file order so the fixture state from earlier tests is
    available here.
    """

    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "empresas_busca_nome.sql"

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute without errors."""
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        with test_db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'empresas_busca_nome'")
            assert cur.fetchone() is not None, "empresas_busca_nome table not created"

    def test_row_count_matches_active_matriz_join(self, test_db):
        """Row count must equal empresas JOIN estabelecimentos USING (cnpj_basico)
        filtered to active matriz. LEFT JOINs on cnaes and municipios must
        not add or drop rows."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas e
                JOIN estabelecimentos est USING (cnpj_basico)
                WHERE est.situacao_cadastral = '02'
                  AND est.identificador_matriz_filial = 1
            """)
            expected = cur.fetchone()[0]

        actual = _count_rows(test_db, "empresas_busca_nome")
        assert actual > 0, "empresas_busca_nome is empty - fixture has no active-matriz overlap"
        assert actual == expected, (
            f"empresas_busca_nome ({actual}) != active-matriz join ({expected}) - "
            "reference LEFT JOINs on cnaes/municipios must preserve all rows"
        )

    def test_only_active_matriz_rows(self, test_db):
        """Every row must satisfy the filter predicate. Guards against the
        WHERE clause being weakened or against the type comparison drifting
        (identificador_matriz_filial is INTEGER, situacao_cadastral is text)."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas_busca_nome
                WHERE situacao_cadastral <> '02'
                   OR identificador_matriz_filial <> 1
            """)
            violations = cur.fetchone()[0]
        assert violations == 0, f"{violations} rows violate the active-matriz predicate"

    def test_cnpj_column_is_concatenation(self, test_db):
        """cnpj column = cnpj_basico || cnpj_ordem || cnpj_dv, same convention
        as empresa_detalhe."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT cnpj, cnpj_basico, cnpj_ordem, cnpj_dv
                FROM empresas_busca_nome
                LIMIT 10
            """)
            rows = cur.fetchall()
            assert rows, "no rows to verify cnpj concatenation"
            for cnpj, basico, ordem, dv in rows:
                assert cnpj == basico + ordem + dv, f"{cnpj} != {basico}+{ordem}+{dv}"
                assert len(cnpj) == 14, f"cnpj wrong length: {cnpj}"

    def test_reference_descriptions_joined(self, test_db):
        """When estabelecimento codes have matching reference rows the
        denormalized description columns should be populated."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas_busca_nome
                WHERE cnae_descricao IS NOT NULL
                  AND municipio_nome IS NOT NULL
            """)
            count = cur.fetchone()[0]
            assert count > 0, "no rows have both cnae_descricao and municipio_nome joined"

    def test_expected_indexes_exist(self, test_db):
        """The composite indexes that justify the recipe must all be present.
        If one is dropped or renamed, the recipe no longer provides the
        documented access path."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'empresas_busca_nome'
            """)
            indexes = {row[0] for row in cur.fetchall()}

        expected = {
            "pk_empresas_busca_nome",
            "idx_empresas_busca_nome_cnpj",
            "idx_empresas_busca_nome_razao_prefix",
            "idx_empresas_busca_nome_uf_razao",
            "idx_empresas_busca_nome_uf_municipio_razao",
            "idx_empresas_busca_nome_uf_cnae_razao",
        }
        missing = expected - indexes
        assert not missing, f"missing expected indexes: {missing}"

    def test_no_derived_columns_leaked(self, test_db):
        """The recipe filters rows but does not synthesize labels or
        booleans. Source codes stay; no is_ativa, no situacao_cadastral_descricao."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'empresas_busca_nome'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "is_ativa",
            "is_matriz",
            "situacao_cadastral_descricao",
            "identificador_matriz_filial_descricao",
        }
        leaked = cols & forbidden
        assert not leaked, f"recipe leaked opinionated columns: {leaked}"

    def test_idempotent(self, test_db):
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = _count_rows(test_db, "empresas_busca_nome")

        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        count_after = _count_rows(test_db, "empresas_busca_nome")
        assert count_before == count_after, f"re-running recipe changed row count: {count_before} -> {count_after}"


class TestRecipeEmpresasBuscaNomeCounts:
    """Apply recipes/postgres/empresas_busca_nome_counts.sql against the
    fixture-loaded database and verify the rollup table has the expected
    shape and exact totals.

    Depends on empresas_busca_nome being built first (TestRecipeEmpresasBuscaNome
    runs before this class because pytest preserves file order).
    """

    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "empresas_busca_nome_counts.sql"

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute without errors."""
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        with test_db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'empresas_busca_nome_counts'")
            assert cur.fetchone() is not None, "empresas_busca_nome_counts table not created"

    def test_three_kinds_present(self, test_db):
        """The rollup carries exactly three kinds: uf, uf_municipio, uf_cnae."""
        with test_db.conn.cursor() as cur:
            cur.execute("SELECT DISTINCT kind FROM empresas_busca_nome_counts ORDER BY kind")
            kinds = [row[0] for row in cur.fetchall()]
        assert kinds == ["uf", "uf_cnae", "uf_municipio"], kinds

    def test_sums_equal_main_table_per_kind(self, test_db):
        """Each kind's totals must sum to the row count of the source
        table. If they diverge, the GROUP BY missed a row or the source
        changed between the build of the main recipe and the counts."""
        with test_db.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM empresas_busca_nome")
            base = cur.fetchone()[0]

            cur.execute("SELECT kind, SUM(total) FROM empresas_busca_nome_counts GROUP BY kind")
            per_kind = dict(cur.fetchall())

        # SUM is returned as Decimal by psycopg2; coerce to int for the
        # equality comparison.
        for kind in ("uf", "uf_municipio", "uf_cnae"):
            assert int(per_kind[kind]) == base, f"{kind} sums to {per_kind[kind]}, expected {base}"

    def test_uf_municipio_has_both_codigo_and_nome(self, test_db):
        """kind='uf_municipio' rows carry both municipio_codigo and
        municipio_nome. Consumers using either as the join key should get
        the same row."""
        with test_db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM empresas_busca_nome_counts
                WHERE kind = 'uf_municipio'
                  AND (municipio_codigo IS NULL OR municipio_nome IS NULL)
                """
            )
            missing = cur.fetchone()[0]
        assert missing == 0, f"{missing} uf_municipio rows missing codigo or nome"

    def test_lookup_by_name_and_by_codigo_agree(self, test_db):
        """Looking up the same bucket via municipio_nome or municipio_codigo
        must return the same total."""
        with test_db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT uf, municipio_codigo, municipio_nome, total
                FROM empresas_busca_nome_counts
                WHERE kind = 'uf_municipio'
                ORDER BY total DESC
                LIMIT 5
                """
            )
            samples = cur.fetchall()
            assert samples, "no uf_municipio rows to verify"

            for uf, codigo, nome, total in samples:
                cur.execute(
                    "SELECT total FROM empresas_busca_nome_counts "
                    "WHERE kind='uf_municipio' AND uf=%s AND municipio_codigo=%s",
                    (uf, codigo),
                )
                by_codigo = cur.fetchone()[0]
                cur.execute(
                    "SELECT total FROM empresas_busca_nome_counts "
                    "WHERE kind='uf_municipio' AND uf=%s AND municipio_nome=%s",
                    (uf, nome),
                )
                by_nome = cur.fetchone()[0]
                assert by_codigo == by_nome == total, (
                    f"divergent totals for ({uf}, {codigo}, {nome}): "
                    f"by_codigo={by_codigo} by_nome={by_nome} stored={total}"
                )

    def test_partial_unique_indexes_present(self, test_db):
        """Every documented lookup pattern needs its partial unique index.
        Missing any of these means the lookup falls back to a sequential
        scan, defeating the rollup's purpose."""
        with test_db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'empresas_busca_nome_counts'
                """
            )
            indexes = {row[0] for row in cur.fetchall()}

        expected = {
            "idx_empresas_busca_nome_counts_uf",
            "idx_empresas_busca_nome_counts_uf_municipio",
            "idx_empresas_busca_nome_counts_uf_municipio_codigo",
            "idx_empresas_busca_nome_counts_uf_cnae",
        }
        missing = expected - indexes
        assert not missing, f"missing expected indexes: {missing}"

    def test_idempotent(self, test_db):
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = _count_rows(test_db, "empresas_busca_nome_counts")

        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        count_after = _count_rows(test_db, "empresas_busca_nome_counts")
        assert count_before == count_after, f"re-running recipe changed row count: {count_before} -> {count_after}"


class TestDataQualityReportMeasurements:
    """Exercise scripts/data_quality_report.py orphan + enriched-coverage
    measurements against the fixture-loaded database. The crafted fixture rows
    (motivo 32, pais 150/008/994, qualificacao_responsavel 36) make the
    monthly-vs-enriched gap observable.
    """

    ENRICHED_RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "reference_domains_enriched.sql"

    @staticmethod
    def _report_module():
        import sys

        scripts_dir = str(Path(__file__).parent.parent / "scripts")
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        import data_quality_report

        return data_quality_report

    def _ensure_enriched(self, test_db):
        with test_db.conn.cursor() as cur:
            cur.execute(self.ENRICHED_RECIPE_PATH.read_text())
        test_db.conn.commit()

    def test_new_orphan_checks_present_and_detect_code_36(self, test_db):
        """The orphan report now covers qualificacao_responsavel and the socios
        relationships. Code 36 (unverified) is a real orphan and must surface."""
        dqr = self._report_module()
        results = {r["label"]: r["orphans"] for r in dqr.measure_orphan_fks(test_db.conn)}

        for label in (
            "empresas.qualificacao_responsavel ∉ qualificacoes_socios",
            "socios.pais ∉ paises",
            "socios.qualificacao_do_socio ∉ qualificacoes_socios",
            "socios.qualificacao_do_representante_legal ∉ qualificacoes_socios (≠ '00')",
        ):
            assert label in results, f"missing orphan check: {label}"

        # crafted empresa 99000004 carries qualificacao_responsavel '36'
        assert results["empresas.qualificacao_responsavel ∉ qualificacoes_socios"] >= 1

    def test_enriched_coverage_shows_gap_closed(self, test_db):
        """Enriched coverage reports monthly vs enriched orphans. Supplemental
        codes close the gap: motivo 32, pais 150/994, and qualificacao 36
        (the legacy Gerente-Delegado code, carried by fixture empresa 99000004)."""
        self._ensure_enriched(test_db)
        dqr = self._report_module()
        result = dqr.measure_enriched_orphans(test_db.conn)
        assert result["available"] is True
        rows = {r["label"]: r for r in result["rows"]}

        motivo = rows["estabelecimentos.motivo_situacao_cadastral"]
        assert motivo["monthly_orphans"] > motivo["enriched_orphans"], "motivo 32 should close the gap"

        pais = rows["estabelecimentos.pais"]
        assert pais["monthly_orphans"] > pais["enriched_orphans"], "pais 150/994 should close the gap"
        assert pais["enriched_orphans"] >= 1, "spurious pais 008 must remain unresolved"

        qual = rows["empresas.qualificacao_responsavel"]
        assert qual["monthly_orphans"] > qual["enriched_orphans"], "legacy code 36 should close the gap"

    def test_enriched_coverage_absent_when_tables_missing(self, test_db):
        """When the enriched tables do not exist, the measurement degrades to
        available=False instead of erroring."""
        with test_db.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS motivos_enriched")
            cur.execute("DROP TABLE IF EXISTS paises_enriched")
            cur.execute("DROP TABLE IF EXISTS qualificacoes_socios_enriched")
        test_db.conn.commit()

        dqr = self._report_module()
        result = dqr.measure_enriched_orphans(test_db.conn)
        assert result == {"available": False, "rows": []}

        # restore for any later test that expects them
        self._ensure_enriched(test_db)
