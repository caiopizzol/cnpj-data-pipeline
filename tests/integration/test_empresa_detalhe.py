import pytest

from database import Database
from tests.integration.support import (
    ENRICHED_SUPPLEMENTAL_MOTIVO_32,
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("empresa_detalhe")
class TestRecipeEmpresaDetalhe:
    """Verify empresa detalhe against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "empresa_detalhe.sql"

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute without errors. It depends on
        the enriched lookups and the static domain-label tables, so apply those
        first."""

        # Table exists
        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'empresa_detalhe'")
            assert cur.fetchone() is not None, "empresa_detalhe table not created"

    def test_row_count_matches_empresa_estabelecimento_join(self, test_db: Database) -> None:
        """The recipe INNER-JOINs empresas with estabelecimentos and then
        LEFT-JOINs reference tables + dados_simples. So the output row count
        must equal the cardinality of empresas JOIN estabelecimentos USING
        (cnpj_basico) - no rows lost to the LEFT JOINs, no rows gained.

        In real prod data this is also = COUNT(*) FROM estabelecimentos
        because every estabelecimento has a parent empresa. The test
        fixtures sample independently so overlap is partial, which is why
        we assert against the JOIN cardinality, not the raw count."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas e
                JOIN estabelecimentos s USING (cnpj_basico)
            """)
            expected = fetch_row(cur)[0]
        ed = count_rows(test_db, "empresa_detalhe")
        assert ed > 0, "empresa_detalhe is empty"
        assert ed == expected, (
            f"empresa_detalhe ({ed}) != empresas⋈estabelecimentos ({expected}) - "
            f"LEFT JOINs on reference tables or dados_simples must be preserving "
            f"all rows from the base inner join"
        )

    def test_cnpj_column_is_concatenation(self, test_db: Database) -> None:
        """cnpj column = cnpj_basico || cnpj_ordem || cnpj_dv."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT cnpj, cnpj_basico, cnpj_ordem, cnpj_dv
                FROM empresa_detalhe
                LIMIT 10
            """)
            for cnpj, basico, ordem, dv in cur.fetchall():
                assert cnpj == basico + ordem + dv, f"{cnpj} != {basico}+{ordem}+{dv}"
                assert len(cnpj) == 14, f"cnpj wrong length: {cnpj}"

    def test_reference_descriptions_joined(self, test_db: Database) -> None:
        """When a code has a matching reference-table row, the description
        column should be populated."""
        with test_db.connect().cursor() as cur:
            # At least some rows should have all reference descriptions
            cur.execute("""
                SELECT COUNT(*) FROM empresa_detalhe
                WHERE cnae_fiscal_principal_descricao IS NOT NULL
                  AND municipio_nome IS NOT NULL
                  AND natureza_juridica_descricao IS NOT NULL
            """)
            count = fetch_row(cur)[0]
            assert count > 0, "No rows have all reference descriptions joined"

    def test_enriched_motivo_description_resolves(self, test_db: Database) -> None:
        """The crafted motivo-32 estabelecimento (cnpj_basico 99000001) gets its
        description from the enriched supplemental row, not NULL."""
        with test_db.connect().cursor() as cur:
            cur.execute(
                "SELECT motivo_situacao_cadastral, motivo_situacao_cadastral_descricao "
                "FROM empresa_detalhe WHERE cnpj_basico = '99000001'"
            )
            row = fetch_row(cur)
        assert row is not None, "crafted motivo-32 estabelecimento missing"
        assert row[0] == "32"
        assert row[1] == ENRICHED_SUPPLEMENTAL_MOTIVO_32, repr(row[1])

    def test_enriched_pais_resolved_and_unresolved(self, test_db: Database) -> None:
        """pais 150/994 resolve via enriched supplements; the spurious 008 code
        stays NULL so the gap stays visible."""
        with test_db.connect().cursor() as cur:
            cur.execute("SELECT pais, pais_descricao FROM empresa_detalhe WHERE cnpj_basico = '99000002'")
            assert fetch_row(cur) == ("150", "JERSEY, ILHA DO CANAL")
            cur.execute("SELECT pais, pais_descricao FROM empresa_detalhe WHERE cnpj_basico = '99000004'")
            assert fetch_row(cur) == ("994", "A DESIGNAR")
            cur.execute("SELECT pais, pais_descricao FROM empresa_detalhe WHERE cnpj_basico = '99000003'")
            pais, descricao = fetch_row(cur)
            assert pais == "008" and descricao is None, "unresolved pais 008 must stay NULL"

    def test_qualificacao_responsavel_descricao(self, test_db: Database) -> None:
        """The new qualificacao_responsavel_descricao column resolves codes via
        the enriched lookup, including the legacy code 36 (Gerente-Delegado)."""
        with test_db.connect().cursor() as cur:
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
            assert fetch_row(cur) == ("36", "Gerente-Delegado"), "legacy code 36 should resolve"

            # at least some rows resolve to a non-null description.
            cur.execute("SELECT COUNT(*) FROM empresa_detalhe WHERE qualificacao_responsavel_descricao IS NOT NULL")
            assert fetch_row(cur)[0] > 0, "no qualificacao_responsavel descriptions resolved"

    def test_dados_simples_columns_present(self, test_db: Database) -> None:
        """dados_simples LEFT JOIN should expose raw columns. Some rows may
        have NULL Simples (no record), but the columns must exist."""
        with test_db.connect().cursor() as cur:
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

    def test_no_derived_columns_leaked(self, test_db: Database) -> None:
        """The recipe should NOT add opinionated columns like is_ativa or
        is_matriz, and must not SUBSTITUTE source codes with labels: the raw
        code columns (situacao_cadastral, porte) stay as codes. Adding a
        parallel *_descricao column alongside the code is expected and is
        verified by test_domain_label_descriptions_resolve, not forbidden
        here. Sanity check against scope creep."""
        with test_db.connect().cursor() as cur:
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
                "cnpj_formatado",
                "endereco_completo",
            }
            leaked = cols & forbidden
            assert not leaked, f"Recipe leaked opinionated columns: {leaked}"
            # The raw code columns must survive (not be replaced by labels).
            assert {"situacao_cadastral", "porte", "identificador_matriz_filial"} <= cols, (
                "raw enum code columns must be preserved alongside their descriptions"
            )

    def test_domain_label_descriptions_resolve(self, test_db: Database) -> None:
        """The three static domain-label LEFT JOINs (portes_empresa,
        situacoes_cadastrais, indicadores_matriz_filial) populate the new
        *_descricao columns. Known codes resolve to their verbatim label;
        every accepted code is covered so no known code yields NULL; an
        unknown code keeps the row with a NULL descricao (LEFT, not INNER)."""
        with test_db.connect().cursor() as cur:
            # Columns exist.
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'empresa_detalhe'
                  AND column_name IN (
                    'porte_descricao',
                    'situacao_cadastral_descricao',
                    'identificador_matriz_filial_descricao'
                  )
            """)
            cols = {row[0] for row in cur.fetchall()}
            assert cols == {
                "porte_descricao",
                "situacao_cadastral_descricao",
                "identificador_matriz_filial_descricao",
            }, f"Missing domain-label descricao columns: {cols}"

            # Known codes resolve to their verbatim labels. The crafted
            # estabelecimento 99000001 is guaranteed in the join and carries
            # porte 01, situacao 02, matriz 1.
            cur.execute("""
                SELECT porte, porte_descricao,
                       situacao_cadastral, situacao_cadastral_descricao,
                       identificador_matriz_filial,
                       identificador_matriz_filial_descricao
                FROM empresa_detalhe WHERE cnpj_basico = '99000001'
            """)
            assert fetch_row(cur) == ("01", "Microempresa", "02", "Ativa", 1, "Matriz")

            # situacao_cadastral 05 (Ativa Não Regular) is in the SERPRO domain
            # but outside the open-data layout regex; the ingest validator only
            # warns, so it survives and must resolve end to end. Crafted
            # estabelecimento 99000006 carries it; a regression that drops 05
            # from situacoes_cadastrais (or makes the join INNER) is caught here.
            cur.execute("""
                SELECT situacao_cadastral, situacao_cadastral_descricao
                FROM empresa_detalhe WHERE cnpj_basico = '99000006'
            """)
            assert fetch_row(cur) == ("05", "Ativa Não Regular"), (
                "situacao_cadastral 05 must resolve to 'Ativa Não Regular' end to end"
            )

            # porte '00' (NÃO INFORMADO) is absent from the SERPRO porte CSV but
            # accepted by the ingest validator, so the label table must carry it.
            # Its label is verbatim from the Receita layout PDF, which writes it
            # uppercase ("00 – NÃO INFORMADO").
            cur.execute("SELECT descricao FROM portes_empresa WHERE codigo = '00'")
            assert fetch_row(cur) == ("NÃO INFORMADO",), "porte '00' must resolve to verbatim 'NÃO INFORMADO'"

            # Coverage: every code that IS in a label table must resolve, so no
            # row whose code is in the label set may have a NULL descricao. The
            # check is symmetric across all three columns: a code outside the
            # label set is allowed to be NULL (that is the unknown path, asserted
            # below), but a code inside it must never be NULL. The ingest
            # validator only WARNS on out-of-domain porte/situacao codes - it
            # neither drops nor nullifies them - so porte/situacao can carry
            # unknown codes too (the crafted row 99000005 does), exactly like
            # matriz/filial.
            cur.execute("""
                SELECT COUNT(*) FROM empresa_detalhe
                WHERE (porte IN (SELECT codigo FROM portes_empresa)
                       AND porte_descricao IS NULL)
                   OR (situacao_cadastral IN (SELECT codigo FROM situacoes_cadastrais)
                       AND situacao_cadastral_descricao IS NULL)
                   OR (identificador_matriz_filial IN (
                           SELECT codigo FROM indicadores_matriz_filial)
                       AND identificador_matriz_filial_descricao IS NULL)
            """)
            assert fetch_row(cur)[0] == 0, (
                "every label-covered porte/situacao_cadastral/matriz_filial code must resolve to a description"
            )

            # Code 2 -> Filial. The crafted estabelecimento 99000001 exercises
            # the matriz/filial LEFT JOIN for code 1 (-> Matriz) end to end, and
            # 99000003 exercises the unknown path (3 -> NULL), but no code-2
            # estabelecimento survives empresa_detalhe's INNER JOIN (the fixture
            # samples estabelecimentos and empresas independently, and the code-2
            # rows have no parent empresa). Assert code 2 against the label table
            # directly so a removed or relabeled (2, 'Filial') row is caught, the
            # same way porte '00' is checked above.
            cur.execute("SELECT descricao FROM indicadores_matriz_filial WHERE codigo = 2")
            assert fetch_row(cur) == ("Filial",), "matriz/filial code 2 must resolve to 'Filial'"

            # Unknown code keeps the row with a NULL descricao. The crafted
            # orphan estabelecimento 99000003 carries identificador_matriz_filial
            # = 3, which is not in indicadores_matriz_filial (1/2 only), so the
            # LEFT JOIN must preserve the row and leave the description NULL.
            cur.execute("""
                SELECT identificador_matriz_filial,
                       identificador_matriz_filial_descricao
                FROM empresa_detalhe WHERE cnpj_basico = '99000003'
            """)
            row = fetch_row(cur)
            assert row is not None, "orphan estabelecimento 99000003 must be kept"
            assert row == (3, None), "unknown matriz/filial code must stay NULL, row preserved"

            # Unknown porte AND situacao keep the row with NULL descricoes. The
            # crafted row 99000005 carries porte 07 and situacao_cadastral 07,
            # neither of which is in portes_empresa/situacoes_cadastrais, so the
            # static-label LEFT JOINs must preserve the row and leave both
            # descriptions NULL. This proves the joins are LEFT (not INNER) for
            # porte/situacao too, not only for matriz/filial.
            cur.execute("""
                SELECT porte, porte_descricao,
                       situacao_cadastral, situacao_cadastral_descricao
                FROM empresa_detalhe WHERE cnpj_basico = '99000005'
            """)
            row = fetch_row(cur)
            assert row is not None, "row with unknown porte/situacao must be kept"
            assert row == ("07", None, "07", None), "unknown porte/situacao codes must stay NULL, row preserved"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = count_rows(test_db, "empresa_detalhe")

        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()

        count_after = count_rows(test_db, "empresa_detalhe")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"
