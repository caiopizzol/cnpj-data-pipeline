-- recipes/postgres/socios_quality_flags.sql
--
-- recipeVersion: 3
--
-- Narrow per-socio table of data-quality signals. One row per socio,
-- keyed by socios.socio_id (UUID, deterministic). The old triple
-- (cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio) is kept
-- alongside as lookup columns but is no longer unique (issue #78).
--
-- No source columns are changed or duplicated here. This recipe only
-- materializes predicates that consumers can use later in their own clean
-- tables or reports.
--
-- Apply after the pipeline finishes ingest. The *_enriched_lookup_missing
-- flags compare against the enriched lookups, so run that recipe first:
--     psql "$DATABASE_URL" -f recipes/postgres/reference_domains_enriched.sql
--     psql "$DATABASE_URL" -f recipes/postgres/socios_quality_flags.sql
--
-- Dependencies: reference_domains_enriched.sql (paises_enriched,
-- qualificacoes_socios_enriched).
--
-- Design notes (see docs/data-audit.md for source-by-source rationale):
--   - Two lookup signals per domain: *_lookup_missing checks the raw MONTHLY
--     lookup (the Receita delivery's internal gaps); *_enriched_lookup_missing
--     checks the ENRICHED lookup (what is still unresolved after official
--     supplemental rows). The two differ on supplemented codes: pais orphans
--     from the SERPRO table, and qualificacao 36 (the legacy Gerente-Delegado
--     code) - see reference_domains_enriched.
--   - representante_is_placeholder uses the observed pair
--     representante_legal='***000000**' and
--     qualificacao_do_representante_legal='00'. The masked CPF form is
--     consistent with public CPF masking rules, but the "sem representante"
--     meaning is empirical.
--   - faixa_etaria_nao_se_aplica reflects the documented '0' value.
--   - Lookup flags are defensive. Some are zero in current snapshots, but
--     keeping them here makes drift visible without mutating the source.
--   - qualificacao_representante_lookup_missing excludes '00' on purpose -
--     that's the placeholder qualification that pairs with the placeholder
--     representante_legal, not an orphan.

DROP TABLE IF EXISTS socios_quality_flags;
CREATE TABLE socios_quality_flags AS
SELECT
    s.socio_id,
    s.cnpj_basico,
    s.identificador_de_socio,
    s.cnpj_cpf_do_socio,
    (
        s.representante_legal = '***000000**'
        AND s.qualificacao_do_representante_legal = '00'
    ) AS representante_is_placeholder,
    (
        s.pais IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = s.pais)
    ) AS pais_lookup_missing,
    (
        s.qualificacao_do_socio IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM qualificacoes_socios q
            WHERE q.codigo = s.qualificacao_do_socio
        )
    ) AS qualificacao_socio_lookup_missing,
    (
        s.qualificacao_do_representante_legal IS NOT NULL
        AND s.qualificacao_do_representante_legal <> '00'
        AND NOT EXISTS (
            SELECT 1 FROM qualificacoes_socios q
            WHERE q.codigo = s.qualificacao_do_representante_legal
        )
    ) AS qualificacao_representante_lookup_missing,
    (
        s.pais IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM paises_enriched p WHERE p.codigo = s.pais)
    ) AS pais_enriched_lookup_missing,
    (
        s.qualificacao_do_socio IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM qualificacoes_socios_enriched q
            WHERE q.codigo = s.qualificacao_do_socio
        )
    ) AS qualificacao_socio_enriched_lookup_missing,
    (
        s.qualificacao_do_representante_legal IS NOT NULL
        AND s.qualificacao_do_representante_legal <> '00'
        AND NOT EXISTS (
            SELECT 1 FROM qualificacoes_socios_enriched q
            WHERE q.codigo = s.qualificacao_do_representante_legal
        )
    ) AS qualificacao_representante_enriched_lookup_missing,
    (s.faixa_etaria = '0') AS faixa_etaria_nao_se_aplica
FROM socios s;

CREATE UNIQUE INDEX IF NOT EXISTS idx_socios_quality_flags_socio_id
    ON socios_quality_flags (socio_id);
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_basico
    ON socios_quality_flags (cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_pais_missing
    ON socios_quality_flags (cnpj_basico) WHERE pais_lookup_missing;
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_qual_socio_missing
    ON socios_quality_flags (cnpj_basico) WHERE qualificacao_socio_lookup_missing;
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_qual_representante_missing
    ON socios_quality_flags (cnpj_basico) WHERE qualificacao_representante_lookup_missing;
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_pais_enriched_missing
    ON socios_quality_flags (cnpj_basico) WHERE pais_enriched_lookup_missing;
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_qual_socio_enriched_missing
    ON socios_quality_flags (cnpj_basico) WHERE qualificacao_socio_enriched_lookup_missing;
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_qual_representante_enriched_missing
    ON socios_quality_flags (cnpj_basico) WHERE qualificacao_representante_enriched_lookup_missing;

ANALYZE socios_quality_flags;
