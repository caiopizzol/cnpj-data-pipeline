-- recipes/postgres/data_quality_flags.sql
--
-- recipeVersion: 2
--
-- Narrow per-estabelecimento table of data-quality signals. One row per
-- estabelecimento (keyed by cnpj_basico + cnpj_ordem + cnpj_dv). No source
-- columns are mutated or duplicated - this recipe only emits flags so a
-- later estabelecimentos_clean.sql can apply the interpretations using
-- these same predicates as its single source of truth.
--
-- Apply after the pipeline finishes ingest. The *_enriched_lookup_missing
-- flags compare against the enriched lookups, so run that recipe first:
--     psql "$DATABASE_URL" -f recipes/postgres/reference_domains_enriched.sql
--     psql "$DATABASE_URL" -f recipes/postgres/data_quality_flags.sql
--
-- Dependencies: reference_domains_enriched.sql (motivos_enriched, paises_enriched).
--
-- Design notes (see docs/data-audit.md for source-by-source rationale):
--   - Two lookup signals per domain, deliberately kept separate:
--     pais_lookup_missing / motivo_lookup_missing check the raw MONTHLY lookup
--     (paises / motivos), so they measure the Receita delivery's internal gaps.
--     pais_enriched_lookup_missing / motivo_enriched_lookup_missing check the
--     ENRICHED lookup, so they measure what is still unresolved after official
--     supplemental rows. A code present only in the supplement (e.g. motivo 32)
--     is monthly-missing but enriched-resolved.
--   - Sócios are intentionally out of scope - different grain.
--     recipes/postgres/socios_quality_flags.sql covers them.
--   - cep_status assumes the v1.21.0+ pipeline has already padded
--     7-digit numeric CEPs. If you load older data without that fix,
--     7-digit values will appear as 'malformed' here.
--   - capital_social_is_suspicious_sentinel uses exact equality against
--     999999999999. The RFB layout does not document this value; the
--     "suspicious sentinel" framing matches docs/data-audit.md.
--   - is_exterior reflects the observed convention (uf='EX'). The layout
--     PDF does not document EX explicitly; this flag should be read as
--     "matches the observed exterior pattern", not "is officially
--     exterior".

DROP TABLE IF EXISTS data_quality_flags;
CREATE TABLE data_quality_flags AS
SELECT
    e.cnpj_basico,
    e.cnpj_ordem,
    e.cnpj_dv,
    e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
    CASE
        WHEN e.cep IS NULL THEN 'missing'
        WHEN e.cep = '00000000' THEN 'zero_sentinel'
        WHEN e.cep ~ '^\d{8}$' THEN 'valid_shape'
        ELSE 'malformed'
    END AS cep_status,
    (e.uf = 'EX') AS is_exterior,
    (
        e.pais IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = e.pais)
    ) AS pais_lookup_missing,
    (
        e.motivo_situacao_cadastral IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM motivos m WHERE m.codigo = e.motivo_situacao_cadastral)
    ) AS motivo_lookup_missing,
    (
        e.pais IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM paises_enriched p WHERE p.codigo = e.pais)
    ) AS pais_enriched_lookup_missing,
    (
        e.motivo_situacao_cadastral IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM motivos_enriched m WHERE m.codigo = e.motivo_situacao_cadastral)
    ) AS motivo_enriched_lookup_missing,
    (emp.capital_social = 999999999999) AS capital_social_is_suspicious_sentinel
FROM estabelecimentos e
JOIN empresas emp USING (cnpj_basico);

CREATE INDEX IF NOT EXISTS idx_data_quality_flags_cnpj ON data_quality_flags (cnpj);
CREATE INDEX IF NOT EXISTS idx_data_quality_flags_basico ON data_quality_flags (cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_data_quality_flags_cep_status ON data_quality_flags (cep_status);
CREATE INDEX IF NOT EXISTS idx_data_quality_flags_is_exterior ON data_quality_flags (is_exterior) WHERE is_exterior;
CREATE INDEX IF NOT EXISTS idx_data_quality_flags_pais_enriched_missing
    ON data_quality_flags (cnpj_basico) WHERE pais_enriched_lookup_missing;
CREATE INDEX IF NOT EXISTS idx_data_quality_flags_motivo_enriched_missing
    ON data_quality_flags (cnpj_basico) WHERE motivo_enriched_lookup_missing;

ANALYZE data_quality_flags;
