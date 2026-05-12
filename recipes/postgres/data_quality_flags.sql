-- recipes/postgres/data_quality_flags.sql
--
-- recipeVersion: 1
--
-- Narrow per-estabelecimento table of data-quality signals. One row per
-- estabelecimento (keyed by cnpj_basico + cnpj_ordem + cnpj_dv). No source
-- columns are mutated or duplicated - this recipe only emits flags so a
-- later estabelecimentos_clean.sql can apply the interpretations using
-- these same predicates as its single source of truth.
--
-- Apply after the pipeline finishes ingest:
--     psql "$DATABASE_URL" -f recipes/postgres/data_quality_flags.sql
--
-- Design notes (see docs/data-audit.md for source-by-source rationale):
--   - Sócios are intentionally out of scope - different grain. A future
--     recipes/postgres/socios_quality_flags.sql will cover them.
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
    (emp.capital_social = 999999999999) AS capital_social_is_suspicious_sentinel
FROM estabelecimentos e
JOIN empresas emp USING (cnpj_basico);

CREATE INDEX IF NOT EXISTS idx_data_quality_flags_cnpj ON data_quality_flags (cnpj);
CREATE INDEX IF NOT EXISTS idx_data_quality_flags_basico ON data_quality_flags (cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_data_quality_flags_cep_status ON data_quality_flags (cep_status);
CREATE INDEX IF NOT EXISTS idx_data_quality_flags_is_exterior ON data_quality_flags (is_exterior) WHERE is_exterior;

ANALYZE data_quality_flags;
