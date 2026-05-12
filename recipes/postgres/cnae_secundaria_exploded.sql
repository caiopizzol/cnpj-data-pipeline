-- recipes/postgres/cnae_secundaria_exploded.sql
--
-- recipeVersion: 1
--
-- Side table that explodes the comma-separated cnae_fiscal_secundaria
-- string into one row per (estabelecimento, secondary CNAE code).
-- Lets consumers query "all estabelecimentos with CNAE X as secondary
-- activity" without scanning a string column.
--
-- Apply after the pipeline finishes ingest:
--     psql "$DATABASE_URL" -f recipes/postgres/cnae_secundaria_exploded.sql
--
-- Design choices (see docs/data-audit.md for the broader recipe roadmap):
--   - Trim whitespace and drop empty entries: handles trailing commas
--     and stray spaces in the source string. Shape-only, deterministic.
--   - Do not deduplicate: if the same code appears twice in the source
--     string for the same estabelecimento, both rows are kept. Preserves
--     source shape. A future clean recipe can dedup if needed.
--   - Do not validate against the cnaes lookup. Same preserve-and-measure
--     stance as motivo/pais orphans. Consumers LEFT JOIN cnaes when they
--     want descriptions; orphans surface as NULL there.
--   - Do not check that codes are 7 digits. RFB documents them as
--     7-digit, but anything else is preserved as-is for transparency.
--   - No position column. Adds bytes for a use case nobody has asked
--     for. Easy to add later if needed.

DROP TABLE IF EXISTS cnae_secundaria_exploded;
CREATE TABLE cnae_secundaria_exploded AS
SELECT
    e.cnpj_basico,
    e.cnpj_ordem,
    e.cnpj_dv,
    e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
    trim(code.raw_code) AS cnae_codigo
FROM estabelecimentos e
CROSS JOIN LATERAL unnest(string_to_array(e.cnae_fiscal_secundaria, ',')) AS code(raw_code)
WHERE e.cnae_fiscal_secundaria IS NOT NULL
  AND trim(code.raw_code) <> '';

CREATE INDEX IF NOT EXISTS idx_cnae_secundaria_exploded_codigo
    ON cnae_secundaria_exploded (cnae_codigo);
CREATE INDEX IF NOT EXISTS idx_cnae_secundaria_exploded_cnpj
    ON cnae_secundaria_exploded (cnpj);
CREATE INDEX IF NOT EXISTS idx_cnae_secundaria_exploded_estabelecimento
    ON cnae_secundaria_exploded (cnpj_basico, cnpj_ordem, cnpj_dv);

ANALYZE cnae_secundaria_exploded;
