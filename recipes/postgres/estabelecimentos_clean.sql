-- recipes/postgres/estabelecimentos_clean.sql
--
-- recipeVersion: 1
-- depends on: recipes/postgres/data_quality_flags.sql (recipeVersion 2)
--   (v2 added pais_enriched_lookup_missing / motivo_enriched_lookup_missing;
--    this recipe selects explicit columns and is unaffected by the additions)
--
-- First recipe that actually mutates values. Narrow contract by design:
--   - one row per estabelecimento
--   - joins estabelecimentos + empresas + data_quality_flags
--   - preserves raw columns alongside clean columns
--   - uses ONLY predicates from data_quality_flags (single source of
--     truth for "what counts as suspicious")
--   - no new interpretation logic. If a rule changes, it changes in
--     data_quality_flags.sql, and this recipe picks it up automatically
--
-- What this recipe is NOT:
--   - it is not empresa_detalhe replacement. No reference-table joins,
--     no descriptions, no enum labels.
--   - it does not include sócios (different grain - see future
--     socios_quality_flags / socios_clean).
--   - it does not synthesize booleans like is_ativa or is_matriz.
--   - it does not concatenate addresses.
--
-- Apply after data_quality_flags has been built:
--     psql "$DATABASE_URL" -f recipes/postgres/data_quality_flags.sql
--     psql "$DATABASE_URL" -f recipes/postgres/estabelecimentos_clean.sql

DROP TABLE IF EXISTS estabelecimentos_clean;
CREATE TABLE estabelecimentos_clean AS
SELECT
    f.cnpj_basico,
    f.cnpj_ordem,
    f.cnpj_dv,
    f.cnpj,
    -- CEP: keep the raw value RFB delivered (after pipeline-level
    -- 7-digit zfill), and a cleaned version that is null for anything
    -- other than a valid 8-digit shape.
    e.cep AS cep_raw,
    CASE WHEN f.cep_status = 'valid_shape' THEN e.cep ELSE NULL END AS cep_clean,
    -- capital_social: keep the raw value, and a cleaned version that
    -- nulls out the suspected 999999999999 sentinel.
    emp.capital_social AS capital_social_raw,
    CASE WHEN f.capital_social_is_suspicious_sentinel THEN NULL ELSE emp.capital_social END AS capital_social_clean,
    -- Flags passed through verbatim from data_quality_flags so the
    -- consumer can filter or audit without a second join.
    f.cep_status,
    f.capital_social_is_suspicious_sentinel,
    f.pais_lookup_missing,
    f.motivo_lookup_missing,
    f.is_exterior
FROM data_quality_flags f
JOIN estabelecimentos e
    ON e.cnpj_basico = f.cnpj_basico
   AND e.cnpj_ordem = f.cnpj_ordem
   AND e.cnpj_dv = f.cnpj_dv
JOIN empresas emp ON emp.cnpj_basico = f.cnpj_basico;

CREATE INDEX IF NOT EXISTS idx_estabelecimentos_clean_cnpj ON estabelecimentos_clean (cnpj);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_clean_basico ON estabelecimentos_clean (cnpj_basico);

ANALYZE estabelecimentos_clean;
