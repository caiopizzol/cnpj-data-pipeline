-- recipes/postgres/socios_clean.sql
--
-- recipeVersion: 1
-- depends on: recipes/postgres/socios_quality_flags.sql (recipeVersion 1)
--
-- Sócios-grain counterpart to estabelecimentos_clean. Narrow contract:
--   - one row per sócio (cnpj_basico + identificador_de_socio +
--     cnpj_cpf_do_socio)
--   - built from socios_quality_flags joined to socios
--   - preserves raw columns alongside clean columns
--   - uses ONLY predicates from socios_quality_flags (single source of
--     truth for "what counts as suspicious")
--   - no new interpretation logic. If a rule changes, it changes in
--     socios_quality_flags.sql, and this recipe picks it up automatically
--
-- What this recipe is NOT:
--   - no labels for PF / PJ / Estrangeiro
--   - no qualification or country descriptions
--   - no derived booleans (is_representante_pj etc.)
--   - no nome_socio normalization
--   - no cnpj_cpf_do_socio mutation - the masked source stays as-is
--
-- Apply after socios_quality_flags has been built:
--     psql "$DATABASE_URL" -f recipes/postgres/socios_quality_flags.sql
--     psql "$DATABASE_URL" -f recipes/postgres/socios_clean.sql

DROP TABLE IF EXISTS socios_clean;
CREATE TABLE socios_clean AS
SELECT
    f.cnpj_basico,
    f.identificador_de_socio,
    f.cnpj_cpf_do_socio,
    -- Representante trio: cleared together when the placeholder pattern
    -- is present. Pairing matches socios_quality_flags' single predicate -
    -- one flag, one nullification group.
    s.representante_legal AS representante_legal_raw,
    CASE WHEN f.representante_is_placeholder THEN NULL ELSE s.representante_legal END AS representante_legal_clean,
    s.nome_do_representante AS nome_do_representante_raw,
    CASE WHEN f.representante_is_placeholder THEN NULL ELSE s.nome_do_representante END AS nome_do_representante_clean,
    s.qualificacao_do_representante_legal AS qualificacao_do_representante_legal_raw,
    CASE WHEN f.representante_is_placeholder THEN NULL ELSE s.qualificacao_do_representante_legal END AS qualificacao_do_representante_legal_clean,
    -- faixa_etaria: nulled when the documented '0' "não se aplica" value.
    s.faixa_etaria AS faixa_etaria_raw,
    CASE WHEN f.faixa_etaria_nao_se_aplica THEN NULL ELSE s.faixa_etaria END AS faixa_etaria_clean,
    -- Flags passed through verbatim from socios_quality_flags so the
    -- consumer can filter or audit without a second join.
    f.representante_is_placeholder,
    f.pais_lookup_missing,
    f.qualificacao_socio_lookup_missing,
    f.qualificacao_representante_lookup_missing,
    f.faixa_etaria_nao_se_aplica
FROM socios_quality_flags f
JOIN socios s
    ON s.cnpj_basico = f.cnpj_basico
   AND s.identificador_de_socio = f.identificador_de_socio
   AND s.cnpj_cpf_do_socio = f.cnpj_cpf_do_socio;

-- Composite index on the source key. Covers join-back and prefix
-- queries on cnpj_basico alone.
CREATE INDEX IF NOT EXISTS idx_socios_clean_key
    ON socios_clean (cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio);

ANALYZE socios_clean;
