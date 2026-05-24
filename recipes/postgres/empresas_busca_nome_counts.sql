-- recipes/postgres/empresas_busca_nome_counts.sql
--
-- recipeVersion: 1
-- depends on: recipes/postgres/empresas_busca_nome.sql (recipeVersion 1)
--
-- Precomputed count rollups for the three filter shapes consumer-facing
-- search surfaces typically expose alongside an exact "X results found"
-- total: by UF, by UF + município, by UF + CNAE.
--
-- Why this exists. COUNT(*) over the main search table is O(matching
-- rows) even with a covering index, so a broad filter like uf='SP'
-- walks ~8 M index entries and returns in seconds. That's fine for
-- batch reports but kills request latency budgets. This rollup is
-- O(1) per lookup and refreshes alongside the main table.
--
-- One table with a `kind` discriminator and three partial unique
-- indexes. Lookups query a single kind at a time, so the partial
-- indexes give point-lookup latency without splitting the data into
-- three physical tables.
--
-- Apply after empresas_busca_nome has been built/refreshed:
--     psql "$DATABASE_URL" -f recipes/postgres/empresas_busca_nome.sql
--     psql "$DATABASE_URL" -f recipes/postgres/empresas_busca_nome_counts.sql
--
-- Re-run after each monthly ingest to refresh.
--
-- Design choices:
--   - Single table + kind column keeps the operational surface small:
--     one DROP + CREATE, one ANALYZE, one swap for blue/green refresh.
--   - Partial unique indexes (one per kind) give point-lookup latency
--     without splitting the data into three physical tables.
--   - municipio_nome, municipio_codigo, and cnae_fiscal_principal are
--     all nullable so the `kind='uf'` and `kind='uf_cnae'` rows can
--     leave them NULL rather than carrying a sentinel. Consumers
--     always filter on kind first.
--   - kind='uf_municipio' rows carry BOTH the descricao (municipio_nome)
--     and the RFB código (municipio_codigo). Consumers using the
--     numeric code get a stable, escape-free lookup key; consumers
--     using the text name still work. Each (uf, municipio_nome) maps
--     to exactly one código in the source data, so the dual column
--     does not change the row count.
--   - Includes a NULL bucket for cnae_fiscal_principal because some
--     estabelecimento rows in the source have no CNAE; consumers can
--     decide whether to surface it or filter it out.

DROP TABLE IF EXISTS empresas_busca_nome_counts;

CREATE TABLE empresas_busca_nome_counts (
    kind TEXT NOT NULL,
    uf VARCHAR(2) NOT NULL,
    municipio_codigo VARCHAR(4),
    municipio_nome TEXT,
    cnae_fiscal_principal VARCHAR(7),
    total BIGINT NOT NULL
);

INSERT INTO empresas_busca_nome_counts (kind, uf, municipio_codigo, municipio_nome, cnae_fiscal_principal, total)
SELECT 'uf', uf, NULL, NULL, NULL, COUNT(*)
FROM empresas_busca_nome
GROUP BY uf
UNION ALL
-- (uf, municipio_nome) maps 1:1 to municipio_codigo in the source, so
-- MIN(municipio_codigo) collapses to the single value per group.
SELECT 'uf_municipio', uf, MIN(municipio_codigo), municipio_nome, NULL, COUNT(*)
FROM empresas_busca_nome
GROUP BY uf, municipio_nome
UNION ALL
SELECT 'uf_cnae', uf, NULL, NULL, cnae_fiscal_principal, COUNT(*)
FROM empresas_busca_nome
GROUP BY uf, cnae_fiscal_principal;

-- Partial unique indexes per kind. Each lookup hits exactly one.

CREATE UNIQUE INDEX IF NOT EXISTS idx_empresas_busca_nome_counts_uf
    ON empresas_busca_nome_counts (uf)
    WHERE kind = 'uf';

CREATE UNIQUE INDEX IF NOT EXISTS idx_empresas_busca_nome_counts_uf_municipio
    ON empresas_busca_nome_counts (uf, municipio_nome)
    WHERE kind = 'uf_municipio';

-- Same kind, indexed by code so consumers can join by RFB código
-- without a name-literal trip through SQL escaping.
CREATE UNIQUE INDEX IF NOT EXISTS idx_empresas_busca_nome_counts_uf_municipio_codigo
    ON empresas_busca_nome_counts (uf, municipio_codigo)
    WHERE kind = 'uf_municipio';

CREATE UNIQUE INDEX IF NOT EXISTS idx_empresas_busca_nome_counts_uf_cnae
    ON empresas_busca_nome_counts (uf, cnae_fiscal_principal)
    WHERE kind = 'uf_cnae';

ANALYZE empresas_busca_nome_counts;

-- Lookup patterns (consumer side):
--
--   SELECT total FROM empresas_busca_nome_counts
--    WHERE kind = 'uf' AND uf = $1;
--
--   -- by município name (must escape single quotes in the value)
--   SELECT total FROM empresas_busca_nome_counts
--    WHERE kind = 'uf_municipio' AND uf = $1 AND municipio_nome = $2;
--
--   -- by RFB código (stable, no text escaping)
--   SELECT total FROM empresas_busca_nome_counts
--    WHERE kind = 'uf_municipio' AND uf = $1 AND municipio_codigo = $2;
--
--   SELECT total FROM empresas_busca_nome_counts
--    WHERE kind = 'uf_cnae' AND uf = $1 AND cnae_fiscal_principal = $2;
--
-- All four return one row (or zero if the bucket is empty) via the
-- partial unique index for that kind. Measured ~0.05 ms cold cache
-- against a 27 M-row main table.

-- Storage check (run separately):
--   SELECT
--       pg_size_pretty(pg_total_relation_size('empresas_busca_nome_counts')) AS total,
--       pg_size_pretty(pg_relation_size('empresas_busca_nome_counts')) AS heap,
--       pg_size_pretty(pg_indexes_size('empresas_busca_nome_counts')) AS indexes;
