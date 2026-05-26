-- recipes/postgres/empresas_busca_nome.sql
--
-- recipeVersion: 1
-- depends on: empresas, estabelecimentos, cnaes, municipios (base pipeline tables)
--
-- Search-by-name serving table for the most common access pattern over
-- the CNPJ dataset: "find an active headquarters by razao_social prefix,
-- optionally narrowed by UF / município / CNAE, ordered by name."
--
-- Filters to active matriz only:
--   - situacao_cadastral = '02'        (active)
--   - identificador_matriz_filial = 1  (headquarters, not branches)
--
-- Branches and inactive companies are intentionally excluded. Consumers
-- that need those rows can query estabelecimentos directly or build a
-- companion recipe with a different predicate.
--
-- Apply after the pipeline finishes ingest:
--     psql "$DATABASE_URL" -f recipes/postgres/empresas_busca_nome.sql
--
-- Re-run after each monthly ingest to refresh.
--
-- Design choices:
--   - One row per matching estabelecimento. The active-matriz predicate
--     normally yields one row per company per snapshot, but the source
--     does not enforce that invariant and the recipe does not assume it.
--   - cnpj column is materialized as basico||ordem||dv, the same pattern
--     as empresa_detalhe.sql. Lets consumers point-lookup by the
--     14-digit string without repeating the concatenation.
--   - Source codes are preserved alongside denormalized descriptions
--     (municipio_codigo + municipio_nome, cnae_fiscal_principal +
--     cnae_descricao) so consumers can re-join the reference tables
--     for richer surfaces without re-querying the base.
--   - Composite indexes cover (filter, razao_social, PK-suffix) to
--     support prefix filtering and common ordering patterns. They do
--     not guarantee a sort-free plan in every case; verify with EXPLAIN
--     for your workload, especially under non-default collations or
--     when stacking multiple filters.
--   - text_pattern_ops on razao_social enables LIKE 'PREFIX%' index use.
--     Substring LIKE ('%PREFIX%') needs GIN + pg_trgm; see the opt-in
--     block at the bottom.

DROP TABLE IF EXISTS empresas_busca_nome;

CREATE TABLE empresas_busca_nome AS
SELECT
    e.cnpj_basico,
    est.cnpj_ordem,
    est.cnpj_dv,
    e.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj,
    e.razao_social,
    est.nome_fantasia,
    est.uf,
    est.municipio AS municipio_codigo,
    m.descricao AS municipio_nome,
    est.situacao_cadastral,
    est.identificador_matriz_filial,
    est.cnae_fiscal_principal,
    c.descricao AS cnae_descricao,
    est.data_inicio_atividade
FROM empresas e
JOIN estabelecimentos est USING (cnpj_basico)
LEFT JOIN cnaes c ON c.codigo = est.cnae_fiscal_principal
LEFT JOIN municipios m ON m.codigo = est.municipio
WHERE est.situacao_cadastral = '02'
  AND est.identificador_matriz_filial = 1;

-- Primary key on the component CNPJ tuple. Covers point-lookup access
-- and gives every secondary index a small unique-suffix tiebreaker.
ALTER TABLE empresas_busca_nome
    ADD CONSTRAINT pk_empresas_busca_nome
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);

-- Single-column index on the materialized cnpj for callers that look
-- up by the concatenated 14-digit string instead of the component tuple.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_cnpj
    ON empresas_busca_nome (cnpj);

-- Prefix LIKE on razao_social plus the PK suffix as a tiebreaker.
-- Supports the LIKE 'PREFIX%' predicate; whether Postgres can use it to
-- avoid a sort under a specific ORDER BY depends on collation and the
-- rest of the query (see the EXPLAIN note in the header).
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_razao_prefix
    ON empresas_busca_nome
    (razao_social text_pattern_ops, cnpj_basico, cnpj_ordem);

-- UF filter + sort by razao_social.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_razao
    ON empresas_busca_nome
    (uf, razao_social, cnpj_basico, cnpj_ordem);

-- UF filter + LIKE 'PREFIX%' on razao_social. The default-opclass
-- uf_razao above supports sort-by-razao under uf= equality but cannot
-- range-scan LIKE 'PREFIX%' under non-C collations. This composite
-- uses text_pattern_ops on razao_social so PG can enter at
-- (uf=$1, razao_social>='PREFIX') and walk only matching rows in
-- order — Index Only Scan, no heap fetch, sub-30ms even for broad
-- common prefixes (e.g. 'COMERC%' over 27M rows).
--
-- Pairs with idx_..._razao_prefix above: that one serves uf-less
-- prefix lookups (e.g. chat name resolution); this one serves
-- uf-filtered search list queries.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_razao_prefix
    ON empresas_busca_nome
    (uf, razao_social text_pattern_ops, cnpj_basico, cnpj_ordem);

-- UF + município (denormalized name) + sort. Indexes municipio_nome
-- because that's typically what consumer-facing filters expose. Add a
-- parallel index on municipio_codigo if your callers filter by RFB code.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_municipio_razao
    ON empresas_busca_nome
    (uf, municipio_nome, razao_social, cnpj_basico, cnpj_ordem);

-- UF + CNAE + sort by razao_social.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_cnae_razao
    ON empresas_busca_nome
    (uf, cnae_fiscal_principal, razao_social, cnpj_basico, cnpj_ordem);

ANALYZE empresas_busca_nome;

-- Optional: substring LIKE on razao_social via trigram GIN.
-- Uncomment if you need LIKE '%PREFIX%' (or ILIKE) patterns. The index
-- is significantly larger than the b-tree indexes above and refresh is
-- slower; only enable when substring search is a real requirement.
--
--   CREATE EXTENSION IF NOT EXISTS pg_trgm;
--   CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_razao_trgm
--       ON empresas_busca_nome
--       USING gin (razao_social gin_trgm_ops);

-- Storage check (run separately):
--   SELECT
--       pg_size_pretty(pg_total_relation_size('empresas_busca_nome')) AS total,
--       pg_size_pretty(pg_relation_size('empresas_busca_nome')) AS heap,
--       pg_size_pretty(pg_indexes_size('empresas_busca_nome')) AS indexes;
