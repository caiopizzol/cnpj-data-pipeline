-- recipes/postgres/empresas_busca_nome.sql
--
-- recipeVersion: 2
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
--     14-character string without repeating the concatenation.
--   - Source codes are preserved alongside denormalized descriptions
--     (municipio_codigo + municipio_nome, cnae_fiscal_principal +
--     cnae_descricao) so consumers can re-join the reference tables
--     for richer surfaces without re-querying the base.
--   - Locale-ordered indexes serve browse queries. C-collated companions
--     serve LIKE 'PREFIX%' plus sort-free ORDER BY ... COLLATE "C" for
--     unfiltered, UF, municipality-name/code, and CNAE search shapes.
--   - GIN trigram indexes serve ILIKE and substring search on both
--     razao_social and nome_fantasia.

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

-- Primary key on the full component CNPJ tuple for point lookups.
-- Sort indexes below include basico and ordem, but omit dv; their suffix
-- is not guaranteed unique. Use the full key when unique ordering matters.
ALTER TABLE empresas_busca_nome
    ADD CONSTRAINT pk_empresas_busca_nome
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);

-- Single-column index on the materialized cnpj for callers that look
-- up by the concatenated 14-character string instead of the component tuple.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_cnpj
    ON empresas_busca_nome (cnpj);

-- Prefix LIKE on razao_social followed by basico and ordem. The explicit C
-- collation lets the same index constrain LIKE 'PREFIX%' and provide the
-- requested ordering. Prefix-search callers must use the matching expression:
--   ORDER BY razao_social COLLATE "C", cnpj_basico, cnpj_ordem
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_razao_prefix_c
    ON empresas_busca_nome
    (razao_social COLLATE "C", cnpj_basico, cnpj_ordem);

-- UF filter + sort by razao_social.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_razao
    ON empresas_busca_nome
    (uf, razao_social, cnpj_basico, cnpj_ordem);

-- UF filter + LIKE 'PREFIX%' on razao_social. The default-collation
-- uf_razao above supports locale ordering under uf= equality but cannot
-- range-scan LIKE 'PREFIX%' under non-C collations. This C-collated index
-- supports both the prefix range and ORDER BY ... COLLATE "C".
--
-- Pairs with idx_..._razao_prefix_c above: that one serves uf-less
-- prefix lookups (e.g. chat name resolution); this one serves
-- uf-filtered search list queries.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_razao_prefix_c
    ON empresas_busca_nome
    (uf, razao_social COLLATE "C", cnpj_basico, cnpj_ordem);

-- UF + município (denormalized name) + sort. Indexes municipio_nome
-- because that's typically what consumer-facing filters expose. Add a
-- parallel index on municipio_codigo if your callers filter by RFB code.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_municipio_razao
    ON empresas_busca_nome
    (uf, municipio_nome, razao_social, cnpj_basico, cnpj_ordem);

-- Prefix-search companion for the same filter. Keep the locale-ordered index
-- above for browse queries without a prefix.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_municipio_razao_prefix_c
    ON empresas_busca_nome
    (uf, municipio_nome, razao_social COLLATE "C", cnpj_basico, cnpj_ordem);

-- Equivalent paths for consumers that use the stable RFB municipality code
-- instead of the denormalized display name.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_municipio_codigo_razao
    ON empresas_busca_nome
    (uf, municipio_codigo, razao_social, cnpj_basico, cnpj_ordem);

CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_municipio_codigo_razao_prefix_c
    ON empresas_busca_nome
    (uf, municipio_codigo, razao_social COLLATE "C", cnpj_basico, cnpj_ordem);

-- UF + CNAE + sort by razao_social.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_cnae_razao
    ON empresas_busca_nome
    (uf, cnae_fiscal_principal, razao_social, cnpj_basico, cnpj_ordem);

-- Prefix-search companion for the same filter. Callers must use the matching
-- ORDER BY razao_social COLLATE "C", cnpj_basico, cnpj_ordem expression.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_uf_cnae_razao_prefix_c
    ON empresas_busca_nome
    (uf, cnae_fiscal_principal, razao_social COLLATE "C", cnpj_basico, cnpj_ordem);

-- Case-insensitive and substring searches cannot use the B-tree prefix
-- indexes. The trigram index avoids a full-table scan for ILIKE and
-- LIKE '%TRECHO%', especially for rare or absent terms.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_razao_trgm
    ON empresas_busca_nome
    USING gin (razao_social gin_trgm_ops);

-- nome_fantasia is also exposed by this serving table. Without a trigram
-- index, an absent ILIKE/substring term scans the entire relation.
CREATE INDEX IF NOT EXISTS idx_empresas_busca_nome_fantasia_trgm
    ON empresas_busca_nome
    USING gin (nome_fantasia gin_trgm_ops);

ANALYZE empresas_busca_nome;

-- Storage check (run separately):
--   SELECT
--       pg_size_pretty(pg_total_relation_size('empresas_busca_nome')) AS total,
--       pg_size_pretty(pg_relation_size('empresas_busca_nome')) AS heap,
--       pg_size_pretty(pg_indexes_size('empresas_busca_nome')) AS indexes;
