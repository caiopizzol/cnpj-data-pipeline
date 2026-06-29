-- recipes/postgres/reference_domains_enriched.sql
--
-- recipeVersion: 1
--
-- Enriched reference-domain lookups. Each output table is the monthly Receita
-- lookup PLUS official supplemental rows for codes the monthly delivery
-- references but omits, with provenance on every row. The raw lookup tables
-- (motivos, paises, qualificacoes_socios) are never mutated - they keep
-- reflecting the source delivery. This recipe is the interpretation layer:
-- "the pipeline preserves and measures; recipes interpret" (see
-- docs/post-processing.md and docs/data-audit.md).
--
-- Apply after the pipeline finishes ingest, BEFORE any recipe that wants the
-- enriched descriptions:
--     psql "$DATABASE_URL" -f recipes/postgres/reference_domains_enriched.sql
--     psql "$DATABASE_URL" -f recipes/postgres/empresa_detalhe.sql
--
-- Dependencies: motivos, paises, qualificacoes_socios (base load).
--
-- Design rules:
--   - Monthly rows are preserved verbatim and always win. Supplemental rows are
--     inserted ONLY when the code is absent from the monthly table (NOT EXISTS
--     anti-join), so monthly precedence and a unique codigo are guaranteed.
--   - codigo is the primary key: the enriched table is 1:1 on codigo, so a
--     consumer LEFT JOINing it never changes row counts.
--   - Provenance columns describe where each row came from:
--       source_kind   'receita_monthly' | 'serpro_dominio'
--       source_url     canonical URL for supplemental rows; NULL for monthly
--       is_supplemental true only for the added official rows
--       confidence    'high' | 'medium' (label uncertainty, not code validity)
--       notes         why the supplement exists / caveats
--   - Descriptions are kept verbatim from their source. Monthly rows are
--     UPPERCASE without accents (RFB delivery); SERPRO supplemental rows keep
--     SERPRO's casing and accents. Consumers that want uniform casing normalize
--     downstream; this layer does not rewrite official wording.
--
-- Supplemental rows below were each verified against the official SERPRO domain
-- tables (SERPRO operates the CNPJ cadastre for Receita Federal):
--   https://bcadastros.serpro.gov.br/documentacao/dominios/pj/
-- Codes that could NOT be verified against an official source are intentionally
-- left unresolved (they will still show up in the *_lookup_missing quality
-- flags). Known unresolved cases at the 2026-04 delivery: pais '008'/'009'
-- (absent from every official pais table; appear on rows with Brazilian UFs, so
-- almost certainly spurious) and qualificacao '36' (absent from every official
-- SERPRO qualification table; the "Gerente-Delegado" label sometimes claimed
-- for it has no official source).

-- ---------------------------------------------------------------------------
-- motivos_enriched
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS motivos_enriched;
CREATE TABLE motivos_enriched AS
SELECT
    m.codigo,
    m.descricao,
    'receita_monthly'::text AS source_kind,
    NULL::text              AS source_url,
    false                   AS is_supplemental,
    'high'::text            AS confidence,
    NULL::text              AS notes
FROM motivos m
UNION ALL
SELECT s.codigo, s.descricao, s.source_kind, s.source_url, s.is_supplemental, s.confidence, s.notes
FROM (
    VALUES
    (
        '32',
        'Inexistente De Fato – Ade/Cosar',
        'serpro_dominio',
        'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/motivo_situacao_cadastral.csv',
        true,
        'high',
        'Ausente do Motivos.csv mensal (medido 2026-04); presente na tabela de domínio oficial do SERPRO. Código 15 = "Inexistente De Fato"; 32 é a variante ADE/COSAR.'
    )
) AS s(codigo, descricao, source_kind, source_url, is_supplemental, confidence, notes)
WHERE NOT EXISTS (SELECT 1 FROM motivos m WHERE m.codigo = s.codigo);
ALTER TABLE motivos_enriched ADD PRIMARY KEY (codigo);

-- ---------------------------------------------------------------------------
-- paises_enriched
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS paises_enriched;
CREATE TABLE paises_enriched AS
SELECT
    p.codigo,
    p.descricao,
    'receita_monthly'::text AS source_kind,
    NULL::text              AS source_url,
    false                   AS is_supplemental,
    'high'::text            AS confidence,
    NULL::text              AS notes
FROM paises p
UNION ALL
SELECT s.codigo, s.descricao, s.source_kind, s.source_url, s.is_supplemental, s.confidence, s.notes
FROM (
    VALUES
    (
        '150',
        'JERSEY, ILHA DO CANAL',
        'serpro_dominio',
        'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/pais.csv',
        true,
        'medium',
        'Ilha do Canal. SERPRO rotula "Jersey"; a tabela Siscomex/ME rotula "Guernsey" para o mesmo código. A validade do código é alta; o rótulo exato diverge entre fontes oficiais.'
    ),
    (
        '359',
        'MAN, ILHA DE',
        'serpro_dominio',
        'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/pais.csv',
        true,
        'high',
        'Ausente do Paises.csv mensal; presente na tabela de domínio oficial do SERPRO.'
    ),
    (
        '367',
        'INGLATERRA',
        'serpro_dominio',
        'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/pais.csv',
        true,
        'high',
        'Ausente do Paises.csv mensal; presente na tabela de domínio oficial do SERPRO.'
    ),
    (
        '994',
        'A DESIGNAR',
        'serpro_dominio',
        'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/pais.csv',
        true,
        'high',
        'Placeholder administrativo ("a designar"), não um país real. Presente na tabela de domínio oficial do SERPRO.'
    )
) AS s(codigo, descricao, source_kind, source_url, is_supplemental, confidence, notes)
WHERE NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = s.codigo);
ALTER TABLE paises_enriched ADD PRIMARY KEY (codigo);

-- ---------------------------------------------------------------------------
-- qualificacoes_socios_enriched
-- ---------------------------------------------------------------------------
-- No official supplemental qualification codes are currently verified, so this
-- table is the monthly delivery carried through with provenance columns for a
-- uniform interface. In particular, code '36' (claimed elsewhere as
-- "Gerente-Delegado") is absent from every official SERPRO qualification domain
-- (qualificacao_socio, qualificacao_responsavel, qualificacao_representante_legal)
-- and is left unresolved on purpose. Add VALUES rows here if a future code is
-- verified against an official source.
DROP TABLE IF EXISTS qualificacoes_socios_enriched;
CREATE TABLE qualificacoes_socios_enriched AS
SELECT
    q.codigo,
    q.descricao,
    'receita_monthly'::text AS source_kind,
    NULL::text              AS source_url,
    false                   AS is_supplemental,
    'high'::text            AS confidence,
    NULL::text              AS notes
FROM qualificacoes_socios q;
ALTER TABLE qualificacoes_socios_enriched ADD PRIMARY KEY (codigo);

ANALYZE motivos_enriched;
ANALYZE paises_enriched;
ANALYZE qualificacoes_socios_enriched;
