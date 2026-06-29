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
--       source_kind   'receita_monthly' | 'serpro_dominio' | 'receita_ods'
--       source_url     canonical URL for supplemental rows; NULL for monthly
--       is_supplemental true only for the added official rows
--       confidence    'high' | 'medium' (label uncertainty, not code validity)
--       notes         caveats (legacy code, label divergence, etc.)
--   - Descriptions are kept verbatim from their source. Consumers that want
--     uniform casing normalize downstream; this layer does not rewrite official
--     wording.
--
-- Sources (see docs/data-audit.md "Fontes oficiais"):
--   serpro_dominio: SERPRO domain CSVs (SERPRO runs the CNPJ cadastre),
--     https://bcadastros.serpro.gov.br/documentacao/dominios/pj/
--   receita_ods: Receita Federal open-data spreadsheet, used for qualificacao 36
--     (a legacy code absent from the current SERPRO collection CSVs).
-- Codes absent from both supplemental sources (SERPRO + Receita ODS) are left
-- unresolved on purpose; they still surface in the *_lookup_missing quality
-- flags. Known unresolved pais codes: 008, 009 (appear on Brazilian UFs, so
-- spurious) and 452 (absent from SERPRO; would need an MDIC/BACEN source).

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
SELECT
    s.codigo, s.descricao,
    'serpro_dominio'::text AS source_kind,
    'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/motivo_situacao_cadastral.csv'::text AS source_url,
    true AS is_supplemental,
    s.confidence, s.notes
FROM (
    VALUES
    ('32', 'Inexistente De Fato – Ade/Cosar', 'high',
     'Ausente do Motivos.csv mensal; presente na tabela de domínio SERPRO. Código 15 = "Inexistente De Fato"; 32 é a variante ADE/COSAR.')
) AS s(codigo, descricao, confidence, notes)
WHERE NOT EXISTS (SELECT 1 FROM motivos m WHERE m.codigo = s.codigo);
ALTER TABLE motivos_enriched ADD PRIMARY KEY (codigo);

-- NOTE: SERPRO's pais.csv stores codes unpadded (e.g. "15", "42"); the pipeline
-- pads estabelecimentos/socios.pais to 3 digits (zfill). The supplemental rows
-- below use the 3-digit form so they match the data's padded codes.

-- ---------------------------------------------------------------------------
-- paises_enriched
-- ---------------------------------------------------------------------------
-- Supplemental rows are the SERPRO country codes seen as orphans in the monthly
-- delivery. The single source_url applies to all of them.
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
SELECT
    s.codigo, s.descricao,
    'serpro_dominio'::text AS source_kind,
    'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/pais.csv'::text AS source_url,
    true AS is_supplemental,
    s.confidence, s.notes
FROM (
    VALUES
    ('015', 'ALAND, ILHAS', 'high', NULL),
    ('042', 'ANTÁRTICA', 'high', NULL),
    ('150', 'JERSEY, ILHA DO CANAL', 'medium',
     'SERPRO rotula "Jersey"; a tabela Siscomex/ME rotula "Guernsey" o mesmo código. Validade do código alta; rótulo divergente entre fontes.'),
    ('151', 'CANÁRIAS, ILHAS', 'high', NULL),
    ('200', 'CURACAO', 'high', NULL),
    ('321', 'GUERNSEY', 'high', NULL),
    ('359', 'MAN, ILHA DE', 'high', NULL),
    ('367', 'INGLATERRA', 'high', NULL),
    ('393', 'JERSEY', 'high', NULL),
    ('449', 'MACEDÔNIA, ANT.REP.IUGOSLAVA', 'high', NULL),
    ('498', 'MONTENEGRO', 'high', NULL),
    ('578', 'PALESTINA', 'high', NULL),
    ('678', 'SAINT KITTS E NEVIS', 'high', NULL),
    ('693', 'SAO BARTOLOMEU', 'high', NULL),
    ('699', 'SÃO MARTINHO, ILHA DE (PARTE HOLANDESA)', 'high', NULL),
    ('737', 'SERVIA', 'high', NULL),
    ('755', 'SVALBARD E JAN MAYEN', 'high', NULL),
    ('994', 'A DESIGNAR', 'high', 'Placeholder administrativo ("a designar"), não um país real.')
) AS s(codigo, descricao, confidence, notes)
WHERE NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = s.codigo);
ALTER TABLE paises_enriched ADD PRIMARY KEY (codigo);

-- ---------------------------------------------------------------------------
-- qualificacoes_socios_enriched
-- ---------------------------------------------------------------------------
-- Code 36 (Gerente-Delegado) is a legacy qualification: Receita's open-data
-- table marks it COLETADO ATUALMENTE = "Não", which is why it is absent from
-- the current SERPRO collection CSVs but still appears in old records.
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
FROM qualificacoes_socios q
UNION ALL
SELECT
    s.codigo, s.descricao,
    'receita_ods'::text AS source_kind,
    'https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/cnpj/tabela-de-qualificacao-do-socio-representante.ods'::text AS source_url,
    true AS is_supplemental,
    s.confidence, s.notes
FROM (
    VALUES
    ('36', 'Gerente-Delegado', 'high',
     'Código legado (COLETADO ATUALMENTE="Não" na tabela ODS oficial da Receita); ausente das CSVs de coleta atuais do SERPRO. Corroborado pela norma idArquivoBinario=18132.')
) AS s(codigo, descricao, confidence, notes)
WHERE NOT EXISTS (SELECT 1 FROM qualificacoes_socios q WHERE q.codigo = s.codigo);
ALTER TABLE qualificacoes_socios_enriched ADD PRIMARY KEY (codigo);

ANALYZE motivos_enriched;
ANALYZE paises_enriched;
ANALYZE qualificacoes_socios_enriched;
