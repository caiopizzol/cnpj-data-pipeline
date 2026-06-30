-- recipes/postgres/reference_domain_labels.sql
--
-- recipeVersion: 1
--
-- Static official domain labels for Receita enum fields that the raw monthly
-- CNPJ package ships NO lookup table for: empresas.porte,
-- estabelecimentos.situacao_cadastral, and
-- estabelecimentos.identificador_matriz_filial. The monthly delivery encodes
-- these as bare codes with no accompanying *.csv dictionary, so unlike
-- motivos/paises/qualificacoes_socios there is no base table to enrich - the
-- labels have to be materialized from the official SERPRO domain CSVs (SERPRO
-- runs the CNPJ cadastre). These are pure static dictionaries, not
-- monthly+supplemental tables, so they carry no is_supplemental/monthly
-- machinery. Almost every row is SERPRO-sourced; the lone exception is porte
-- '00' (NÃO INFORMADO), which the SERPRO porte CSV omits but the Receita CNPJ
-- layout defines and the monthly package emits, so it is sourced from the
-- Receita layout metadata instead (see the porte block below).
--
-- This recipe is additive: it creates new lookup tables and never mutates the
-- raw tables (empresas, estabelecimentos). It is idempotent (DROP TABLE IF
-- EXISTS + CREATE TABLE) and safe to re-run after any ingest.
--
-- Apply after the pipeline finishes ingest, BEFORE any recipe that wants these
-- descriptions:
--     psql "$DATABASE_URL" -f recipes/postgres/reference_domain_labels.sql
--
-- Dependencies: none (static data; the JOIN targets are validated below).
--
-- Provenance columns on every row:
--     source_kind  'serpro_dominio' for codes carried in the SERPRO domain CSV;
--                  'receita_layout' for porte '00' (NÃO INFORMADO), which the
--                  SERPRO porte CSV omits but the Receita CNPJ layout defines.
--     source_url   the SERPRO domain CSV the code/label comes from (or the
--                  Receita layout PDF for porte '00')
-- descricao is kept VERBATIM from its source; consumers that want uniform
-- casing normalize downstream, consistent with the sibling recipe.
--
-- AIDEV-NOTE: codigo's SQL type MUST match how the raw column is stored, or the
-- LEFT JOIN in empresa_detalhe silently yields NULL (or errors on a text=int
-- mismatch). Verified against initial.sql + processor.py:
--   - empresas.porte                              VARCHAR(2), zero-padded text -> codigo text  '00'/'01'/'03'/'05'
--   - estabelecimentos.situacao_cadastral         VARCHAR(2), zero-padded text -> codigo text  '01'..'08'
--   - estabelecimentos.identificador_matriz_filial INTEGER (cast Int32)         -> codigo integer 1/2
-- Do not "normalize" the matriz/filial codes to text: the raw column is INTEGER.

-- ---------------------------------------------------------------------------
-- portes_empresa
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS portes_empresa;
CREATE TABLE portes_empresa (
    codigo      text PRIMARY KEY,
    descricao   text NOT NULL,
    source_kind text NOT NULL,
    source_url  text NOT NULL
);
-- AIDEV-NOTE: porte '00' (NÃO INFORMADO) is NOT in the SERPRO porte_empresa.csv
-- (which lists only 01/03/05), but the monthly EMPRECSV emits it and the ingest
-- validator accepts it (processor.py _FORMAT_RULES: ^(00|01|03|05)$). It is a
-- known code, so it must resolve to a description instead of NULL. Its label
-- comes from the Receita CNPJ layout, hence source_kind 'receita_layout'.
INSERT INTO portes_empresa (codigo, descricao, source_kind, source_url) VALUES
    ('00', 'NÃO INFORMADO',            'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('01', 'Microempresa',             'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/porte_empresa.csv'),
    ('03', 'Empresa de Pequeno Porte', 'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/porte_empresa.csv'),
    ('05', 'Demais',                   'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/porte_empresa.csv');

-- ---------------------------------------------------------------------------
-- situacoes_cadastrais
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS situacoes_cadastrais;
CREATE TABLE situacoes_cadastrais (
    codigo      text PRIMARY KEY,
    descricao   text NOT NULL,
    source_kind text NOT NULL,
    source_url  text NOT NULL
);
-- AIDEV-NOTE: SERPRO's full situacao_cadastral domain, verbatim (01/02/03/04/05/08).
-- The ingest validator (processor.py _FORMAT_RULES: ^(01|02|03|04|08)$) only LOGS
-- a warning on out-of-set codes; _validate nullifies dates/UF/capital but never an
-- enum value, so a raw '05' (Ativa Não Regular) survives ingest. It is an official
-- SERPRO code and must resolve to its label, not silently become NULL via the LEFT
-- JOIN. Keep every SERPRO domain code here, same rationale as porte '00' above.
INSERT INTO situacoes_cadastrais (codigo, descricao, source_kind, source_url) VALUES
    ('01', 'Nula',              'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/situacao_cadastral.csv'),
    ('02', 'Ativa',             'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/situacao_cadastral.csv'),
    ('03', 'Suspensa',          'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/situacao_cadastral.csv'),
    ('04', 'Inapta',            'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/situacao_cadastral.csv'),
    ('05', 'Ativa Não Regular', 'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/situacao_cadastral.csv'),
    ('08', 'Baixada',           'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/situacao_cadastral.csv');

-- ---------------------------------------------------------------------------
-- indicadores_matriz_filial
-- ---------------------------------------------------------------------------
-- codigo is integer here (not zero-padded text) to match
-- estabelecimentos.identificador_matriz_filial, which the processor casts to
-- Int32 / stores as INTEGER. See the AIDEV-NOTE above.
DROP TABLE IF EXISTS indicadores_matriz_filial;
CREATE TABLE indicadores_matriz_filial (
    codigo      integer PRIMARY KEY,
    descricao   text NOT NULL,
    source_kind text NOT NULL,
    source_url  text NOT NULL
);
INSERT INTO indicadores_matriz_filial (codigo, descricao, source_kind, source_url) VALUES
    (1, 'Matriz', 'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/indicador_matriz.csv'),
    (2, 'Filial', 'serpro_dominio', 'https://bcadastros.serpro.gov.br/documentacao/dominios/pj/indicador_matriz.csv');

ANALYZE portes_empresa;
ANALYZE situacoes_cadastrais;
ANALYZE indicadores_matriz_filial;
