-- recipes/postgres/reference_domain_labels.sql
--
-- recipeVersion: 2
--
-- Static official domain labels for Receita enum fields that the raw monthly
-- CNPJ package ships NO lookup table for. Two grains:
--   - empresa / estabelecimento (consumed by empresa_detalhe): empresas.porte,
--     estabelecimentos.situacao_cadastral,
--     estabelecimentos.identificador_matriz_filial.
--   - socio (consumed by socios_detalhe): socios.identificador_de_socio,
--     socios.faixa_etaria.
-- The monthly delivery encodes all of these as bare codes with no accompanying
-- *.csv dictionary, so unlike motivos/paises/qualificacoes_socios there is no
-- base table to enrich - the labels have to be materialized from official
-- sources. These are pure static dictionaries, not monthly+supplemental tables,
-- so they carry no is_supplemental/monthly machinery.
--
-- This recipe is additive: it creates new lookup tables and never mutates the
-- raw tables (empresas, estabelecimentos, socios). It is idempotent (DROP TABLE
-- IF EXISTS + CREATE TABLE) and safe to re-run after any ingest.
--
-- Apply after the pipeline finishes ingest, BEFORE any recipe that wants these
-- descriptions (empresa_detalhe, socios_detalhe):
--     psql "$DATABASE_URL" -f recipes/postgres/reference_domain_labels.sql
--
-- Dependencies: none (static data; the JOIN targets are validated below).
--
-- Provenance columns on every row:
--     source_kind  'serpro_dominio' for codes carried in a SERPRO domain CSV
--                  (porte/situacao_cadastral/matriz_filial); 'receita_layout'
--                  for codes the SERPRO CSVs do not ship but the Receita CNPJ
--                  layout defines - every identificador_de_socio and
--                  faixa_etaria row, plus the lone porte '00' (NÃO INFORMADO).
--     source_url   the SERPRO domain CSV the code/label comes from, or the
--                  Receita layout PDF for the receita_layout rows.
-- descricao casing: the SERPRO-CSV rows (porte/situacao/matriz) and porte '00'
-- are kept VERBATIM from their source. The identificador_de_socio and
-- faixa_etaria rows are layout-DERIVED readable labels - the Receita layout
-- gives those two enums in prose, not a code|descricao table, so there is no
-- byte-verbatim string to copy; the labels follow the readable, title-cased
-- style of the rest of this recipe. Consumers that want uniform casing
-- normalize downstream, consistent with the sibling recipe.
--
-- AIDEV-NOTE: codigo's SQL type MUST match how the raw column is stored, or the
-- LEFT JOIN in empresa_detalhe / socios_detalhe silently yields NULL (or errors
-- on a text=int mismatch). Verified against initial.sql + processor.py:
--   - empresas.porte                              VARCHAR(2), zero-padded text -> codigo text  '00'/'01'/'03'/'05'
--   - estabelecimentos.situacao_cadastral         VARCHAR(2), zero-padded text -> codigo text  '01'..'08'
--   - estabelecimentos.identificador_matriz_filial INTEGER (cast Int32)         -> codigo integer 1/2
--   - socios.identificador_de_socio               VARCHAR(1), text             -> codigo text  '1'/'2'/'3'
--   - socios.faixa_etaria                         VARCHAR(1), text             -> codigo text  '0'..'9'
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

-- ---------------------------------------------------------------------------
-- identificadores_socio
-- ---------------------------------------------------------------------------
-- socios.identificador_de_socio (the sócio's type). The monthly SOCIOCSV ships
-- it as a bare code and SERPRO publishes no domain CSV for it, so the labels
-- come from the Receita CNPJ layout prose (cnpj-metadados.pdf): hence
-- source_kind 'receita_layout' on every row. codigo is text to match the raw
-- VARCHAR(1) column (see the AIDEV-NOTE above).
DROP TABLE IF EXISTS identificadores_socio;
CREATE TABLE identificadores_socio (
    codigo      text PRIMARY KEY,
    descricao   text NOT NULL,
    source_kind text NOT NULL,
    source_url  text NOT NULL
);
INSERT INTO identificadores_socio (codigo, descricao, source_kind, source_url) VALUES
    ('1', 'Pessoa Jurídica', 'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('2', 'Pessoa Física',   'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('3', 'Estrangeiro',     'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf');

-- ---------------------------------------------------------------------------
-- faixas_etarias
-- ---------------------------------------------------------------------------
-- socios.faixa_etaria (the sócio's age band). Same situation as
-- identificador_de_socio: bare code in the monthly delivery, no SERPRO domain
-- CSV, labels from the Receita CNPJ layout prose. Code '0' (NÃO SE APLICA) is a
-- real documented value, not a sentinel to drop here - that interpretation
-- belongs to socios_clean. codigo is text to match the raw VARCHAR(1) column.
DROP TABLE IF EXISTS faixas_etarias;
CREATE TABLE faixas_etarias (
    codigo      text PRIMARY KEY,
    descricao   text NOT NULL,
    source_kind text NOT NULL,
    source_url  text NOT NULL
);
INSERT INTO faixas_etarias (codigo, descricao, source_kind, source_url) VALUES
    ('0', 'Não se aplica',     'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('1', '0 a 12 anos',       'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('2', '13 a 20 anos',      'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('3', '21 a 30 anos',      'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('4', '31 a 40 anos',      'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('5', '41 a 50 anos',      'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('6', '51 a 60 anos',      'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('7', '61 a 70 anos',      'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('8', '71 a 80 anos',      'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'),
    ('9', 'Maiores de 80 anos', 'receita_layout', 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf');

ANALYZE portes_empresa;
ANALYZE situacoes_cadastrais;
ANALYZE indicadores_matriz_filial;
ANALYZE identificadores_socio;
ANALYZE faixas_etarias;
