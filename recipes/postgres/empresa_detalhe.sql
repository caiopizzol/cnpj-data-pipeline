-- recipes/postgres/empresa_detalhe.sql
--
-- Per-estabelecimento denormalization joining empresas, estabelecimentos,
-- reference tables (cnaes, municipios, motivos, paises, naturezas_juridicas,
-- qualificacoes_socios), and dados_simples. Preserves all source codes
-- alongside their descriptions; no derived booleans, no label-substituted enums.
--
-- Apply after the pipeline finishes ingest. motivo/pais/qualificacao
-- descriptions come from the enriched lookups, so run that recipe first:
--     psql "$DATABASE_URL" -f recipes/postgres/reference_domains_enriched.sql
--     psql "$DATABASE_URL" -f recipes/postgres/empresa_detalhe.sql
--
-- Re-run after each monthly ingest to refresh.
--
-- Dependencies: reference_domains_enriched.sql (motivos_enriched,
-- paises_enriched, qualificacoes_socios_enriched).
--
-- Design choices (see docs/data-audit.md for the field-by-field rationale):
--   - motivo/pais/qualificacao descriptions come from the *_enriched lookups so
--     officially-resolved supplemental codes (e.g. motivo 32) get a description
--     instead of NULL. Each enriched table is 1:1 on codigo, so these LEFT JOINs
--     preserve row count exactly as the raw lookups did.
--   - LEFT JOIN on reference tables: defensive against retired codes in
--     historical snapshots. Codes with no row in any source stay NULL.
--   - dados_simples is LEFT JOINed (some companies have no record). The
--     columns are cnpj_basico-keyed - they repeat across every
--     estabelecimento of the same company by design.
--   - cnae_fiscal_secundaria stays as the source comma-separated string.
--     A future recipes/postgres/cnae_secundaria_exploded.sql will provide
--     the side table.
--   - cnpj column is materialized as basico||ordem||dv. Trivial in SQL
--     but avoids repeating the concatenation in every consumer query.

DROP TABLE IF EXISTS empresa_detalhe;
CREATE TABLE empresa_detalhe AS
SELECT
    e.cnpj_basico,
    s.cnpj_ordem,
    s.cnpj_dv,
    e.cnpj_basico || s.cnpj_ordem || s.cnpj_dv AS cnpj,
    -- empresa (company-level)
    e.razao_social,
    e.natureza_juridica,
    n.descricao AS natureza_juridica_descricao,
    e.qualificacao_responsavel,
    qr.descricao AS qualificacao_responsavel_descricao,
    e.capital_social,
    e.porte,
    e.ente_federativo_responsavel,
    -- estabelecimento (location-level)
    s.nome_fantasia,
    s.identificador_matriz_filial,
    s.situacao_cadastral,
    s.data_situacao_cadastral,
    s.motivo_situacao_cadastral,
    mo.descricao AS motivo_situacao_cadastral_descricao,
    s.nome_cidade_exterior,
    s.pais,
    p.descricao AS pais_descricao,
    s.data_inicio_atividade,
    s.cnae_fiscal_principal,
    c.descricao AS cnae_fiscal_principal_descricao,
    s.cnae_fiscal_secundaria,
    s.tipo_logradouro,
    s.logradouro,
    s.numero,
    s.complemento,
    s.bairro,
    s.cep,
    s.uf,
    s.municipio,
    m.descricao AS municipio_nome,
    s.ddd_1,
    s.telefone_1,
    s.ddd_2,
    s.telefone_2,
    s.ddd_fax,
    s.fax,
    s.correio_eletronico,
    s.situacao_especial,
    s.data_situacao_especial,
    -- dados_simples (company-level; repeats across estabelecimentos)
    ds.opcao_pelo_simples,
    ds.data_opcao_pelo_simples,
    ds.data_exclusao_do_simples,
    ds.opcao_pelo_mei,
    ds.data_opcao_pelo_mei,
    ds.data_exclusao_do_mei
FROM empresas e
JOIN estabelecimentos s USING (cnpj_basico)
LEFT JOIN cnaes c ON c.codigo = s.cnae_fiscal_principal
LEFT JOIN municipios m ON m.codigo = s.municipio
LEFT JOIN motivos_enriched mo ON mo.codigo = s.motivo_situacao_cadastral
LEFT JOIN paises_enriched p ON p.codigo = s.pais
LEFT JOIN qualificacoes_socios_enriched qr ON qr.codigo = e.qualificacao_responsavel
LEFT JOIN naturezas_juridicas n ON n.codigo = e.natureza_juridica
LEFT JOIN dados_simples ds ON ds.cnpj_basico = e.cnpj_basico;

-- Indexes for the most common filter shapes. Drop any you don't query.
CREATE INDEX IF NOT EXISTS idx_empresa_detalhe_uf ON empresa_detalhe (uf);
CREATE INDEX IF NOT EXISTS idx_empresa_detalhe_municipio ON empresa_detalhe (municipio);
CREATE INDEX IF NOT EXISTS idx_empresa_detalhe_cnae ON empresa_detalhe (cnae_fiscal_principal);
CREATE INDEX IF NOT EXISTS idx_empresa_detalhe_situacao ON empresa_detalhe (situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_empresa_detalhe_cnpj ON empresa_detalhe (cnpj);
CREATE INDEX IF NOT EXISTS idx_empresa_detalhe_basico ON empresa_detalhe (cnpj_basico);

ANALYZE empresa_detalhe;
