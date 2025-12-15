-- CNPJ Data Pipeline - Database Schema
-- Run automatically on first docker compose up

-- ============================================================================
-- Reference Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS cnaes (
    codigo VARCHAR(7) PRIMARY KEY,
    descricao TEXT,
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS motivos (
    codigo VARCHAR(2) PRIMARY KEY,
    descricao TEXT,
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS municipios (
    codigo VARCHAR(7) PRIMARY KEY,
    descricao TEXT,
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS naturezas_juridicas (
    codigo VARCHAR(4) PRIMARY KEY,
    descricao TEXT,
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS paises (
    codigo VARCHAR(3) PRIMARY KEY,
    descricao TEXT,
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS qualificacoes_socios (
    codigo VARCHAR(2) PRIMARY KEY,
    descricao TEXT,
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL
);

-- ============================================================================
-- Main Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS empresas (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    razao_social TEXT,
    natureza_juridica VARCHAR(4),
    qualificacao_responsavel VARCHAR(2),
    capital_social DOUBLE PRECISION,
    porte VARCHAR(2),
    ente_federativo_responsavel TEXT,
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS estabelecimentos (
    cnpj_basico VARCHAR(8) NOT NULL,
    cnpj_ordem VARCHAR(4) NOT NULL,
    cnpj_dv VARCHAR(2) NOT NULL,
    identificador_matriz_filial INTEGER,
    nome_fantasia TEXT,
    situacao_cadastral VARCHAR(2),
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral VARCHAR(2),
    nome_cidade_exterior TEXT,
    pais VARCHAR(3),
    data_inicio_atividade DATE,
    cnae_fiscal_principal VARCHAR(7),
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep VARCHAR(8),
    uf VARCHAR(2),
    municipio VARCHAR(7),
    ddd_1 VARCHAR(4),
    telefone_1 VARCHAR(8),
    ddd_2 VARCHAR(4),
    telefone_2 VARCHAR(8),
    ddd_fax VARCHAR(4),
    fax VARCHAR(8),
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial DATE,
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);

CREATE TABLE IF NOT EXISTS socios (
    cnpj_basico VARCHAR(8) NOT NULL,
    identificador_de_socio VARCHAR(1) NOT NULL,
    nome_socio TEXT,
    cnpj_cpf_do_socio VARCHAR(14) NOT NULL,
    qualificacao_do_socio VARCHAR(2),
    data_entrada_sociedade DATE,
    pais VARCHAR(3),
    representante_legal VARCHAR(11),
    nome_do_representante TEXT,
    qualificacao_do_representante_legal VARCHAR(2),
    faixa_etaria VARCHAR(1),
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL,
    PRIMARY KEY (cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio)
);

CREATE TABLE IF NOT EXISTS dados_simples (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    opcao_pelo_simples VARCHAR(1),
    data_opcao_pelo_simples DATE,
    data_exclusao_do_simples DATE,
    opcao_pelo_mei VARCHAR(1),
    data_opcao_pelo_mei DATE,
    data_exclusao_do_mei DATE,
    data_criacao TIMESTAMP DEFAULT NOW() NOT NULL,
    data_atualizacao TIMESTAMP DEFAULT NOW() NOT NULL
);

-- ============================================================================
-- Tracking Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS processed_files (
    directory VARCHAR(50) NOT NULL,
    filename VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (directory, filename)
);

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_estabelecimentos_uf ON estabelecimentos(uf);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_municipio ON estabelecimentos(municipio);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_situacao ON estabelecimentos(situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnae ON estabelecimentos(cnae_fiscal_principal);
CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios(cnpj_basico);
