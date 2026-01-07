# CNPJ Data Pipeline (v2)

[![Release](https://img.shields.io/github/v/release/cnpj-chat/cnpj-data-pipeline)](https://github.com/cnpj-chat/cnpj-data-pipeline/releases)
[![Python](https://img.shields.io/badge/python-3.11+-blue)](https://www.python.org)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![codecov](https://codecov.io/gh/cnpj-chat/cnpj-data-pipeline/graph/badge.svg)](https://codecov.io/gh/cnpj-chat/cnpj-data-pipeline)

Baixa e processa dados de empresas brasileiras da Receita Federal para PostgreSQL.

## Requisitos

- [uv](https://docs.astral.sh/uv/) - `brew install uv`
- [just](https://github.com/casey/just) - `brew install just`
- Docker

## Início Rápido

```bash
cp .env.example .env
just up      # Iniciar PostgreSQL
just run     # Executar pipeline
```

## Comandos

```bash
just install # Instalar dependências
just up      # Iniciar PostgreSQL
just down    # Parar PostgreSQL
just db      # Entrar no banco (psql)
just run     # Executar pipeline
just reset   # Limpar e reiniciar banco
```

## Configuração

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5435/cnpj
BATCH_SIZE=50000
DOWNLOAD_WORKERS=4
```

## Schema

```
EMPRESAS (1) ─── (N) ESTABELECIMENTOS
         ├─── (N) SOCIOS
         └─── (1) DADOS_SIMPLES
```

### empresas

| Campo             | Descrição                |
| ----------------- | ------------------------ |
| cnpj_basico       | PK - 8 primeiros dígitos |
| razao_social      | Nome empresarial         |
| natureza_juridica | FK → naturezas_juridicas |
| capital_social    | Capital em R$            |
| porte             | 01=ME, 03=EPP, 05=Demais |

### estabelecimentos

| Campo                            | Descrição                                    |
| -------------------------------- | -------------------------------------------- |
| cnpj_basico, cnpj_ordem, cnpj_dv | PK composta (CNPJ completo)                  |
| identificador_matriz_filial      | 1=Matriz, 2=Filial                           |
| situacao_cadastral               | 02=Ativa, 03=Suspensa, 04=Inapta, 08=Baixada |
| cnae_fiscal_principal            | FK → cnaes                                   |
| municipio                        | FK → municipios                              |

### socios

| Campo                  | Descrição                    |
| ---------------------- | ---------------------------- |
| cnpj_basico            | FK → empresas                |
| identificador_de_socio | 1=PJ, 2=PF, 3=Estrangeiro    |
| cnpj_cpf_do_socio      | CPF mascarado (**\*XXXXXX**) |
| qualificacao_do_socio  | FK → qualificacoes_socios    |

### dados_simples

| Campo              | Descrição         |
| ------------------ | ----------------- |
| cnpj_basico        | PK, FK → empresas |
| opcao_pelo_simples | S=Sim, N=Não      |
| opcao_pelo_mei     | S=Sim, N=Não      |

## Fonte de Dados

- **URL**: https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj
- **Encoding**: ISO-8859-1
- **Separador**: `;`
- **Datas nulas**: `0` ou `00000000`
- **Atualização**: Mensal
