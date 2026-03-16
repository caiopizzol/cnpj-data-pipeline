# CNPJ Data Pipeline (v2)

[![Release](https://img.shields.io/github/v/release/caiopizzol/cnpj-data-pipeline)](https://github.com/caiopizzol/cnpj-data-pipeline/releases)
[![Python](https://img.shields.io/badge/python-3.11+-blue)](https://www.python.org)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![codecov](https://codecov.io/gh/caiopizzol/cnpj-data-pipeline/graph/badge.svg)](https://codecov.io/gh/caiopizzol/cnpj-data-pipeline)

Baixa e processa dados de empresas brasileiras da Receita Federal para PostgreSQL.

> [!IMPORTANT]
> **Novo em v1.3.2** — _A Receita Federal migrou os arquivos CNPJ para um novo repositório Nextcloud. Esta versão já suporta a nova URL e realiza downloads via WebDAV automaticamente. Nenhuma configuração adicional necessária._

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
just up-api  # Iniciar API (com build)
just down    # Parar PostgreSQL
just db      # Entrar no banco (psql)
just run     # Executar pipeline
just reset   # Limpar e reiniciar banco
just lint    # Verificar código
just format  # Formatar código
just test    # Rodar testes
just check   # Rodar todos (lint, format, test)
```

## Uso

```bash
just run                          # Processar mês mais recente
just run --list                   # Listar meses disponíveis
just run --month 2024-11          # Processar mês específico
just run --month 2024-11 --force  # Forçar reprocessamento
```

## Configuração

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5435/cnpj
BATCH_SIZE=500000
TEMP_DIR=./temp
DOWNLOAD_WORKERS=4
RETRY_ATTEMPTS=3
RETRY_DELAY=5
CONNECT_TIMEOUT=30
READ_TIMEOUT=300
KEEP_DOWNLOADED_FILES=false
```
## API

A API expõe os dados de empresas via HTTP. Para iniciar:

Configurações do servidor de api

```bash
# No .env
API_PORT=<Porta> # Porta do servidor api (8080 padrão)
API_WORKERS=<Workers> # Quantidade de workers ajuda a lidar com muitas requisições (2 padrão)
API_AUTH_ENABLED=false # Habilita autenticação via Bearer no Header (false padrão)
API_TOKEN=<SEU_TOKEN_AQUI> # Token de autenticação (null padrão)
```

```bash
just up-api  # Sobe PostgreSQL + API na porta 8080
```

### Endpoints

| Método | Rota               | Descrição                              |
|--------|--------------------|-----------------------------------------|
| GET    | `/empresa/{cnpj}`  | Retorna dados da empresa (14 dígitos)   |

### Exemplo

```bash
# sem token de autenticação
curl "http://127.0.0.1:8080/empresa/90400888000142"
# com autenticação
curl "http://127.0.0.1:8080/empresa/90400888000142" \
  -H "Authorization: Bearer <SUA_API_TOKEN>" 
```

### Ngrok (opcional)

Para expor a API publicamente via [ngrok](https://ngrok.com/):

```bash
# No .env
NGROK_ENABLED=true # Habilita o ngrok
NGROK_URL=<seu-dominio>.ngrok-free.dev # Seu domínio público no Ngrok
NGROK_AUTH_TOKEN=<seu-token> # Seu token de autenticação do ngrok
```

Depois, `just up-api` já subirá o túnel automaticamente.

<span style="color:#FFFF90">Obs: Caso não informe o token, o mesmo não será iniciado, iniciando apenas a api local rodando na porta definida no env.</span>

### Query
A query é baseada no schema da api do "CNPJÁ", para alterá-la ao seu gosto basta definir uma nova variável dentro do arquivo "query.py"

## Schema

> Documentação completa: [docs/data-schema.md](docs/data-schema.md)

```
EMPRESAS (1) ─── (N) ESTABELECIMENTOS
         ├─── (N) SOCIOS
         └─── (1) DADOS_SIMPLES
```

## Fonte de Dados

- **URL**: https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9
- **Atualização**: Mensal
