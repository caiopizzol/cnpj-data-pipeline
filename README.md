<p align="center">
  <img src="https://github.com/user-attachments/assets/2ab351f0-a940-4b55-a875-d5773ddf0515" width="80" height="80" alt="cnpj.chat">
</p>

<h1 align="center">CNPJ Data Pipeline</h1>

<p align="center">
  Baixa e processa dados de empresas brasileiras da Receita Federal para PostgreSQL.
  <br>
  Parte do <a href="https://cnpj.chat">cnpj.chat</a> — dados públicos de empresas, acessíveis para todos.
</p>

<p align="center">
  <a href="https://github.com/caiopizzol/cnpj-data-pipeline/releases"><img src="https://img.shields.io/github/v/release/caiopizzol/cnpj-data-pipeline" alt="Release"></a>
  <a href="https://www.python.org"><img src="https://img.shields.io/badge/python-3.11+-blue" alt="Python"></a>
  <a href="https://github.com/astral-sh/ruff"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json" alt="Ruff"></a>
  <a href="https://codecov.io/gh/caiopizzol/cnpj-data-pipeline"><img src="https://codecov.io/gh/caiopizzol/cnpj-data-pipeline/graph/badge.svg" alt="codecov"></a>
</p>

> [!IMPORTANT]
> **Desde v1.3.2** — _A Receita Federal migrou os arquivos CNPJ para um novo repositório Nextcloud. Esta versão já suporta a nova URL e realiza downloads via WebDAV automaticamente. Nenhuma configuração adicional necessária._

> [!TIP]
> **Novo** — _Estratégia de carga configurável. Use `LOADING_STRATEGY=replace` para carga completa mais rápida (TRUNCATE + INSERT) ou `upsert` (default) para manter disponibilidade durante a carga._

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
LOADING_STRATEGY=upsert  # "upsert" ou "replace"
```

### Estratégia de carga

| Estratégia | Comando | Quando usar |
|------------|---------|-------------|
| `upsert` | `LOADING_STRATEGY=upsert just run` | Atualização incremental. Banco continua acessível durante a carga. |
| `replace` | `LOADING_STRATEGY=replace just run` | Carga completa mensal. Mais rápido — faz TRUNCATE e insere direto. |

## Schema

> Documentação completa: [docs/data-schema.md](docs/data-schema.md)

```
EMPRESAS (1) ─── (N) ESTABELECIMENTOS
         ├─── (N) SOCIOS
         └─── (1) DADOS_SIMPLES
```

## Fonte de Dados

Estes dados são **públicos e oficiais**, disponibilizados pela própria Receita Federal do Brasil.

| | |
|---|---|
| **Fonte** | [Portal de Dados Abertos — CNPJ](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) |
| **Repositório** | [Receita Federal — Nextcloud](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9) |
| **Atualização** | Mensal |
| **Formato** | CSV (`;` separador, ISO-8859-1) |
| **Base legal** | [Lei 12.527/2011](https://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm) (Lei de Acesso à Informação), art. 8° |
| **Regulamentação** | [Decreto 10.046/2019](https://www.planalto.gov.br/ccivil_03/_ato2019-2022/2019/decreto/D10046.htm) |
| **Nota Técnica** | [RFB/COCAD n° 47/2024](https://arquivos.receitafederal.gov.br/index.php/s/MPPfFit7g7zdA8C) (alterada pela NT 86/2024) |

A Coordenação-Geral de Gestão de Cadastros e Benefícios Fiscais (Cocad) classifica estes dados como **dados públicos, de livre acesso a qualquer interessado** (NT 47/2024, item 10). CPFs de sócios são mascarados conforme art. 198 da [Lei 5.172/1966](https://www.planalto.gov.br/ccivil_03/leis/l5172compilado.htm) (CTN).

Estes dados **não** são vazados, obtidos ilegalmente, ou protegidos por sigilo fiscal.

## Contribuidores

<a href="https://github.com/caiopizzol"><img src="https://github.com/caiopizzol.png" width="50" height="50" alt="caiopizzol" title="Caio Pizzol" /></a>
<a href="https://github.com/fabriciopereiradiniz"><img src="https://github.com/fabriciopereiradiniz.png" width="50" height="50" alt="fabriciopereiradiniz" title="Fabrício Pereira Diniz" /></a>
<a href="https://github.com/dversoza"><img src="https://github.com/dversoza.png" width="50" height="50" alt="dversoza" title="dversoza" /></a>
