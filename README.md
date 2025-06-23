# 🇧🇷 CNPJ Data Pipeline

Um script modular e configurável para processar arquivos CNPJ da Receita Federal do Brasil. Processamento inteligente de 63+ milhões de empresas com suporte a múltiplos bancos de dados.

## Características Principais

- **Arquitetura Modular**: Separação clara de responsabilidades com camada de abstração de banco de dados
- **Multi-Banco**: PostgreSQL totalmente suportado, com placeholders para MySQL, BigQuery e SQLite
- **Processamento Inteligente**: Adaptação automática da estratégia baseada em recursos disponíveis
- **Downloads Paralelos**: Estratégia configurável para otimizar velocidade de download
- **Processamento Incremental**: Rastreamento de arquivos processados para evitar duplicações
- **Performance Otimizada**: Operações bulk eficientes com tratamento de conflitos
- **Configuração Simples**: Setup interativo + variáveis de ambiente

## Início Rápido

### Opção 1: Setup Interativo (Recomendado)

```bash
# Clone o repositório
git clone https://github.com/cnpj-chat/cnpj-data-pipeline
cd cnpj-data-pipeline

# Execute o assistente de configuração
python setup.py
```

O assistente irá:
- Detectar recursos do sistema
- Configurar conexão com banco de dados
- Instalar dependências necessárias
- Criar configuração otimizada

### Opção 2: Configuração Manual

```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar ambiente
cp env.example .env
# Editar .env com suas configurações

# Executar
python main.py
```

### Docker

```bash
# PostgreSQL (padrão)
docker-compose --profile postgres up --build

# Com configurações customizadas
DATABASE_BACKEND=postgresql BATCH_SIZE=100000 docker-compose --profile postgres up

# Com filtros de dados
docker-compose run --rm pipeline --filter-uf SP --filter-cnae 62
```

## Filtragem de Dados

Processe apenas os dados que você precisa com filtros via linha de comando:

```bash
# Filtrar por estado (UF)
python main.py --filter-uf SP
python main.py --filter-uf SP,RJ,MG

# Filtrar por atividade econômica (CNAE)
python main.py --filter-cnae 62
python main.py --filter-cnae 62,47

# Filtrar por porte da empresa
python main.py --filter-porte 1,3  # ME e EPP

# Combinar filtros
python main.py --filter-uf SP --filter-cnae 62 --filter-porte 1

# Listar filtros disponíveis
python main.py --list-filters
```

### Filtros Disponíveis

| Filtro | Descrição | Exemplo |
|--------|-----------|---------|
| `--filter-uf` | Estados brasileiros | `SP,RJ,MG` |
| `--filter-cnae` | Códigos de atividade (prefixo) | `62,47` |
| `--filter-porte` | Porte: 1=ME, 3=EPP, 5=Demais | `1,3` |

## Configuração

### Seleção de Backend

```bash
# PostgreSQL (padrão e recomendado)
DATABASE_BACKEND=postgresql

# Suporte futuro
# DATABASE_BACKEND=mysql
# DATABASE_BACKEND=bigquery
# DATABASE_BACKEND=sqlite
```

### Estratégias de Processamento

O sistema detecta automaticamente a estratégia ideal:

| Memória | Estratégia | Descrição |
|---------|------------|-----------|
| <8GB | `memory_constrained` | Processamento em chunks pequenos |
| 8-32GB | `high_memory` | Batches maiores, cache otimizado |
| >32GB | `distributed` | Processamento paralelo máximo |

### Variáveis de Configuração

| Variável | Padrão | Descrição |
|----------|---------|-----------|
| `BATCH_SIZE` | `50000` | Tamanho do lote para operações |
| `MAX_MEMORY_PERCENT` | `80` | Uso máximo de memória |
| `TEMP_DIR` | `./temp` | Diretório temporário |
| `DB_HOST` | `localhost` | Host PostgreSQL |
| `DB_PORT` | `5432` | Porta PostgreSQL |
| `DB_NAME` | `cnpj` | Nome do banco |

### Otimização de Performance

| Variável | Padrão | Descrição |
|----------|---------|-----------|
| `DOWNLOAD_STRATEGY` | `sequential` | `sequential` ou `parallel` |
| `DOWNLOAD_WORKERS` | `4` | Número de downloads paralelos |
| `KEEP_DOWNLOADED_FILES` | `false` | Manter arquivos para re-execuções |

## Deployment

Este é um job batch que processa dados CNPJ e finaliza. A Receita Federal atualiza os dados mensalmente, então agende a execução mensal.

### Execução Manual

```bash
# Executar uma vez
docker-compose up

# Com downloads paralelos
DOWNLOAD_STRATEGY=parallel DOWNLOAD_WORKERS=3 docker-compose up

# Manter arquivos para re-execuções (economiza bandwidth)
KEEP_DOWNLOADED_FILES=true docker-compose up

# Ou sem Docker
python main.py

# Com filtros
python main.py --filter-uf SP --filter-cnae 62
```

### Execução Agendada (Mensal)

**Linux/Mac (cron):**
```bash
# Executar no dia 5 de cada mês às 2h da manhã
crontab -e
# Adicionar:
0 2 5 * * cd /caminho/para/cnpj-data-pipeline && docker-compose up >> /var/log/cnpj-pipeline.log 2>&1
```

**Windows (Task Scheduler):**
- Criar tarefa agendada mensal
- Comando: `docker-compose up`

**Kubernetes:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: cnpj-pipeline
spec:
  schedule: "0 2 5 * *"  # Dia 5 às 2h
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: cnpj-pipeline
            image: sua-imagem
          restartPolicy: OnFailure
```

**GitHub Actions:**
```yaml
on:
  schedule:
    - cron: '0 2 5 * *'  # Dia 5 às 2h UTC
```

### Plataformas que Requerem Containers Ativos

Algumas plataformas (PaaS) esperam que containers permaneçam em execução. Se necessário:

```bash
# Manter container ativo
docker run -d --name cnpj sua-imagem tail -f /dev/null

# Agendar execução mensal do comando:
docker exec cnpj python main.py
```

## Arquitetura

```
cnpj-data-pipeline/
├── src/
│   ├── config.py          # Configuração com auto-detecção
│   ├── downloader.py      # Download e extração
│   ├── processor.py       # Parsing e transformação
│   ├── filters/           # Sistema de filtros
│   │   ├── base.py        # Interface de filtros
│   │   ├── location.py    # Filtros geográficos
│   │   ├── business.py    # Filtros de negócio
│   │   └── registry.py    # Factory de filtros
│   ├── download_strategies/ # Estratégias de download
│   │   ├── sequential.py  # Download sequencial
│   │   └── parallel.py    # Download paralelo
│   └── database/          # Abstração de banco de dados
│       ├── base.py        # Interface abstrata
│       ├── factory.py     # Factory pattern
│       └── postgres.py    # Implementação PostgreSQL
├── main.py                # Ponto de entrada
└── setup.py               # Assistente de configuração
```

## Fluxo de Processamento

1. **Descoberta**: Localiza diretório mais recente de dados CNPJ
2. **Download**: Baixa e extrai arquivos ZIP com retry automático (paralelo opcional)
3. **Filtragem**: Aplica filtros selecionados para reduzir dados processados
4. **Processamento**: Parse dos CSVs com estratégia adaptativa
5. **Carga**: Bulk upsert otimizado no banco de dados
6. **Rastreamento**: Marca arquivos como processados

## Tipos de Arquivo Suportados

| Arquivo | Tabela | Descrição |
|---------|--------|-----------|
| `CNAECSV` | `cnaes` | Classificações de atividade econômica |
| `EMPRECSV` | `empresas` | Registros de empresas |
| `ESTABELECSV` | `estabelecimentos` | Dados de estabelecimentos |
| `MOTICSV` | `motivos_situacao_cadastral` | Motivos de situação cadastral |
| `MUNICCSV` | `municipios` | Códigos de municípios |
| `NATJUCSV` | `naturezas_juridicas` | Naturezas jurídicas |
| `PAISCSV` | `paises` | Códigos de países |
| `QUALSCSV` | `qualificacoes_socios` | Qualificações de sócios |
| `SIMPLECSV` | `dados_simples` | Dados do Simples Nacional |
| `SOCIOCSV` | `socios` | Quadro societário |

## Performance

Tempos típicos de processamento:

| Sistema | Memória | Tempo (63M+ empresas) |
|---------|---------|---------------------|
| VPS básico | 4GB | ~8 horas |
| Servidor padrão | 16GB | ~2 horas |
| Servidor high-end | 64GB+ | ~1 hora |

## Desenvolvimento

### Princípios de Design

- **Modular**: Cada componente com responsabilidade única
- **Resiliente**: Tratamento de erros e retry automático
- **Eficiente**: Uso otimizado de memória e operações bulk
- **Adaptativo**: Ajuste automático aos recursos disponíveis

### Adicionando Novo Backend

1. Criar adapter em `src/database/seu_banco.py`
2. Implementar métodos abstratos de `DatabaseAdapter`
3. Registrar no factory em `src/database/factory.py`
4. Criar arquivo de requirements em `requirements/seu_banco.txt`

---

# 🇧🇷 CNPJ Data Pipeline (English)

A configurable, modular data pipeline for Brazilian CNPJ registry files. Smart processing of 60+ million companies with multi-database support.

## Key Features

- **Modular Architecture**: Clean separation with database abstraction
- **Multi-Database**: Full PostgreSQL support, placeholders for others
- **Smart Processing**: Auto-adapts to available resources
- **Advanced Filtering**: Filter by state, CNAE, and company size via CLI
- **Parallel Downloads**: Configurable strategy for optimized download speed
- **Incremental**: Tracks processed files
- **Optimized**: Efficient bulk operations
- **Easy Config**: Interactive setup + env vars

## Quick Start

### Interactive Setup

```bash
python setup.py
```

### Manual Setup

```bash
pip install -r requirements.txt
cp env.example .env
python main.py
```

### Docker

```bash
docker-compose --profile postgres up --build

# With data filtering
docker-compose run --rm pipeline --filter-uf SP --filter-cnae 62

# With parallel downloads
DOWNLOAD_STRATEGY=parallel DOWNLOAD_WORKERS=3 docker-compose up
```

## Data Filtering

Process only the data you need using command-line filters:

```bash
# Filter by state
python main.py --filter-uf SP,RJ

# Filter by economic activity (CNAE code prefix)
python main.py --filter-cnae 62,47

# Filter by company size (1=ME, 3=EPP, 5=Others)
python main.py --filter-porte 1,3

# Combine filters
python main.py --filter-uf SP --filter-cnae 62 --filter-porte 1

# List available filters
python main.py --list-filters
```

## Deployment

This is a batch job that processes CNPJ data and exits. Schedule it to run monthly.

### Manual Execution

```bash
# Run once
docker-compose up
```

### Scheduled Execution (Monthly)

**Linux/Mac (cron):**
```bash
# Run on the 5th of each month at 2 AM
0 2 5 * * cd /path/to/cnpj-pipeline && docker-compose up
```

**Other platforms:** Use your platform's scheduler (Task Scheduler, Kubernetes CronJob, GitHub Actions, etc.)

### Note for PaaS Platforms

If your platform requires containers to stay running:

```bash
# Keep container alive
docker run -d --name cnpj your-image tail -f /dev/null

# Schedule this command monthly:
docker exec cnpj python main.py
```

## Configuration

Set `DATABASE_BACKEND`, `PROCESSING_STRATEGY`, and optimization options in `.env` file:

```bash
# Performance optimizations
DOWNLOAD_STRATEGY=parallel    # sequential|parallel
DOWNLOAD_WORKERS=4           # Number of parallel downloads
KEEP_DOWNLOADED_FILES=false  # Keep files for re-runs (saves bandwidth)
```

## Architecture

Factory pattern for database adapters, intelligent resource detection, chunked processing for large files.

## Performance

Processes 63M+ records in 1-12 hours depending on system resources.

Made with engineering excellence for the Brazilian tech community.

## 📋 Legalidade e Origem dos Dados

### ✅ Dados 100% Legais e Oficiais

Estes dados são **públicos e oficiais**, disponibilizados pela própria Receita Federal através do Portal de Dados Abertos do Governo Federal:

- **Fonte oficial**: [Portal de Dados Abertos](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
- **Base legal**: [Lei de Acesso à Informação (Lei 12.527/2011)](https://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm)
- **Atualização**: Mensal pela Receita Federal

### ⚠️ O que NÃO são estes dados:
- ❌ NÃO são dados vazados ou obtidos ilegalmente
- ❌ NÃO contêm informações pessoais de pessoas físicas
- ❌ NÃO são os "CDs de dados" vendidos ilegalmente
- ❌ NÃO violam LGPD ou privacidade

### 📌 Uso Responsável
Embora os dados sejam públicos, use-os com responsabilidade:
- ✅ Análises de mercado e econômicas
- ✅ Pesquisa acadêmica
- ✅ Desenvolvimento de produtos B2B
- ✅ Jornalismo de dados
- ⚠️ Sempre respeite limites éticos
- ⚠️ Não use para spam ou assédio comercial
- 🔒 Respeite sempre a [LGPD](https://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm).

[![Dados Oficiais](https://img.shields.io/badge/Dados-Oficiais%20Receita%20Federal-green)](https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos)
[![Lei de Acesso à Informação](https://img.shields.io/badge/LAI-12.527%2F2011-blue)](http://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm)
[![LGPD Compliant](https://img.shields.io/badge/LGPD-Compliant-brightgreen)](https://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm)


=======
Made with ❤️ for the Brazilian tech community
