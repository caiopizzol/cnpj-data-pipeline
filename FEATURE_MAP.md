# Mapa de features

Como usar as features do CNPJ Data Pipeline 1.38.3. Veja o escopo em [PRODUCT.md](PRODUCT.md).
`verified` = comando executado e referências conferidas. `partial` = falta testar a rota completa.

Comece na raiz de uma cópia limpa do repositório, com Python 3.11+ e `uv`:

```bash
uv sync --frozen
```

Use `uv run --frozen python main.py`: `uv sync` não instala o atalho `cnpj-pipeline`.
As opções, variáveis e nomes de tabelas abaixo servem de referência para automação.
Não há interface gráfica nem atalhos de teclado próprios. Opções curtas: `-h`, `-l`, `-m` e `-f`.

### Banco para testar

O relatório e as receitas precisam de Docker, `psql` e porta 5435 livre.
Use o banco descartável abaixo: as receitas recriam tabelas, então não rode em produção.

```bash
docker run -d --name cnpj-feature-map-db \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=cnpj_map -p 127.0.0.1:5435:5432 postgres:16-alpine
export DATABASE_URL=postgresql://postgres:postgres@localhost:5435/cnpj_map
for attempt in $(seq 1 30); do
  pg_isready -h localhost -p 5435 && break
  sleep 1
done
pg_isready -h localhost -p 5435
uv run --frozen python - <<'PYCODE'
import os
from pathlib import Path
from database import Database
from processor import process_file

db = Database(os.environ['DATABASE_URL'])
db.ensure_schema()
for name in ('CNAECSV.csv', 'MOTICSV.csv', 'MUNICCSV.csv', 'NATJUCSV.csv',
             'PAISCSV.csv', 'QUALSCSV.csv', 'EMPRECSV.csv', 'ESTABELE.csv',
             'SOCIOCSV.csv', 'SIMPLESCSV.csv'):
    for batch, table, columns in process_file(Path('tests/fixtures') / name):
        db.bulk_upsert(batch, table, columns)
db.disconnect()
PYCODE
```

Isso carrega os dados de teste do repositório (fixtures), sem baixar o mês completo.
Ao terminar: `docker rm -f cnpj-feature-map-db`.

### O que foi testado

Os comandos `verified` foram repetidos em outra cópia limpa, com banco novo. Nas receitas, isso confirma execução e leitura dos dados de teste, sem medir desempenho na base completa.

Os testes automatizados são uma checagem separada. Os testes SQL preparam suas próprias dependências. Rode todos ou escolha um arquivo:

```bash
uv run --frozen pytest tests/integration -q
```

Use o PostgreSQL acima: os testes criam e removem `cnpj_test`, `cnpj_test_empty` e `cnpj_test_source`.
Nas receitas, a classe indicada fica em [tests/integration](tests/integration); o método `test_recipe_executes` confirma a criação das tabelas.
Todas as receitas partem do banco de teste carregado. Cada bloco já inclui suas dependências.

## Listar meses

**CLI · verified.** Precisa de internet; dispensa banco.

```bash
uv run --frozen python main.py --help
CONNECT_TIMEOUT=10 READ_TIMEOUT=20 uv run --frozen python main.py --list
```

Referências: `--list` (`-l`), `--help` (`-h`). Código: [main.py](main.py) `parse_args/main`, [downloader.py](downloader.py) `get_available_directories`.
Executado: ajuda e listagem na Receita. Testes: [test_main.py](tests/test_main.py) `TestMain.test_list_mode_never_touches_database` confirma que listar não abre o banco; [test_downloader.py](tests/test_downloader.py) `TestGetAvailableDirectories` cobre a resposta WebDAV. A fonte pode ficar indisponível.

## Rodar pelo Docker

**Docker · verified para ajuda e listagem.** Precisa de Docker e internet.

```bash
docker build -t cnpj-feature-map:local .
docker run --rm cnpj-feature-map:local --help
docker run --rm -e CONNECT_TIMEOUT=10 -e READ_TIMEOUT=20 cnpj-feature-map:local --list
```

Referências: `--help`, `--list`. Código: [Dockerfile](Dockerfile), [main.py](main.py).
Executado: build, ajuda e listagem. Sem teste dedicado da imagem; `TestMain` em [test_main.py](tests/test_main.py) cobre a lógica Python. Imagem publicada e carga via Compose não testadas.

## Carregar um mês no PostgreSQL

**CLI · partial.** Precisa do banco acima, internet e espaço para o mês completo.

```bash
OUTPUT_FORMAT=postgres LOADING_STRATEGY=upsert uv run --frozen python main.py --month 2024-11
```

Referências: `--month` (`-m`), `OUTPUT_FORMAT`, `LOADING_STRATEGY`, `processed_files`. Sem `--month`, usa o mês mais recente.
Código: [main.py](main.py) `main/pg_worker`, [database.py](database.py), [initial.sql](initial.sql).
Executado apenas com dados de teste, pelo preparo do banco. Download mensal não testado.
Testes: [testes de integração](tests/integration) `TestFullPipeline` verifica carga e recarga; [test_main.py](tests/test_main.py) `TestParseArgs` verifica as opções. Isso não garante atualizar a base inteira de uma só vez.

## Retomar ou reprocessar a carga

**CLI PostgreSQL · partial.** Use a configuração e o mês da carga anterior. Retomar pula arquivos já registrados; `--force` limpa o registro do mês.

```bash
OUTPUT_FORMAT=postgres LOADING_STRATEGY=upsert uv run --frozen python main.py --month 2024-11
OUTPUT_FORMAT=postgres LOADING_STRATEGY=upsert uv run --frozen python main.py --month 2024-11 --force
```

Referências: `processed_files`, `--force` (`-f`). Código: [main.py](main.py), [database.py](database.py) `get_processed_files/clear_processed_files`.
Mês completo não testado. Teste: [test_main.py](tests/test_main.py) `TestMain.test_force_clears_processed_files` verifica a limpeza com dependências simuladas; [test_database.py](tests/test_database.py) cobre o registro. `--force` só vale para PostgreSQL.

## Substituir tabelas na carga

**CLI PostgreSQL · partial.** Use banco descartável e espaço para o mês completo. `replace` apaga o conteúdo das tabelas carregadas antes de inserir os dados novos.

```bash
OUTPUT_FORMAT=postgres LOADING_STRATEGY=replace uv run --frozen python main.py --month 2024-11 --force
```

Referências: `LOADING_STRATEGY=replace`, `--force`. Código: [main.py](main.py), [database.py](database.py) `bulk_insert/truncate_table`.
Mês completo não testado. Teste: [testes de integração](tests/integration) `TestFullPipeline.test_replace_handles_cross_batch_pk_overlap` verifica chaves repetidas entre lotes; `test_replace_strategy` verifica a substituição com fixtures.

## Baixar e processar em paralelo

**CLI · partial.** Precisa de banco descartável, internet e espaço para o mês completo.

```bash
OUTPUT_FORMAT=postgres DOWNLOAD_WORKERS=4 PROCESS_WORKERS=2 uv run --frozen python main.py --month 2024-11
```

Referências: `DOWNLOAD_WORKERS`, `PROCESS_WORKERS`, `STALL_TIMEOUT`, `STALL_DEGRADE_THRESHOLD`. Código: [downloader.py](downloader.py), [main.py](main.py), [config.py](config.py).
Mês completo e falhas de rede não testados ao vivo. Testes: [test_downloader.py](tests/test_downloader.py) `TestResumeEdgeCases` e `TestAdaptiveDownloadIntegration` cobrem retomada e redução de downloads simultâneos com rede simulada; [test_main.py](tests/test_main.py) `TestParallelProcessing` cobre o processamento em paralelo.

## Exportar Parquet

**CLI · partial.** Precisa de internet e pasta de saída nova, com espaço para o mês. Dispensa banco.

```bash
OUTPUT_FORMAT=parquet PARQUET_OUTPUT_DIR=./parquet-string PARQUET_TYPED_OUTPUT=false uv run --frozen python main.py --month 2024-11
```

Referências: `OUTPUT_FORMAT`, `PARQUET_OUTPUT_DIR`, `manifest.json`, `schemaVersion`, `sourceMonth`. Código: [main.py](main.py), [parquet_writer.py](parquet_writer.py).
Mês completo não testado. Testes: [test_main.py](tests/test_main.py) `TestParquetOutput.test_writes_parquet_and_manifest` usa download simulado; [test_parquet_writer.py](tests/test_parquet_writer.py) verifica arquivos, metadados e ZSTD reais.
Arquivo existente faz a tabela ser pulada, mesmo se incompleto ou de outro mês. `TestParquetResume` cobre essa regra.

## Exportar Parquet com tipos

**CLI · partial.** Mesmos requisitos do Parquet, com outra pasta nova.

```bash
OUTPUT_FORMAT=parquet PARQUET_OUTPUT_DIR=./parquet-typed PARQUET_TYPED_OUTPUT=true uv run --frozen python main.py --month 2024-11
```

Referência: `PARQUET_TYPED_OUTPUT`. Código: [processor.py](processor.py) `apply_typed_casts`, [main.py](main.py).
Mês completo não testado. Teste: [test_processor.py](tests/test_processor.py) `TestTypedCasts` verifica tipos de datas e números. `schemaVersion` é igual nos modos string e tipado; confira os tipos no arquivo.

## Rodar um comando após exportar cada tabela

**CLI Parquet · partial.** Mesmos requisitos do Parquet, com pasta nova. O exemplo imprime o caminho de cada arquivo fechado.

```bash
OUTPUT_FORMAT=parquet PARQUET_OUTPUT_DIR=./parquet-hook POST_FILE_COMMAND=echo uv run --frozen python main.py --month 2024-11
```

Referência: `POST_FILE_COMMAND`. Código: [main.py](main.py) `main`.
Mês completo não testado. Teste: [test_main.py](tests/test_main.py) `TestParquetOutput.test_post_file_command_runs_per_table` verifica a chamada por tabela com subprocesso simulado. O comando é separado por espaços e recebe o caminho como último argumento; não é um script de shell.

## Medir qualidade

**CLI PostgreSQL · verified nos dados de teste.** Use o banco acima. Sem as receitas, referências enriquecidas aparecem como indisponíveis; depois delas, entram na medição.

```bash
uv run --frozen python scripts/data_quality_report.py
uv run --frozen python scripts/data_quality_report.py --full
uv run --frozen python scripts/data_quality_report.py --sample-pct 0.5
```

Referências: `DATABASE_URL`, `--full`, `--sample-pct`. Código: [scripts/data_quality_report.py](scripts/data_quality_report.py) `main`.
Executado: os três modos geraram relatório. Testes: [test_data_quality_report.py](tests/test_data_quality_report.py) verifica DV e amostragem; [testes de integração](tests/integration) `TestDataQualityReportMeasurements` verifica referências ausentes e enriquecidas.
Só o dígito verificador (DV) usa amostra por padrão; o restante lê tudo. Problemas encontrados não fazem o comando terminar com erro.

## Referências enriquecidas

**SQL · verified nos dados de teste.** Acrescenta códigos oficiais ausentes do mês e registra a origem.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM motivos_enriched LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM paises_enriched LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM qualificacoes_socios_enriched LIMIT 1;"
```

Referências: `motivos_enriched`, `paises_enriched`, `qualificacoes_socios_enriched`; colunas `codigo, source_kind, source_url, is_supplemental`. Código: [receita](recipes/postgres/reference_domains_enriched.sql).
Teste: `TestRecipeReferenceDomainsEnriched`.

## Rótulos de códigos

**SQL · verified nos dados de teste.** Cria descrições para códigos que não têm CSV próprio.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domain_labels.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM portes_empresa LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM situacoes_cadastrais LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM indicadores_matriz_filial LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM identificadores_socio LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM faixas_etarias LIMIT 1;"
```

Referências: `portes_empresa`, `situacoes_cadastrais`, `indicadores_matriz_filial`, `identificadores_socio`, `faixas_etarias`; colunas `codigo, descricao`. Código: [receita](recipes/postgres/reference_domain_labels.sql).
Teste: `TestRecipeDomainLabels`.

## Detalhes por estabelecimento

**SQL · verified nos dados de teste.** Junta dados da empresa, estabelecimento, referências e Simples.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domain_labels.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/empresa_detalhe.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM empresa_detalhe LIMIT 1;"
```

Referências: `empresa_detalhe`; colunas `cnpj, cnpj_basico`. Código: [receita](recipes/postgres/empresa_detalhe.sql).
Teste: `TestRecipeEmpresaDetalhe`.

## Sinais de qualidade por estabelecimento

**SQL · verified nos dados de teste.** Marca problemas sem mudar os valores da fonte.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/data_quality_flags.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM data_quality_flags LIMIT 1;"
```

Referências: `data_quality_flags`; colunas `cnpj, cep_status`. Código: [receita](recipes/postgres/data_quality_flags.sql).
Teste: `TestRecipeDataQualityFlags`.

## Valores limpos por estabelecimento

**SQL · verified nos dados de teste.** Mantém valores crus ao lado dos tratados, usando os sinais de qualidade.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/data_quality_flags.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/estabelecimentos_clean.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM estabelecimentos_clean LIMIT 1;"
```

Referências: `estabelecimentos_clean`; colunas `cnpj`. Código: [receita](recipes/postgres/estabelecimentos_clean.sql).
Teste: `TestRecipeEstabelecimentosClean`.

## CNAEs secundários em linhas

**SQL · verified nos dados de teste.** Separa a lista de CNAEs secundários em linhas.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/cnae_secundaria_exploded.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM cnae_secundaria_exploded LIMIT 1;"
```

Referências: `cnae_secundaria_exploded`; colunas `cnpj`. Código: [receita](recipes/postgres/cnae_secundaria_exploded.sql).
Teste: `TestRecipeCnaeSecundariaExploded`.

## Hierarquia CNAE

**SQL · verified nos dados de teste.** Liga subclasses a seção, divisão, grupo e classe.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/cnaes_hierarquia.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM cnaes_hierarquia LIMIT 1;"
```

Referências: `cnaes_hierarquia`; colunas `codigo, secao, divisao, grupo, classe`. Código: [receita](recipes/postgres/cnaes_hierarquia.sql).
Teste: `TestRecipeCnaesHierarquia`.

## Sinais de qualidade por sócio

**SQL · verified nos dados de teste.** Marca problemas de referência e valores de preenchimento por sócio.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/socios_quality_flags.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM socios_quality_flags LIMIT 1;"
```

Referências: `socios_quality_flags`; colunas `socio_id`. Código: [receita](recipes/postgres/socios_quality_flags.sql).
Teste: `TestRecipeSociosQualityFlags`.

## Valores limpos por sócio

**SQL · verified nos dados de teste.** Mantém valores crus e tratados do representante e da faixa etária.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/socios_quality_flags.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/socios_clean.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM socios_clean LIMIT 1;"
```

Referências: `socios_clean`; colunas `socio_id`. Código: [receita](recipes/postgres/socios_clean.sql).
Teste: `TestRecipeSociosClean`.

## Detalhes por sócio

**SQL · verified nos dados de teste.** Acrescenta descrições sem mudar os valores originais do sócio.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domain_labels.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/socios_detalhe.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM socios_detalhe LIMIT 1;"
```

Referências: `socios_detalhe`; colunas `socio_id`. Código: [receita](recipes/postgres/socios_detalhe.sql).
Teste: `TestRecipeSociosDetalhe`.

## Busca por nome

**SQL · verified nos dados de teste.** Prepara busca por prefixo da razão social em matrizes ativas.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/empresas_busca_nome.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM empresas_busca_nome LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT cnpj, razao_social FROM empresas_busca_nome WHERE razao_social LIKE 'A%' ORDER BY razao_social LIMIT 5;"
```

Referências: `empresas_busca_nome`; colunas `razao_social, uf, municipio_codigo, cnae_fiscal_principal`. Código: [receita](recipes/postgres/empresas_busca_nome.sql).
Teste: `TestRecipeEmpresasBuscaNome`. Uso de índices não medido.

## Totais para busca

**SQL · verified nos dados de teste.** Calcula totais por UF, município e CNAE.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/empresas_busca_nome.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/empresas_busca_nome_counts.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM empresas_busca_nome_counts LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT uf, total FROM empresas_busca_nome_counts WHERE kind = 'uf' ORDER BY uf;"
```

Referências: `empresas_busca_nome_counts`; colunas `kind, uf, total`. Código: [receita](recipes/postgres/empresas_busca_nome_counts.sql).
Teste: `TestRecipeEmpresasBuscaNomeCounts`. A receita assume um código de município por par UF/nome, mas não confere isso na fonte.
