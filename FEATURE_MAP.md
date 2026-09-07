# Mapa de features

Rotas do CNPJ Data Pipeline 1.38.3. O escopo do produto está em [PRODUCT.md](PRODUCT.md).
Aqui, `verified` vale para a rota descrita; `partial` indica o que falta testar.

Todas as rotas começam na raiz de um checkout limpo, com Python 3.11+ e `uv`:

```bash
uv sync --frozen
```

Use `uv run --frozen python main.py`: `uv sync` não instala o atalho `cnpj-pipeline`.
As âncoras são opções de CLI, variáveis de configuração e nomes de tabelas/colunas.
Não há seletores de tela nem atalhos de teclado próprios. As opções curtas são `-h`, `-l`, `-m` e `-f`.

### Banco de replay

Para o relatório e as receitas, use Docker e `psql`. A porta 5435 precisa estar livre.
Este banco é descartável; as receitas recriam suas tabelas. Não aponte o replay para produção.

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

Isso carrega as fixtures rastreadas pelo Git usando o processador e o banco reais.
Não testa download nem uma carga mensal completa. Ao terminar: `docker rm -f cnpj-feature-map-db`.

### Checks existentes

Os checks são evidência separada do replay. As classes SQL compartilham estado, então rode o arquivo inteiro:

```bash
uv run --frozen pytest tests/test_integration.py -q
```

Requer o PostgreSQL acima. O teste cria e remove `cnpj_test`; não use um servidor com dados importantes nesse banco.
Cada entrada aponta o check mais próximo e o que ele verifica. O replay SQL apenas confirma execução e leitura nas fixtures, sem provar desempenho na base completa. As rotas marcadas como `verified` também foram repetidas em outro checkout limpo, com um banco novo.

## Listar meses

**CLI · verified.** Início: checkout preparado e acesso à Receita, sem banco.

```bash
uv run --frozen python main.py --help
CONNECT_TIMEOUT=10 READ_TIMEOUT=20 uv run --frozen python main.py --list
```

Âncoras: `--list` (`-l`), `--help` (`-h`). Código: [main.py](main.py) `parse_args/main`, [downloader.py](downloader.py) `get_available_directories`.
Replay: ajuda e listagem WebDAV reais. Check: [test_main.py](tests/test_main.py) `TestMain.test_list_mode_never_touches_database` verifica que listar não abre o banco; [test_downloader.py](tests/test_downloader.py) `TestGetAvailableDirectories` cobre a leitura da resposta. A disponibilidade futura da fonte depende da Receita.

## Rodar pelo Docker

**Docker CLI · verified para ajuda e listagem.** Início: checkout limpo e Docker funcionando.

```bash
docker build -t cnpj-feature-map:local .
docker run --rm cnpj-feature-map:local --help
docker run --rm -e CONNECT_TIMEOUT=10 -e READ_TIMEOUT=20 cnpj-feature-map:local --list
```

Âncoras: `--help`, `--list`. Código: [Dockerfile](Dockerfile), [main.py](main.py).
Replay: imagem construída; ajuda e listagem WebDAV reais funcionaram. Check: não há teste dedicado da imagem; `TestMain` em [test_main.py](tests/test_main.py) cobre a lógica Python. Não cobre a imagem publicada nem uma carga via Compose.

## Carregar um mês no PostgreSQL

**CLI · partial.** Início: banco descartável acima, internet e espaço para a base completa. A fixture pequena não substitui esse download.

```bash
OUTPUT_FORMAT=postgres LOADING_STRATEGY=upsert uv run --frozen python main.py --month 2024-11
```

Âncoras: `--month` (`-m`), `OUTPUT_FORMAT`, `LOADING_STRATEGY`, `processed_files`. Sem `--month`, usa o mês mais recente.
Código: [main.py](main.py) `main/_pg_worker`, [database.py](database.py), [initial.sql](initial.sql).
Replay: o preparo do banco verificou processamento e upsert de fixtures; a CLI com download mensal não foi executada.
Check: [test_integration.py](tests/test_integration.py) `TestFullPipeline` verifica carga e recarga; [test_main.py](tests/test_main.py) `TestParseArgs` verifica as opções. Não prova uma atualização atômica de toda a base.

## Retomar ou reprocessar a carga

**CLI PostgreSQL · partial.** Início: mesma configuração e mês da carga anterior. Retomar pula arquivos registrados; `--force` limpa esse registro para o mês.

```bash
OUTPUT_FORMAT=postgres LOADING_STRATEGY=upsert uv run --frozen python main.py --month 2024-11
OUTPUT_FORMAT=postgres LOADING_STRATEGY=upsert uv run --frozen python main.py --month 2024-11 --force
```

Âncoras: `processed_files`, `--force` (`-f`). Código: [main.py](main.py), [database.py](database.py) `get_processed_files/clear_processed_files`.
Replay mensal não executado. Check: [test_main.py](tests/test_main.py) `TestMain.test_force_clears_processed_files` verifica a limpeza com dependências simuladas; [test_database.py](tests/test_database.py) cobre o registro. `--force` só vale para PostgreSQL.

## Substituir tabelas na carga

**CLI PostgreSQL · partial.** Início: banco descartável e recursos para o mês completo. `replace` apaga o conteúdo das tabelas carregadas antes de inserir.

```bash
OUTPUT_FORMAT=postgres LOADING_STRATEGY=replace uv run --frozen python main.py --month 2024-11 --force
```

Âncoras: `LOADING_STRATEGY=replace`, `--force`. Código: [main.py](main.py), [database.py](database.py) `bulk_insert/truncate_table`.
Replay mensal não executado. Check: [test_integration.py](tests/test_integration.py) `TestFullPipeline.test_replace_handles_cross_batch_pk_overlap` verifica chaves repetidas entre lotes; `test_replace_strategy` verifica a substituição com fixtures.

## Baixar e processar em paralelo

**CLI · partial.** Início: banco descartável, internet e recursos para o mês completo.

```bash
OUTPUT_FORMAT=postgres DOWNLOAD_WORKERS=4 PROCESS_WORKERS=2 uv run --frozen python main.py --month 2024-11
```

Âncoras: `DOWNLOAD_WORKERS`, `PROCESS_WORKERS`, `STALL_TIMEOUT`, `STALL_DEGRADE_THRESHOLD`. Código: [downloader.py](downloader.py), [main.py](main.py), [config.py](config.py).
Replay mensal e falhas de rede não provocados. Checks: [test_downloader.py](tests/test_downloader.py) `TestResumeEdgeCases` e `TestAdaptiveDownloadIntegration` verificam retomada e redução de concorrência com rede simulada; [test_main.py](tests/test_main.py) `TestParallelProcessing` cobre os workers.

## Exportar Parquet

**CLI · partial.** Início: internet e diretório de saída novo, com espaço para o mês completo; dispensa banco.

```bash
OUTPUT_FORMAT=parquet PARQUET_OUTPUT_DIR=./parquet-string PARQUET_TYPED_OUTPUT=false uv run --frozen python main.py --month 2024-11
```

Âncoras: `OUTPUT_FORMAT`, `PARQUET_OUTPUT_DIR`, `manifest.json`, `schemaVersion`, `sourceMonth`. Código: [main.py](main.py), [parquet_writer.py](parquet_writer.py).
Replay mensal não executado. Checks: [test_main.py](tests/test_main.py) `TestParquetOutput.test_writes_parquet_and_manifest` usa download simulado; [test_parquet_writer.py](tests/test_parquet_writer.py) verifica arquivos, metadados e ZSTD reais.
Arquivos existentes fazem a tabela ser pulada, sem validar mês ou conclusão. `TestParquetResume` verifica esse comportamento, não uma retomada segura de arquivos incompletos.

## Exportar Parquet com tipos

**CLI · partial.** Início: mesmas condições do Parquet, com outro diretório novo.

```bash
OUTPUT_FORMAT=parquet PARQUET_OUTPUT_DIR=./parquet-typed PARQUET_TYPED_OUTPUT=true uv run --frozen python main.py --month 2024-11
```

Âncora: `PARQUET_TYPED_OUTPUT`. Código: [processor.py](processor.py) `_apply_typed_casts`, [main.py](main.py).
Replay mensal não executado. Check: [test_processor.py](tests/test_processor.py) `TestTypedCasts` verifica tipos de datas e números. `schemaVersion` não distingue esse modo do modo string; leia os tipos no Parquet.

## Rodar um comando após exportar cada tabela

**CLI Parquet · partial.** Início: mesmas condições do Parquet e diretório novo. Este exemplo imprime o caminho de cada arquivo fechado.

```bash
OUTPUT_FORMAT=parquet PARQUET_OUTPUT_DIR=./parquet-hook POST_FILE_COMMAND=echo uv run --frozen python main.py --month 2024-11
```

Âncora: `POST_FILE_COMMAND`. Código: [main.py](main.py) `main`.
Replay mensal não executado. Check: [test_main.py](tests/test_main.py) `TestParquetOutput.test_post_file_command_runs_per_table` verifica a chamada por tabela com subprocesso simulado. O comando é separado por espaços e recebe o caminho como último argumento; não é um script de shell.

## Medir qualidade

**CLI PostgreSQL · verified nas fixtures.** Início: banco de replay carregado. Rode antes das receitas para ver referências enriquecidas como indisponíveis; depois delas para medir essa cobertura também.

```bash
uv run --frozen python scripts/data_quality_report.py
uv run --frozen python scripts/data_quality_report.py --full
uv run --frozen python scripts/data_quality_report.py --sample-pct 0.5
```

Âncoras: `DATABASE_URL`, `--full`, `--sample-pct`. Código: [scripts/data_quality_report.py](scripts/data_quality_report.py) `main`.
Replay: os três modos geraram relatório. Checks: [test_data_quality_report.py](tests/test_data_quality_report.py) verifica DV e amostragem; [test_integration.py](tests/test_integration.py) `TestDataQualityReportMeasurements` verifica referências ausentes e enriquecidas.
Só o DV usa amostragem por padrão; o restante lê tudo. Achados não mudam o código de saída para falha.

## Referências enriquecidas

**SQL PostgreSQL · verified nas fixtures.** Acrescenta códigos oficiais ausentes do mês e registra a origem. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM motivos_enriched LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM paises_enriched LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM qualificacoes_socios_enriched LIMIT 1;"
```

Âncoras: `motivos_enriched`, `paises_enriched`, `qualificacoes_socios_enriched`; colunas `codigo, source_kind, source_url, is_supplemental`. Código: [receita](recipes/postgres/reference_domains_enriched.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeReferenceDomainsEnriched` verifica a receita; `test_recipe_executes` confirma criação.

## Rótulos de códigos

**SQL PostgreSQL · verified nas fixtures.** Cria descrições para códigos que não têm CSV próprio. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domain_labels.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM portes_empresa LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM situacoes_cadastrais LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM indicadores_matriz_filial LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM identificadores_socio LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM faixas_etarias LIMIT 1;"
```

Âncoras: `portes_empresa`, `situacoes_cadastrais`, `indicadores_matriz_filial`, `identificadores_socio`, `faixas_etarias`; colunas `codigo, descricao`. Código: [receita](recipes/postgres/reference_domain_labels.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeDomainLabels` verifica a receita; `test_recipe_executes` confirma criação.

## Detalhes por estabelecimento

**SQL PostgreSQL · verified nas fixtures.** Junta dados da empresa, estabelecimento, referências e Simples. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domain_labels.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/empresa_detalhe.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM empresa_detalhe LIMIT 1;"
```

Âncoras: `empresa_detalhe`; colunas `cnpj, cnpj_basico`. Código: [receita](recipes/postgres/empresa_detalhe.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeEmpresaDetalhe` verifica a receita; `test_recipe_executes` confirma criação.

## Sinais de qualidade por estabelecimento

**SQL PostgreSQL · verified nas fixtures.** Marca problemas sem mudar os valores da fonte. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/data_quality_flags.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM data_quality_flags LIMIT 1;"
```

Âncoras: `data_quality_flags`; colunas `cnpj, cep_status`. Código: [receita](recipes/postgres/data_quality_flags.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeDataQualityFlags` verifica a receita; `test_recipe_executes` confirma criação.

## Valores limpos por estabelecimento

**SQL PostgreSQL · verified nas fixtures.** Mantém valores crus ao lado dos tratados, usando os sinais de qualidade. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/data_quality_flags.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/estabelecimentos_clean.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM estabelecimentos_clean LIMIT 1;"
```

Âncoras: `estabelecimentos_clean`; colunas `cnpj`. Código: [receita](recipes/postgres/estabelecimentos_clean.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeEstabelecimentosClean` verifica a receita; `test_recipe_executes` confirma criação.

## CNAEs secundários em linhas

**SQL PostgreSQL · verified nas fixtures.** Separa a lista de CNAEs secundários em linhas. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/cnae_secundaria_exploded.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM cnae_secundaria_exploded LIMIT 1;"
```

Âncoras: `cnae_secundaria_exploded`; colunas `cnpj`. Código: [receita](recipes/postgres/cnae_secundaria_exploded.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeCnaeSecundariaExploded` verifica a receita; `test_recipe_executes` confirma criação.

## Hierarquia CNAE

**SQL PostgreSQL · verified nas fixtures.** Liga subclasses a seção, divisão, grupo e classe. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/cnaes_hierarquia.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM cnaes_hierarquia LIMIT 1;"
```

Âncoras: `cnaes_hierarquia`; colunas `codigo, secao, divisao, grupo, classe`. Código: [receita](recipes/postgres/cnaes_hierarquia.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeCnaesHierarquia` verifica a receita; `test_recipe_executes` confirma criação.

## Sinais de qualidade por sócio

**SQL PostgreSQL · verified nas fixtures.** Marca problemas de referência e valores de preenchimento por sócio. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/socios_quality_flags.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM socios_quality_flags LIMIT 1;"
```

Âncoras: `socios_quality_flags`; colunas `socio_id`. Código: [receita](recipes/postgres/socios_quality_flags.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeSociosQualityFlags` verifica a receita; `test_recipe_executes` confirma criação.

## Valores limpos por sócio

**SQL PostgreSQL · verified nas fixtures.** Mantém valores crus e tratados do representante e da faixa etária. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/socios_quality_flags.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/socios_clean.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM socios_clean LIMIT 1;"
```

Âncoras: `socios_clean`; colunas `socio_id`. Código: [receita](recipes/postgres/socios_clean.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeSociosClean` verifica a receita; `test_recipe_executes` confirma criação.

## Detalhes por sócio

**SQL PostgreSQL · verified nas fixtures.** Acrescenta descrições sem mudar os valores originais do sócio. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domains_enriched.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/reference_domain_labels.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/socios_detalhe.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM socios_detalhe LIMIT 1;"
```

Âncoras: `socios_detalhe`; colunas `socio_id`. Código: [receita](recipes/postgres/socios_detalhe.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeSociosDetalhe` verifica a receita; `test_recipe_executes` confirma criação.

## Busca por nome

**SQL PostgreSQL · verified nas fixtures.** Prepara busca por prefixo da razão social em matrizes ativas. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/empresas_busca_nome.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM empresas_busca_nome LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT cnpj, razao_social FROM empresas_busca_nome WHERE razao_social LIKE 'A%' ORDER BY razao_social LIMIT 5;"
```

Âncoras: `empresas_busca_nome`; colunas `razao_social, uf, municipio_codigo, cnae_fiscal_principal`. Código: [receita](recipes/postgres/empresas_busca_nome.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeEmpresasBuscaNome` verifica a receita; `test_recipe_executes` confirma criação. O replay não mede uso de índices ou desempenho na base completa.

## Totais para busca

**SQL PostgreSQL · verified nas fixtures.** Calcula totais por UF, município e CNAE. Início: banco de replay carregado.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/empresas_busca_nome.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/postgres/empresas_busca_nome_counts.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT * FROM empresas_busca_nome_counts LIMIT 1;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT uf, total FROM empresas_busca_nome_counts WHERE kind = 'uf' ORDER BY uf;"
```

Âncoras: `empresas_busca_nome_counts`; colunas `kind, uf, total`. Código: [receita](recipes/postgres/empresas_busca_nome_counts.sql).
Replay: SQL executado e tabelas consultadas. Check: [test_integration.py](tests/test_integration.py) `TestRecipeEmpresasBuscaNomeCounts` verifica a receita; `test_recipe_executes` confirma criação. Totais por código de município pressupõem um código por par UF/nome; essa condição da fonte não é validada pela receita.
