# Medir a carga PostgreSQL

Use uma amostra fixa e um servidor PostgreSQL descartável. O comando cria um banco com nome aleatório, aplica o schema, carrega a amostra, confere os valores e remove o banco. A conta precisa de permissão `CREATEDB`. Não use o servidor de produção.

Na raiz do repositório, prepare uma amostra (ou reutilize uma já salva):

```sh
uv run --frozen python -m scripts.benchmark_pipeline prepare 2026-08 Socios1.zip /tmp/cnpj-sample --rows 250000
```

Use o caminho `sample` registrado em `/tmp/cnpj-sample/sample.json`, mantendo o sufixo original do arquivo:

```sh
export BENCHMARK_DATABASE_URL='postgresql://postgres:postgres@localhost:5435/postgres'
uv run --frozen python -m scripts.benchmark_postgres /caminho/amostra.SOCIOCSV /tmp/cnpj-upsert --strategy upsert
uv run --frozen python -m scripts.benchmark_postgres /caminho/amostra.SOCIOCSV /tmp/cnpj-replace --strategy replace
```

O comando exige `BENCHMARK_DATABASE_URL`; não usa `DATABASE_URL`. Cada execução precisa de uma pasta de saída nova. Rode sequencialmente, em processos novos, usando a mesma amostra, servidor e `--batch-size` (padrão: 500000). Alterne as estratégias e repita antes de comparar resultados.

`measurement.json` contém o hash da entrada, linhas lidas e gravadas, duplicatas, número de lotes, versões, configurações do PostgreSQL e:

- Tempo de processamento do CSV e tempo de escrita, incluindo commits.
- Tempo total da carga e linhas de entrada por segundo.
- CPU e pico de memória do processo Python, capturados antes da conferência. O pico de memória inclui as importações e a preparação do processo. Não incluem a memória ou CPU do servidor PostgreSQL.

Criar o banco, aplicar o schema, conferir valores e remover o banco ficam fora do tempo da carga. Download, controle de ZIPs concluídos, receitas SQL e recarga de um banco existente também ficam fora deste teste. Os arquivos lidos podem estar no cache do sistema.

A conferência repete a normalização e insere os valores por parâmetros SQL, sem usar a serialização CSV do carregador. Compara todas as colunas de origem e IDs gerados; ignora os timestamps de auditoria. Duplicatas idênticas são aceitas. Valores diferentes para a mesma chave fazem a conferência falhar, inclusive entre lotes. Isso evita escolher um vencedor que a carga não garante dentro de um lote. A conferência valida o transporte e a gravação, não as regras de normalização compartilhadas.

O relatório só é salvo após a conferência e a remoção do banco. Uma interrupção forçada pode deixar um banco `cnpj_benchmark_*` no servidor descartável. Nesses casos, remova o recurso de teste antes de repetir.

Em um banco vazio, `replace` usa o mesmo upsert após um truncate inicial. Uma diferença pequena entre as estratégias não demonstra ganho em uma recarga real. O benchmark existente de CSV e Parquet continua disponível em `scripts.benchmark_pipeline run`.
