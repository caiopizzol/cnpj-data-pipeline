# CNPJ Data Pipeline

Baixa os dados mensais do CNPJ da Receita Federal e prepara tabelas PostgreSQL ou arquivos Parquet para consultas, análises e ML. É a parte de preparação de dados do cnpj.chat.

Inclui empresas, estabelecimentos, sócios, Simples/MEI e tabelas de referência. O conjunto separado de Regimes Tributários ainda não é suportado. Veja como usar no [README](README.md).

## O que fica aqui

O foco é carregar dados de forma previsível para diferentes usos. O núcleo baixa, ajusta e grava os dados. As [receitas SQL](recipes/README.md) adicionam descrições, referências com origem registrada, indicadores de qualidade, tabelas de busca e resumos. Você escolhe quais rodar, segue as dependências e as roda de novo após cada carga.

Os dados seguem as colunas e os códigos da Receita, com ajustes de texto, datas, decimais e zeros à esquerda. O pipeline também gera `socios.socio_id` de forma repetível. Aceita CNPJ numérico e alfanumérico; um aviso de formato não significa que o valor será descartado.

Essa separação guia novas mudanças: ajustes úteis a todos ficam no núcleo; interpretações ficam nas receitas ou na aplicação. Os detalhes estão em [Normalização e receitas](docs/post-processing.md).

Agendamento, hospedagem do banco, API, telas de consulta, regras de negócio e checagem externa de endereços ficam com quem usa os dados. Publicar e corrigir a fonte cabe à Receita Federal.

## O que faz e onde fica

Execute com `cnpj-pipeline`, `python main.py`, `just run` ou Docker.

| O que faz | Como usar | Código | Testes existentes |
| --- | --- | --- | --- |
| Escolhe o mês e baixa arquivos, com retomada e novas tentativas após falhas | `--list`, `--month` | [main.py](main.py), [downloader.py](downloader.py), [config.py](config.py) | [Downloads](tests/test_downloader.py): listagem e retomada; [execução](tests/test_main.py) |
| Ajusta os registros, gera IDs de sócios e detecta mudanças no número de colunas | Automático nas duas saídas | [processor.py](processor.py) | [Processamento](tests/test_processor.py): ajustes, IDs e layout |
| Carrega PostgreSQL e registra arquivos concluídos por mês | `OUTPUT_FORMAT=postgres`, `LOADING_STRATEGY=upsert` ou `replace`; `--force` para reprocessar | [database.py](database.py), [initial.sql](initial.sql), [main.py](main.py) | [Integração](tests/test_integration.py): `TestFullPipeline`, recarga e chaves repetidas entre lotes; [controle de arquivos](tests/test_database.py) |
| Exporta um Parquet por tabela, com ZSTD e manifesto de origem e versões | `OUTPUT_FORMAT=parquet`; tipos opcionais com `PARQUET_TYPED_OUTPUT` | [parquet_writer.py](parquet_writer.py), [processor.py](processor.py), [main.py](main.py) | [Exportação](tests/test_parquet_writer.py): arquivos e metadados; [tipos](tests/test_processor.py) |
| Mede problemas de qualidade no PostgreSQL | `just data-quality-report` | [data_quality_report.py](scripts/data_quality_report.py) | [Relatório](tests/test_data_quality_report.py): dígitos verificadores e amostragem; [medições no banco](tests/test_integration.py): `TestDataQualityReportMeasurements` |
| Cria tabelas derivadas que você escolher | Rode as receitas SQL manualmente | [recipes/postgres](recipes/postgres) | [Integração](tests/test_integration.py): `TestRecipe*`, contagens, valores, preservação de campos e reexecução |

## Antes de usar

- Cada execução processa um mês e termina. No PostgreSQL, a base não é atualizada inteira de uma só vez. Bancos existentes podem precisar de [atualização do schema](docs/upgrading.md).
- No Parquet, um arquivo existente faz a tabela ser pulada, mesmo sem conferir se está completo, se é do mês certo ou se tem o schema esperado. Gerencie o diretório de saída. `schemaVersion` é igual nos modos string e tipado: confira os tipos no próprio arquivo.
- O relatório aponta problemas, mas não bloqueia a carga ou a publicação. Por padrão, só os dígitos verificadores usam uma amostra; as outras medições leem a base toda. Você define o que é aceitável. Os testes cobrem os casos do repositório, não garantem a qualidade de toda a base da Receita.
