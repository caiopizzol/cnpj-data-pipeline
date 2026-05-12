# Normalização e pós-processamento

Esta página define a política do projeto sobre o que entra no pipeline e o que fica de fora.

## Princípios

1. **A saída padrão é fiel à fonte.** As tabelas reproduzem o layout dos arquivos CSV da Receita Federal. Nomes de colunas, granularidade e códigos seguem o documento [layout oficial](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

2. **Normalização no núcleo só corrige representação e paridade.** Conversão de encoding (ISO-8859-1 → UTF-8), `0`/`00000000` → null em datas, vírgula decimal em `capital_social`, validação de UF, validação de datas impossíveis, padding de código de país. Tudo isso já acontece em `processor.py` e existe para que os arquivos sejam carregáveis em formatos previsíveis (PostgreSQL com tipos, Parquet com tipos quando `PARQUET_TYPED_OUTPUT=true`).

3. **Tabelas derivadas vivem como receitas SQL.** Denormalizações como `empresa_detalhe` (joins de empresas + estabelecimentos + cnaes + municípios), tabelas de lookup para busca por prefixo, agregações por UF/CNAE — nada disso está no pipeline. Elas são entregues como arquivos SQL em `recipes/` que o usuário aplica manualmente.

4. **Receitas são opcionais, inspecionáveis e forkáveis.** O usuário abre o `.sql`, lê o que ele faz, copia/modifica/ignora. Não há código Python escondido executando a derivação. Não há tabelas que aparecem sem você pedir.

5. **Se um runner de pós-processamento for adicionado no futuro, ele executa as receitas — não reimplementa a lógica em Python.** Os arquivos SQL continuam sendo a fonte da verdade. O runner é só conveniência (`cnpj-pipeline postprocess --recipe empresa_detalhe`).

## Por que essa separação

O valor do projeto open-source é ser o carregador canônico dos dados da Receita Federal. Consumidores variam: PostgreSQL, BigQuery, Snowflake, ClickHouse, DuckDB sobre Parquet, pipelines de ML. Cada um tem necessidades de denormalização diferentes.

Embutir uma única visão "curada" (por exemplo, a do [cnpj.chat](https://cnpj.chat)) no pipeline forçaria todos os outros consumidores a pagar o custo de manutenção e armazenamento de tabelas que eles não usam. Receitas resolvem isso: o usuário escolhe o que aplicar, e a lógica é transparente.

## O que entra no núcleo do pipeline

Mudanças passam neste teste:

> Um consumidor PostgreSQL, um consumidor Parquet, e um consumidor de pipeline de ML — todos os três querem essa transformação, e não há caso razoável para a forma não-transformada?

Se sim, vai para o núcleo. Casos atuais que passaram:

- Conversão de encoding
- Limpeza de placeholders de data (`0` → null)
- Vírgula decimal em `capital_social`
- Padding zero em código de país
- Cast tipado em Parquet (`PARQUET_TYPED_OUTPUT=true`) para paridade com a saída PostgreSQL

## O que NÃO entra no núcleo

Mesmo sob flag opt-in:

- `empresa_detalhe` ou qualquer tabela denormalizada
- Tabelas de lookup para busca por prefixo (`lookup_empresas_nome`, etc.)
- Agregações pré-computadas
- Colunas derivadas como `is_ativa`, `is_matriz`, `is_mei` (depende da definição de "ativa")
- CNPJ formatado com máscara como coluna separada
- Endereço concatenado em um único campo
- Remoção de acentos em razão social (alguns consumidores querem os acentos)

Esses casos são receitas, não pipeline.

## Versionamento e metadados

Toda saída Parquet inclui `manifest.json` com:

- `pipelineVersion` — versão do pacote que produziu o arquivo
- `schemaVersion` — versão da forma da saída (incrementada quando colunas mudam, tabelas são renomeadas etc.)
- `sourceMonth` — diretório de origem na Receita (ex: `2024-11`)

Consumidores que mantêm suas próprias derivações usam esses campos para decidir quando re-executá-las.
