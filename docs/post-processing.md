# Normalização e receitas

Esta página define o que o pipeline faz, o que ele não faz, e onde entram as receitas SQL opcionais.

> **Regra simples:** o pipeline preserva e mede. Receitas interpretam.

## Princípios

1. **A saída padrão é fiel à fonte.** As tabelas seguem o layout dos arquivos CSV da Receita Federal. Nomes de colunas, granularidade e códigos acompanham o [layout oficial](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

2. **Normalização no núcleo só corrige forma e paridade.** Conversão de encoding (ISO-8859-1 → UTF-8), `0`/`00000000` → null em datas, vírgula decimal em `capital_social`, validação de UF, validação de datas impossíveis e padding de código de país. Isso existe para que os arquivos sejam carregáveis em formatos previsíveis: PostgreSQL com tipos e Parquet com tipos quando `PARQUET_TYPED_OUTPUT=true`.

3. **Tabelas derivadas vivem como receitas SQL.** Denormalizações como `empresa_detalhe`, tabelas de busca por prefixo e agregações por UF/CNAE não fazem parte da carga padrão. Quando fizerem sentido para mais de um consumidor, entram como arquivos SQL em `recipes/`, aplicados manualmente.

4. **Receitas são opcionais e legíveis.** O usuário abre o `.sql`, lê o que ele faz, copia, modifica ou ignora. Não há código Python escondido criando tabelas derivadas.

5. **Se um runner de receitas for adicionado no futuro, ele executa os arquivos SQL.** A lógica continua nas receitas. O runner seria apenas conveniência, não uma segunda implementação em Python.

## Por que essa separação

O valor do projeto open-source é carregar os dados da Receita Federal de forma previsível. Consumidores variam: PostgreSQL, BigQuery, Snowflake, ClickHouse, DuckDB sobre Parquet, pipelines de ML. Cada um tem necessidades diferentes de denormalização.

Embutir uma única visão "curada" no pipeline faria todos os consumidores pagarem o custo de tabelas que talvez não usem. Receitas resolvem isso: o usuário escolhe o que aplicar, e a lógica fica explícita.

## O que entra no núcleo do pipeline

Uma mudança entra no núcleo quando passa neste teste:

> Um consumidor PostgreSQL, um consumidor Parquet e um consumidor de pipeline de ML querem essa transformação? Existe algum caso razoável para manter a forma original?

Se a resposta for "sim" para a primeira pergunta e "não" para a segunda, a mudança pode entrar no núcleo. Casos atuais:

- Conversão de encoding
- Limpeza de placeholders de data (`0` → null)
- Vírgula decimal em `capital_social`
- Padding zero em código de país
- Cast tipado em Parquet (`PARQUET_TYPED_OUTPUT=true`) para paridade com a saída PostgreSQL

## O que NÃO entra no núcleo

Mesmo com flag opt-in:

- `empresa_detalhe` ou qualquer tabela denormalizada
- Tabelas de lookup para busca por prefixo (`lookup_empresas_nome`, etc.)
- Agregações pré-computadas
- Colunas derivadas como `is_ativa`, `is_matriz`, `is_mei`
- CNPJ formatado com máscara como coluna separada
- Endereço concatenado em um único campo
- Remoção de acentos em razão social (alguns consumidores querem os acentos)

Esses casos pertencem a receitas, não à carga padrão.

## Versionamento e metadados

Toda saída Parquet inclui um `manifest.json` com:

- `pipelineVersion` — versão do pacote que produziu o arquivo
- `schemaVersion` — versão da forma da saída, incrementada quando colunas mudam ou tabelas são renomeadas
- `sourceMonth` — diretório de origem na Receita (ex: `2024-11`)

Consumidores que mantêm suas próprias derivações usam esses campos para decidir quando re-executá-las.
