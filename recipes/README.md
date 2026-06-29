# Receitas

Receitas são arquivos SQL opcionais para quem quer uma camada além da carga bruta. O pipeline não roda esses arquivos automaticamente. Você lê, escolhe, adapta e executa quando fizer sentido.

A regra está em [../docs/post-processing.md](../docs/post-processing.md): o pipeline preserva e mede. Receitas interpretam.

## Disponíveis

| Receita | Arquivo | O que faz |
|---|---|---|
| `reference_domains_enriched` | [`postgres/reference_domains_enriched.sql`](postgres/reference_domains_enriched.sql) | Materializa `motivos_enriched`, `paises_enriched` e `qualificacoes_socios_enriched`: a tabela mensal mais linhas suplementares oficiais (SERPRO/Receita ODS) para códigos ausentes do mês, com proveniência por linha. Não altera as tabelas cruas. Pré-requisito de `empresa_detalhe`. |
| `empresa_detalhe` | [`postgres/empresa_detalhe.sql`](postgres/empresa_detalhe.sql) | Junta empresas, estabelecimentos, tabelas de referência (descrições de motivo/país/qualificação vêm das tabelas enriquecidas) e dados do Simples Nacional em uma tabela por estabelecimento. Preserva códigos e valores da fonte. |
| `data_quality_flags` | [`postgres/data_quality_flags.sql`](postgres/data_quality_flags.sql) | Mede sinais de qualidade por estabelecimento, sem alterar valores. |
| `estabelecimentos_clean` | [`postgres/estabelecimentos_clean.sql`](postgres/estabelecimentos_clean.sql) | Usa `data_quality_flags` para emitir pares cru/limpo de CEP e capital social. |
| `cnae_secundaria_exploded` | [`postgres/cnae_secundaria_exploded.sql`](postgres/cnae_secundaria_exploded.sql) | Transforma `cnae_fiscal_secundaria` em uma tabela lateral: uma linha por CNAE secundário. |
| `socios_quality_flags` | [`postgres/socios_quality_flags.sql`](postgres/socios_quality_flags.sql) | Mede sinais de qualidade por sócio, incluindo representante legal com valor de preenchimento e referências ausentes. |
| `socios_clean` | [`postgres/socios_clean.sql`](postgres/socios_clean.sql) | Usa `socios_quality_flags` para emitir pares cru/limpo do trio do representante e de `faixa_etaria`. |
| `empresas_busca_nome` | [`postgres/empresas_busca_nome.sql`](postgres/empresas_busca_nome.sql) | Tabela de serviço para busca por `razao_social` em matrizes ativas. Inclui descrições de município e CNAE e índices compostos para LIKE prefixo combinado com filtros de UF, município ou CNAE. |
| `empresas_busca_nome_counts` | [`postgres/empresas_busca_nome_counts.sql`](postgres/empresas_busca_nome_counts.sql) | Rollups de contagem para `empresas_busca_nome` por UF, UF + município (descrição e código) e UF + CNAE. Cada lookup é O(1) via índice único parcial; serve totais exatos sem varrer milhões de linhas a cada request. |

## Como aplicar

Depois da carga (`just run`), rode apenas as receitas que você precisa:

```bash
# Domínios de referência enriquecidos (pré-requisito de empresa_detalhe)
psql "$DATABASE_URL" -f recipes/postgres/reference_domains_enriched.sql

# Tabela denormalizada para consulta (depende de reference_domains_enriched)
psql "$DATABASE_URL" -f recipes/postgres/empresa_detalhe.sql

# Sinais de qualidade e camada limpa de estabelecimentos
psql "$DATABASE_URL" -f recipes/postgres/data_quality_flags.sql
psql "$DATABASE_URL" -f recipes/postgres/estabelecimentos_clean.sql

# CNAEs secundários em tabela lateral
psql "$DATABASE_URL" -f recipes/postgres/cnae_secundaria_exploded.sql

# Sinais de qualidade e camada limpa por sócio
psql "$DATABASE_URL" -f recipes/postgres/socios_quality_flags.sql
psql "$DATABASE_URL" -f recipes/postgres/socios_clean.sql

# Tabela de serviço para busca por nome em matrizes ativas
psql "$DATABASE_URL" -f recipes/postgres/empresas_busca_nome.sql

# Rollups de contagem para empresas_busca_nome
psql "$DATABASE_URL" -f recipes/postgres/empresas_busca_nome_counts.sql
```

Rode novamente após cada carga mensal para atualizar. Quando uma receita depende de outra, isso aparece no cabeçalho do SQL.

## Consumidores usando Parquet

As receitas hoje são escritas para Postgres porque esse é o caminho testado. Quem usa Parquet pode tratar os arquivos SQL como referência e traduzir para DuckDB, Spark, Polars, Athena ou outra ferramenta que leia Parquet.

Exemplo em DuckDB: substituir uma tabela como `empresas` por `read_parquet('parquet/empresas.parquet')` e ajustar a sintaxe quando necessário. Variantes para outras ferramentas são bem-vindas quando houver demanda clara.

## Princípios para novas receitas

Antes de propor uma receita nova:

1. Serve mais de um consumidor? Se só você usa, mantenha no seu repositório.
2. É SQL puro e fácil de ler? Se precisa de Python, provavelmente é outra ferramenta, não uma receita.
3. Deixa claro o que interpreta? Uma receita pode limpar, marcar ou juntar dados, mas precisa dizer qual regra usou.
4. Preserva a fonte quando possível? Receitas `*_clean` devem manter o valor cru ao lado do valor tratado.
5. Documenta dependências? O cabeçalho deve dizer quais tabelas ou receitas precisam existir antes.

Receitas em discussão:

- `socios_detalhe` — junções de sócios com qualificação e país. Ainda precisa decidir como expor descrições e valores sentinela.
- `descricoes` — colunas adicionais com a descrição legível de enums (`situacao_cadastral_descricao`, `porte_descricao` etc.).
- `booleanos` — colunas convenientes como `is_ativa`, `is_matriz`, `is_optante_simples_atual`. Cada uma com a regra documentada.
