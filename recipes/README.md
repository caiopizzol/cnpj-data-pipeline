# Receitas

Tabelas derivadas opcionais. Cada arquivo é SQL puro, inspecionável, com cabeçalho documentando o que faz e como aplicar. O pipeline não executa nada daqui — você roda manualmente.

Política completa: [../docs/post-processing.md](../docs/post-processing.md). Resumo: a saída padrão do pipeline é fiel à fonte; receitas são camadas opcionais que o usuário escolhe aplicar.

## Disponíveis

| Receita | Arquivo | O que faz |
|---|---|---|
| `empresa_detalhe` | [`postgres/empresa_detalhe.sql`](postgres/empresa_detalhe.sql) | Junta empresas + estabelecimentos + tabelas de referência (cnaes, municipios, motivos, paises, naturezas_juridicas) + dados_simples em uma tabela por estabelecimento. |

## Como aplicar

Depois do ingest (`just run`):

```bash
psql "$DATABASE_URL" -f recipes/postgres/empresa_detalhe.sql
```

Re-rodar após cada ingest mensal para atualizar.

## Consumidores usando Parquet

As receitas hoje são escritas para Postgres. Para DuckDB, Spark, Polars, Athena ou qualquer outro engine que leia Parquet, a SQL é direta de traduzir — basta substituir as tabelas-fonte por `read_parquet('parquet/empresa.parquet')` (ou o equivalente do seu engine) e ajustar a sintaxe de JOIN se necessário.

PRs adicionando variantes específicas de engine são bem-vindos quando houver demanda clara.

## Princípios para novas receitas

Antes de propor uma receita nova:

1. Tem caso de uso para mais de um consumidor? Se só você usa, mantenha no seu repositório.
2. É puramente SQL? Lógica que precisa de Python não é receita, é fork do pipeline.
3. Tem header documentando: o que produz, premissas (ex: tabelas necessárias), e qualquer escolha opinativa.

Receitas em discussão (não implementadas ainda):

- `cnae_secundaria_exploded` — `cnae_fiscal_secundaria` (string com códigos separados por vírgula) explodido em tabela lateral `(cnpj_basico, cnpj_ordem, cnpj_dv, cnae_codigo)`. Próxima a ser entregue.
- `socios_detalhe` — junções de sócios com qualificação e país. Decisões opinativas pendentes (tratamento do sentinel `***000000**` para representante legal, forma do label de `faixa_etaria`).
- `labels` — colunas adicionais com a descrição legível de enums (`situacao_cadastral_descricao`, `porte_descricao` etc.).
- `booleans` — colunas convenientes como `is_ativa`, `is_matriz`, `is_optante_simples_atual`. Cada uma com a regra documentada.
