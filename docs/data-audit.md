# Auditoria dos dados — normalização vs receitas

Este documento mapeia cada campo das tabelas principais e classifica o que é tratado pelo pipeline (núcleo) e o que pode ser entregue como receita opcional. A política está em [post-processing.md](post-processing.md); aqui é a leitura de campo-a-campo que motiva as receitas.

> **Notas empíricas datadas:** as contagens citadas abaixo foram medidas em **12/05/2026** contra um snapshot recente carregado em PostgreSQL pelo próprio pipeline. A forma dos dados é estável mês a mês, mas tratar números absolutos como invariantes é um erro — re-medir quando uma decisão depender da magnitude.

## Resumo

- **Forma da fonte é boa**: tipagem mínima já acontece (datas como `DATE`, capital como `DOUBLE PRECISION`, encoding UTF-8). Não há trabalho urgente de saneamento.
- **A primeira receita compensa**: junções de referência (cnaes, municípios, naturezas jurídicas, motivos, países) e `dados_simples` em uma única `empresa_detalhe`. É o primeiro `recipes/postgres/empresa_detalhe.sql`.
- **A segunda receita também é genérica**: `cnae_fiscal_secundaria` está armazenada como string com códigos separados por vírgula. Explodir em tabela lateral `(cnpj_basico, cnpj_ordem, cnpj_dv, cnae_codigo)` resolve um padrão de consulta comum.
- **Booleans, labels de enum (`is_ativa`, `porte_descricao`) e CNPJ formatado ficam para mais tarde**, em receitas separadas. São opinativos.

## Como ler a tabela

- **Forma na fonte** — o que a Receita publica, antes de qualquer transformação.
- **Normalização atual** — o que o pipeline já faz hoje (em `processor.py` ou via tipagem em `initial.sql`).
- **Possível normalização core** — mudanças universais que poderiam entrar no núcleo. Vazio = nada a fazer; o campo já está bem.
- **Receita relacionada** — onde a derivação aplicável vive (ou viveria).
- **Prioridade** — relativa entre as receitas, não entre normalizações.

## empresas

| Campo | Forma na fonte | Normalização atual | Possível normalização core | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` | 8 dígitos, string | validação regex `^\d{8}$` | — | usado em todas | — |
| `razao_social` | TEXT, ALL CAPS, sem acentos | — | trim de espaços (a confirmar) | — | — |
| `natureza_juridica` | 4 dígitos, string | validação regex `^\d{4}$` | — | join descrição em `empresa_detalhe` | alta |
| `qualificacao_responsavel` | 2 dígitos, string | validação regex `^\d{2}$` | — | join descrição (qualificacoes_socios) | média |
| `capital_social` | "1.234,56" no CSV, depois "1234.56" string, DOUBLE PRECISION em Postgres | conversão de vírgula decimal, negativos → null | já tipado em Parquet com `PARQUET_TYPED_OUTPUT=true` (v1.18+) | — | — |
| `porte` | "01" \| "03" \| "05" \| null | validação regex; ~50M são `01` (Microempresa), ~15M `05` (Demais), ~2M `03` (EPP), 3K null | — | label expansion em receita futura | baixa |
| `ente_federativo_responsavel` | TEXT, quase sempre vazio | — | — | — | — |

## estabelecimentos

| Campo | Forma na fonte | Normalização atual | Possível normalização core | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` + `cnpj_ordem` + `cnpj_dv` | strings de 8+4+2 dígitos | — | — | coluna concatenada `cnpj` em `empresa_detalhe` | alta |
| `identificador_matriz_filial` | "1" \| "2" no CSV, `INTEGER` em Postgres | tipagem via schema | tipado em Parquet (v1.18+) | label expansion futura | baixa |
| `nome_fantasia` | TEXT, ALL CAPS, sem acentos | — | trim (a confirmar) | — | — |
| `situacao_cadastral` | "01" \| "02" \| "03" \| "04" \| "08" | validação regex | — | label expansion + booleans (`is_ativa`) em receita futura | baixa |
| `data_situacao_cadastral`, `data_inicio_atividade`, `data_situacao_especial` | YYYYMMDD ou "0"/"00000000" | placeholder → null, parse + range check (1900..hoje), `DATE` em Postgres | tipado em Parquet (v1.18+) | — | — |
| `motivo_situacao_cadastral` | 2 dígitos, string | — | — | join descrição em `empresa_detalhe` | alta |
| `cnae_fiscal_principal` | 7 dígitos, string | validação regex `^\d{7}$` | — | join descrição em `empresa_detalhe` | alta |
| `cnae_fiscal_secundaria` | string com códigos de 7 dígitos separados por vírgula, ex: "5914600,8230002,9001999" | — | — | **explodir em tabela lateral `estabelecimentos_cnae_secundaria(cnpj_basico, cnpj_ordem, cnpj_dv, cnae_codigo)`** | alta (segunda receita) |
| `pais` | 3 dígitos com zero-padding | padding `zfill(3)` | — | join descrição (rara, geralmente null) | baixa |
| `uf` | 2 letras | validação contra lista de 27 UFs + "EX" | — | — | — |
| `municipio` | código RFB do município, string (geralmente 4 dígitos; coluna aceita até 7) | — | — | join descrição em `empresa_detalhe` | alta |
| `tipo_logradouro`, `logradouro`, `numero`, `complemento`, `bairro`, `cep` | TEXT, ALL CAPS, sem acentos | — | — | concatenação em receita futura (opcional) | baixa |
| `ddd_1`, `telefone_1`, etc. | strings de dígitos, sem formatação | — | — | — | — |
| `correio_eletronico` | TEXT, ALL CAPS | — | — | — | — |

## socios

| Campo | Forma na fonte | Normalização atual | Possível normalização core | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` | 8 dígitos | validação regex | — | — | — |
| `identificador_de_socio` | "1" \| "2" \| "3" | validação regex | — | label expansion em `socios_detalhe` | baixa |
| `nome_socio` | TEXT | — | — | — | — |
| `cnpj_cpf_do_socio` | já mascarado pela RFB: `***123456**` (CPF) ou CNPJ completo | fallback para "00000000000000" quando null (PK) | — | — | — |
| `qualificacao_do_socio` | 2 dígitos | — | — | join descrição em `socios_detalhe` | baixa |
| `data_entrada_sociedade` | YYYYMMDD | mesma normalização que outras datas | tipado em Parquet (v1.18+) | — | — |
| `pais` | 3 dígitos zero-padded | padding | — | join descrição | baixa |
| `representante_legal` | `***000000**` quando não há | — | — | sentinel → null em receita futura (`socios_cleanup`) | baixa |
| `qualificacao_do_representante_legal` | "00" quando não há | — | — | sentinel → null em receita futura | baixa |
| `faixa_etaria` | "1".."9" | validação regex | — | label expansion (`socios_detalhe`) | baixa |

## dados_simples

| Campo | Forma na fonte | Normalização atual | Possível normalização core | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` | 8 dígitos, PK | validação regex | — | — | — |
| `opcao_pelo_simples` | "S" \| "N" | validação regex | — | incluído cru em `empresa_detalhe` | alta |
| `data_opcao_pelo_simples`, `data_exclusao_do_simples`, `data_opcao_pelo_mei`, `data_exclusao_do_mei` | YYYYMMDD ou null | normalização de datas | tipado em Parquet (v1.18+) | incluído cru em `empresa_detalhe` | alta |
| `opcao_pelo_mei` | "S" \| "N" | validação regex | — | incluído cru em `empresa_detalhe` | alta |

> Observação: `dados_simples` é por `cnpj_basico` (empresa-nível), não por estabelecimento. Em `empresa_detalhe` essas colunas se repetem em todas as linhas de uma mesma empresa.

## Tabelas de referência (cnaes, motivos, municipios, naturezas_juridicas, paises, qualificacoes_socios)

No PostgreSQL, todas têm a mesma forma: `(codigo, descricao, data_criacao, data_atualizacao)`. Nos arquivos de origem e no Parquet, a forma é apenas `(codigo, descricao)`. Não há normalização ou receita aplicável — são lookup puro.

> Medição em 12/05/2026: zero órfãos em `estabelecimentos.cnae_fiscal_principal` e `estabelecimentos.municipio` contra suas respectivas tabelas de referência. `LEFT JOIN` continua sendo a escolha defensiva por causa de snapshots históricos (códigos retirados ao longo dos anos), mas para o mês atual `INNER JOIN` produziria o mesmo resultado.

## Decisões para a primeira receita (empresa_detalhe)

A receita `recipes/postgres/empresa_detalhe.sql` implementa:

- **LEFT JOIN** com `cnaes`, `municipios`, `motivos`, `paises`, `naturezas_juridicas` — preserva linhas mesmo com códigos retirados em snapshots históricos.
- **LEFT JOIN** com `dados_simples` — inclui colunas cruas (`opcao_pelo_simples`, datas, `opcao_pelo_mei`). Sem booleans derivados.
- **Coluna `cnpj`** = `cnpj_basico || cnpj_ordem || cnpj_dv`. Trivial mas evita repetir a concatenação em consultas.
- **`CREATE TABLE AS`** (não view, não materializada) — modelo "rodar receita depois do ingest" é o esperado.
- **Sem labels de enum** (`situacao_cadastral` continua sendo "02", não "Ativa"). Sem booleans (`is_ativa`, `is_matriz`). Essas ficam para receitas futuras opinativas.

## Receitas planejadas após a primeira

1. **`cnae_secundaria_exploded`** — tabela lateral para `cnae_fiscal_secundaria`. Alta utilidade dado o volume de estabelecimentos com múltiplos CNAEs secundários (~20M+ linhas medidos em 12/05/2026).
2. **`socios_detalhe`** — junções com `qualificacoes_socios`, `paises`. Tratamento opcional do sentinel `***000000**` / `00` para representante legal. Deprioridade: muitas decisões opinativas (forma do label `faixa_etaria`, etc.).
3. **`labels`** — expansão de enums (`situacao_cadastral` → `situacao_cadastral_descricao`, `porte` → `porte_descricao`, etc.). Puramente código → label.
4. **`booleans`** — colunas convenientes como `is_ativa`, `is_matriz`, `is_optante_simples_atual`. Cada uma com a regra documentada no header da receita.

Tabelas de busca específicas (`lookup_empresas_nome`, `lookup_nome_fantasia`) e agregações por UF/CNAE/ano não estão no roadmap das receitas genéricas — são casos de uso suficientemente específicos que pertencem ao repositório do consumidor.
