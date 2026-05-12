# Auditoria dos dados — normalização vs receitas

Este documento mapeia os campos das tabelas principais e mostra o que fica no núcleo do pipeline e o que pode virar receita opcional. A política geral está em [post-processing.md](post-processing.md).

> **Notas empíricas datadas:** as contagens citadas abaixo foram medidas em **12/05/2026** contra um snapshot recente carregado em PostgreSQL pelo próprio pipeline. A forma dos dados é estável mês a mês, mas os números absolutos mudam. Refaça a medição quando uma decisão depender da magnitude.

## Resumo

- **A fonte já é boa para carga:** datas, capital social e encoding têm tratamento suficiente para PostgreSQL e Parquet tipado.
- **A primeira receita útil é `empresa_detalhe`:** junta empresas, estabelecimentos, tabelas de referência e `dados_simples` em uma tabela por estabelecimento.
- **A próxima normalização clara é `cnae_fiscal_secundaria`:** hoje é uma string com códigos separados por vírgula. Uma tabela lateral resolve consultas por CNAE secundário.
- **Booleanos, descrições de enum e CNPJ formatado ficam para receitas futuras.** São conveniências, não fatos novos da fonte.

## Como ler a tabela

- **Forma na fonte** — o que a Receita publica antes de qualquer transformação.
- **Normalização atual** — o que o pipeline já faz hoje em `processor.py` ou via tipagem em `initial.sql`.
- **Possível normalização no núcleo** — mudanças universais que poderiam entrar na carga padrão. Vazio = nada a fazer.
- **Receita relacionada** — onde a derivação aplicável vive, ou viveria.
- **Prioridade** — relativa entre as receitas, não entre normalizações.

## empresas

| Campo | Forma na fonte | Normalização atual | Possível normalização no núcleo | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` | 8 dígitos, string | validação regex `^\d{8}$` | — | usado em todas | — |
| `razao_social` | TEXT, ALL CAPS, sem acentos | — | trim de espaços (a confirmar) | — | — |
| `natureza_juridica` | 4 dígitos, string | validação regex `^\d{4}$` | — | descrição em `empresa_detalhe` | alta |
| `qualificacao_responsavel` | 2 dígitos, string | validação regex `^\d{2}$` | — | descrição em receita futura | média |
| `capital_social` | "1.234,56" no CSV, depois "1234.56" string, `DOUBLE PRECISION` em PostgreSQL | conversão de vírgula decimal, negativos → null | já tipado em Parquet com `PARQUET_TYPED_OUTPUT=true` (v1.18+) | — | — |
| `porte` | "01" \| "03" \| "05" \| null | validação regex; ~50M são `01` (Microempresa), ~15M `05` (Demais), ~2M `03` (EPP), 3K null | — | descrições em receita futura | baixa |
| `ente_federativo_responsavel` | TEXT, quase sempre vazio | — | — | — | — |

## estabelecimentos

| Campo | Forma na fonte | Normalização atual | Possível normalização no núcleo | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` + `cnpj_ordem` + `cnpj_dv` | strings de 8+4+2 dígitos | — | — | coluna concatenada `cnpj` em `empresa_detalhe` | alta |
| `identificador_matriz_filial` | "1" \| "2" no CSV, `INTEGER` em PostgreSQL | tipagem via schema | tipado em Parquet (v1.18+) | descrição em receita futura | baixa |
| `nome_fantasia` | TEXT, ALL CAPS, sem acentos | — | trim (a confirmar) | — | — |
| `situacao_cadastral` | "01" \| "02" \| "03" \| "04" \| "08" | validação regex | — | descrição e booleanos (`is_ativa`) em receita futura | baixa |
| `data_situacao_cadastral`, `data_inicio_atividade`, `data_situacao_especial` | YYYYMMDD ou "0"/"00000000" | placeholder → null, parse + range check (1900..hoje), `DATE` em PostgreSQL | tipado em Parquet (v1.18+) | — | — |
| `motivo_situacao_cadastral` | 2 dígitos, string | — | — | descrição em `empresa_detalhe` | alta |
| `cnae_fiscal_principal` | 7 dígitos, string | validação regex `^\d{7}$` | — | descrição em `empresa_detalhe` | alta |
| `cnae_fiscal_secundaria` | string com códigos de 7 dígitos separados por vírgula, ex: "5914600,8230002,9001999" | — | — | tabela lateral `estabelecimentos_cnae_secundaria(cnpj_basico, cnpj_ordem, cnpj_dv, cnae_codigo)` | alta |
| `pais` | 3 dígitos com zero-padding | padding `zfill(3)` | — | descrição em `empresa_detalhe` quando existir | baixa |
| `uf` | 2 letras | validação contra lista de 27 UFs + "EX" | — | — | — |
| `municipio` | código RFB do município, string (geralmente 4 dígitos; coluna aceita até 7) | — | — | descrição em `empresa_detalhe` | alta |
| `tipo_logradouro`, `logradouro`, `numero`, `complemento`, `bairro` | TEXT, ALL CAPS, sem acentos | — | — | concatenação em receita futura (opcional) | baixa |
| `cep` | 8 dígitos, string | padding `zfill(8)` quando o valor é exatamente 7 dígitos numéricos (RFB perde o zero à esquerda em ~0,1% das linhas, sobretudo CEPs `0xxxxxxx` de São Paulo) | — | flag `cep_is_zero_sentinel` / `cep_is_malformed` em `data_quality_flags` | média |
| `ddd_1`, `telefone_1`, etc. | strings de dígitos, sem formatação | — | — | — | — |
| `correio_eletronico` | TEXT, ALL CAPS | — | — | — | — |

## socios

| Campo | Forma na fonte | Normalização atual | Possível normalização no núcleo | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` | 8 dígitos | validação regex | — | — | — |
| `identificador_de_socio` | "1" \| "2" \| "3" | validação regex | — | descrições em `socios_detalhe` | baixa |
| `nome_socio` | TEXT | — | — | — | — |
| `cnpj_cpf_do_socio` | já mascarado pela RFB: `***123456**` (CPF) ou CNPJ completo | fallback para "00000000000000" quando null (PK) | — | — | — |
| `qualificacao_do_socio` | 2 dígitos | — | — | descrição em `socios_detalhe` | baixa |
| `data_entrada_sociedade` | YYYYMMDD | mesma normalização que outras datas | tipado em Parquet (v1.18+) | — | — |
| `pais` | 3 dígitos zero-padded | padding | — | descrição em `socios_detalhe` | baixa |
| `representante_legal` | `***000000**` quando não há | — | — | valor sentinela → null em receita futura (`socios_cleanup`) | baixa |
| `qualificacao_do_representante_legal` | "00" quando não há | — | — | valor sentinela → null em receita futura | baixa |
| `faixa_etaria` | "1".."9" | validação regex | — | descrições (`socios_detalhe`) | baixa |

## dados_simples

| Campo | Forma na fonte | Normalização atual | Possível normalização no núcleo | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` | 8 dígitos, PK | validação regex | — | — | — |
| `opcao_pelo_simples` | "S" \| "N" | validação regex | — | incluído cru em `empresa_detalhe` | alta |
| `data_opcao_pelo_simples`, `data_exclusao_do_simples`, `data_opcao_pelo_mei`, `data_exclusao_do_mei` | YYYYMMDD ou null | normalização de datas | tipado em Parquet (v1.18+) | incluído cru em `empresa_detalhe` | alta |
| `opcao_pelo_mei` | "S" \| "N" | validação regex | — | incluído cru em `empresa_detalhe` | alta |

> Observação: `dados_simples` é por `cnpj_basico` (empresa-nível), não por estabelecimento. Em `empresa_detalhe` essas colunas se repetem em todas as linhas de uma mesma empresa.

## Tabelas de referência (cnaes, motivos, municipios, naturezas_juridicas, paises, qualificacoes_socios)

No PostgreSQL, todas têm a mesma forma: `(codigo, descricao, data_criacao, data_atualizacao)`. Nos arquivos de origem e no Parquet, a forma é apenas `(codigo, descricao)`. Não há normalização aplicável: são tabelas de referência.

> Medição em 12/05/2026: zero órfãos em `estabelecimentos.cnae_fiscal_principal` e `estabelecimentos.municipio` contra suas tabelas de referência. `LEFT JOIN` continua sendo a escolha defensiva para snapshots históricos, mas no mês medido `INNER JOIN` produziria o mesmo resultado.

## Quirks da fonte (preservados, não corrigidos)

A entrega mensal da RFB tem inconsistências internas que o pipeline preserva intencionalmente. O `scripts/data_quality_report.py` mede cada uma; receitas opcionais podem mascarar ou nulificar quando o consumidor preferir. Medições abaixo em 12/05/2026 contra a entrega 2026-04.

- **`estabelecimentos.motivo_situacao_cadastral = '32'`** — 18.672 linhas referenciam um código presente em `Estabelecimentos.csv` mas ausente do `Motivos.csv` da mesma entrega. O layout oficial da RFB não publica a lista de motivos válidos, então não conseguimos confirmar o status do código (retirado, erro de entrega, etc.) a partir das fontes oficiais. Preservado; aparecerá como `NULL` no `LEFT JOIN` com `motivos` em receitas.
- **`estabelecimentos.pais` órfãos** — 14 códigos distintos, 1.220 linhas. Mais frequentes: `150` (583), `367` (483), `359` (97). Quase todos em `uf='EX'`; nove linhas em UFs brasileiras (códigos `008`, `009`). Drift entre `Estabelecimentos.csv` e `Paises.csv` da mesma entrega. O layout oficial da RFB também não publica a lista de países válidos, então não inferimos status individual (retirado vs. ativo) sem fonte adicional.
- **`estabelecimentos.uf = 'EX'`** — 170.865 linhas. Padrão observado para registros no exterior: as mesmas linhas costumam ter `NOME DA CIDADE NO EXTERIOR` preenchido e a coluna `pais` ativa. O layout oficial da RFB não documenta o código `EX` explicitamente; tratamos como código convencional usado pela RFB, não como código oficial citado em norma.
- **`empresas.capital_social = 999999999999`** — 124 linhas. Valor suspeito de sentinela para capital não informado/desconhecido. O layout oficial da RFB não documenta este sentinela. Preservado para que o sinal continue visível; uma receita pode mascarar.
- **`socios.representante_legal = '***000000**'` + `qualificacao_do_representante_legal = '00'`** — 26.730.045 linhas (97% dos sócios). A forma `***000000**` é consistente com a regra pública de mascaramento de CPF (LDO 2018, art. 129 §2º — ocultar os três primeiros dígitos e os dois dígitos verificadores), aplicada sobre um CPF de origem `00000000000`. A leitura "sem representante legal separado" é empírica (97% dos registros), não documentada. Preservado; uma receita pode expor `has_representante_legal` quando o consumidor quiser tratar como `NULL`.
- **`estabelecimentos.cep` residual após padding** — após o `zfill(8)` aplicado a valores com exatamente 7 dígitos numéricos (v1.21.0+), restam ~2.914 valores não conformes (`'0'`, `'       0'`, 8 caracteres com letras, etc.). Preservados como vieram. Resumo da política:
  - Correios define CEP como 8 algarismos numéricos.
  - A RFB entrega alguns CEPs com 7 dígitos numéricos.
  - O pipeline padroniza exclusivamente os valores com exatamente 7 dígitos numéricos.
  - Validação de existência contra Correios/DNE está fora do núcleo.

## Fontes oficiais

Onde cada afirmação acima foi (ou não) verificada contra fonte autoritativa.

- **Algoritmo do dígito verificador do CNPJ** — Receita Federal / Serpro, documento técnico [`manual-dv-cnpj.pdf`](https://www.gov.br/receitafederal/pt-br/centrais-de-conteudo/publicacoes/documentos-tecnicos/cnpj/manual-dv-cnpj.pdf): módulo 11, pesos 5,4,3,2,9,8,7,6,5,4,3,2 (DV1) e 6,5,4,3,2,9,8,7,6,5,4,3,2 (DV2). Regra: se `resto = soma mod 11 ∈ {0,1}`, DV = 0; caso contrário, DV = `11 - resto`. O algoritmo permanece o mesmo no CNPJ alfanumérico que entra em vigor em julho/2026 ([nota RFB](https://www.gov.br/receitafederal/pt-br/assuntos/noticias/2024/outubro/cnpj-tera-letras-e-numeros-a-partir-de-julho-de-2026)); CNPJs numéricos existentes continuam válidos.
- **Forma do CEP (8 algarismos numéricos)** — Correios: ["O CEP é um conjunto numérico constituído de oito algarismos"](https://www.correios.com.br/enviar/precisa-de-ajuda/tudo-sobre-cep).
- **Existência de um CEP específico** — autoridade é o DNE/Correios (proprietário). **Fora do escopo do núcleo do pipeline**: introduzir validação por chamada externa adicionaria dependência de auth/rate-limit/licenciamento e reduziria a reprodutibilidade da carga. Receitas/ferramentas opcionais podem fazer essa checagem em amostra.
- **Códigos de município e UF (geografia)** — IBGE é a autoridade para enriquecimento geográfico (códigos de município, microrregião, mesorregião). IBGE **não é** autoridade para validar CEP.
- **Layout dos dados abertos do CNPJ** — Receita Federal: [`cnpj-metadados.pdf`](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf). É um documento curto; não enumera valores válidos de `motivo`, `pais`, `uf` (além do que aparece em prosa), nem documenta sentinelas como `999999999999` em capital_social ou `'***000000**'` em representante_legal.

## Decisões para a primeira receita (empresa_detalhe)

A receita `recipes/postgres/empresa_detalhe.sql` implementa:

- **LEFT JOIN** com `cnaes`, `municipios`, `motivos`, `paises`, `naturezas_juridicas`: preserva linhas mesmo com códigos retirados em snapshots históricos.
- **LEFT JOIN** com `dados_simples`: inclui colunas cruas (`opcao_pelo_simples`, datas, `opcao_pelo_mei`). Sem booleanos derivados.
- **Coluna `cnpj`** = `cnpj_basico || cnpj_ordem || cnpj_dv`: evita repetir a concatenação em consultas.
- **`CREATE TABLE AS`**: modelo esperado para receitas aplicadas depois do ingest.
- **Sem descrições de enum**: `situacao_cadastral` continua sendo "02", não "Ativa". Sem booleanos (`is_ativa`, `is_matriz`). Essas escolhas ficam para receitas futuras.

## Receitas planejadas após a primeira

1. **`cnae_secundaria_exploded`** — tabela lateral para `cnae_fiscal_secundaria`. Útil pelo volume de estabelecimentos com múltiplos CNAEs secundários (~20M+ linhas medidos em 12/05/2026).
2. **`socios_detalhe`** — junções com `qualificacoes_socios` e `paises`. Tratamento opcional do valor sentinela `***000000**` / `00` para representante legal.
3. **`labels`** — expansão de enums (`situacao_cadastral` → `situacao_cadastral_descricao`, `porte` → `porte_descricao`, etc.).
4. **`booleanos`** — colunas convenientes como `is_ativa`, `is_matriz`, `is_optante_simples_atual`. Cada uma deve documentar a regra usada.

Tabelas de busca específicas (`lookup_empresas_nome`, `lookup_nome_fantasia`) e agregações por UF/CNAE/ano não estão no roadmap das receitas genéricas. São casos de uso específicos o bastante para ficar no repositório do consumidor.
