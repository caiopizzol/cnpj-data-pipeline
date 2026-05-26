# Auditoria dos dados: núcleo e receitas

Este documento registra o que a Receita Federal entrega, o que o pipeline normaliza e o que deve ficar em receitas SQL opcionais. A regra geral está em [post-processing.md](post-processing.md).

> **Medições datadas:** as contagens citadas abaixo foram feitas em **12/05/2026** contra a entrega 2026-04 carregada em PostgreSQL pelo próprio pipeline. A forma dos dados costuma ser estável, mas os números mudam todo mês. Refaça a medição quando uma decisão depender do volume.

## Resumo

- **A base já carrega bem:** datas, capital social e encoding têm tratamento suficiente para PostgreSQL e Parquet tipado.
- **A primeira receita útil é `empresa_detalhe`:** uma linha por estabelecimento, com empresas, tabelas de referência e `dados_simples`.
- **O próximo caso claro é `cnae_fiscal_secundaria`:** hoje é uma string com códigos separados por vírgula. Uma tabela lateral torna consultas por CNAE secundário mais simples.
- **Booleanos, descrições de códigos e CNPJ formatado ficam para receitas futuras.** São conveniências de uso, não fatos novos da fonte.

## Como ler a tabela

- **Forma na fonte** — o que a Receita publica antes de qualquer transformação.
- **Normalização atual** — o que o pipeline já faz hoje em `processor.py` ou via tipagem em `initial.sql`.
- **Possível normalização no núcleo** — mudanças universais que poderiam entrar na carga padrão. Vazio = nada a fazer agora.
- **Receita relacionada** — onde a derivação aplicável vive, ou viveria.
- **Prioridade** — relativa entre as receitas, não entre normalizações.

## empresas

| Campo | Forma na fonte | Normalização atual | Possível normalização no núcleo | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` | 8 dígitos, string | validação regex `^\d{8}$` | — | usado em todas | — |
| `razao_social` | TEXT, maiúsculas, sem acentos | — | trim de espaços (a confirmar) | — | — |
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
| `nome_fantasia` | TEXT, maiúsculas, sem acentos | — | trim (a confirmar) | — | — |
| `situacao_cadastral` | "01" \| "02" \| "03" \| "04" \| "08" | validação regex | — | descrição e booleanos (`is_ativa`) em receita futura | baixa |
| `data_situacao_cadastral`, `data_inicio_atividade`, `data_situacao_especial` | YYYYMMDD ou "0"/"00000000" | placeholder → null, parse + range check (1900..hoje), `DATE` em PostgreSQL | tipado em Parquet (v1.18+) | — | — |
| `motivo_situacao_cadastral` | 2 dígitos, string | — | — | descrição em `empresa_detalhe` | alta |
| `cnae_fiscal_principal` | 7 dígitos, string | validação regex `^\d{7}$` | — | descrição em `empresa_detalhe` | alta |
| `cnae_fiscal_secundaria` | string com códigos de 7 dígitos separados por vírgula, ex: "5914600,8230002,9001999" | — | — | tabela lateral `estabelecimentos_cnae_secundaria(cnpj_basico, cnpj_ordem, cnpj_dv, cnae_codigo)` | alta |
| `pais` | 3 dígitos com zero-padding | padding `zfill(3)` | — | descrição em `empresa_detalhe` quando existir | baixa |
| `uf` | 2 letras | validação contra lista de 27 UFs + "EX" | — | — | — |
| `municipio` | código de município da Receita Federal, string (geralmente 4 dígitos; coluna aceita até 7) | — | — | descrição em `empresa_detalhe` | alta |
| `tipo_logradouro`, `logradouro`, `numero`, `complemento`, `bairro` | TEXT, maiúsculas, sem acentos | — | — | concatenação em receita futura (opcional) | baixa |
| `cep` | 8 dígitos, string | padding `zfill(8)` quando o valor é exatamente 7 dígitos numéricos (a Receita Federal perde o zero à esquerda em ~0,1% das linhas, sobretudo CEPs `0xxxxxxx` de São Paulo) | — | flag `cep_is_zero_sentinel` / `cep_is_malformed` em `data_quality_flags` | média |
| `ddd_1`, `telefone_1`, etc. | strings de dígitos, sem formatação | — | — | — | — |
| `correio_eletronico` | TEXT, maiúsculas | — | — | — | — |

## socios

| Campo | Forma na fonte | Normalização atual | Possível normalização no núcleo | Receita relacionada | Prioridade |
|---|---|---|---|---|---|
| `cnpj_basico` | 8 dígitos | validação regex | — | — | — |
| `identificador_de_socio` | "1" \| "2" \| "3" | validação regex | — | descrições em `socios_detalhe` | baixa |
| `nome_socio` | TEXT | — | — | — | — |
| `cnpj_cpf_do_socio` | já mascarado pela Receita Federal: `***123456**` (CPF) ou CNPJ completo | substitui null por "00000000000000" para manter a chave primária | — | — | — |
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

> Medição em 12/05/2026: zero órfãos em `estabelecimentos.cnae_fiscal_principal` e `estabelecimentos.municipio` contra suas tabelas de referência. `LEFT JOIN` continua sendo a escolha defensiva para entregas históricas, mas no mês medido `INNER JOIN` produziria o mesmo resultado.

## Pontos de atenção da fonte

A entrega mensal da Receita Federal tem alguns desencontros entre arquivos. O pipeline preserva esses valores e o `scripts/data_quality_report.py` mede cada caso. Receitas opcionais podem marcar, mascarar ou transformar valores em `NULL` quando o consumidor quiser essa interpretação. Medições abaixo em 12/05/2026 contra a entrega 2026-04.

- **`estabelecimentos.motivo_situacao_cadastral = '32'`** — 18.672 linhas referenciam um código presente em `Estabelecimentos.csv` mas ausente do `Motivos.csv` da mesma entrega. O layout oficial da Receita Federal não publica a lista de motivos válidos, então não conseguimos confirmar o status do código (retirado, erro de entrega, etc.) a partir das fontes oficiais. Preservado; aparecerá como `NULL` no `LEFT JOIN` com `motivos` em receitas.
- **`estabelecimentos.pais` órfãos** — 14 códigos distintos, 1.220 linhas. Mais frequentes: `150` (583), `367` (483), `359` (97). Quase todos em `uf='EX'`; nove linhas em UFs brasileiras (códigos `008`, `009`). É uma diferença entre `Estabelecimentos.csv` e `Paises.csv` da mesma entrega. O layout oficial da Receita Federal também não publica a lista de países válidos, então não inferimos status individual (retirado vs. ativo) sem fonte adicional.
- **`estabelecimentos.uf = 'EX'`** — 170.865 linhas. Padrão observado para registros no exterior: as mesmas linhas costumam ter `NOME DA CIDADE NO EXTERIOR` preenchido e a coluna `pais` preenchida. O layout oficial da Receita Federal não documenta o código `EX` explicitamente; tratamos como código convencional usado pela Receita Federal, não como código oficial citado em norma.
- **`empresas.capital_social = 999999999999`** — 124 linhas. Valor suspeito de sentinela para capital não informado/desconhecido. O layout oficial da Receita Federal não documenta este sentinela. Preservado para que o sinal continue visível; uma receita pode mascarar.
- **`socios.representante_legal = '***000000**'` + `qualificacao_do_representante_legal = '00'`** — 26.730.045 linhas (97% dos sócios). A forma `***000000**` é consistente com a regra pública de mascaramento de CPF (LDO 2018, art. 129 §2º — ocultar os três primeiros dígitos e os dois dígitos verificadores), aplicada sobre um CPF de origem `00000000000`. A leitura "sem representante legal separado" é empírica (97% dos registros), não documentada. Preservado; uma receita pode expor `has_representante_legal` quando o consumidor quiser tratar como `NULL`.
- **`estabelecimentos.cep` residual após padding** — após o `zfill(8)` aplicado a valores com exatamente 7 dígitos numéricos (v1.21.0+), restam ~2.914 valores não conformes (`'0'`, `'       0'`, 8 caracteres com letras, etc.). Preservados como vieram. Resumo da política:
  - Correios define CEP como 8 algarismos numéricos.
  - A Receita Federal entrega alguns CEPs com 7 dígitos numéricos.
  - O pipeline padroniza exclusivamente os valores com exatamente 7 dígitos numéricos.
  - Validação de existência contra Correios/DNE está fora do núcleo.

## Fontes oficiais

Onde cada afirmação acima foi (ou não) verificada contra uma fonte oficial.

- **Algoritmo do dígito verificador do CNPJ** — Receita Federal / Serpro, documento técnico [`manual-dv-cnpj.pdf`](https://www.gov.br/receitafederal/pt-br/centrais-de-conteudo/publicacoes/documentos-tecnicos/cnpj/manual-dv-cnpj.pdf): módulo 11, pesos 5,4,3,2,9,8,7,6,5,4,3,2 (DV1) e 6,5,4,3,2,9,8,7,6,5,4,3,2 (DV2). Regra: se `resto = soma mod 11 ∈ {0,1}`, DV = 0; caso contrário, DV = `11 - resto`. O algoritmo permanece o mesmo no CNPJ alfanumérico que entra em vigor em julho/2026 ([nota da Receita Federal](https://www.gov.br/receitafederal/pt-br/assuntos/noticias/2024/outubro/cnpj-tera-letras-e-numeros-a-partir-de-julho-de-2026)); CNPJs numéricos existentes continuam válidos.
- **Forma do CEP (8 algarismos numéricos)** — Correios: ["O CEP é um conjunto numérico constituído de oito algarismos"](https://www.correios.com.br/enviar/precisa-de-ajuda/tudo-sobre-cep).
- **Existência de um CEP específico** — a base de referência é o DNE/Correios. **Fora do escopo do núcleo do pipeline**: validar por chamada externa adicionaria dependência de autenticação, limite de uso e licenciamento, além de reduzir a reprodutibilidade da carga. Receitas ou ferramentas opcionais podem fazer essa checagem em amostra.
- **Códigos de município e UF (geografia)** — IBGE é a fonte para enriquecimento geográfico (códigos de município, microrregião, mesorregião). IBGE **não é** fonte para validar CEP.
- **Layout dos dados abertos do CNPJ** — Receita Federal: [`cnpj-metadados.pdf`](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf). É um documento curto; não enumera valores válidos de `motivo`, `pais`, `uf` (além do que aparece em prosa), nem documenta sentinelas como `999999999999` em `capital_social` ou `'***000000**'` em `representante_legal`.

## Decisões para a primeira receita (empresa_detalhe)

A receita `recipes/postgres/empresa_detalhe.sql` implementa:

- **LEFT JOIN** com `cnaes`, `municipios`, `motivos`, `paises`, `naturezas_juridicas`: preserva linhas mesmo com códigos retirados em entregas históricas.
- **LEFT JOIN** com `dados_simples`: inclui colunas cruas (`opcao_pelo_simples`, datas, `opcao_pelo_mei`). Sem booleanos derivados.
- **Coluna `cnpj`** = `cnpj_basico || cnpj_ordem || cnpj_dv`: evita repetir a concatenação em consultas.
- **`CREATE TABLE AS`**: modelo esperado para receitas aplicadas depois do ingest.
- **Sem descrições de enum**: `situacao_cadastral` continua sendo "02", não "Ativa". Sem booleanos (`is_ativa`, `is_matriz`). Essas escolhas ficam para receitas futuras.

## Receitas planejadas após a primeira

1. **`data_quality_flags`** (v1.22.0+) — tabela estreita, uma linha por estabelecimento, com sinais sem mutação de valor: `cep_status`, `is_exterior`, `pais_lookup_missing`, `motivo_lookup_missing`, `capital_social_is_suspicious_sentinel`. Serve como predicate-source para a futura `estabelecimentos_clean`. Sócios ficam para uma receita separada (`socios_quality_flags`) por terem grão diferente.
2. **`estabelecimentos_clean`** (v1.23.0+) — junta `estabelecimentos`, `empresas` e `data_quality_flags`. Primeira receita que altera valores: emite `cep_clean` (NULL quando `cep_status != 'valid_shape'`) e `capital_social_clean` (NULL quando `capital_social_is_suspicious_sentinel`). Preserva os valores crus (`cep_raw`, `capital_social_raw`) ao lado das colunas limpas. Usa exclusivamente os predicados de `data_quality_flags` — qualquer mudança de interpretação acontece lá, não aqui.
3. **`cnae_secundaria_exploded`** (v1.24.0+) — tabela lateral que faz unnest de `cnae_fiscal_secundaria`. Uma linha por (estabelecimento, CNAE secundário). Medido em 12/05/2026 contra a entrega 2026-04: 33.187.235 estabelecimentos com `cnae_fiscal_secundaria` preenchido produzem 119.193.214 linhas; 100% dos códigos são 7 dígitos numéricos, zero órfãos contra `cnaes`. Sem deduplicação (preserva a forma da fonte), sem `position`, sem JOIN com descrições.
4. **`socios_quality_flags`** (v1.25.0+, recipeVersion 2 desde a correção do issue #78) — tabela estreita, uma linha por sócio, chave `socio_id` (UUID determinístico em `socios.socio_id`). O trio antigo (`cnpj_basico + identificador_de_socio + cnpj_cpf_do_socio`) permanece como colunas de lookup mas não é único: dois sócios PF da mesma empresa podem compartilhar os 6 dígitos visíveis do CPF mascarado. Sinais sem mutação de valor: `representante_is_placeholder`, `pais_lookup_missing`, `qualificacao_socio_lookup_missing`, `qualificacao_representante_lookup_missing` (excluindo `'00'`, que é o placeholder), `faixa_etaria_nao_se_aplica` (`= '0'`). Serve como predicate-source para `socios_clean`.
5. **`socios_clean`** (v1.26.0+) — camada limpa sobre `socios_quality_flags`. Preserva pares cru/limpo para o trio do representante (`representante_legal`, `nome_do_representante`, `qualificacao_do_representante_legal` — nulificados juntos quando `representante_is_placeholder`) e para `faixa_etaria` (nulificado quando `= '0'`). Sem labels, sem joins de descrição, sem novos booleanos. Usa exclusivamente os predicados de `socios_quality_flags` como fonte única de interpretação.
6. **`socios_detalhe`** — junções com `qualificacoes_socios` e `paises` para descrições. Sem mutação de valor; pareceria a `empresa_detalhe` na função.
7. **`labels`** — expansão de enums (`situacao_cadastral` → `situacao_cadastral_descricao`, `porte` → `porte_descricao`, etc.).
8. **`booleanos`** — colunas convenientes como `is_ativa`, `is_matriz`, `is_optante_simples_atual`. Cada uma deve documentar a regra usada.

Tabelas de busca específicas (`lookup_empresas_nome`, `lookup_nome_fantasia`) e agregações por UF/CNAE/ano não estão no roadmap das receitas genéricas. São casos de uso específicos o bastante para ficar no repositório do consumidor.
