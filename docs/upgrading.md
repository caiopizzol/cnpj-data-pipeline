# Atualizando bancos existentes

## `socios.socio_id`

A tabela `socios` agora usa `socio_id` como chave primária (issue #78).

Antes, a chave dependia de `cnpj_cpf_do_socio`, mas esse campo não é único na base pública da Receita: CPFs vêm mascarados (`***NNNNNN**`, só os 6 dígitos do meio aparecem) e sócios estrangeiros podem vir sem CPF. Isso podia causar erro na carga ou deixar linhas de fora silenciosamente, dependendo do `LOADING_STRATEGY` em uso.

Para recarregar só `socios` e as receitas derivadas, troque `2026-05` pelo mês carregado no seu banco:

```bash
psql "$DATABASE_URL" -c 'DROP TABLE IF EXISTS socios_clean, socios_quality_flags, socios CASCADE;'
psql "$DATABASE_URL" -f initial.sql
psql "$DATABASE_URL" -c "DELETE FROM processed_files WHERE directory = '2026-05' AND filename ILIKE 'Socios%.zip';"
just run --month 2026-05
```

O `psql -f initial.sql` é necessário porque `ensure_schema()` só roda em bancos sem `processed_files`. Derrubar apenas `socios` (e as tabelas-receita) não dispara o bootstrap automático.

Se preferir recarregar o mês inteiro em vez de só sócios:

```bash
just run --month 2026-05 --force
```
