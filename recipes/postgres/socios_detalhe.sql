-- recipes/postgres/socios_detalhe.sql
--
-- recipeVersion: 1
--
-- Per-sócio denormalization joining socios to the reference lookups that
-- describe its coded fields. The sócio-grain counterpart to empresa_detalhe:
-- it preserves every source code and adds the description beside it, with no
-- derived booleans and no value mutation.
--
-- Apply after the pipeline finishes ingest. qualificacao/pais descriptions come
-- from the enriched lookups, and identificador_de_socio/faixa_etaria
-- descriptions come from the static domain-label tables, so run both of those
-- recipes first:
--     psql "$DATABASE_URL" -f recipes/postgres/reference_domains_enriched.sql
--     psql "$DATABASE_URL" -f recipes/postgres/reference_domain_labels.sql
--     psql "$DATABASE_URL" -f recipes/postgres/socios_detalhe.sql
--
-- Re-run after each monthly ingest to refresh.
--
-- Dependencies: reference_domains_enriched.sql (paises_enriched,
-- qualificacoes_socios_enriched) and reference_domain_labels.sql
-- (identificadores_socio, faixas_etarias).
--
-- Design choices (see docs/data-audit.md for the field-by-field rationale):
--   - qualificacao/pais descriptions come from the *_enriched lookups so
--     officially-resolved supplemental codes (e.g. qualificacao 36, the legacy
--     Gerente-Delegado) get a description instead of NULL. identificador_de_socio
--     and faixa_etaria descriptions come from the static label tables: the
--     monthly package ships those as bare codes with no domain CSV.
--   - Every lookup table is 1:1 on its codigo key, so all five LEFT JOINs
--     preserve the socios row count exactly; an unknown code keeps the row with
--     a NULL descricao.
--   - LEFT JOIN throughout: defensive against retired codes in historical
--     snapshots, and against the documented placeholders. Crucially NO value
--     mutation - a representante with qualificacao '00' or representante_legal
--     '***000000**' keeps its raw value here; nulling those placeholders is the
--     job of socios_clean (which owns that interpretation), not this recipe.
--
-- AIDEV-NOTE: grain is socio_id (the PK of socios, a deterministic UUID). The
-- old triple (cnpj_basico + identificador_de_socio + cnpj_cpf_do_socio) is NOT
-- unique - two PF sócios of the same company can share the 6 visible digits of
-- the masked CPF (issue #78) - so it must never be used as this table's key.

DROP TABLE IF EXISTS socios_detalhe;
CREATE TABLE socios_detalhe AS
SELECT
    s.socio_id,
    s.cnpj_basico,
    s.identificador_de_socio,
    ids.descricao AS identificador_de_socio_descricao,
    s.nome_socio,
    s.cnpj_cpf_do_socio,
    s.qualificacao_do_socio,
    q1.descricao AS qualificacao_do_socio_descricao,
    s.data_entrada_sociedade,
    s.pais,
    p.descricao AS pais_descricao,
    s.representante_legal,
    s.nome_do_representante,
    s.qualificacao_do_representante_legal,
    q2.descricao AS qualificacao_do_representante_legal_descricao,
    s.faixa_etaria,
    fe.descricao AS faixa_etaria_descricao
FROM socios s
LEFT JOIN identificadores_socio ids ON ids.codigo = s.identificador_de_socio
LEFT JOIN qualificacoes_socios_enriched q1 ON q1.codigo = s.qualificacao_do_socio
LEFT JOIN paises_enriched p ON p.codigo = s.pais
LEFT JOIN qualificacoes_socios_enriched q2 ON q2.codigo = s.qualificacao_do_representante_legal
LEFT JOIN faixas_etarias fe ON fe.codigo = s.faixa_etaria;

-- socio_id is the grain: unique by construction (PK of socios). cnpj_basico is
-- the company-level lookup, non-unique (a company has many sócios).
CREATE UNIQUE INDEX IF NOT EXISTS idx_socios_detalhe_socio_id ON socios_detalhe (socio_id);
CREATE INDEX IF NOT EXISTS idx_socios_detalhe_basico ON socios_detalhe (cnpj_basico);

ANALYZE socios_detalhe;
