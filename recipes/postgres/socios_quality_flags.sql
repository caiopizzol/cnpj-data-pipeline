-- recipes/postgres/socios_quality_flags.sql
--
-- recipeVersion: 1
--
-- Narrow per-socio table of data-quality signals. One row per socio,
-- keyed the same way as the source table: cnpj_basico +
-- identificador_de_socio + cnpj_cpf_do_socio.
--
-- No source columns are changed or duplicated here. This recipe only
-- materializes predicates that consumers can use later in their own clean
-- tables or reports.
--
-- Apply after the pipeline finishes ingest:
--     psql "$DATABASE_URL" -f recipes/postgres/socios_quality_flags.sql
--
-- Design notes (see docs/data-audit.md for source-by-source rationale):
--   - representante_is_placeholder uses the observed pair
--     representante_legal='***000000**' and
--     qualificacao_do_representante_legal='00'. The masked CPF form is
--     consistent with public CPF masking rules, but the "sem representante"
--     meaning is empirical.
--   - faixa_etaria_nao_se_aplica reflects the documented '0' value.
--   - Lookup flags are defensive. Some are zero in current snapshots, but
--     keeping them here makes drift visible without mutating the source.
--   - qualificacao_representante_lookup_missing excludes '00' on purpose -
--     that's the placeholder qualification that pairs with the placeholder
--     representante_legal, not an orphan.

DROP TABLE IF EXISTS socios_quality_flags;
CREATE TABLE socios_quality_flags AS
SELECT
    s.cnpj_basico,
    s.identificador_de_socio,
    s.cnpj_cpf_do_socio,
    (
        s.representante_legal = '***000000**'
        AND s.qualificacao_do_representante_legal = '00'
    ) AS representante_is_placeholder,
    (
        s.pais IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = s.pais)
    ) AS pais_lookup_missing,
    (
        s.qualificacao_do_socio IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM qualificacoes_socios q
            WHERE q.codigo = s.qualificacao_do_socio
        )
    ) AS qualificacao_socio_lookup_missing,
    (
        s.qualificacao_do_representante_legal IS NOT NULL
        AND s.qualificacao_do_representante_legal <> '00'
        AND NOT EXISTS (
            SELECT 1 FROM qualificacoes_socios q
            WHERE q.codigo = s.qualificacao_do_representante_legal
        )
    ) AS qualificacao_representante_lookup_missing,
    (s.faixa_etaria = '0') AS faixa_etaria_nao_se_aplica
FROM socios s;

CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_basico
    ON socios_quality_flags (cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_pais_missing
    ON socios_quality_flags (cnpj_basico) WHERE pais_lookup_missing;
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_qual_socio_missing
    ON socios_quality_flags (cnpj_basico) WHERE qualificacao_socio_lookup_missing;
CREATE INDEX IF NOT EXISTS idx_socios_quality_flags_qual_representante_missing
    ON socios_quality_flags (cnpj_basico) WHERE qualificacao_representante_lookup_missing;

ANALYZE socios_quality_flags;
