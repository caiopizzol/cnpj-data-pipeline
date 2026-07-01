-- recipes/postgres/cnaes_hierarquia.sql
--
-- recipeVersion: 1
--
-- Derived side table exposing the CNAE hierarchy (seção / divisão / grupo /
-- classe) for every CNAE subclasse in the `cnaes` reference table. Lets
-- consumers facet and group by hierarchy level without changing the flat
-- reference table.
--
-- Apply after the pipeline finishes ingest:
--     psql "$DATABASE_URL" -f recipes/postgres/cnaes_hierarquia.sql
--
-- Re-run after each monthly ingest to refresh.
--
-- Grain: one row per cnaes.codigo (same cardinality as the reference table).
--
-- Design choices (see docs/data-audit.md for the rationale):
--   - divisao/grupo/classe are pure substrings of the 7-digit codigo, no
--     external source: divisao = codigo[1..2] (01), grupo = codigo[1..3] (011),
--     classe = codigo[1..4]-codigo[5] (0111-3, the DDDD-D form).
--   - secao (A-U) is NOT derivable from the code: it maps to ranges of divisões
--     set by IBGE. Source: the official IBGE/CONCLA "CNAE-Subclasses 2.3 -
--     Estrutura Detalhada" workbook, Resolução CONCLA nº 2, de 19/11/2018
--     (https://concla.ibge.gov.br/images/concla/documentacao/CNAE_Subclasses_2_3_Estrutura_Detalhada.xlsx).
--     The 87 divisão->seção pairs in secao_por_divisao were derived by parsing
--     that workbook (21 seções, 87 divisões; each divisão -> exactly one seção).
--   - LEFT JOIN: a codigo whose divisão is not in the map (retired/unknown) is
--     kept with secao = NULL, not dropped.
--   - Carries cnaes.descricao for labels; does not add seção/divisão/grupo names
--     (would need a second IBGE transcription, out of scope).

DROP TABLE IF EXISTS cnaes_hierarquia;
CREATE TABLE cnaes_hierarquia AS
WITH secao_por_divisao (divisao, secao) AS (
    VALUES
        ('01', 'A'), ('02', 'A'), ('03', 'A'), ('05', 'B'), ('06', 'B'), ('07', 'B'), ('08', 'B'), ('09', 'B'),
        ('10', 'C'), ('11', 'C'), ('12', 'C'), ('13', 'C'), ('14', 'C'), ('15', 'C'), ('16', 'C'), ('17', 'C'),
        ('18', 'C'), ('19', 'C'), ('20', 'C'), ('21', 'C'), ('22', 'C'), ('23', 'C'), ('24', 'C'), ('25', 'C'),
        ('26', 'C'), ('27', 'C'), ('28', 'C'), ('29', 'C'), ('30', 'C'), ('31', 'C'), ('32', 'C'), ('33', 'C'),
        ('35', 'D'), ('36', 'E'), ('37', 'E'), ('38', 'E'), ('39', 'E'), ('41', 'F'), ('42', 'F'), ('43', 'F'),
        ('45', 'G'), ('46', 'G'), ('47', 'G'), ('49', 'H'), ('50', 'H'), ('51', 'H'), ('52', 'H'), ('53', 'H'),
        ('55', 'I'), ('56', 'I'), ('58', 'J'), ('59', 'J'), ('60', 'J'), ('61', 'J'), ('62', 'J'), ('63', 'J'),
        ('64', 'K'), ('65', 'K'), ('66', 'K'), ('68', 'L'), ('69', 'M'), ('70', 'M'), ('71', 'M'), ('72', 'M'),
        ('73', 'M'), ('74', 'M'), ('75', 'M'), ('77', 'N'), ('78', 'N'), ('79', 'N'), ('80', 'N'), ('81', 'N'),
        ('82', 'N'), ('84', 'O'), ('85', 'P'), ('86', 'Q'), ('87', 'Q'), ('88', 'Q'), ('90', 'R'), ('91', 'R'),
        ('92', 'R'), ('93', 'R'), ('94', 'S'), ('95', 'S'), ('96', 'S'), ('97', 'T'), ('99', 'U')
)
SELECT
    c.codigo,
    c.descricao,
    m.secao,
    substr(c.codigo, 1, 2) AS divisao,
    substr(c.codigo, 1, 3) AS grupo,
    substr(c.codigo, 1, 4) || '-' || substr(c.codigo, 5, 1) AS classe
FROM cnaes c
LEFT JOIN secao_por_divisao m ON m.divisao = substr(c.codigo, 1, 2);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cnaes_hierarquia_codigo ON cnaes_hierarquia (codigo);
CREATE INDEX IF NOT EXISTS idx_cnaes_hierarquia_secao ON cnaes_hierarquia (secao);
CREATE INDEX IF NOT EXISTS idx_cnaes_hierarquia_divisao ON cnaes_hierarquia (divisao);
CREATE INDEX IF NOT EXISTS idx_cnaes_hierarquia_grupo ON cnaes_hierarquia (grupo);
CREATE INDEX IF NOT EXISTS idx_cnaes_hierarquia_classe ON cnaes_hierarquia (classe);

ANALYZE cnaes_hierarquia;
