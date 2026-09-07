import pytest

from database import Database
from tests.integration.support import (
    apply_recipe,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("cnaes_hierarquia")
class TestRecipeCnaesHierarquia:
    """Verify cnaes hierarquia against prepared PostgreSQL fixtures."""

    # Independent restatement of the official IBGE/CONCLA CNAE-Subclasses 2.3
    # divisão->seção correspondence (Resolução CONCLA nº 2, de 19/11/2018),
    # kept separate from the recipe's 87-pair VALUES as a double-entry check:
    # each seção mapped to the inclusive range(s) of 2-digit divisões it owns.
    _SECTION_RANGES = {
        "A": [(1, 3)],
        "B": [(5, 9)],
        "C": [(10, 33)],
        "D": [(35, 35)],
        "E": [(36, 39)],
        "F": [(41, 43)],
        "G": [(45, 47)],
        "H": [(49, 53)],
        "I": [(55, 56)],
        "J": [(58, 63)],
        "K": [(64, 66)],
        "L": [(68, 68)],
        "M": [(69, 75)],
        "N": [(77, 82)],
        "O": [(84, 84)],
        "P": [(85, 85)],
        "Q": [(86, 88)],
        "R": [(90, 93)],
        "S": [(94, 96)],
        "T": [(97, 97)],
        "U": [(99, 99)],
    }

    @classmethod
    def _expected_secao(cls, divisao: str) -> str | None:
        n = int(divisao)
        for secao, ranges in cls._SECTION_RANGES.items():
            if any(lo <= n <= hi for lo, hi in ranges):
                return secao
        return None

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute, creating the table."""
        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'cnaes_hierarquia'")
            assert cur.fetchone() is not None, "cnaes_hierarquia table not created"

    def test_row_count_matches_cnaes(self, test_db: Database) -> None:
        """Grain is one row per cnaes.codigo - same cardinality as cnaes."""
        expected = count_rows(test_db, "cnaes")
        got = count_rows(test_db, "cnaes_hierarquia")
        assert got > 0, "cnaes_hierarquia is empty (was cnaes loaded?)"
        assert got == expected, f"cnaes_hierarquia ({got}) != cnaes ({expected})"

    def test_known_example(self, test_db: Database) -> None:
        """Issue #94 contract: 0111301 -> divisao 01, grupo 011, classe 0111-3, secao A."""
        with test_db.connect().cursor() as cur:
            cur.execute(
                "SELECT divisao, grupo, classe, secao FROM cnaes_hierarquia WHERE codigo = %s",
                ("0111301",),
            )
            row = fetch_row(cur)
        assert row is not None, "0111301 missing from cnaes_hierarquia"
        assert row == ("01", "011", "0111-3", "A"), f"unexpected derivation for 0111301: {row}"

    def test_substring_fields_match_codigo(self, test_db: Database) -> None:
        """divisao/grupo/classe are pure substrings of the 7-digit codigo."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM cnaes_hierarquia
                WHERE divisao <> left(codigo, 2)
                   OR grupo <> left(codigo, 3)
                   OR classe <> left(codigo, 4) || '-' || substr(codigo, 5, 1)
            """)
            bad = fetch_row(cur)[0]
        assert bad == 0, f"{bad} rows have substring fields inconsistent with codigo"

    def test_secao_never_null_for_real_codes(self, test_db: Database) -> None:
        """Every real RFB CNAE code belongs to an official divisão, so secao
        must be non-NULL for all fixture rows."""
        with test_db.connect().cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cnaes_hierarquia WHERE secao IS NULL")
            nulls = fetch_row(cur)[0]
        assert nulls == 0, f"{nulls} rows have NULL secao - a divisão is missing from the official map"

    def test_secao_matches_official_map(self, test_db: Database) -> None:
        """Every row's secao must equal the seção the official IBGE CNAE 2.3
        divisão->seção correspondence assigns to its divisão. Checked against an
        independent restatement of the mapping (not imported from the recipe)."""
        with test_db.connect().cursor() as cur:
            cur.execute("SELECT codigo, divisao, secao FROM cnaes_hierarquia")
            rows = cur.fetchall()
        mismatches = [
            (codigo, divisao, secao, self._expected_secao(divisao))
            for codigo, divisao, secao in rows
            if secao != self._expected_secao(divisao)
        ]
        assert not mismatches, f"secao mismatches vs official map: {mismatches[:10]}"

    def test_codigo_is_unique(self, test_db: Database) -> None:
        """codigo is the natural key - one row per subclasse."""
        with test_db.connect().cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT codigo) FROM cnaes_hierarquia")
            total, distinct = fetch_row(cur)
        assert total == distinct, f"codigo not unique: {total} rows, {distinct} distinct"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running drops+recreates without error and yields the same count."""
        before = count_rows(test_db, "cnaes_hierarquia")
        apply_recipe(test_db, "cnaes_hierarquia")
        after = count_rows(test_db, "cnaes_hierarquia")
        assert before == after, f"row count changed on re-run: {before} -> {after}"
