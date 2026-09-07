"""Data quality report for the loaded CNPJ database.

Opt-in measurement tool. Reads the loaded PostgreSQL database (no Parquet
support yet) and emits a markdown summary of data-quality signals. Never
mutates data - matches the project principle: "the pipeline preserves
and measures; recipes interpret."

Usage:
    uv run python scripts/data_quality_report.py            # sampled
    uv run python scripts/data_quality_report.py --full     # full table scan
    uv run python scripts/data_quality_report.py --sample-pct 0.5

    just data-quality-report                                # via justfile

Only CNPJ check-digit validation is sampled by default (Bernoulli, 0.1%).
All other measurements query the full tables. Use --full for exact
check-digit counts too. Findings do not change the exit status; a CI gate
needs a separate check against the reported counts.

Currently measured:
- CNPJ check-digit validity (DV computed from the standard RFB
  modulus-11 weighted-sum algorithm vs the stored cnpj_dv).
- Orphan FK counts against the reference tables, covering estabelecimentos
  (cnae, municipio, motivo, pais), empresas (natureza_juridica,
  qualificacao_responsavel) and socios (pais, qualificacao_do_socio,
  qualificacao_do_representante_legal excluding the '00' placeholder).
- Enriched-domain coverage: orphans against the raw monthly lookup vs the
  enriched lookup (reference_domains_enriched), so the gap closed by official
  supplemental rows is visible. Skipped when the enriched tables are absent.
- EX (Exterior) UF count in estabelecimentos.
- Sentinel-like value counts: capital_social = 999999999999,
  representante_legal = '***000000**'.
- CEP format validity (null, malformed, '00000000' sentinel).
"""

import argparse
import math
import os
import sys
from contextlib import closing
from datetime import datetime, timezone
from typing import TypedDict

import psycopg2
from psycopg2.extensions import connection, cursor


class CheckDigitExample(TypedDict):
    basico: str
    ordem: str
    stored: str
    expected: str


class CheckDigitMeasurement(TypedDict):
    total: int
    valid: int
    invalid: int
    examples: list[CheckDigitExample]
    scan_mode: str


class OrphanMeasurement(TypedDict):
    label: str
    orphans: int


class EnrichedRow(TypedDict):
    label: str
    monthly_orphans: int
    enriched_orphans: int


class EnrichedMeasurement(TypedDict):
    available: bool
    rows: list[EnrichedRow]


class Measurements(TypedDict):
    cnpj_check_digits: CheckDigitMeasurement
    orphan_fks: list[OrphanMeasurement]
    enriched_orphans: EnrichedMeasurement
    exterior_uf: dict[str, int]
    capital_sentinel: dict[str, int]
    representante_sentinel: dict[str, int]
    cep_validity: dict[str, int]


def _count(cur: cursor) -> int:
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Count query returned no row")
    return int(row[0])


# CNPJ check-digit algorithm (Brazilian RFB modulus-11 weighted sum).
# Given the 12-character stem, compute DV1 with weights _W1, then DV2 with
# weights _W2 over the 13 values (12 + DV1). Rule: if (11 - sum%11) >= 10,
# the digit is 0; otherwise (11 - sum%11).
#
# AIDEV-NOTE: cnpj-alphanumeric: from 2026-07 the stem (basico+ordem) may
# contain A-Z as well as 0-9 (Receita Federal / Serpro). The published rule
# values each character as ord(c) - 48, so '0'..'9' -> 0..9 and 'A'..'Z' ->
# 17..42; the weights, modulus, and the two check digits stay numeric and
# unchanged. Existing all-digit CNPJs are a strict subset and still validate.
_W1 = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_W2 = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_CNPJ_STEM_CHARS = frozenset("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def cnpj_expected_dv(first_12: str) -> str:
    """Compute the 2-character check-digit string for a 12-character CNPJ stem.

    The stem is the 8-char basico + 4-char ordem, each char in 0-9 or A-Z.
    Raises ValueError if input isn't exactly 12 such characters.
    """
    if len(first_12) != 12 or any(c not in _CNPJ_STEM_CHARS for c in first_12):
        raise ValueError(f"expected 12 alphanumeric (0-9, A-Z) chars, got {first_12!r}")
    d = [ord(c) - 48 for c in first_12]
    s1 = sum(d[i] * _W1[i] for i in range(12))
    dv1 = 11 - (s1 % 11)
    if dv1 >= 10:
        dv1 = 0
    d.append(dv1)
    s2 = sum(d[i] * _W2[i] for i in range(13))
    dv2 = 11 - (s2 % 11)
    if dv2 >= 10:
        dv2 = 0
    return f"{dv1}{dv2}"


def measure_cnpj_check_digits(conn: connection, sample_pct: float | None = None) -> CheckDigitMeasurement:
    """Walk estabelecimentos and count valid vs invalid stored check digits.

    Args:
        conn: psycopg2 connection.
        sample_pct: TABLESAMPLE BERNOULLI percentage (e.g. 0.1 for 0.1%).
            None = full table scan.

    Returns dict with counts + up to 10 example mismatches.
    """
    total = 0
    valid = 0
    invalid = 0
    examples: list[CheckDigitExample] = []

    sample_clause = f"TABLESAMPLE BERNOULLI ({sample_pct})" if sample_pct is not None else ""
    query = f"""
        SELECT cnpj_basico, cnpj_ordem, cnpj_dv
        FROM estabelecimentos {sample_clause}
    """

    with conn.cursor(name="cnpj_dv_scan") as cur:
        cur.itersize = 100_000
        cur.execute(query)
        for basico, ordem, stored in cur:
            total += 1
            try:
                expected = cnpj_expected_dv(basico + ordem)
            except ValueError:
                # Ingest logs malformed field values but preserves them;
                # column-count checks do not validate the CNPJ stem.
                invalid += 1
                if len(examples) < 10:
                    examples.append(
                        {"basico": basico, "ordem": ordem, "stored": stored, "expected": "<malformed-input>"}
                    )
                continue
            if stored == expected:
                valid += 1
            else:
                invalid += 1
                if len(examples) < 10:
                    examples.append({"basico": basico, "ordem": ordem, "stored": stored, "expected": expected})

    return {
        "total": total,
        "valid": valid,
        "invalid": invalid,
        "examples": examples,
        "scan_mode": "full" if sample_pct is None else f"sample {sample_pct}%",
    }


_ORPHAN_FK_CHECKS = [
    {
        "label": "estabelecimentos.cnae_fiscal_principal ∉ cnaes",
        "table": "estabelecimentos",
        "column": "cnae_fiscal_principal",
        "ref_table": "cnaes",
        "ref_column": "codigo",
    },
    {
        "label": "estabelecimentos.municipio ∉ municipios",
        "table": "estabelecimentos",
        "column": "municipio",
        "ref_table": "municipios",
        "ref_column": "codigo",
    },
    {
        "label": "estabelecimentos.motivo_situacao_cadastral ∉ motivos",
        "table": "estabelecimentos",
        "column": "motivo_situacao_cadastral",
        "ref_table": "motivos",
        "ref_column": "codigo",
    },
    {
        "label": "estabelecimentos.pais ∉ paises",
        "table": "estabelecimentos",
        "column": "pais",
        "ref_table": "paises",
        "ref_column": "codigo",
    },
    {
        "label": "empresas.natureza_juridica ∉ naturezas_juridicas",
        "table": "empresas",
        "column": "natureza_juridica",
        "ref_table": "naturezas_juridicas",
        "ref_column": "codigo",
    },
    {
        "label": "empresas.qualificacao_responsavel ∉ qualificacoes_socios",
        "table": "empresas",
        "column": "qualificacao_responsavel",
        "ref_table": "qualificacoes_socios",
        "ref_column": "codigo",
    },
    {
        "label": "socios.pais ∉ paises",
        "table": "socios",
        "column": "pais",
        "ref_table": "paises",
        "ref_column": "codigo",
    },
    {
        "label": "socios.qualificacao_do_socio ∉ qualificacoes_socios",
        "table": "socios",
        "column": "qualificacao_do_socio",
        "ref_table": "qualificacoes_socios",
        "ref_column": "codigo",
    },
    {
        # '00' is the placeholder qualification that pairs with the placeholder
        # representante_legal, not an orphan - exclude it like the recipe does.
        "label": "socios.qualificacao_do_representante_legal ∉ qualificacoes_socios (≠ '00')",
        "table": "socios",
        "column": "qualificacao_do_representante_legal",
        "ref_table": "qualificacoes_socios",
        "ref_column": "codigo",
        "extra_predicate": "t.qualificacao_do_representante_legal <> '00'",
    },
]


def _count_orphans(
    cur: cursor, table: str, column: str, ref_table: str, ref_column: str, extra_predicate: str = ""
) -> int:
    """Count rows whose FK value is non-NULL but absent from ref_table."""
    extra = f"AND {extra_predicate}" if extra_predicate else ""
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {table} t
        WHERE t.{column} IS NOT NULL
          {extra}
          AND NOT EXISTS (
              SELECT 1 FROM {ref_table} r
              WHERE r.{ref_column} = t.{column}
          )
        """
    )
    return _count(cur)


def measure_orphan_fks(conn: connection) -> list[OrphanMeasurement]:
    """For each lookup relationship, count rows whose FK value has no
    match in the reference table. NULLs are excluded - they're absence,
    not orphans."""
    results: list[OrphanMeasurement] = []
    with conn.cursor() as cur:
        for check in _ORPHAN_FK_CHECKS:
            count = _count_orphans(
                cur,
                check["table"],
                check["column"],
                check["ref_table"],
                check["ref_column"],
                check.get("extra_predicate", ""),
            )
            results.append({"label": check["label"], "orphans": count})
    return results


# Domains that the reference_domains_enriched recipe supplements. For each, the
# report shows how many orphans remain against the raw MONTHLY lookup vs the
# ENRICHED lookup, so the gap closed by official supplemental rows is visible.
# Skipped gracefully when the enriched tables are not present (recipe not run).
_ENRICHED_FK_CHECKS = [
    {
        "label": "estabelecimentos.motivo_situacao_cadastral",
        "table": "estabelecimentos",
        "column": "motivo_situacao_cadastral",
        "monthly_ref": "motivos",
        "enriched_ref": "motivos_enriched",
    },
    {
        "label": "estabelecimentos.pais",
        "table": "estabelecimentos",
        "column": "pais",
        "monthly_ref": "paises",
        "enriched_ref": "paises_enriched",
    },
    {
        "label": "empresas.qualificacao_responsavel",
        "table": "empresas",
        "column": "qualificacao_responsavel",
        "monthly_ref": "qualificacoes_socios",
        "enriched_ref": "qualificacoes_socios_enriched",
    },
    {
        "label": "socios.pais",
        "table": "socios",
        "column": "pais",
        "monthly_ref": "paises",
        "enriched_ref": "paises_enriched",
    },
    {
        "label": "socios.qualificacao_do_socio",
        "table": "socios",
        "column": "qualificacao_do_socio",
        "monthly_ref": "qualificacoes_socios",
        "enriched_ref": "qualificacoes_socios_enriched",
    },
    {
        "label": "socios.qualificacao_do_representante_legal (≠ '00')",
        "table": "socios",
        "column": "qualificacao_do_representante_legal",
        "monthly_ref": "qualificacoes_socios",
        "enriched_ref": "qualificacoes_socios_enriched",
        "extra_predicate": "t.qualificacao_do_representante_legal <> '00'",
    },
]


def measure_enriched_orphans(conn: connection) -> EnrichedMeasurement:
    """Compare orphan counts against the monthly lookup vs the enriched lookup.

    Returns {'available': bool, 'rows': [{label, monthly_orphans, enriched_orphans}]}.
    available is False (and rows empty) when the enriched tables do not exist,
    i.e. reference_domains_enriched was not applied.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.motivos_enriched')")
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Reference lookup query returned no row")
        if row[0] is None:
            return {"available": False, "rows": []}

        rows: list[EnrichedRow] = []
        for check in _ENRICHED_FK_CHECKS:
            extra = check.get("extra_predicate", "")
            monthly = _count_orphans(cur, check["table"], check["column"], check["monthly_ref"], "codigo", extra)
            enriched = _count_orphans(cur, check["table"], check["column"], check["enriched_ref"], "codigo", extra)
            rows.append(
                {
                    "label": check["label"],
                    "monthly_orphans": monthly,
                    "enriched_orphans": enriched,
                }
            )
    return {"available": True, "rows": rows}


def measure_exterior_uf(conn: connection) -> dict[str, int]:
    """Count uf='EX', the observed convention for exterior addresses.

    The interpretation is empirical; see docs/data-audit.md.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM estabelecimentos WHERE uf = 'EX'")
        ex = _count(cur)
        cur.execute("SELECT COUNT(*) FROM estabelecimentos")
        total = _count(cur)
    return {"total": total, "exterior": ex}


def measure_capital_sentinel(conn: connection) -> dict[str, int]:
    """Count rows with the suspicious high-water capital_social value
    999999999999. Downstream consumers often want to inspect, mask, or
    exclude this value before ranking companies by capital."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM empresas")
        total = _count(cur)
        cur.execute("SELECT COUNT(*) FROM empresas WHERE capital_social = 999999999999")
        sentinel = _count(cur)
        cur.execute("SELECT COUNT(*) FROM empresas WHERE capital_social IS NULL")
        nulls = _count(cur)
    return {"total": total, "sentinel": sentinel, "nulls": nulls}


def measure_representante_sentinel(conn: connection) -> dict[str, int]:
    """Count rows with representante_legal='***000000**' and
    qualificacao_do_representante_legal='00'. This pattern commonly
    behaves like "no representative" in the source data."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM socios")
        total = _count(cur)
        cur.execute(
            """
            SELECT COUNT(*) FROM socios
            WHERE representante_legal = '***000000**'
              AND qualificacao_do_representante_legal = '00'
            """
        )
        sentinel = _count(cur)
    return {"total": total, "sentinel": sentinel}


def measure_cep_validity(conn: connection) -> dict[str, int]:
    """CEP should be 8 digits. RFB sometimes carries NULL or the
    '00000000' sentinel. Count malformed shapes without inferring their cause."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM estabelecimentos")
        total = _count(cur)
        cur.execute("SELECT COUNT(*) FROM estabelecimentos WHERE cep IS NULL")
        nulls = _count(cur)
        cur.execute("SELECT COUNT(*) FROM estabelecimentos WHERE cep = '00000000'")
        zero_sentinel = _count(cur)
        cur.execute(r"SELECT COUNT(*) FROM estabelecimentos WHERE cep IS NOT NULL AND cep !~ '^\d{8}$'")
        malformed = _count(cur)
    return {
        "total": total,
        "nulls": nulls,
        "zero_sentinel": zero_sentinel,
        "malformed": malformed,
    }


def _pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "n/a"
    return f"{numerator / denominator * 100:.4f}%"


def format_report(measurements: Measurements, scope: dict[str, str]) -> str:
    """Render the markdown report from a dict of measurement results."""
    lines: list[str] = []
    lines.append("# Data quality report")
    lines.append("")
    lines.append(f"Generated: `{datetime.now(timezone.utc).isoformat(timespec='seconds')}`  ")
    lines.append(f"Scope: {scope['scope_str']}")
    lines.append("")

    # --- CNPJ check digits ---
    cnpj = measurements["cnpj_check_digits"]
    total = cnpj["total"]
    lines.append("## CNPJ check-digit validation")
    lines.append("")
    lines.append(f"Scan mode: `{cnpj['scan_mode']}`")
    lines.append("")
    lines.append("| Metric | Count | % |")
    lines.append("|---|---:|---:|")
    lines.append(f"| Total rows scanned | {total:,} | 100% |")
    if total > 0:
        lines.append(f"| Valid check digits | {cnpj['valid']:,} | {_pct(cnpj['valid'], total)} |")
        lines.append(f"| Invalid check digits | {cnpj['invalid']:,} | {_pct(cnpj['invalid'], total)} |")
    if cnpj["examples"]:
        lines.append("")
        lines.append("### First invalid examples")
        lines.append("")
        lines.append("| cnpj_basico | cnpj_ordem | stored DV | expected DV |")
        lines.append("|---|---|---|---|")
        for ex in cnpj["examples"]:
            lines.append(f"| `{ex['basico']}` | `{ex['ordem']}` | `{ex['stored']}` | `{ex['expected']}` |")
    lines.append("")

    # --- Orphan FKs ---
    lines.append("## Orphan foreign-key references")
    lines.append("")
    lines.append(
        "Rows whose lookup code has no match in the reference table. "
        "NULLs excluded. Treat these as signals to inspect; historical "
        "or retired codes may still be legitimate source data."
    )
    lines.append("")
    lines.append("| Relationship | Orphan count |")
    lines.append("|---|---:|")
    for row in measurements["orphan_fks"]:
        lines.append(f"| {row['label']} | {row['orphans']:,} |")
    lines.append("")

    # --- Enriched-domain coverage ---
    enriched = measurements["enriched_orphans"]
    lines.append("## Enriched-domain coverage")
    lines.append("")
    if not enriched["available"]:
        lines.append(
            "Enriched lookup tables not found - apply "
            "`recipes/postgres/reference_domains_enriched.sql` to measure how many "
            "orphans the official supplemental rows resolve."
        )
        lines.append("")
    else:
        lines.append(
            "Orphans against the raw monthly lookup vs the enriched lookup. The "
            "difference is what official supplemental rows resolve; the enriched "
            "column is what stays genuinely unresolved."
        )
        lines.append("")
        lines.append("| Relationship | Monthly orphans | After enrichment |")
        lines.append("|---|---:|---:|")
        for row in enriched["rows"]:
            lines.append(f"| {row['label']} | {row['monthly_orphans']:,} | {row['enriched_orphans']:,} |")
        lines.append("")

    # --- Exterior UF ---
    ex = measurements["exterior_uf"]
    lines.append("## Exterior (uf='EX') estabelecimentos")
    lines.append("")
    lines.append("| Metric | Count | % |")
    lines.append("|---|---:|---:|")
    lines.append(f"| Total estabelecimentos | {ex['total']:,} | 100% |")
    lines.append(f"| With uf='EX' | {ex['exterior']:,} | {_pct(ex['exterior'], ex['total'])} |")
    lines.append("")

    # --- Capital sentinel ---
    cap = measurements["capital_sentinel"]
    lines.append("## capital_social sentinel (999999999999)")
    lines.append("")
    lines.append("| Metric | Count | % |")
    lines.append("|---|---:|---:|")
    lines.append(f"| Total empresas | {cap['total']:,} | 100% |")
    lines.append(f"| capital_social = 999999999999 | {cap['sentinel']:,} | {_pct(cap['sentinel'], cap['total'])} |")
    lines.append(f"| capital_social IS NULL | {cap['nulls']:,} | {_pct(cap['nulls'], cap['total'])} |")
    lines.append("")

    # --- Representante sentinel ---
    rep = measurements["representante_sentinel"]
    lines.append("## representante_legal sentinel ('***000000**' + qualificacao '00')")
    lines.append("")
    lines.append("| Metric | Count | % |")
    lines.append("|---|---:|---:|")
    lines.append(f"| Total socios | {rep['total']:,} | 100% |")
    lines.append(f"| Sentinel rows | {rep['sentinel']:,} | {_pct(rep['sentinel'], rep['total'])} |")
    lines.append("")

    # --- CEP validity ---
    cep = measurements["cep_validity"]
    lines.append("## CEP validity")
    lines.append("")
    lines.append("| Metric | Count | % |")
    lines.append("|---|---:|---:|")
    lines.append(f"| Total estabelecimentos | {cep['total']:,} | 100% |")
    lines.append(f"| cep IS NULL | {cep['nulls']:,} | {_pct(cep['nulls'], cep['total'])} |")
    lines.append(f"| cep = '00000000' | {cep['zero_sentinel']:,} | {_pct(cep['zero_sentinel'], cep['total'])} |")
    lines.append(f"| cep malformed (not 8 digits) | {cep['malformed']:,} | {_pct(cep['malformed'], cep['total'])} |")

    return "\n".join(lines) + "\n"


def sample_pct(value: str) -> float:
    """Parse and validate TABLESAMPLE percentage."""
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("sample percentage must be a number") from exc

    if not math.isfinite(parsed) or parsed <= 0 or parsed > 100:
        raise argparse.ArgumentTypeError("sample percentage must be > 0 and <= 100")

    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    parser.add_argument(
        "--full",
        action="store_true",
        help="Scan all rows for check digits too. Other measurements always scan full tables.",
    )
    parser.add_argument(
        "--sample-pct",
        type=sample_pct,
        default=0.1,
        help="Check-digit sample percentage when not using --full. Default: 0.1.",
    )
    args = parser.parse_args(argv)

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set", file=sys.stderr)
        return 1

    sample = None if args.full else args.sample_pct
    scope_str = (
        "full table scan"
        if args.full
        else f"Check digits: Bernoulli sample {sample}%; other measurements: full table scans"
    )

    print(f"Connecting to database... (scope: {scope_str})", file=sys.stderr)
    with closing(psycopg2.connect(db_url)) as conn:
        print("Measuring CNPJ check digits...", file=sys.stderr)
        cnpj_results = measure_cnpj_check_digits(conn, sample_pct=sample)

        print("Measuring orphan FK references...", file=sys.stderr)
        orphan_fk_results = measure_orphan_fks(conn)

        print("Measuring enriched-domain coverage...", file=sys.stderr)
        enriched_orphan_results = measure_enriched_orphans(conn)

        print("Measuring Exterior UF count...", file=sys.stderr)
        exterior_uf_results = measure_exterior_uf(conn)

        print("Measuring capital_social sentinel...", file=sys.stderr)
        capital_results = measure_capital_sentinel(conn)

        print("Measuring representante_legal sentinel...", file=sys.stderr)
        representante_results = measure_representante_sentinel(conn)

        print("Measuring CEP validity...", file=sys.stderr)
        cep_results = measure_cep_validity(conn)

    report = format_report(
        {
            "cnpj_check_digits": cnpj_results,
            "orphan_fks": orphan_fk_results,
            "enriched_orphans": enriched_orphan_results,
            "exterior_uf": exterior_uf_results,
            "capital_sentinel": capital_results,
            "representante_sentinel": representante_results,
            "cep_validity": cep_results,
        },
        scope={"scope_str": scope_str},
    )
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
