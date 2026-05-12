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

The tool defaults to a Bernoulli sample (0.1% of rows) because some
measurements scan tens of millions of rows. The CNPJ check-digit
validation in particular is Python-loop-bound; a full scan over ~70M
estabelecimentos takes minutes. Use --full when you need definitive
counts (e.g. before a release or as a CI gate).

Currently measured:
- CNPJ check-digit validity (DV computed from the standard RFB
  modulus-11 weighted-sum algorithm vs the stored cnpj_dv).

Planned (separate commits, one measurement at a time):
- Phone format validity (numeric-only, valid DDD codes, length).
- CEP format validity (8 digits, not the "00000000" sentinel).
- Orphan FK counts (estabelecimentos.cnae_fiscal_principal ∉ cnaes etc).
- Sentinel value counts: capital_social = 999999999999,
  representante_legal = '***000000**'.
- EX (Exterior) UF count, broken out from real Brazilian UFs.
- Same-day Simples/MEI opt-in/opt-out anomaly count.
- Null coverage by important field.
"""

import argparse
import math
import os
import sys
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional

import psycopg2

# CNPJ check-digit algorithm (Brazilian RFB modulus-11 weighted sum).
# Given 12 leading digits, compute DV1 with weights _W1, then DV2 with
# weights _W2 over the 13 digits (12 + DV1). Rule: if (11 - sum%11) >= 10,
# the digit is 0; otherwise (11 - sum%11).
_W1 = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_W2 = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)


def cnpj_expected_dv(first_12: str) -> str:
    """Compute the 2-character check-digit string for a 12-digit CNPJ stem.

    Raises ValueError if input isn't exactly 12 digits.
    """
    if len(first_12) != 12 or not first_12.isdigit():
        raise ValueError(f"expected 12 digits, got {first_12!r}")
    d = [int(c) for c in first_12]
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


def measure_cnpj_check_digits(conn, sample_pct: Optional[float] = None) -> dict:
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
    examples = []

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
                # Malformed basico/ordem itself - count as invalid; the
                # layout-drift check at ingest should normally prevent
                # this, but be defensive.
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


def format_report(measurements: dict, scope: dict) -> str:
    """Render the markdown report from a dict of measurement results."""
    lines = []
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
        lines.append(f"| Valid check digits | {cnpj['valid']:,} | {cnpj['valid'] / total * 100:.4f}% |")
        lines.append(f"| Invalid check digits | {cnpj['invalid']:,} | {cnpj['invalid'] / total * 100:.4f}% |")
    if cnpj["examples"]:
        lines.append("")
        lines.append("### First invalid examples")
        lines.append("")
        lines.append("| cnpj_basico | cnpj_ordem | stored DV | expected DV |")
        lines.append("|---|---|---|---|")
        for ex in cnpj["examples"]:
            lines.append(f"| `{ex['basico']}` | `{ex['ordem']}` | `{ex['stored']}` | `{ex['expected']}` |")

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


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--full",
        action="store_true",
        help="Scan all rows. Default is sampled (0.1%%) which is much faster.",
    )
    parser.add_argument(
        "--sample-pct",
        type=sample_pct,
        default=0.1,
        help="Sample percentage when not using --full. Default: 0.1.",
    )
    args = parser.parse_args(argv)

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set", file=sys.stderr)
        return 1

    sample = None if args.full else args.sample_pct
    scope_str = "full table scan" if args.full else f"Bernoulli sample {sample}%"

    print(f"Connecting to database... (scope: {scope_str})", file=sys.stderr)
    with closing(psycopg2.connect(db_url)) as conn:
        print("Measuring CNPJ check digits...", file=sys.stderr)
        cnpj_results = measure_cnpj_check_digits(conn, sample_pct=sample)

    report = format_report(
        {"cnpj_check_digits": cnpj_results},
        scope={"scope_str": scope_str},
    )
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
