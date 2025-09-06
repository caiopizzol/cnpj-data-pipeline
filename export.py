#!/usr/bin/env python3
"""
CNPJ Data Export Script
Export CNPJ data to Parquet files for analysis

Usage:
    python export.py sp_full          # Export ALL São Paulo companies (3M+)
    python export.py sample           # Export 10k sample for testing

After exporting, upload to GitHub Releases:
    1. Go to: github.com/caiopizzol/cnpj-data-pipeline/releases/new
    2. Upload the .parquet file from exports/
    3. Use the URL in notebooks/demos
"""

import sys
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Queries
QUERIES = {
    "sp_full": """
        SELECT
            e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
            emp.razao_social,
            e.nome_fantasia,
            e.data_inicio_atividade,
            e.municipio,
            e.cnae_fiscal_principal,
            emp.capital_social,
            emp.porte
        FROM estabelecimentos e
        JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
        WHERE e.uf = 'SP'
          AND e.situacao_cadastral = '02'
    """,
    "sample": """
        SELECT
            e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
            emp.razao_social,
            e.nome_fantasia,
            e.data_inicio_atividade,
            e.municipio,
            e.cnae_fiscal_principal,
            emp.capital_social,
            emp.porte
        FROM estabelecimentos e
        JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
        WHERE e.uf = 'SP'
          AND e.situacao_cadastral = '02'
        LIMIT 10000
    """,
}


def export_to_parquet(query, output_name, chunk_size=100000):
    """Export query results to Parquet using psycopg2."""

    # Connection parameters
    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "cnpj"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }

    # Output path
    output_dir = Path("./exports")
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d")
    output_path = output_dir / f"{output_name}_{timestamp}.parquet"

    logger.info(f"Starting export: {output_name}")

    # Connect and export
    conn = psycopg2.connect(**conn_params)

    try:
        # Read directly into DataFrame (simpler for smaller datasets)
        df = pd.read_sql_query(query, conn)

        # Convert date columns
        if "data_inicio_atividade" in df.columns:
            df["data_inicio_atividade"] = pd.to_datetime(
                df["data_inicio_atividade"], errors="coerce"
            )

        # Save to Parquet
        df.to_parquet(output_path, compression="snappy", index=False)

        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"Export complete: {len(df):,} rows, {file_size_mb:.1f} MB")

        return output_path

    finally:
        conn.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    export_type = sys.argv[1].lower()

    if export_type not in QUERIES:
        print(f"Unknown export: {export_type}")
        print(f"Available: {', '.join(QUERIES.keys())}")
        sys.exit(1)

    try:
        path = export_to_parquet(QUERIES[export_type], export_type)

        print("\n✅ Export complete!")
        print(f"📁 File: {path}")
        print(f"📊 Size: {path.stat().st_size / (1024 * 1024):.1f} MB")

        print("\n📤 Next steps:")
        print("1. Go to: https://github.com/caiopizzol/cnpj-data-pipeline/releases/new")
        print(f"2. Upload: {path}")
        print("3. Copy the download URL for use in notebooks")

    except Exception as e:
        logger.error(f"Export failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
