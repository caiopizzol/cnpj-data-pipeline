"""CSV processing and transformation for CNPJ data files using Polars."""

import logging
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import polars as pl

logger = logging.getLogger(__name__)

# File pattern → table name mapping
FILE_MAPPINGS = {
    "CNAECSV": "cnaes",
    "MOTICSV": "motivos",
    "MUNICCSV": "municipios",
    "NATJUCSV": "naturezas_juridicas",
    "PAISCSV": "paises",
    "QUALSCSV": "qualificacoes_socios",
    "EMPRECSV": "empresas",
    "ESTABELE": "estabelecimentos",
    "SOCIOCSV": "socios",
    "SIMPLESCSV": "dados_simples",
}

# Column names by file type
COLUMNS = {
    "CNAECSV": ["codigo", "descricao"],
    "MOTICSV": ["codigo", "descricao"],
    "MUNICCSV": ["codigo", "descricao"],
    "NATJUCSV": ["codigo", "descricao"],
    "PAISCSV": ["codigo", "descricao"],
    "QUALSCSV": ["codigo", "descricao"],
    "EMPRECSV": [
        "cnpj_basico", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social", "porte",
        "ente_federativo_responsavel",
    ],
    "ESTABELE": [
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
        "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
        "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
        "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
        "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2",
        "ddd_fax", "fax", "correio_eletronico", "situacao_especial",
        "data_situacao_especial",
    ],
    "SOCIOCSV": [
        "cnpj_basico", "identificador_de_socio", "nome_socio", "cnpj_cpf_do_socio",
        "qualificacao_do_socio", "data_entrada_sociedade", "pais",
        "representante_legal", "nome_do_representante",
        "qualificacao_do_representante_legal", "faixa_etaria",
    ],
    "SIMPLESCSV": [
        "cnpj_basico", "opcao_pelo_simples", "data_opcao_pelo_simples",
        "data_exclusao_do_simples", "opcao_pelo_mei", "data_opcao_pelo_mei",
        "data_exclusao_do_mei",
    ],
}


def get_file_type(filename: str) -> Optional[str]:
    """Determine file type from filename."""
    filename_upper = filename.upper()
    for pattern in FILE_MAPPINGS:
        if pattern in filename_upper:
            return pattern
    return None


def process_file(
    file_path: Path, batch_size: int = 50000
) -> Generator[Tuple[pl.DataFrame, str, List[str]], None, None]:
    """Process a CSV file and yield batches as Polars DataFrames."""
    file_type = get_file_type(file_path.name)
    if not file_type:
        logger.warning(f"Unknown file type: {file_path.name}")
        return

    table_name = FILE_MAPPINGS[file_type]
    columns = COLUMNS[file_type]

    # Read CSV with Polars (handles encoding, very fast)
    df = pl.read_csv(
        file_path,
        separator=";",
        has_header=False,
        new_columns=columns,
        encoding="ISO-8859-1",
        infer_schema_length=0,  # All strings
        null_values=[""],
    )

    # Apply transformations
    df = _transform(df, file_type)

    # Yield batches
    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        yield df.slice(start, end - start), table_name, columns


def _transform(df: pl.DataFrame, file_type: str) -> pl.DataFrame:
    """Apply transformations based on file type."""

    # Capital social: "1.234,56" → "1234.56"
    if file_type == "EMPRECSV" and "capital_social" in df.columns:
        df = df.with_columns(
            pl.col("capital_social")
            .str.replace_all(r"\.", "")
            .str.replace(",", ".")
        )

    # Date columns: "0" → null
    date_cols = {
        "ESTABELE": ["data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"],
        "SIMPLESCSV": ["data_opcao_pelo_simples", "data_exclusao_do_simples", "data_opcao_pelo_mei", "data_exclusao_do_mei"],
        "SOCIOCSV": ["data_entrada_sociedade"],
    }
    if file_type in date_cols:
        for col in date_cols[file_type]:
            if col in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(col) == "0").then(None).otherwise(pl.col(col)).alias(col)
                )

    # Estabelecimentos: pad country code
    if file_type == "ESTABELE" and "pais" in df.columns:
        df = df.with_columns(
            pl.col("pais").str.zfill(3)
        )

    # Socios: ensure cnpj_cpf_do_socio is not null (PK)
    if file_type == "SOCIOCSV" and "cnpj_cpf_do_socio" in df.columns:
        df = df.with_columns(
            pl.col("cnpj_cpf_do_socio").fill_null("00000000000000")
        )

    return df
