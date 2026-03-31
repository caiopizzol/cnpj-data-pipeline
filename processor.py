"""CSV processing and transformation for CNPJ data files using Polars."""

import logging
import os
import tempfile
from datetime import datetime
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
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte",
        "ente_federativo_responsavel",
    ],
    "ESTABELE": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ],
    "SOCIOCSV": [
        "cnpj_basico",
        "identificador_de_socio",
        "nome_socio",
        "cnpj_cpf_do_socio",
        "qualificacao_do_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_do_representante",
        "qualificacao_do_representante_legal",
        "faixa_etaria",
    ],
    "SIMPLESCSV": [
        "cnpj_basico",
        "opcao_pelo_simples",
        "data_opcao_pelo_simples",
        "data_exclusao_do_simples",
        "opcao_pelo_mei",
        "data_opcao_pelo_mei",
        "data_exclusao_do_mei",
    ],
}


def get_file_type(filename: str) -> Optional[str]:
    """Determine file type from filename."""
    filename_upper = filename.upper()

    # Special case for Simples files that have different naming pattern
    if "SIMPLES" in filename_upper:
        return "SIMPLESCSV"

    for pattern in FILE_MAPPINGS:
        if pattern in filename_upper:
            return pattern
    return None


def _convert_encoding(file_path: Path) -> Path:
    """Convert ISO-8859-1 to UTF-8. Returns path to converted file."""
    fd, tmp_path = tempfile.mkstemp(suffix=".utf8.csv")
    os.close(fd)
    utf8_file = Path(tmp_path)
    with open(file_path, "r", encoding="ISO-8859-1") as infile:
        with open(utf8_file, "w", encoding="UTF-8") as outfile:
            for chunk in iter(lambda: infile.read(50 * 1024 * 1024), ""):  # 50MB chunks
                outfile.write(chunk)
    return utf8_file


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

    # Convert encoding first (faster for Polars to read UTF-8)
    utf8_file = _convert_encoding(file_path)

    try:
        try:
            reader = pl.read_csv_batched(
                utf8_file,
                separator=";",
                has_header=False,
                new_columns=columns,
                encoding="utf8",
                infer_schema_length=0,
                null_values=[""],
                ignore_errors=True,
                low_memory=False,
                batch_size=batch_size,
            )
        except pl.exceptions.NoDataError:
            return

        while (batches := reader.next_batches(1)) is not None:
            for df in batches:
                if df.is_empty():
                    continue
                df = _transform(df, file_type)
                yield df, table_name, columns
    finally:
        utf8_file.unlink(missing_ok=True)


def _transform(df: pl.DataFrame, file_type: str) -> pl.DataFrame:
    """Apply transformations based on file type."""

    # Capital social: "1.234,56" → "1234.56", negative → null
    if file_type == "EMPRECSV" and "capital_social" in df.columns:
        df = df.with_columns(pl.col("capital_social").str.replace_all(r"\.", "").str.replace(",", "."))
        is_negative = pl.col("capital_social").str.starts_with("-")
        invalid_count = df.filter(is_negative).height
        if invalid_count > 0:
            logger.warning(f"capital_social: {invalid_count} negative values → null")
        df = df.with_columns(
            pl.when(is_negative).then(None).otherwise(pl.col("capital_social")).alias("capital_social")
        )

    # Date columns: "0" or "00000000" → null
    date_cols = {
        "ESTABELE": ["data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"],
        "SIMPLESCSV": [
            "data_opcao_pelo_simples",
            "data_exclusao_do_simples",
            "data_opcao_pelo_mei",
            "data_exclusao_do_mei",
        ],
        "SOCIOCSV": ["data_entrada_sociedade"],
    }
    if file_type in date_cols:
        today = datetime.now().strftime("%Y%m%d")
        for col in date_cols[file_type]:
            if col in df.columns:
                # "0" or "00000000" → null
                df = df.with_columns(
                    pl.when((pl.col(col) == "0") | (pl.col(col) == "00000000") | (pl.col(col).is_null()))
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
                # Future dates or dates before 1900 → null
                invalid = (pl.col(col).is_not_null()) & ((pl.col(col) > today) | (pl.col(col) < "19000101"))
                invalid_count = df.filter(invalid).height
                if invalid_count > 0:
                    logger.warning(f"{col}: {invalid_count} invalid dates → null (future or before 1900)")
                df = df.with_columns(pl.when(invalid).then(None).otherwise(pl.col(col)).alias(col))

    # Estabelecimentos: pad country code
    if file_type == "ESTABELE" and "pais" in df.columns:
        df = df.with_columns(pl.col("pais").str.zfill(3))

    # Socios: ensure cnpj_cpf_do_socio is not null (PK)
    if file_type == "SOCIOCSV" and "cnpj_cpf_do_socio" in df.columns:
        df = df.with_columns(pl.col("cnpj_cpf_do_socio").fill_null("00000000000000"))

    return df
