"""Source file names, table layouts and processing order. No I/O dependencies."""

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

# Output column lists for tables whose target schema includes columns that
# don't appear in the source CSV. Synthetic columns are emitted by transform.
# Other file types insert exactly COLUMNS[file_type].
#
# SOCIOCSV: socio_id is a deterministic UUID derived in transform from the
# canonical identity tuple. It is the primary key of socios; the masked CPF
# alone is not unique (issue #78).
OUTPUT_COLUMNS = {
    "SOCIOCSV": ["socio_id"] + COLUMNS["SOCIOCSV"],
}


def get_file_type(filename: str) -> str | None:
    """Determine file type from filename."""
    filename_upper = filename.upper()

    # Special case for Simples files that have different naming pattern
    if "SIMPLES" in filename_upper:
        return "SIMPLESCSV"

    for pattern in FILE_MAPPINGS:
        if pattern in filename_upper:
            return pattern
    return None


# Dependency groups — files within the same group have no inter-dependencies
# and can be processed in parallel. Groups must be processed in order.
DEPENDENCY_GROUPS = [
    ["CNAECSV", "MOTICSV", "MUNICCSV", "NATJUCSV", "PAISCSV", "QUALSCSV"],  # references
    ["EMPRECSV"],  # empresas
    ["ESTABELE", "SOCIOCSV", "SIMPLESCSV"],  # depends on empresas
]

# Flat processing order derived from dependency groups (for sorting)
PROCESSING_ORDER = [ft for group in DEPENDENCY_GROUPS for ft in group]

# ZIP filename prefix → file type (zip names differ from CSV names inside)
ZIP_PREFIX_MAP = [
    ("SIMPLES", "SIMPLESCSV"),
    ("CNAE", "CNAECSV"),
    ("MOTI", "MOTICSV"),
    ("MUNIC", "MUNICCSV"),
    ("NATUR", "NATJUCSV"),
    ("PAIS", "PAISCSV"),
    ("QUALIFICAC", "QUALSCSV"),
    ("EMPRES", "EMPRECSV"),
    ("ESTABELE", "ESTABELE"),
    ("SOCIO", "SOCIOCSV"),
]


def get_zip_file_type(zip_filename: str) -> str | None:
    """Determine file type from ZIP filename."""
    name = zip_filename.upper()
    for prefix, file_type in ZIP_PREFIX_MAP:
        if name.startswith(prefix):
            return file_type
    return None


def get_file_priority(filename: str) -> int:
    """Get processing priority for a file (lower = first)."""
    file_type = get_zip_file_type(filename) or get_file_type(filename)
    if file_type in PROCESSING_ORDER:
        return PROCESSING_ORDER.index(file_type)
    return 999


def group_files_by_dependency(files: list[str]) -> list[list[str]]:
    """Group pending files by dependency level. Returns ordered list of groups."""
    groups: list[list[str]] = [[] for _ in DEPENDENCY_GROUPS]
    for f in files:
        file_type = get_zip_file_type(f)
        if not file_type:
            continue
        for i, dep_types in enumerate(DEPENDENCY_GROUPS):
            if file_type in dep_types:
                groups[i].append(f)
                break
    return groups


# SIMPLES CSV member names use several suffixes, so match their shared stem.
CNPJ_FILE_PATTERNS = ["SIMPLES" if name == "SIMPLESCSV" else name for name in FILE_MAPPINGS]

# Reference tables (must be processed first)
REFERENCE_FILES = {
    "Cnaes.zip",
    "Motivos.zip",
    "Municipios.zip",
    "Naturezas.zip",
    "Paises.zip",
    "Qualificacoes.zip",
}
