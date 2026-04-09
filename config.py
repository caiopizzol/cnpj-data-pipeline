"""Configuration for CNPJ data pipeline."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    """Pipeline configuration with sensible defaults."""

    database_url: str
    batch_size: int = 500000
    temp_dir: str = "./temp"
    download_workers: int = 4
    process_workers: int = 1
    retry_attempts: int = 3
    retry_delay: int = 5
    connect_timeout: int = 30
    read_timeout: int = 300
    keep_files: bool = False
    loading_strategy: str = "upsert"  # "upsert" or "replace"
    output_format: str = "postgres"  # "postgres" or "parquet"
    parquet_output_dir: str = "./parquet"
    post_file_command: str = ""  # Command to run after each parquet file (receives file path as arg)
    base_url: str = "https://arquivos.receitafederal.gov.br/public.php/webdav"
    share_token: str = "YggdBLfdninEJX9"

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls(
            database_url=os.getenv("DATABASE_URL", ""),
            batch_size=int(os.getenv("BATCH_SIZE", "500000")),
            temp_dir=os.getenv("TEMP_DIR", "./temp"),
            download_workers=int(os.getenv("DOWNLOAD_WORKERS", "4")),
            process_workers=int(os.getenv("PROCESS_WORKERS", "1")),
            retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
            retry_delay=int(os.getenv("RETRY_DELAY", "5")),
            connect_timeout=int(os.getenv("CONNECT_TIMEOUT", "30")),
            read_timeout=int(os.getenv("READ_TIMEOUT", "300")),
            keep_files=os.getenv("KEEP_DOWNLOADED_FILES", "false").lower() == "true",
            loading_strategy=os.getenv("LOADING_STRATEGY", "upsert").lower(),
            output_format=os.getenv("OUTPUT_FORMAT", "postgres").lower(),
            parquet_output_dir=os.getenv("PARQUET_OUTPUT_DIR", "./parquet"),
            post_file_command=os.getenv("POST_FILE_COMMAND", ""),
            base_url=os.getenv("BASE_URL", "https://arquivos.receitafederal.gov.br/public.php/webdav"),
            share_token=os.getenv("SHARE_TOKEN", "YggdBLfdninEJX9"),
        )


config = Config.from_env()
