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
    retry_attempts: int = 3
    retry_delay: int = 5
    connect_timeout: int = 30
    read_timeout: int = 300
    keep_files: bool = False
    base_url: str = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls(
            database_url=os.getenv("DATABASE_URL", ""),
            batch_size=int(os.getenv("BATCH_SIZE", "500000")),
            temp_dir=os.getenv("TEMP_DIR", "./temp"),
            download_workers=int(os.getenv("DOWNLOAD_WORKERS", "4")),
            retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
            retry_delay=int(os.getenv("RETRY_DELAY", "5")),
            connect_timeout=int(os.getenv("CONNECT_TIMEOUT", "30")),
            read_timeout=int(os.getenv("READ_TIMEOUT", "300")),
            keep_files=os.getenv("KEEP_DOWNLOADED_FILES", "false").lower() == "true",
        )


config = Config.from_env()
