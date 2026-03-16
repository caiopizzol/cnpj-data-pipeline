import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import config
from database import Database
from api.query import EMPRESA

# Initialize security scheme if API authentication is enabled
security = None
if config.api_auth_enabled:
    security = HTTPBearer()

logger = logging.getLogger(__name__)

db = Database(config.database_url)

# Lifespan context manager to handle database connection lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    db.connect()
    yield
    db.disconnect()


app = FastAPI(lifespan=lifespan)


def _verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> None:
    """Verify API token from the Authorization header."""
    if credentials.credentials != config.api_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido.")


def _is_valid_cnpj(cnpj: str) -> bool:
    """Validate CNPJ format (14 numeric characters)."""
    return len(cnpj) == 14 and cnpj.isdigit()


def _fetch_empresa_data(cnpj: str) -> dict[str, Any]:
    """Fetch company JSON payload by CNPJ parts."""
    basico = cnpj[:8]
    ordem = cnpj[8:12]
    dv = cnpj[12:14]

    with db.conn.cursor() as cur:
        cur.execute(EMPRESA, (basico, ordem, dv))
        row = cur.fetchone()

    return row[0] if row else {}


@app.get("/empresa/{cnpj}", dependencies=[] if not config.api_auth_enabled else [Depends(_verify_token)])
def get_empresa(cnpj: str) -> dict[str, Any]:
    """Return company data for a valid CNPJ."""
    if not _is_valid_cnpj(cnpj):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="CNPJ inválido. Informe 14 dígitos numéricos.",
        )

    try:
        data = _fetch_empresa_data(cnpj)
    except Exception:
        logger.exception("Erro ao consultar empresa para o CNPJ informado")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro interno ao consultar empresa.",
        )

    if not data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="CNPJ não encontrado.",
        )
    return data
