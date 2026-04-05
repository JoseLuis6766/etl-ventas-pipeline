"""
config.py — Configuración central del ETL
"""

import os
import logging
from pathlib import Path


# RUTAS DE ARCHIVOS

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Asegurar que existen los directorios
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Archivos fuente
TXT_FILE     = DATA_DIR / "ventas_2025.txt"
EXCEL_FILE   = DATA_DIR / "Camisas_inventario.xlsx"
EXCEL_SHEET_VENTAS   = "Ventas"
EXCEL_SHEET_CATALOGOS = "Catalogos"


# CONEXIÓN POSTGRESQL

DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_DB",       "etl_camisas"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "---"), #se oculta este dato
}

# DSN para psycopg2
def get_dsn() -> str:
    c = DB_CONFIG
    return (
        f"host={c['host']} port={c['port']} dbname={c['database']} "
        f"user={c['user']} password={c['password']}"
    )


# PARÁMETROS DEL ETL

AÑO_DEFAULT = 2025
ENCODING_TXT = "utf-8-sig"   # UTF-8 con BOM
NIVEL_CONFIANZA_MIN_HOMOLOGAR = 20.0   # % mínimo para intentar homologar
NIVEL_CONFIANZA_MIN_GOLD      = 80.0   # % mínimo para llegar a gold

# Atributos requeridos para calcular nivel_confianza
ATRIBUTOS_REQUERIDOS = ["fecha", "cantidad", "marca", "color", "talla"]
ATRIBUTOS_OPCIONALES = ["tipo_cuello", "manga", "estampado", "precio", "tipo_tela"]


# LOGGING

LOG_FILE = LOGS_DIR / "etl_camisas.log"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s"
LOG_LEVEL  = logging.DEBUG

def setup_logging(name: str = "etl_camisas") -> logging.Logger:
    """Configura y retorna un logger con handler a archivo y consola."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # ya configurado
    logger.setLevel(LOG_LEVEL)

    formatter = logging.Formatter(LOG_FORMAT)

    # Consola
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    # Archivo
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(LOG_LEVEL)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger
