"""
loaders/raw_loader.py — Registra el archivo fuente en raw.archivos_cargados.
"""

import hashlib
import logging
from pathlib import Path
import psycopg2
from config import get_dsn, ENCODING_TXT, setup_logging

logger = setup_logging("etl_camisas.raw_loader")


def calcular_md5(ruta: str | Path) -> str:
    """Calcula hash MD5 del archivo."""
    h = hashlib.md5()
    with open(ruta, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def registrar_archivo(ruta: str | Path) -> int:
    """
    Registra el archivo en raw.archivos_cargados.
    Retorna el id_archivo generado.
    """
    ruta = Path(ruta)
    if not ruta.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {ruta}")

    # Contar líneas
    with open(ruta, encoding=ENCODING_TXT, errors="replace") as f:
        lineas = f.readlines()
    total_lineas = len(lineas)

    md5 = calcular_md5(ruta)
    logger.info(f"Registrando archivo: {ruta.name} | {total_lineas} líneas | MD5={md5[:8]}...")

    sql = """
        INSERT INTO raw.archivos_cargados
            (nombre_archivo, ruta_archivo, total_lineas, encoding, hash_md5, estado)
        VALUES (%s, %s, %s, %s, %s, 'cargado')
        RETURNING id_archivo
    """
    conn = psycopg2.connect(get_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    ruta.name, str(ruta.absolute()),
                    total_lineas, ENCODING_TXT, md5
                ))
                id_archivo = cur.fetchone()[0]
        logger.info(f"Archivo registrado con id_archivo={id_archivo}")
        return id_archivo
    finally:
        conn.close()


def actualizar_estado_archivo(id_archivo: int, estado: str, observaciones: str = None):
    """Actualiza el estado del archivo en raw.archivos_cargados."""
    sql = """
        UPDATE raw.archivos_cargados
        SET estado = %s, observaciones = %s
        WHERE id_archivo = %s
    """
    conn = psycopg2.connect(get_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (estado, observaciones, id_archivo))
        logger.info(f"Estado archivo id={id_archivo} → {estado}")
    finally:
        conn.close()
