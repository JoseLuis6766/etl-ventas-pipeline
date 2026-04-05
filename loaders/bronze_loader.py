"""
loaders/bronze_loader.py — Lee ventas_2025.txt y carga bronze.lineas_txt.

Pasos:
  1. Lee el archivo con codificación UTF-8-BOM
  2. Clasifica cada línea (fecha/venta/ruido/desconocida)
  3. Inserta en bronze.lineas_txt con bloque_fecha y fecha_contexto
  4. Retorna estadísticas del proceso
"""

import logging
import psycopg2
import psycopg2.extras
from pathlib import Path
from config import get_dsn, ENCODING_TXT, setup_logging
from parsers.linea_classifier import clasificar_lote
from loaders.raw_loader import registrar_archivo, actualizar_estado_archivo

logger = setup_logging("etl_camisas.bronze_loader")


def cargar_bronze(ruta_txt: str | Path) -> dict:
    """
    Proceso principal: lee el TXT y carga bronze.lineas_txt.

    Returns:
        dict con estadísticas: id_archivo, total, por_tipo
    """
    ruta_txt = Path(ruta_txt)
    logger.info("=" * 60)
    logger.info(f"INICIO CARGA BRONZE: {ruta_txt.name}")
    logger.info("=" * 60)

    # ── 1. Registrar archivo en raw ───────────────────────────
    id_archivo = registrar_archivo(ruta_txt)

    try:
        # ── 2. Leer líneas ────────────────────────────────────
        with open(ruta_txt, encoding=ENCODING_TXT, errors="replace") as f:
            lineas = [l.rstrip('\n').rstrip('\r') for l in f]

        logger.info(f"Leídas {len(lineas)} líneas del archivo")

        # ── 3. Clasificar ─────────────────────────────────────
        clasificadas = clasificar_lote(lineas)

        # ── 4. Insertar en bronze.lineas_txt ──────────────────
        total_insertadas = _insertar_bronze(id_archivo, clasificadas)

        # ── 5. Estadísticas ───────────────────────────────────
        conteos = {}
        for r in clasificadas:
            t = r["tipo_linea"]
            conteos[t] = conteos.get(t, 0) + 1

        stats = {
            "id_archivo":     id_archivo,
            "total_lineas":   len(lineas),
            "insertadas":     total_insertadas,
            "por_tipo":       conteos,
        }

        logger.info(f"Bronze cargado: {stats}")
        actualizar_estado_archivo(id_archivo, "bronze_cargado")
        return stats

    except Exception as e:
        logger.error(f"Error en carga bronze: {e}", exc_info=True)
        actualizar_estado_archivo(id_archivo, "error", str(e))
        raise


def _insertar_bronze(id_archivo: int, clasificadas: list[dict]) -> int:
    """
    Inserta los registros clasificados en bronze.lineas_txt.
    Usa execute_values para eficiencia. Una transacción por lote.
    """
    sql = """
        INSERT INTO bronze.lineas_txt
            (id_archivo, num_linea, contenido_raw, tipo_linea,
             bloque_fecha, fecha_contexto)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    filas = [
        (
            id_archivo,
            r["num_linea"],
            r["contenido_raw"],
            r["tipo_linea"],
            r["bloque_fecha"],
            r["fecha_contexto"],
        )
        for r in clasificadas
    ]

    conn = psycopg2.connect(get_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, filas, page_size=500)
                total = cur.rowcount
        logger.info(f"Insertadas {len(filas)} filas en bronze.lineas_txt")
        return len(filas)
    finally:
        conn.close()


def obtener_lineas_bronze(id_archivo: int, solo_tipo: str = None) -> list[dict]:
    """
    Recupera líneas de bronze para el archivo dado.
    Opcionalmente filtra por tipo_linea.
    """
    sql = """
        SELECT id_linea, num_linea, contenido_raw, tipo_linea,
               bloque_fecha, fecha_contexto
        FROM bronze.lineas_txt
        WHERE id_archivo = %s
    """
    params = [id_archivo]
    if solo_tipo:
        sql += " AND tipo_linea = %s"
        params.append(solo_tipo)
    sql += " ORDER BY num_linea"

    conn = psycopg2.connect(get_dsn())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
        logger.debug(f"Recuperadas {len(rows)} líneas bronze (tipo={solo_tipo})")
        return rows
    finally:
        conn.close()
