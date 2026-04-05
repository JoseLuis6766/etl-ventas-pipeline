"""
loaders/silver_loader.py — Carga datos en las tablas silver.
  - silver.ventas_candidatas   (resultado del parseo)
  - silver.ventas_homologadas  (normalizadas contra catálogo)
  - silver.ventas_rechazadas   (no procesables)
"""

import logging
import psycopg2
import psycopg2.extras
from config import get_dsn, setup_logging, NIVEL_CONFIANZA_MIN_HOMOLOGAR
from utils.text_utils import construir_id_camisa

logger = setup_logging("etl_camisas.silver_loader")


# ─────────────────────────────────────
# CANDIDATAS
# ─────────────────────────────────────

def insertar_candidatas(id_archivo: int, registros: list[dict]) -> list[int]:
    """
    Inserta registros en silver.ventas_candidatas.
    Retorna lista de id_candidata generados.
    """
    sql = """
        INSERT INTO silver.ventas_candidatas
            (id_linea, id_archivo, linea_original, fecha, cantidad, marca,
             tipo_cuello, manga, estampado, color, talla, precio, tipo_tela,
             nivel_confianza, estado, reglas_aplicadas, observaciones)
        VALUES %s
        RETURNING id_candidata
    """
    filas = [
        (
            r.get("id_linea"),
            id_archivo,
            r.get("linea_original"),
            r.get("fecha"),
            r.get("cantidad"),
            r.get("marca"),
            r.get("tipo_cuello"),
            r.get("manga"),
            r.get("estampado"),
            r.get("color"),
            r.get("talla"),
            r.get("precio"),
            r.get("tipo_tela"),
            r.get("nivel_confianza"),
            r.get("estado"),
            r.get("reglas_aplicadas"),
            r.get("observaciones"),
        )
        for r in registros
    ]

    ids = []
    conn = psycopg2.connect(get_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                rows = psycopg2.extras.execute_values(
                    cur, sql, filas, page_size=200, fetch=True
                )
                ids = [row[0] for row in rows] if rows else []
        logger.info(f"Insertadas {len(ids)} candidatas en silver")
        return ids
    except Exception as e:
        logger.error(f"Error insertando candidatas: {e}", exc_info=True)
        raise
    finally:
        conn.close()


# ─────────────────────────────────────
# RECHAZADAS
# ─────────────────────────────────────

def insertar_rechazadas(id_archivo: int, registros: list[dict]) -> int:
    """Inserta registros en silver.ventas_rechazadas."""
    sql = """
        INSERT INTO silver.ventas_rechazadas
            (id_linea, id_archivo, linea_original,
             motivo_rechazo, detalle_rechazo, nivel_confianza)
        VALUES %s
    """
    filas = [
        (
            r.get("id_linea"),
            id_archivo,
            r.get("linea_original") or r.get("contenido_raw"),
            r.get("motivo_rechazo"),
            r.get("detalle_rechazo"),
            r.get("nivel_confianza", 0),
        )
        for r in registros
    ]
    if not filas:
        return 0

    conn = psycopg2.connect(get_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, filas, page_size=200)
        logger.info(f"Insertadas {len(filas)} rechazadas en silver")
        return len(filas)
    finally:
        conn.close()


# ─────────────────────────────────────
# HOMOLOGADAS
# ─────────────────────────────────────

def insertar_homologadas(registros: list[dict]) -> list[int]:
    """
    Inserta registros normalizados en silver.ventas_homologadas.
    Construye el id_camisa automáticamente.
    """
    sql = """
        INSERT INTO silver.ventas_homologadas
            (id_candidata, id_camisa, fecha, cantidad, marca, tipo_cuello,
             manga, estampado, color, talla, precio, tipo_tela,
             nivel_confianza, reglas_aplicadas, observaciones)
        VALUES %s
        RETURNING id_homologada
    """
    filas = []
    for r in registros:
        id_camisa = construir_id_camisa(
            r.get("marca",""), r.get("tipo_cuello",""),
            r.get("manga",""), r.get("estampado",""),
            r.get("color",""), r.get("talla","")
        )
        filas.append((
            r.get("id_candidata"),
            id_camisa,
            r.get("fecha"),
            r.get("cantidad"),
            r.get("marca"),
            r.get("tipo_cuello"),
            r.get("manga"),
            r.get("estampado"),
            r.get("color"),
            r.get("talla"),
            r.get("precio"),
            r.get("tipo_tela"),
            r.get("nivel_confianza"),
            r.get("reglas_aplicadas"),
            r.get("observaciones"),
        ))

    ids = []
    conn = psycopg2.connect(get_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                rows = psycopg2.extras.execute_values(
                    cur, sql, filas, page_size=200, fetch=True
                )
                ids = [row[0] for row in rows] if rows else []
        logger.info(f"Insertadas {len(ids)} homologadas en silver")
        return ids
    finally:
        conn.close()


def obtener_candidatas_para_homologar(id_archivo: int) -> list[dict]:
    """
    Recupera candidatas con nivel_confianza suficiente para intentar homologar.
    Incluye parciales y completas; excluye solo las fallidas sin marca.
    """
    sql = """
        SELECT *
        FROM silver.ventas_candidatas
        WHERE id_archivo = %s
          AND estado IN ('completa', 'parcial')
          AND nivel_confianza >= %s
          AND marca IS NOT NULL
        ORDER BY id_candidata
    """
    conn = psycopg2.connect(get_dsn())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, (id_archivo, NIVEL_CONFIANZA_MIN_HOMOLOGAR))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
