"""
loaders/gold_loader.py — Carga la capa gold.
  - gold.hecho_ventas    (tabla analítica final)
  - gold.metricas_calidad (KPIs del proceso ETL)
"""

import logging
import psycopg2
import psycopg2.extras
from datetime import date
from config import get_dsn, setup_logging

logger = setup_logging("etl_camisas.gold_loader")


def cargar_hecho_ventas(homologadas: list[dict], ids_homologadas: list[int]) -> int:
    """
    Inserta registros en gold.hecho_ventas.
    Enriquece con dimensiones de fecha (año, mes, semana, día semana).
    """
    sql = """
        INSERT INTO gold.hecho_ventas
            (id_homologada, id_camisa, fecha, anio, mes, semana, dia_semana,
             cantidad, precio_unitario, precio_total,
             marca, tipo_cuello, manga, estampado, color, talla, tipo_tela)
        VALUES %s
    """
    DIAS = {0:'lunes',1:'martes',2:'miércoles',3:'jueves',
            4:'viernes',5:'sábado',6:'domingo'}

    filas = []
    omitidas = 0
    for reg, id_hom in zip(homologadas, ids_homologadas):
        # REGLA GOLD: solo pasan registros con id_camisa completo
        id_camisa = _construir_id_gold(reg)
        if not id_camisa:
            omitidas += 1
            logger.debug(f"Omitida en gold (id_camisa null): marca={reg.get(chr(39)+'marca'+chr(39))} talla={reg.get(chr(39)+'talla'+chr(39))} color={reg.get(chr(39)+'color'+chr(39))}")
            continue

        fecha = reg.get("fecha")
        if isinstance(fecha, date):
            anio    = fecha.year
            mes     = fecha.month
            semana  = fecha.isocalendar()[1]
            dia_sem = DIAS.get(fecha.weekday(), "")
        else:
            anio = mes = semana = None
            dia_sem = None

        precio_u = reg.get("precio")
        cantidad = reg.get("cantidad") or 1
        precio_t = (precio_u * cantidad) if precio_u else None

        filas.append((
            id_hom, id_camisa,
            fecha, anio, mes, semana, dia_sem,
            cantidad, precio_u, precio_t,
            reg.get("marca"), reg.get("tipo_cuello"), reg.get("manga"),
            reg.get("estampado"), reg.get("color"), reg.get("talla"),
            reg.get("tipo_tela"),
        ))

    if omitidas:
        logger.info(f"Omitidas en gold por id_camisa null: {omitidas}")
    if not filas:
        logger.warning("No hay filas con id_camisa completo para gold.hecho_ventas")
        return 0

    conn = psycopg2.connect(get_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, filas, page_size=200)
        logger.info(f"Insertadas {len(filas)} filas en gold.hecho_ventas")
        return len(filas)
    finally:
        conn.close()


def _construir_id_gold(reg: dict) -> str | None:
    partes = [
        reg.get("marca",""), reg.get("tipo_cuello",""),
        reg.get("manga",""), reg.get("estampado",""),
        reg.get("color",""), reg.get("talla","")
    ]
    if any(not p for p in partes):
        return None
    return "-".join(p.upper().replace(" ","_") for p in partes)


def calcular_y_guardar_metricas(
    id_archivo: int,
    stats_bronze: dict,
    candidatas: list[dict],
    homologadas: list[dict],
    rechazadas: list[dict],
) -> dict:
    """
    Calcula KPIs del pipeline ETL y los guarda en gold.metricas_calidad.
    Verifica si se cumplen las metas del Prompt Maestro.
    """
    por_tipo        = stats_bronze.get("por_tipo", {})
    total_lineas    = stats_bronze.get("total_lineas", 0)
    lineas_fecha    = por_tipo.get("fecha", 0)
    lineas_ruido    = por_tipo.get("ruido", 0)
    lineas_venta    = por_tipo.get("venta", 0)
    lineas_desc     = por_tipo.get("desconocida", 0)

    lineas_utiles   = total_lineas - lineas_ruido - lineas_fecha
    cand_total      = len(candidatas)
    cand_completa   = sum(1 for c in candidatas if c.get("estado") == "completa")
    cand_parcial    = sum(1 for c in candidatas if c.get("estado") == "parcial")
    cand_fallida    = sum(1 for c in candidatas if c.get("estado") == "fallida")
    homolog_total   = len(homologadas)
    rech_total      = len(rechazadas)

    # KPIs
    pct_recuperacion  = round((lineas_venta / lineas_utiles * 100), 2) if lineas_utiles > 0 else 0
    pct_homologacion  = round((homolog_total / cand_total * 100), 2) if cand_total > 0 else 0
    pct_rechazo       = round((rech_total / cand_total * 100), 2) if cand_total > 0 else 0

    # Completitud: % candidatas completas
    pct_completitud   = round((cand_completa / cand_total * 100), 2) if cand_total > 0 else 0

    # Consistencia ID: homologadas con id_camisa válido
    with_id = sum(1 for h in homologadas if _construir_id_gold(h))
    pct_consistencia  = round((with_id / homolog_total * 100), 2) if homolog_total > 0 else 0

    # Verificar metas
    cumple_recuperacion = pct_recuperacion >= 85
    cumple_homologacion = pct_homologacion >= 80
    cumple_rechazo      = pct_rechazo <= 15

    metricas = {
        "id_archivo":          id_archivo,
        "total_lineas":        total_lineas,
        "lineas_fecha":        lineas_fecha,
        "lineas_ruido":        lineas_ruido,
        "lineas_venta":        lineas_venta,
        "lineas_desconocida":  lineas_desc,
        "candidatas_total":    cand_total,
        "candidatas_completa": cand_completa,
        "candidatas_parcial":  cand_parcial,
        "candidatas_fallida":  cand_fallida,
        "homologadas_total":   homolog_total,
        "rechazadas_total":    rech_total,
        "pct_recuperacion":    pct_recuperacion,
        "pct_homologacion":    pct_homologacion,
        "pct_rechazo":         pct_rechazo,
        "pct_completitud":     pct_completitud,
        "pct_consistencia_id": pct_consistencia,
        "cumple_recuperacion": cumple_recuperacion,
        "cumple_homologacion": cumple_homologacion,
        "cumple_rechazo":      cumple_rechazo,
    }

    # Guardar en DB
    sql = """
        INSERT INTO gold.metricas_calidad
            (id_archivo, total_lineas, lineas_fecha, lineas_ruido, lineas_venta,
             lineas_desconocida, candidatas_total, candidatas_completa,
             candidatas_parcial, candidatas_fallida, homologadas_total,
             rechazadas_total, pct_recuperacion, pct_homologacion, pct_rechazo,
             pct_completitud, pct_consistencia_id,
             cumple_recuperacion, cumple_homologacion, cumple_rechazo)
        VALUES (%(id_archivo)s, %(total_lineas)s, %(lineas_fecha)s,
                %(lineas_ruido)s, %(lineas_venta)s, %(lineas_desconocida)s,
                %(candidatas_total)s, %(candidatas_completa)s,
                %(candidatas_parcial)s, %(candidatas_fallida)s,
                %(homologadas_total)s, %(rechazadas_total)s,
                %(pct_recuperacion)s, %(pct_homologacion)s, %(pct_rechazo)s,
                %(pct_completitud)s, %(pct_consistencia_id)s,
                %(cumple_recuperacion)s, %(cumple_homologacion)s,
                %(cumple_rechazo)s)
    """
    conn = psycopg2.connect(get_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, metricas)
        logger.info("Métricas de calidad guardadas en gold")
    finally:
        conn.close()

    # Log del resumen
    _log_metricas(metricas)
    return metricas


def _log_metricas(m: dict):
    logger.info("─" * 55)
    logger.info("📊  MÉTRICAS DE CALIDAD ETL")
    logger.info("─" * 55)
    logger.info(f"  Total líneas:          {m['total_lineas']:>6}")
    logger.info(f"  Líneas fecha:          {m['lineas_fecha']:>6}")
    logger.info(f"  Líneas ruido:          {m['lineas_ruido']:>6}")
    logger.info(f"  Líneas venta:          {m['lineas_venta']:>6}")
    logger.info(f"  Candidatas:            {m['candidatas_total']:>6}  (completas={m['candidatas_completa']}, parciales={m['candidatas_parcial']}, fallidas={m['candidatas_fallida']})")
    logger.info(f"  Homologadas:           {m['homologadas_total']:>6}")
    logger.info(f"  Rechazadas:            {m['rechazadas_total']:>6}")
    logger.info("─" * 55)
    logger.info(f"  % Recuperación:  {m['pct_recuperacion']:>6.1f}%  {'✅' if m['cumple_recuperacion'] else '❌'} (meta ≥85%)")
    logger.info(f"  % Homologación:  {m['pct_homologacion']:>6.1f}%  {'✅' if m['cumple_homologacion'] else '❌'} (meta ≥80%)")
    logger.info(f"  % Rechazo:       {m['pct_rechazo']:>6.1f}%  {'✅' if m['cumple_rechazo'] else '❌'} (meta ≤15%)")
    logger.info(f"  % Completitud:   {m['pct_completitud']:>6.1f}%  (meta ≥90%)")
    logger.info(f"  % ID_camisa:     {m['pct_consistencia_id']:>6.1f}%  (meta ≥95%)")
    logger.info("─" * 55)
