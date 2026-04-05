"""
main.py — Orquestador principal del ETL Camisas.

"""

import sys
import argparse
import logging
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent))

from config import TXT_FILE, setup_logging, NIVEL_CONFIANZA_MIN_HOMOLOGAR
from loaders.bronze_loader import cargar_bronze, obtener_lineas_bronze
from loaders.silver_loader import (
    insertar_candidatas, insertar_rechazadas,
    insertar_homologadas, obtener_candidatas_para_homologar
)
from loaders.gold_loader import cargar_hecho_ventas, calcular_y_guardar_metricas
from parsers.desdoblador import desdoblar, necesita_desdoblamiento
from normalizers.homologador import homologar_lote

logger = setup_logging("etl_camisas.main")


def run_etl(ruta_txt: Path, solo_bronze: bool = False) -> dict:
    """
    Ejecuta el pipeline ETL completo.

    Returns:
        dict con resumen del proceso
    """
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║      ETL CAMISAS 2025 — INICIO           ║")
    logger.info("╚══════════════════════════════════════════╝")

   
    # CAPA 1: RAW + BRONZE
    
    logger.info("\n── CAPA BRONZE ──────────────────────────")
    stats_bronze = cargar_bronze(ruta_txt)
    id_archivo = stats_bronze["id_archivo"]

    if solo_bronze:
        logger.info("Modo solo-bronze: deteniendo aquí.")
        return stats_bronze

    
    # CAPA 2: SILVER — Parseo y candidatas
   
    logger.info("\n── CAPA SILVER (parseo) ─────────────────")

    # Recuperar líneas de tipo 'venta' y 'desconocida' del bronze
    lineas_venta = obtener_lineas_bronze(id_archivo, solo_tipo="venta")
    lineas_desc  = obtener_lineas_bronze(id_archivo, solo_tipo="desconocida")
    lineas_a_parsear = lineas_venta + lineas_desc

    logger.info(
        f"Líneas a parsear: {len(lineas_a_parsear)} "
        f"(ventas={len(lineas_venta)}, desconocidas={len(lineas_desc)})"
    )

    candidatas_raw  = []
    rechazadas_raw  = []

    for linea_bronze in lineas_a_parsear:
        contenido = linea_bronze["contenido_raw"]
        fecha     = linea_bronze.get("fecha_contexto")
        id_linea  = linea_bronze["id_linea"]

        try:
            # Desdoblar si necesario (R11)
            if necesita_desdoblamiento(contenido):
                sub_registros = desdoblar(contenido, fecha)
            else:
                sub_registros = [None]  # señal para parsear simple

            if sub_registros == [None]:
                # Parseo simple
                from parsers.venta_parser import parsear_venta
                reg = parsear_venta(contenido, fecha)
                reg["id_linea"] = id_linea
                _clasificar_candidata_o_rechazada(reg, candidatas_raw, rechazadas_raw)
            else:
                for reg in sub_registros:
                    reg["id_linea"] = id_linea
                    _clasificar_candidata_o_rechazada(reg, candidatas_raw, rechazadas_raw)

        except Exception as e:
            logger.error(f"Error parseando línea {id_linea}: {e}", exc_info=True)
            rechazadas_raw.append({
                "id_linea":       id_linea,
                "id_archivo":     id_archivo,
                "linea_original": contenido,
                "motivo_rechazo": "error_parseo",
                "detalle_rechazo": str(e),
                "nivel_confianza": 0,
            })

    # Insertar candidatas
    for r in candidatas_raw:
        r["id_archivo"] = id_archivo
    ids_candidatas = insertar_candidatas(id_archivo, candidatas_raw)

    # Asignar IDs de vuelta
    for reg, id_cand in zip(candidatas_raw, ids_candidatas):
        reg["id_candidata"] = id_cand

    # Insertar rechazadas de parseo
    for r in rechazadas_raw:
        r["id_archivo"] = id_archivo
    insertar_rechazadas(id_archivo, rechazadas_raw)

    logger.info(
        f"Silver candidatas: {len(candidatas_raw)} | "
        f"Silver rechazadas (parseo): {len(rechazadas_raw)}"
    )

   
    # CAPA 2b: SILVER — Homologación
    
    logger.info("\n── CAPA SILVER (homologación) ───────────")

    # Recuperar candidatas con confianza suficiente
    candidatas_para_homologar = obtener_candidatas_para_homologar(id_archivo)
    logger.info(f"Candidatas para homologar: {len(candidatas_para_homologar)}")

    homologadas, rechazadas_homolog = homologar_lote(candidatas_para_homologar)

    # Insertar homologadas
    ids_homologadas = insertar_homologadas(homologadas)

    # Insertar rechazadas de homologación
    for r in rechazadas_homolog:
        r["id_archivo"] = id_archivo
    insertar_rechazadas(id_archivo, rechazadas_homolog)

   
    # CAPA 3: GOLD
   
    logger.info("\n── CAPA GOLD ────────────────────────────")

    filas_gold = cargar_hecho_ventas(homologadas, ids_homologadas)

    # Métricas
    todas_rechazadas = rechazadas_raw + rechazadas_homolog
    metricas = calcular_y_guardar_metricas(
        id_archivo=id_archivo,
        stats_bronze=stats_bronze,
        candidatas=candidatas_raw,
        homologadas=homologadas,
        rechazadas=todas_rechazadas,
    )

    logger.info("--------------------------------------------")
    logger.info("|      ETL CAMISAS 2025 — COMPLETADO       |")
    logger.info("--------------------------------------------")

    return {
        "id_archivo":    id_archivo,
        "bronze":        stats_bronze,
        "candidatas":    len(candidatas_raw),
        "homologadas":   len(homologadas),
        "rechazadas":    len(todas_rechazadas),
        "gold_filas":    filas_gold,
        "metricas":      metricas,
    }


def _clasificar_candidata_o_rechazada(reg: dict, candidatas: list, rechazadas: list):
    """Separa un registro en candidata o rechazada según su estado."""
    if reg.get("estado") == "fallida" and reg.get("nivel_confianza", 0) == 0:
        rechazadas.append({
            "id_linea":        reg.get("id_linea"),
            "linea_original":  reg.get("linea_original"),
            "motivo_rechazo":  "fallida_parseo",
            "detalle_rechazo": reg.get("observaciones"),
            "nivel_confianza": 0,
        })
    else:
        candidatas.append(reg)


def run_desde_silver(id_archivo: int):
    """Reanuda el pipeline desde la homologación silver hacia adelante."""
    logger.info(f"Reanudando desde silver para id_archivo={id_archivo}")
    # (Implementación para recuperación de errores)
    candidatas = obtener_candidatas_para_homologar(id_archivo)
    homologadas, rechazadas_homolog = homologar_lote(candidatas)
    ids_homologadas = insertar_homologadas(homologadas)
    for r in rechazadas_homolog:
        r["id_archivo"] = id_archivo
    insertar_rechazadas(id_archivo, rechazadas_homolog)
    cargar_hecho_ventas(homologadas, ids_homologadas)
    logger.info("Reanudación completada.")



# PUNTO DE ENTRADA


def parse_args():
    parser = argparse.ArgumentParser(description="ETL Camisas 2025")
    parser.add_argument("--txt",    type=Path, default=TXT_FILE,
                        help="Ruta al archivo ventas_2025.txt")
    parser.add_argument("--solo-bronze", action="store_true",
                        help="Ejecutar solo hasta la capa bronze")
    parser.add_argument("--desde-silver", type=int, metavar="ID_ARCHIVO",
                        help="Reanudar desde silver con el id_archivo dado")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.desde_silver:
        run_desde_silver(args.desde_silver)
    else:
        resultado = run_etl(args.txt, solo_bronze=args.solo_bronze)
        logger.info(f"Resultado final: {resultado}")
