"""
normalizers/homologador.py — Homologa ventas_candidatas contra ref.catalogo_camisas.
Determina si cada candidata puede pasar a silver.ventas_homologadas o va a rechazadas.
"""

import logging
import psycopg2
import psycopg2.extras
from config import get_dsn, setup_logging, NIVEL_CONFIANZA_MIN_HOMOLOGAR
from normalizers.atributo_normalizer import normalizar_registro, validar_registro
from utils.text_utils import construir_id_camisa, calcular_nivel_confianza, DEFAULTS_MARCA
from config import ATRIBUTOS_REQUERIDOS

logger = setup_logging("etl_camisas.homologador")


def _obtener_catalogo() -> dict:
    """Carga el catálogo de camisas desde ref.catalogo_camisas."""
    sql = "SELECT id_camisa, marca, tipo_cuello, manga, estampado, color, talla, tipo_tela FROM ref.catalogo_camisas WHERE activo = TRUE"
    conn = psycopg2.connect(get_dsn())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        catalogo = {r["id_camisa"]: dict(r) for r in rows}
        logger.info(f"Catálogo cargado: {len(catalogo)} camisas")
        return catalogo
    finally:
        conn.close()


def homologar_lote(candidatas: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Procesa un lote de candidatas y las separa en homologadas y rechazadas.

    Args:
        candidatas: Lista de registros de silver.ventas_candidatas

    Returns:
        (homologadas, rechazadas) — dos listas de dicts
    """
    catalogo = _obtener_catalogo()
    homologadas = []
    rechazadas = []

    for candidata in candidatas:
        try:
            resultado = _homologar_uno(candidata, catalogo)
            if resultado["_homologado"]:
                homologadas.append(resultado)
            else:
                rechazadas.append({
                    "id_linea":       candidata.get("id_linea"),
                    "id_archivo":     candidata.get("id_archivo"),
                    "linea_original": candidata.get("linea_original"),
                    "motivo_rechazo": resultado.get("_motivo_rechazo"),
                    "detalle_rechazo": resultado.get("_detalle_rechazo"),
                    "nivel_confianza": candidata.get("nivel_confianza", 0),
                })
        except Exception as e:
            logger.error(f"Error homologando candidata {candidata.get('id_candidata')}: {e}")
            rechazadas.append({
                "id_linea":       candidata.get("id_linea"),
                "linea_original": candidata.get("linea_original"),
                "motivo_rechazo": "error_interno",
                "detalle_rechazo": str(e),
                "nivel_confianza": 0,
            })

    logger.info(
        f"Homologación completada: {len(homologadas)} homologadas, "
        f"{len(rechazadas)} rechazadas"
    )
    return homologadas, rechazadas


def _homologar_uno(candidata: dict, catalogo: dict) -> dict:
    """
    Homologa un registro individual.
    Prioridad: R12 defaults → normalización → validación → lookup catálogo.
    """
    r = dict(candidata)

    # 1. Aplicar defaults de marca si faltan atributos
    marca = r.get("marca")
    if marca and marca in DEFAULTS_MARCA:
        defaults = DEFAULTS_MARCA[marca]
        for campo, default_val in defaults.items():
            campo_mapeado = "tipo_cuello" if campo == "tipo_cuello" else campo
            if not r.get(campo_mapeado):
                r[campo_mapeado] = default_val
                reglas = r.get("reglas_aplicadas", "") or ""
                r["reglas_aplicadas"] = reglas + f",R12_{campo}_homolog"

    # 2. Normalizar atributos
    r = normalizar_registro(r)

    # 3. Validar atributos requeridos — solo rechazar si NO hay marca
    #    (color y talla faltantes → parcial, no rechazado — R instrucc. 5: priorizar recuperación)
    if not r.get("marca"):
        r["_homologado"] = False
        r["_motivo_rechazo"] = "sin_marca"
        r["_detalle_rechazo"] = "no se detectó marca en la línea"
        return r

    # Advertencia suave para atributos deseables pero no bloqueantes
    faltantes_deseables = [a for a in ["color", "talla"] if not r.get(a)]
    if faltantes_deseables:
        obs = r.get("observaciones") or ""
        r["observaciones"] = (obs + f"; atrib_parciales:{faltantes_deseables}").lstrip("; ")

    # 4. Validar valores en catálogo
    valido, errores = validar_registro(r)
    if not valido and errores:
        # Solo rechazar si la marca es inválida (atributo más crítico)
        errores_criticos = [e for e in errores if e.startswith("marca_invalida")]
        if errores_criticos:
            r["_homologado"] = False
            r["_motivo_rechazo"] = "marca_no_en_catalogo"
            r["_detalle_rechazo"] = "; ".join(errores)
            return r
        # Errores no críticos (color, talla, estampado fuera de catálogo)
        # → registrar en observaciones y reducir confianza, pero NO rechazar
        obs = r.get("observaciones") or ""
        r["observaciones"] = (obs + "; " + "; ".join(errores)).lstrip("; ")
        # Penalizar confianza levemente
        confianza_actual = r.get("nivel_confianza") or 60.0
        r["nivel_confianza"] = max(20.0, float(confianza_actual) - 10.0)

    # 5. Construir id_camisa y buscar en catálogo
    id_camisa = construir_id_camisa(
        r.get("marca",""), r.get("tipo_cuello",""),
        r.get("manga",""), r.get("estampado",""),
        r.get("color",""), r.get("talla","")
    )

    if id_camisa and id_camisa in catalogo:
        # Match exacto en catálogo
        entrada_cat = catalogo[id_camisa]
        if not r.get("tipo_tela") and entrada_cat.get("tipo_tela"):
            r["tipo_tela"] = entrada_cat["tipo_tela"]
        reglas = r.get("reglas_aplicadas", "") or ""
        r["reglas_aplicadas"] = reglas + ",HOMOLOG_EXACTO"
        nivel = 100.0
    else:
        # No está en catálogo pero tiene atributos suficientes → aceptar parcial
        reglas = r.get("reglas_aplicadas", "") or ""
        r["reglas_aplicadas"] = reglas + ",HOMOLOG_PARCIAL"
        nivel = calcular_nivel_confianza(r, ATRIBUTOS_REQUERIDOS)

    r["nivel_confianza"] = nivel
    r["_homologado"] = True
    return r
