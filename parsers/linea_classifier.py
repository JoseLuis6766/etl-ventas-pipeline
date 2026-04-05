"""
parsers/linea_classifier.py — Clasifica cada línea del TXT como:
  - 'fecha'       → encabezado de bloque de fecha (R1)
  - 'venta'       → registro de venta a parsear
  - 'ruido'       → línea a descartar (R2)
  - 'desconocida' → no clasificable con certeza

Aplica las Reglas R1 y R2 del Prompt Maestro.
"""

import re
import logging
from utils.text_utils import (
    PALABRAS_RUIDO, DICT_MARCAS, lower_sin_acento, limpiar_texto
)
from parsers.fecha_parser import es_fecha

logger = logging.getLogger("etl_camisas.linea_classifier")

# ─────────────────────────────────────
# PATRONES DE RUIDO (R2)
# ─────────────────────────────────────

# = 14, =22, =31 (conteos acumulados)
_RE_IGUAL_NUM = re.compile(r'^\s*=\s*\d+\s*$')

# Solo un número (ej. "14", "3")
_RE_SOLO_NUM = re.compile(r'^\s*\d+\s*$')

# Secuencia numérica larga (ej. "014180200138422266")
_RE_SEQ_LARGA = re.compile(r'^\s*\d{8,}\s*$')

# Solo "?"
_RE_SOLO_PREGUNTA = re.compile(r'^\s*\?+\s*$')

# "$xxx de cambio" o "xxx de cambio"
_RE_CAMBIO = re.compile(r'\$?\d+\s+de\s+cambio', re.IGNORECASE)

# "xxx camisas sin contar en el excel"
_RE_SIN_CONTAR = re.compile(r'camisas?\s+sin\s+contar', re.IGNORECASE)

# Patrón de inventario tabulado: "Negra 1M 2G 4Xg"
_RE_INVENTARIO = re.compile(
    r'^[a-záéíóúüñ\s]+(\s+\d+[a-z]{1,2}){2,}$',
    re.IGNORECASE | re.UNICODE
)

# Precio solo (número >= 100 sin descripción de producto)
_RE_SOLO_PRECIO = re.compile(r'^\s*\$?\d{3,5}\s*$')

# Palabras clave de no-venta como único contenido
_RUIDO_EXACTO = {
    'mama', 'mamá', '?', '', 'pantalon', 'pantalón',
    'playera', 'cambio', 'transferencia', 'contabilidad',
    'correo', 'contraseña',
}

# ─────────────────────────────────────
# INDICADORES DE VENTA
# ─────────────────────────────────────
_MARCAS_PATRON = re.compile(
    r'\b(' + '|'.join(re.escape(k) for k in DICT_MARCAS.keys()) + r')\b',
    re.IGNORECASE
)
_TALLAS_PATRON = re.compile(
    r'\b(xg|xl|x\s*g|[mMgG]|ch|CH|grande|median[ao]|chic[ao])\b'
)
_COLORES_PATRON = re.compile(
    r'\b(negr[ao]|blanc[ao]|azul|vino|roj[ao]|gris|lila|melon|melón|'
    r'verde|beige|besh|mostaza|marino|cielo|celeste|rey|palo)\b',
    re.IGNORECASE
)
_CANTIDAD_PATRON = re.compile(r'^\s*\d{1,2}\s+\w')


def clasificar_linea(linea: str) -> tuple[str, str]:
    """
    Clasifica una línea del TXT.

    Returns:
        (tipo, motivo) donde tipo ∈ {'fecha', 'venta', 'ruido', 'desconocida'}
        y motivo es la descripción del criterio aplicado.
    """
    t = limpiar_texto(linea)

    # ── Vacía ──────────────────────────────────────────────────
    if not t:
        return "ruido", "linea_vacia"

    t_lower = lower_sin_acento(t)

    # ── R2: Patrones de ruido por regex ────────────────────────
    if _RE_IGUAL_NUM.match(t):
        return "ruido", "conteo_igual"

    if _RE_SOLO_NUM.match(t):
        return "ruido", "solo_numero"

    if _RE_SEQ_LARGA.match(t):
        return "ruido", "secuencia_numerica_larga"

    if _RE_SOLO_PREGUNTA.match(t):
        return "ruido", "solo_interrogacion"

    if _RE_CAMBIO.search(t):
        return "ruido", "cambio_efectivo"

    if _RE_SIN_CONTAR.search(t):
        return "ruido", "nota_inventario"

    if _RE_SOLO_PRECIO.match(t):
        return "ruido", "precio_solo"

    # ── R2: Palabras de ruido exactas ──────────────────────────
    if t_lower in _RUIDO_EXACTO:
        return "ruido", f"palabra_ruido_exacta: {t_lower}"

    # ── R2: Palabras de ruido parciales ────────────────────────
    for palabra in PALABRAS_RUIDO:
        if lower_sin_acento(palabra) == t_lower:
            return "ruido", f"palabra_ruido: {palabra}"

    # ── R2: Producto diferente a camisa ───────────────────────
    if re.search(r'\bplayera\b', t, re.IGNORECASE):
        return "ruido", "no_es_camisa_playera"
    if re.search(r'\bpantalon(es)?\b', t, re.IGNORECASE | re.UNICODE):
        return "ruido", "no_es_camisa_pantalon"
    if re.search(r'\b5\s+pato\b', t, re.IGNORECASE):
        return "ruido", "no_es_camisa_pato"

    # ── R2: Patrón de inventario tabulado ─────────────────────
    if _RE_INVENTARIO.match(t):
        return "ruido", "inventario_tabulado"

    # ── R1: Fecha ──────────────────────────────────────────────
    if es_fecha(t):
        return "fecha", "contiene_mes_espanol"

    # ── Indicadores positivos de venta ─────────────────────────
    tiene_marca   = bool(_MARCAS_PATRON.search(t))
    tiene_talla   = bool(_TALLAS_PATRON.search(t))
    tiene_color   = bool(_COLORES_PATRON.search(t))
    tiene_cantidad = bool(_CANTIDAD_PATRON.match(t))

    indicadores = sum([tiene_marca, tiene_talla, tiene_color, tiene_cantidad])

    if indicadores >= 2:
        return "venta", f"indicadores_venta: marca={tiene_marca},talla={tiene_talla},color={tiene_color},cant={tiene_cantidad}"

    if indicadores == 1:
        # Clasificar como desconocida para revisión manual, pero podría ser venta parcial
        return "desconocida", f"indicadores_insuficientes ({indicadores}/4)"

    return "ruido", "sin_indicadores_venta"


def clasificar_lote(lineas: list[str]) -> list[dict]:
    """
    Clasifica una lista de líneas y asigna el bloque de fecha contexto.

    Returns:
        Lista de dicts con keys: num_linea, contenido_raw, tipo_linea,
        bloque_fecha, fecha_contexto_str, motivo_clasificacion
    """
    from parsers.fecha_parser import parsear_fecha

    resultados = []
    bloque_actual = 0
    fecha_actual = None

    for i, linea in enumerate(lineas, start=1):
        tipo, motivo = clasificar_linea(linea)

        if tipo == "fecha":
            bloque_actual += 1
            fecha_actual = parsear_fecha(linea)
            logger.info(
                f"L{i:04d} | FECHA bloque={bloque_actual} → {fecha_actual} | '{linea[:60]}'"
            )
        else:
            logger.debug(f"L{i:04d} | {tipo.upper():10} | {motivo[:50]} | '{linea[:60]}'")

        resultados.append({
            "num_linea":             i,
            "contenido_raw":         linea,
            "tipo_linea":            tipo,
            "bloque_fecha":          bloque_actual,
            "fecha_contexto":        fecha_actual,
            "motivo_clasificacion":  motivo,
        })

    # Estadísticas
    counts = {}
    for r in resultados:
        counts[r["tipo_linea"]] = counts.get(r["tipo_linea"], 0) + 1
    logger.info(f"Clasificación completada: {counts}")

    return resultados
