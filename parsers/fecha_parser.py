"""
parsers/fecha_parser.py — Detecta y parsea encabezados de fecha (Regla R1).
Soporta formatos informales: "Miércoles agosto 13", "Nov 29", "22 de noviembre", etc.
"""

import re
import logging
from datetime import date
from typing import Optional
from utils.text_utils import MESES_ES, DIAS_SEMANA, lower_sin_acento

logger = logging.getLogger("etl_camisas.fecha_parser")

AÑO_DEFAULT = 2025

# Patrón: contiene al menos un mes en español
_RE_MES = re.compile(
    r'\b(' + '|'.join(MESES_ES.keys()) + r')\b',
    re.IGNORECASE | re.UNICODE
)
# Patrón: contiene un día de la semana
_RE_DIA_SEMANA = re.compile(
    r'\b(' + '|'.join(DIAS_SEMANA) + r')\b',
    re.IGNORECASE | re.UNICODE
)
# Patrón de número de día (1-31)
_RE_DIA_NUM = re.compile(r'\b(\d{1,2})\b')


def es_fecha(linea: str) -> bool:
    """
    Determina si una línea es un encabezado de fecha (R1).
    True si contiene nombre de mes en español.
    """
    t = lower_sin_acento(linea)
    return bool(_RE_MES.search(t))


def parsear_fecha(linea: str, año: int = AÑO_DEFAULT) -> Optional[date]:
    """
    Extrae la fecha de un encabezado de fecha (R1).
    Retorna un objeto date o None si no puede parsear.

    Formatos soportados:
    - "Miércoles agosto 13"
    - "Febrero 22"
    - "Nov 29"
    - "22 de noviembre"
    - "Noviembre 9"
    - "Octubre 4 sábado"
    - "Sep 28 domingo"
    """
    t = lower_sin_acento(linea)

    # Buscar mes
    mes_match = _RE_MES.search(t)
    if not mes_match:
        logger.debug(f"No se encontró mes en: '{linea}'")
        return None

    mes_texto = mes_match.group(1)
    mes_num = MESES_ES.get(mes_texto)
    if mes_num is None:
        logger.warning(f"Mes no reconocido: '{mes_texto}' en línea: '{linea}'")
        return None

    # Buscar número de día (excluir año si lo hubiera)
    numeros = _RE_DIA_NUM.findall(t)
    dia = None
    for num_str in numeros:
        n = int(num_str)
        if 1 <= n <= 31:
            dia = n
            break  # tomar el primer número válido como día

    if dia is None:
        logger.warning(f"No se encontró día en: '{linea}'")
        return None

    try:
        fecha = date(año, mes_num, dia)
        logger.debug(f"Fecha parseada: {fecha} ← '{linea}'")
        return fecha
    except ValueError as e:
        logger.warning(f"Fecha inválida ({año}-{mes_num}-{dia}) en '{linea}': {e}")
        return None


def extraer_info_fecha(linea: str, año: int = AÑO_DEFAULT) -> dict:
    """
    Extrae toda la información de una línea de fecha.
    Retorna dict con: es_fecha, fecha, mes, dia, dia_semana.
    """
    resultado = {
        "es_fecha": False,
        "fecha": None,
        "mes": None,
        "dia": None,
        "dia_semana": None,
        "linea_original": linea,
    }

    if not es_fecha(linea):
        return resultado

    resultado["es_fecha"] = True
    resultado["fecha"] = parsear_fecha(linea, año)

    # Extraer nombre de mes
    t = lower_sin_acento(linea)
    mes_match = _RE_MES.search(t)
    if mes_match:
        resultado["mes"] = MESES_ES.get(mes_match.group(1))

    # Extraer día semana
    dia_match = _RE_DIA_SEMANA.search(t)
    if dia_match:
        resultado["dia_semana"] = dia_match.group(1)

    # Extraer día numérico
    numeros = _RE_DIA_NUM.findall(t)
    for num_str in numeros:
        n = int(num_str)
        if 1 <= n <= 31:
            resultado["dia"] = n
            break

    return resultado
