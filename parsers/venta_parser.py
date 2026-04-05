"""
parsers/venta_parser.py — Extrae atributos de una línea de venta.
Aplica reglas R3-R14 del Prompt Maestro.
Retorna un dict con todos los atributos detectados y metadatos de confianza.
"""

import re
import logging
from difflib import get_close_matches
from typing import Optional
from utils.text_utils import (
    DICT_MARCAS, DICT_TALLAS, DICT_COLORES, DICT_ESTAMPADOS,
    DICT_CUELLO, DICT_MANGA, DICT_TELA, DEFAULTS_MARCA,
    limpiar_texto, lower_sin_acento, es_numero,
    extraer_numero_inicial, calcular_nivel_confianza,
)
from config import ATRIBUTOS_REQUERIDOS

logger = logging.getLogger("etl_camisas.venta_parser")


# ─────────────────────────────────────
# HELPERS DE EXTRACCIÓN
# ─────────────────────────────────────

def _extraer_precio(tokens: list[str]) -> tuple[Optional[float], list[str]]:
    """
    R10: Extrae precio del final de los tokens.
    Retorna (precio, tokens_sin_precio).
    """
    tokens_filtrados = []
    precio = None
    for tok in reversed(tokens):
        t = tok.strip().lstrip('$')
        if re.match(r'^\d{2,5}$', t):
            n = int(t)
            if n >= 50:  # heurística: precios >= 50
                precio = float(n)
                continue  # no agregar al resultado
        tokens_filtrados.insert(0, tok)
    return precio, tokens_filtrados


def _buscar_en_dict_multi(texto_lower: str, diccionario: dict) -> Optional[str]:
    """Búsqueda en diccionario probando frases de 3, 2 y 1 palabras."""
    palabras = texto_lower.split()
    # Trigramas
    for i in range(len(palabras) - 2):
        k = f"{palabras[i]} {palabras[i+1]} {palabras[i+2]}"
        if k in diccionario:
            return diccionario[k]
    # Bigramas
    for i in range(len(palabras) - 1):
        k = f"{palabras[i]} {palabras[i+1]}"
        if k in diccionario:
            return diccionario[k]
    # Unigramas
    for p in palabras:
        if p in diccionario:
            return diccionario[p]
    return None


def _buscar_dict_con_posicion(tokens: list[str], diccionario: dict) -> tuple[Optional[str], list[str]]:
    """
    Busca tokens en el diccionario (de 3 a 1 palabras).
    Retorna (valor_encontrado, tokens_restantes_sin_el_match).
    """
    n = len(tokens)
    # Trigramas
    for i in range(n - 2):
        k = lower_sin_acento(f"{tokens[i]} {tokens[i+1]} {tokens[i+2]}")
        if k in diccionario:
            return diccionario[k], tokens[:i] + tokens[i+3:]
    # Bigramas
    for i in range(n - 1):
        k = lower_sin_acento(f"{tokens[i]} {tokens[i+1]}")
        if k in diccionario:
            return diccionario[k], tokens[:i] + tokens[i+2:]
    # Unigramas
    for i, tok in enumerate(tokens):
        k = lower_sin_acento(tok)
        if k in diccionario:
            return diccionario[k], tokens[:i] + tokens[i+1:]
    return None, tokens


def _extraer_talla(tokens: list[str]) -> tuple[Optional[str], list[str]]:
    """R4: Detecta y extrae talla, retorna (talla, tokens_restantes)."""
    TALLAS_DICT_NORM = {lower_sin_acento(k): v for k, v in DICT_TALLAS.items()}
    for i, tok in enumerate(tokens):
        k = lower_sin_acento(tok.strip(',.'))
        if k in TALLAS_DICT_NORM:
            return TALLAS_DICT_NORM[k], tokens[:i] + tokens[i+1:]
    return None, tokens


def _extraer_marca(tokens: list[str]) -> tuple[Optional[str], list[str]]:
    """R5: Detecta marca por coincidencia."""
    MARCAS_NORM = {lower_sin_acento(k): v for k, v in DICT_MARCAS.items()}
    # Bigramas primero (ej. "dolce gabbana", "calvin klein")
    for i in range(len(tokens) - 1):
        k = lower_sin_acento(f"{tokens[i]} {tokens[i+1]}")
        if k in MARCAS_NORM:
            return MARCAS_NORM[k], tokens[:i] + tokens[i+2:]
    # Unigramas
    for i, tok in enumerate(tokens):
        k = lower_sin_acento(tok.strip(',.'))
        if k in MARCAS_NORM:
            return MARCAS_NORM[k], tokens[:i] + tokens[i+1:]
    return None, tokens


def _extraer_manga_cuello(tokens: list[str]) -> tuple[Optional[str], Optional[str], list[str]]:
    """
    R6/R7: Detecta manga y cuello del listado de tokens.
    Maneja casos especiales: 'resorte'→mao+estampado_resorte, 'mq'→corta+micro_cuadro
    Retorna (manga, cuello, tokens_restantes).
    """
    MANGA_NORM  = {lower_sin_acento(k): v for k, v in DICT_MANGA.items()}
    CUELLO_NORM = {lower_sin_acento(k): v for k, v in DICT_CUELLO.items()}

    manga = None
    cuello = None
    tokens_restantes = list(tokens)

    # Bigramas para manga
    for i in range(len(tokens_restantes) - 1):
        k = lower_sin_acento(f"{tokens_restantes[i]} {tokens_restantes[i+1]}")
        if k in MANGA_NORM:
            manga = MANGA_NORM[k]
            tokens_restantes = tokens_restantes[:i] + tokens_restantes[i+2:]
            break

    # Unigramas para manga (si no encontró bigrama)
    if manga is None:
        for i, tok in enumerate(tokens_restantes):
            k = lower_sin_acento(tok.strip(',.'))
            if k in MANGA_NORM:
                manga = MANGA_NORM[k]
                tokens_restantes = tokens_restantes[:i] + tokens_restantes[i+1:]
                break

    # Cuello
    for i, tok in enumerate(tokens_restantes):
        k = lower_sin_acento(tok.strip(',.'))
        if k in CUELLO_NORM:
            cuello = CUELLO_NORM[k]
            tokens_restantes = tokens_restantes[:i] + tokens_restantes[i+1:]
            break

    return manga, cuello, tokens_restantes


def _extraer_estampado(tokens: list[str]) -> tuple[Optional[str], list[str]]:
    """R8: Detecta estampado."""
    ESTAMPADO_NORM = {lower_sin_acento(k): v for k, v in DICT_ESTAMPADOS.items()}
    # Bigramas
    for i in range(len(tokens) - 1):
        k = lower_sin_acento(f"{tokens[i]} {tokens[i+1]}")
        if k in ESTAMPADO_NORM:
            return ESTAMPADO_NORM[k], tokens[:i] + tokens[i+2:]
    # Unigramas
    for i, tok in enumerate(tokens):
        k = lower_sin_acento(tok.strip(',.'))
        if k in ESTAMPADO_NORM:
            return ESTAMPADO_NORM[k], tokens[:i] + tokens[i+1:]
    return None, tokens


def _extraer_color(tokens: list[str]) -> tuple[Optional[str], list[str]]:
    """R9: Detecta color."""
    COLORES_NORM = {lower_sin_acento(k): v for k, v in DICT_COLORES.items()}
    # Bigramas primero (ej. "azul marino", "azul cielo", "palo de rosa" → trigrama)
    for i in range(len(tokens) - 2):
        k = lower_sin_acento(f"{tokens[i]} {tokens[i+1]} {tokens[i+2]}")
        if k in COLORES_NORM:
            return COLORES_NORM[k], tokens[:i] + tokens[i+3:]
    for i in range(len(tokens) - 1):
        k = lower_sin_acento(f"{tokens[i]} {tokens[i+1]}")
        if k in COLORES_NORM:
            return COLORES_NORM[k], tokens[:i] + tokens[i+2:]
    for i, tok in enumerate(tokens):
        k = lower_sin_acento(tok.strip(',.'))
        if k in COLORES_NORM:
            return COLORES_NORM[k], tokens[:i] + tokens[i+1:]
    return None, tokens


def _extraer_tela(tokens: list[str]) -> tuple[Optional[str], list[str]]:
    """R13: Detecta tipo de tela."""
    TELA_NORM = {lower_sin_acento(k): v for k, v in DICT_TELA.items()}
    for i in range(len(tokens) - 1):
        k = lower_sin_acento(f"{tokens[i]} {tokens[i+1]}")
        if k in TELA_NORM:
            return TELA_NORM[k], tokens[:i] + tokens[i+2:]
    for i, tok in enumerate(tokens):
        k = lower_sin_acento(tok.strip(',.'))
        if k in TELA_NORM:
            return TELA_NORM[k], tokens[:i] + tokens[i+1:]
    return None, tokens



# PARSER PRINCIPAL
 

def parsear_venta(linea: str, fecha=None) -> dict:
    """
    Extrae todos los atributos de una línea de venta.
    Aplica reglas R3-R14.

    Args:
        linea: Línea de texto cruda de venta
        fecha: Fecha de contexto (del bloque activo)

    Returns:
        dict con atributos + metadatos de confianza
    """
    t = limpiar_texto(linea)
    reglas_aplicadas = []
    observaciones = []

    #  R14: Casos especiales
    if re.match(r'^\s*1\s*\*?\s*$', t):
        return _registro_fallido(linea, fecha, "R14_linea_ambigua_solo_1")

    #  R3: Extraer cantidad 
    cantidad, resto = extraer_numero_inicial(t)
    if cantidad is not None:
        reglas_aplicadas.append("R3_cantidad_explicita")
    else:
        cantidad = 1
        resto = t
        reglas_aplicadas.append("R3_cantidad_implicita_1")

    # Tokenizar el resto
    # Limpiar separadores comunes pero no borrar palabras
    resto_limpio = re.sub(r'[,;]', ' ', resto)
    tokens = [tok for tok in resto_limpio.split() if tok]

    # R10: Extraer precio (del final)
    precio, tokens = _extraer_precio(tokens)
    if precio:
        reglas_aplicadas.append("R10_precio")

    # R13: Detectar tipo de tela 
    tipo_tela, tokens = _extraer_tela(tokens)
    if tipo_tela:
        reglas_aplicadas.append("R13_tela")

    # R4: Extraer talla 
    talla, tokens = _extraer_talla(tokens)
    if talla:
        reglas_aplicadas.append("R4_talla")

    #  R5: Extraer marca 
    marca, tokens = _extraer_marca(tokens)
    if marca:
        reglas_aplicadas.append("R5_marca")
    else:
        # Verificar marcas desconocidas
        for tok in tokens:
            if lower_sin_acento(tok) in {'kobbish', 'huawaina'}:
                marca = tok  # mantener como está → estado parcial
                observaciones.append(f"marca_desconocida: {tok}")
                reglas_aplicadas.append("R5_marca_desconocida")
                tokens.remove(tok)
                break

    #  R6/R7: Extraer manga y cuello 
    # Caso especial 'mq' = manga corta + micro cuadro
    manga = None
    cuello = None
    estampado = None

    if any(lower_sin_acento(t) == 'mq' for t in tokens):
        manga = 'corta'
        estampado = 'micro cuadro'
        tokens = [t for t in tokens if lower_sin_acento(t) != 'mq']
        reglas_aplicadas.append("R7_R8_mq_especial")
    
    # Caso especial 'resorte'/'resort'  cuello=mao + estampado=resorte
    resorte_tokens = [t for t in tokens
                      if lower_sin_acento(t.strip(',.')) in {'resorte','resort','rezrte'}]
    if resorte_tokens:
        cuello = 'mao'
        estampado = 'resorte'
        tokens = [t for t in tokens
                  if lower_sin_acento(t.strip(',.')) not in {'resorte','resort','rezrte'}]
        reglas_aplicadas.append("R6_resorte_mao")

    if manga is None:
        manga, cuello_det, tokens = _extraer_manga_cuello(tokens)
        if cuello is None:
            cuello = cuello_det
        if manga:
            reglas_aplicadas.append("R7_manga")
        if cuello:
            reglas_aplicadas.append("R6_cuello")

    #  R8: Extraer estampado 
    if estampado is None:
        estampado, tokens = _extraer_estampado(tokens)
        if estampado:
            reglas_aplicadas.append("R8_estampado")

    # R9: Extraer color 
    color, tokens = _extraer_color(tokens)
    if color:
        reglas_aplicadas.append("R9_color")

    # R12: Aplicar defaults por marca
    if marca and marca in DEFAULTS_MARCA:
        defaults = DEFAULTS_MARCA[marca]
        if cuello is None:
            cuello = defaults.get('tipo_cuello')
            reglas_aplicadas.append("R12_cuello_default")
        if manga is None:
            manga = defaults.get('manga')
            reglas_aplicadas.append("R12_manga_default")
        if estampado is None:
            estampado = defaults.get('estampado')
            reglas_aplicadas.append("R12_estampado_default")
    else:
        # Defaults globales cuando no hay marca
        if manga is None:
            manga = 'larga'
            reglas_aplicadas.append("R7_manga_default_global")
        if estampado is None:
            estampado = 'lisa'
            reglas_aplicadas.append("R8_estampado_default_global")

    # ─ REGLAS EMPÍRICAS 
    # Regla 3: resorte -Tommy+mao+corta+resorte (si no hay marca ya detectada)
    resorte_en_linea = re.search(r"\b(resorte|resort|rezrte)\b", t, re.IGNORECASE)
    if resorte_en_linea:
        if marca is None:
            marca = "Tommy"
            reglas_aplicadas.append("RE3_resorte_implica_tommy")
        if cuello is None:
            cuello = "mao"
        if manga is None:
            manga = "corta"
        if estampado is None:
            estampado = "resorte"

    # Regla 1: mao + blanca - Calvin (si no hay marca)
    if marca is None and cuello == "mao" and color == "blanca":
        marca = "Calvin"
        reglas_aplicadas.append("RE1_mao_blanca_calvin")

    # Regla 2: levis 
    if re.search(r"\b(levis|leviz|mezclilla|pana)\b", t, re.IGNORECASE):
        observaciones.append("RE2_tela_especial_precio_ref_200")

    # Regla E: fuzzy matching de marca (si no se detectó ninguna)
    if marca is None:
        marca = _fuzzy_marca(tokens)
        if marca:
            reglas_aplicadas.append("RE_fuzzy_marca")

    
    tokens_sobrantes = [t for t in tokens if len(t) > 1 and not es_numero(t)]
    if tokens_sobrantes:
        observaciones.append(f"tokens_no_clasificados: {tokens_sobrantes}")

    # Calcular nivel de confianza 
    # cantidad siempre tiene valor (mínimo 1 por default R3)
    # fecha viene del contexto del bloque  si hay fecha_contexto, cuenta como presente
    registro = {
        "fecha":    fecha,        
        "cantidad": cantidad,     
        "marca":    marca,
        "color":    color,
        "talla":    talla,
    }
    nivel_conf = calcular_nivel_confianza(registro, ATRIBUTOS_REQUERIDOS)

    #  Determinar estado 
    if nivel_conf == 100:
        estado = "completa"
    elif nivel_conf >= 20:   
        estado = "parcial"
    else:
        estado = "fallida"

    logger.debug(
        f"PARSED | conf={nivel_conf:.0f}% | estado={estado} | "
        f"marca={marca} talla={talla} color={color} | '{linea[:60]}'"
    )

    return {
        "linea_original":   linea,
        "fecha":            fecha,
        "cantidad":         cantidad,
        "marca":            marca,
        "tipo_cuello":      cuello,
        "manga":            manga,
        "estampado":        estampado,
        "color":            color,
        "talla":            talla,
        "precio":           precio,
        "tipo_tela":        tipo_tela,
        "nivel_confianza":  nivel_conf,
        "estado":           estado,
        "reglas_aplicadas": ",".join(reglas_aplicadas),
        "observaciones":    "; ".join(observaciones) if observaciones else None,
    }


def _fuzzy_marca(tokens: list) -> str | None:
    """
    Regla E: fuzzy matching de marca para tokens con typos.
    Ej: machini→Manchini, bilberry→Burberry, tommys→Tommy
    Umbral de similitud: 0.75
    """
    from utils.text_utils import DICT_MARCAS
    candidatos = list(set(DICT_MARCAS.keys()))
    for tok in tokens:
        t = tok.lower().strip(",. ")
        if len(t) < 3:
            continue
        matches = get_close_matches(t, candidatos, n=1, cutoff=0.75)
        if matches:
            return DICT_MARCAS[matches[0]]
    return None


def _registro_fallido(linea: str, fecha, motivo: str) -> dict:
    """Retorna un registro fallido con nivel_confianza=0."""
    return {
        "linea_original":   linea,
        "fecha":            fecha,
        "cantidad":         None,
        "marca":            None,
        "tipo_cuello":      None,
        "manga":            None,
        "estampado":        None,
        "color":            None,
        "talla":            None,
        "precio":           None,
        "tipo_tela":        None,
        "nivel_confianza":  0.0,
        "estado":           "fallida",
        "reglas_aplicadas": motivo,
        "observaciones":    f"línea_ambigua: {linea}",
    }
