"""
parsers/desdoblador.py — Desdobla líneas compuestas en registros individuales.

Patrones (orden de prioridad):
  P1  SLASH TALLA        "G/M"                     → mismo SKU, 2 tallas
  P2  CANT+MARCA         "3 dolce y 2 Zara"         → N registros por marca
  P3  CASO B             "2M 1G 1CH"                → uno por cant+talla
  P4  COUNT=COLORES      "4 ch vino, blanca, negra, azul marino Zara"
                         → count inicial == nº colores → split por color
  P5  COUNT=MARCAS       "3 xg Zara, Tommy, Calvin"
                         → count inicial == nº marcas → split por marca
  P6  SEGMENTOS MIXTOS   "4, 2 Zara palo de rosa vino, corta blanca m, mao azul cielo"
                         → el número inicial es total, luego segmentos separados
  P_MULTI_MARCA         "2 g azul cielo Zara y micro cuadro polo azul marino"
                         → split por marca, cada segmento parsea solo, comparte talla
  P_RESORTE SPLIT       "4 ch resorte, vino, blanca, negra y azul marino Zara manga corta"
                         → resorte + colores → N Tommy/mao/resorte, luego marca explícita → su registro
  P7  SEP GENÉRICO       "y" y "," con herencia de contexto
  RN  ASTERISCO          "1 m dolce *" → blanca/larga/con cuello/textura cuadrada
"""

import re
import logging
from typing import Optional
from utils.text_utils import (
    DICT_TALLAS, DICT_COLORES, DICT_MARCAS, DICT_MANGA, DEFAULTS_MARCA,
    lower_sin_acento, limpiar_texto, extraer_numero_inicial
)
from parsers.venta_parser import parsear_venta

logger = logging.getLogger("etl_camisas.desdoblador")

#  Lookups
TALLAS_NORM  = {lower_sin_acento(k): v for k, v in DICT_TALLAS.items()}
COLORES_NORM = {lower_sin_acento(k): v for k, v in DICT_COLORES.items()}
MARCAS_NORM  = {lower_sin_acento(k): v for k, v in DICT_MARCAS.items()}

COLORES_COMPUESTOS = sorted([
    'azul marino', 'azul cielo', 'azul celeste', 'azul rey', 'azul primario',
    'palo de rosa', 'verde fuerte', 'verde militar', 'verde agua', 'rosa chillon',
], key=len, reverse=True)

# Palabras que NO son colores aunque sean tokens flotantes
_NO_COLOR = {
    'resorte', 'resort', 'rezrte', 'manga', 'corta', 'larga', 'mao',
    'micro', 'rayas', 'lisa', 'cuadros', 'flores', 'puntos', 'textura',
    'transferencia', 'cambio', 'efectivo', 'pana', 'franela',
}

#Regla de negocio: asterisco
ASTERISCO_OVERRIDES = {
    "color":       "blanca",
    "manga":       "larga",
    "tipo_cuello": "con cuello",
    "estampado":   "textura cuadrada",
}

#  Regex 
_RE_CANT_TALLA = re.compile(r'(\d+)\s*(xg|xl|x\s*g|ch|[mg])\b', re.IGNORECASE)

_RE_SLASH_TALLA = re.compile(
    r'\b(xg|xl|x\s*g|ch|[mg])\s*/\s*(xg|xl|x\s*g|ch|[mg])'
    r'(?:\s*/\s*(xg|xl|x\s*g|ch|[mg]))?(?:\s*/\s*(xg|xl|x\s*g|ch|[mg]))?',
    re.IGNORECASE
)

_RE_CANT_MARCA = re.compile(
    r'(\d+)\s+(' + '|'.join(re.escape(k) for k in sorted(DICT_MARCAS.keys(), key=len, reverse=True)) + r')\b',
    re.IGNORECASE
)

_RE_ASTERISCO   = re.compile(r'\s*\*{1,4}\s*$')
_RE_PRECIO      = re.compile(r'\$\d+|\b\d{3,4}\b')
_RE_RUIDO_FINAL = re.compile(r'\b(transferencia|cambio|efectivo|contabilidad)\b', re.IGNORECASE)


# Helpers 

def _marcas_en(texto: str) -> list:
    t = lower_sin_acento(texto)
    palabras = t.split()
    encontradas = []
    for i in range(len(palabras)-1):
        k = f"{palabras[i]} {palabras[i+1]}"
        if k in MARCAS_NORM and MARCAS_NORM[k] not in encontradas:
            encontradas.append(MARCAS_NORM[k])
    for p in palabras:
        pk = p.strip(',.* ')
        if pk in MARCAS_NORM and MARCAS_NORM[pk] not in encontradas:
            encontradas.append(MARCAS_NORM[pk])
    return encontradas


def _colores_en(texto: str) -> list:
    """Extrae colores del texto, ignorando tokens que no son colores."""
    t = lower_sin_acento(texto)
    encontrados = []
    # Compuestos primero
    for col in COLORES_COMPUESTOS:
        if lower_sin_acento(col) in t and col not in encontrados:
            encontrados.append(col)
            t = t.replace(lower_sin_acento(col), '__')
    # Simples — excluir tokens que son ruido/estampado/otros
    for p in t.split():
        pk = p.strip(',.* ')
        if pk in _NO_COLOR:
            continue
        if pk in COLORES_NORM and COLORES_NORM[pk] not in encontrados:
            encontrados.append(COLORES_NORM[pk])
    return encontrados


def _limpiar_ruido_linea(linea: str) -> str:
    """Quita precios y palabras de ruido al final (transferencia, cambio, etc.)."""
    t = _RE_RUIDO_FINAL.sub('', linea)
    t = _RE_PRECIO.sub('', t)
    return re.sub(r'\s{2,}', ' ', t).strip(' ,')


def necesita_desdoblamiento(linea: str) -> bool:
    t = lower_sin_acento(limpiar_texto(linea))
    if _RE_SLASH_TALLA.search(t):                         return True
    if len(_RE_CANT_MARCA.findall(t)) >= 2:               return True
    if len(_RE_CANT_TALLA.findall(t)) >= 2:               return True
    if re.search(r'\s+y\s+', t):                          return True
    # Coma con múltiples colores
    if len(_colores_en(t)) >= 2 and ',' in t:             return True
    # Múltiples marcas distintas con y/coma
    if len(_marcas_en(t)) >= 2:
        return True
    # Línea con resorte + marca explícita adicional
    if re.search(r'\b(resorte|resort|rezrte)\b', t) and len(_marcas_en(t)) >= 1:
        return True
    return False


# Orquestador 

def desdoblar(linea: str, fecha=None) -> list[dict]:
    t = limpiar_texto(linea)

    # Extraer y limpiar asterisco
    tiene_asterisco = bool(_RE_ASTERISCO.search(t))
    t_limpia = _RE_ASTERISCO.sub('', t).strip()
    # Limpiar ruido (transferencia, precios) antes de procesar
    t_proc = _limpiar_ruido_linea(t_limpia)

    if not necesita_desdoblamiento(t_proc):
        reg = parsear_venta(t_proc, fecha)
        if tiene_asterisco: reg = _aplicar_asterisco(reg)
        reg["linea_original"] = linea
        return [reg]

    logger.info(f"Desdoblando: '{t_proc[:80]}'")

    # P1: slash talla
    r = _slash_talla(t_proc, fecha, tiene_asterisco)
    if r: logger.info(f"P1-SlashTalla → {len(r)}"); return _orig(r, linea)

    # P2: cant+marca explícito  "3 dolce y 2 Zara"
    r = _cant_marca(t_proc, fecha, tiene_asterisco)
    if r: logger.info(f"P2-CantMarca → {len(r)}"); return _orig(r, linea)

    # P3: múltiples cant+talla  "2M 1G 1CH"
    if len(_RE_CANT_TALLA.findall(lower_sin_acento(t_proc))) >= 2:
        r = _caso_b(t_proc, fecha, tiene_asterisco)
        if r: logger.info(f"P3-CasoB → {len(r)}"); return _orig(r, linea)

    # P_MULTI_MARCA: múltiples marcas distintas en segmentos separados por y/,
    r = _multi_marca(t_proc, fecha, tiene_asterisco)
    if r: logger.info(f"P_MULTI_MARCA → {len(r)}"); return _orig(r, linea)

    # P_RESORTE: resorte + colores → Tommy/mao/resorte; otras marcas explícitas → su registro
    r = _resorte_split(t_proc, fecha, tiene_asterisco)
    if r: logger.info(f"P_RESORTE → {len(r)}"); return _orig(r, linea)

    # P4: count == nº colores detectados
    r = _count_igual_colores(t_proc, fecha, tiene_asterisco)
    if r: logger.info(f"P4-CountColores → {len(r)}"); return _orig(r, linea)

    # P5: count == nº marcas detectadas
    r = _count_igual_marcas(t_proc, fecha, tiene_asterisco)
    if r: logger.info(f"P5-CountMarcas → {len(r)}"); return _orig(r, linea)

    # P6: segmentos mixtos  "4, 2 Zara ..., ..., y ..."
    r = _segmentos_mixtos(t_proc, fecha, tiene_asterisco)
    if r: logger.info(f"P6-SegMixtos → {len(r)}"); return _orig(r, linea)

    # P7: split genérico y/,
    r = _separadores(t_proc, fecha, tiene_asterisco)
    if r: logger.info(f"P7-SepYComa → {len(r)}"); return _orig(r, linea)

    # Fallback
    logger.warning(f"Fallback simple: '{t_proc[:60]}'")
    reg = parsear_venta(t_proc, fecha)
    if tiene_asterisco: reg = _aplicar_asterisco(reg)
    reg["linea_original"] = linea
    return [reg]


# P1: Slash talla 

def _slash_talla(linea: str, fecha, tiene_asterisco: bool) -> Optional[list]:
    m = _RE_SLASH_TALLA.search(lower_sin_acento(linea))
    if not m:
        return None
    tallas = [TALLAS_NORM.get(lower_sin_acento(g.replace(' ',''))) for g in m.groups() if g]
    tallas = [t for t in tallas if t]
    if len(tallas) < 2:
        return None

    linea_base = re.sub(
        r'\b(xg|xl|x\s*g|ch|[mg])\s*/\s*(xg|xl|x\s*g|ch|[mg])'
        r'(?:\s*/\s*(xg|xl|x\s*g|ch|[mg]))?(?:\s*/\s*(xg|xl|x\s*g|ch|[mg]))?',
        tallas[0], linea, flags=re.IGNORECASE, count=1
    )
    base = parsear_venta(linea_base, fecha)
    cant_total, _ = extraer_numero_inicial(linea)
    cant = max(1, (cant_total or len(tallas)) // len(tallas))

    regs = []
    for talla in tallas:
        reg = dict(base)
        reg["talla"] = talla
        reg["cantidad"] = cant
        reg["reglas_aplicadas"] = (base.get("reglas_aplicadas") or "") + ",P1_slash_talla"
        if tiene_asterisco: reg = _aplicar_asterisco(reg)
        regs.append(reg)
    return regs


# P2: Cant+Marca explícito

def _cant_marca(linea: str, fecha, tiene_asterisco: bool) -> Optional[list]:
    t_lower = lower_sin_acento(linea)
    matches = list(_RE_CANT_MARCA.finditer(t_lower))
    if len(matches) < 2:
        return None

    regs = []
    for i, m in enumerate(matches):
        cant = int(m.group(1))
        marca_norm = MARCAS_NORM.get(lower_sin_acento(m.group(2)))
        if not marca_norm: continue

        inicio = m.start()
        fin = matches[i+1].start() if i+1 < len(matches) else len(linea)
        segmento = linea[inicio:fin].strip(' ,y')

        reg = parsear_venta(segmento, fecha)
        reg["marca"] = marca_norm
        reg["cantidad"] = 1

        if marca_norm in DEFAULTS_MARCA:
            d = DEFAULTS_MARCA[marca_norm]
            if not reg.get("tipo_cuello"): reg["tipo_cuello"] = d.get("tipo_cuello")
            if not reg.get("manga"):       reg["manga"]       = d.get("manga")
            if not reg.get("estampado"):   reg["estampado"]   = d.get("estampado")

        reg["reglas_aplicadas"] = (reg.get("reglas_aplicadas") or "") + ",P2_cant_marca"
        if tiene_asterisco: reg = _aplicar_asterisco(reg)

        for _ in range(cant):
            regs.append(dict(reg))

    return regs if len(regs) >= 2 else None


#  P3: Caso B (múltiples cant+talla) 

def _caso_b(linea: str, fecha, tiene_asterisco: bool) -> Optional[list]:
    base = parsear_venta(linea, fecha)
    regs = []
    for m in _RE_CANT_TALLA.finditer(lower_sin_acento(linea)):
        talla = TALLAS_NORM.get(lower_sin_acento(m.group(2).replace(' ','')))
        if talla:
            reg = dict(base)
            reg["cantidad"] = int(m.group(1))
            reg["talla"] = talla
            reg["reglas_aplicadas"] = (base.get("reglas_aplicadas") or "") + ",P3_caso_B"
            if tiene_asterisco: reg = _aplicar_asterisco(reg)
            regs.append(reg)
    return regs if len(regs) >= 2 else None


#  P_MULTI_MARCA: múltiples marcas distintas, cada una con sus propios atributos 

def _multi_marca(linea: str, fecha, tiene_asterisco: bool) -> Optional[list]:
    """
    Patrón: "2 g azul cielo Zara y micro cuadro polo azul marino"
            "2 g Tommy negra y Zara azul marino"
            "3 m Zara blanca y Tommy vino y Calvin negra"

    Reglas de negocio confirmadas:
    - La TALLA es compartida si hay una sola talla en la línea (aparece antes de las marcas)
    - La MANGA es compartida si aparece antes de la primera marca
    - Cada marca parsea SOLO su propio segmento (sin herencia cruzada de color/estampado)
    - Se aplican los defaults de cada marca de forma independiente
    - Si count inicial == nº marcas → cantidad = 1 por registro
    """
    marcas = _marcas_en(linea)
    if len(marcas) < 2:
        return None

    # Dividir la línea en partes por separadores (y / ,)
    partes_raw = _split_inteligente(linea)
    if len(partes_raw) < 2:
        return None

    # Verificar que cada parte tenga su propia marca o pueda asignarse a una
    # Mapear cada parte a su marca dominante
    segmentos = _mapear_partes_a_marcas(partes_raw, marcas)
    if not segmentos or len(segmentos) < 2:
        return None

    # Extraer atributos COMPARTIDOS (talla y manga que aparecen antes de cualquier marca)
    talla_global = _talla_antes_de_marcas(linea, marcas)
    manga_global = _manga_antes_de_marcas(linea, marcas)
    cant_total, _ = extraer_numero_inicial(linea)

    regs = []
    for marca, segmento in segmentos:
        # Parsear el segmento de forma aislada
        reg = parsear_venta(segmento, fecha)

        # Asignar marca (puede que el parser ya la detectó, pero reforzar)
        reg["marca"] = marca

        # Talla compartida: si no detectó talla propia, usar la global
        if not reg.get("talla") and talla_global:
            reg["talla"] = talla_global

        # Manga compartida (si aparece antes de las marcas, es de todas)
        if not reg.get("manga") and manga_global:
            reg["manga"] = manga_global

        # Aplicar defaults de ESTA marca (no del base global)
        if marca in DEFAULTS_MARCA:
            d = DEFAULTS_MARCA[marca]
            if not reg.get("tipo_cuello"): reg["tipo_cuello"] = d.get("tipo_cuello")
            if not reg.get("manga"):       reg["manga"]       = d.get("manga")
            if not reg.get("estampado"):   reg["estampado"]   = d.get("estampado")

        reg["cantidad"] = 1
        reg["fecha"] = fecha
        reg["reglas_aplicadas"] = (reg.get("reglas_aplicadas") or "") + ",P_MULTI_MARCA"

        if tiene_asterisco:
            reg = _aplicar_asterisco(reg)

        regs.append(reg)

    # Ajustar cantidades si count total es explícito
    if cant_total and cant_total == len(regs):
        for r in regs:
            r["cantidad"] = 1  # ya está bien

    return regs if len(regs) >= 2 else None


def _mapear_partes_a_marcas(partes: list, marcas: list) -> list:
    """
    Asocia cada parte de texto con la marca que contiene.
    Si una parte no tiene marca propia, la asocia con la marca del contexto más cercano.
    Retorna lista de (marca, texto_segmento).
    """
    resultado = []
    marcas_pendientes = list(marcas)

    for parte in partes:
        marcas_en_parte = _marcas_en(parte)
        if marcas_en_parte:
            marca_parte = marcas_en_parte[0]
            resultado.append((marca_parte, parte))
            if marca_parte in marcas_pendientes:
                marcas_pendientes.remove(marca_parte)
        elif resultado:
            # No tiene marca propia: podría ser atributos adicionales de la última marca
            # (caso raro) — los ignoramos para no contaminar
            pass
        # Si la primera parte no tiene marca, la ignoramos para este patrón
        # (los atributos sin marca son talla/manga globales, ya se capturan por separado)

    # Si quedaron marcas sin parte, no podemos hacer el split limpio
    # Solo retornar si tenemos al menos 2 segmentos con marca identificada
    return resultado if len(resultado) >= 2 else None


def _talla_antes_de_marcas(linea: str, marcas: list) -> str:
    """
    Extrae la talla que aparece ANTES de la primera marca en la línea.
    Esta talla es compartida por todos los registros.
    """
    import re as _re
    from utils.text_utils import DICT_MARCAS as _DM
    MARCAS_NORM_L = {lower_sin_acento(k): v for k, v in _DM.items()}

    # Encontrar posición de la primera marca
    t = lower_sin_acento(linea)
    pos_primera_marca = len(linea)
    for clave in sorted(MARCAS_NORM_L.keys(), key=len, reverse=True):
        pat = _re.compile(r"\b" + _re.escape(clave) + r"\b", _re.IGNORECASE)
        m = pat.search(t)
        if m and m.start() < pos_primera_marca:
            pos_primera_marca = m.start()

    # Buscar talla en el texto antes de la primera marca
    texto_antes = linea[:pos_primera_marca]
    for p in texto_antes.split():
        pk = lower_sin_acento(p.strip(",.*"))
        if pk in TALLAS_NORM:
            return TALLAS_NORM[pk]
    return None


def _manga_antes_de_marcas(linea: str, marcas: list) -> str:
    """
    Extrae la manga que aparece ANTES de la primera marca (manga compartida).
    """
    import re as _re
    from utils.text_utils import DICT_MARCAS as _DM, DICT_MANGA as _DMA
    MARCAS_NORM_L = {lower_sin_acento(k): v for k, v in _DM.items()}
    MANGA_NORM_L  = {lower_sin_acento(k): v for k, v in _DMA.items()}

    t = lower_sin_acento(linea)
    pos_primera_marca = len(linea)
    for clave in sorted(MARCAS_NORM_L.keys(), key=len, reverse=True):
        pat = _re.compile(r"\b" + _re.escape(clave) + r"\b", _re.IGNORECASE)
        m = pat.search(t)
        if m and m.start() < pos_primera_marca:
            pos_primera_marca = m.start()

    texto_antes = lower_sin_acento(linea[:pos_primera_marca])
    palabras = texto_antes.split()
    # Bigramas primero
    for i in range(len(palabras)-1):
        k = f"{palabras[i]} {palabras[i+1]}"
        if k in MANGA_NORM_L:
            return MANGA_NORM_L[k]
    for p in palabras:
        if p in MANGA_NORM_L:
            return MANGA_NORM_L[p]
    return None


# ─── P_RESORTE: resorte + colores  Tommy; marca explícita su propio registro ────

def _resorte_split(linea: str, fecha, tiene_asterisco: bool) -> Optional[list]:
    """
    Patrón de negocio confirmado:
      "4 ch resorte, vino, blanca, negra y azul marino Zara manga corta"
      → 3 Tommy CH mao resorte (vino, blanca, negra)
      + 1 Zara CH manga corta (azul marino)

    Regla:
      - "resorte" marca el inicio del bloque Tommy/mao/resorte
      - Los colores que siguen (hasta encontrar otra marca) son para Tommy
      - Si aparece otra marca explícita con su propio color, genera su propio registro
    """
    t_lower = lower_sin_acento(linea)

    # Solo activar si hay resorte/resort en la línea
    if not re.search(r"\b(resorte|resort|rezrte)\b", t_lower):
        return None

    # Buscar si hay otra marca explícita después de los colores
    # Patrón: "[colores,...] [MARCA] [atributos]"
    # Dividir la línea en: bloque_tommy | bloque_otra_marca
    # La otra marca aparece acompañada de color o atributos propios

    marcas_en_linea = _marcas_en(linea)
    # Quitar Tommy si está implícito (resorte lo implica)
    otras_marcas = [m for m in marcas_en_linea if m != "Tommy"]

    # Extraer atributos compartidos (talla, manga, etc.) del contexto global
    base_global = parsear_venta(linea, fecha)
    talla_global = base_global.get("talla")
    manga_global = base_global.get("manga")

    regs_tommy = []
    regs_otras  = []

    if otras_marcas:
        primera_otra = otras_marcas[0]

        # Construir regex para encontrar la marca en el texto
        pat_marca = re.compile(
            r"\b(" + "|".join(re.escape(k) for k, v in MARCAS_NORM.items() if v == primera_otra) + r")\b",
            re.IGNORECASE
        )
        m_pos = pat_marca.search(linea)
        if not m_pos:
            return None

        pos_marca = m_pos.start()

        # Buscar el color que aparece JUSTO ANTES de la marca (es el color de esa marca)
        # Escaneamos tokens en reversa desde la posición de la marca
        texto_antes_marca = linea[:pos_marca].strip()
        color_de_otra_marca = None
        pos_inicio_bloque_otra = pos_marca  # por defecto el bloque empieza en la marca

        # Intentar detectar colores compuestos y simples que preceden a la marca
        for col in COLORES_COMPUESTOS:
            patron_col = re.compile(re.escape(col) + r"\s*$", re.IGNORECASE)
            m_col = patron_col.search(texto_antes_marca)
            if m_col:
                color_de_otra_marca = col
                pos_inicio_bloque_otra = m_col.start()
                break

        if color_de_otra_marca is None:
            # Intentar color simple (última palabra antes de la marca)
            palabras_antes = texto_antes_marca.split()
            for pw in reversed(palabras_antes):
                pk = lower_sin_acento(pw.strip(",.* "))
                if pk in COLORES_NORM and pk not in _NO_COLOR:
                    color_de_otra_marca = COLORES_NORM[pk]
                    # Calcular posición de inicio
                    idx = texto_antes_marca.rfind(pw)
                    pos_inicio_bloque_otra = idx
                    break

        # Bloque Tommy: todo antes del color-de-otra-marca
        bloque_tommy = linea[:pos_inicio_bloque_otra].strip(" ,y")
        # Bloque otra marca: color + marca + resto
        bloque_otra  = linea[pos_inicio_bloque_otra:].strip(" ,")

        # Colores del bloque Tommy (excluye resorte y otros no-colores)
        colores_tommy = _colores_en(bloque_tommy)

        # Generar un registro Tommy por cada color
        for color in colores_tommy:
            reg = {
                "linea_original": linea,
                "fecha":          fecha,
                "cantidad":       1,
                "marca":          "Tommy",
                "tipo_cuello":    "mao",
                "manga":          "corta",
                "estampado":      "resorte",
                "color":          color,
                "talla":          talla_global,
                "precio":         base_global.get("precio"),
                "tipo_tela":      None,
                "nivel_confianza": 100.0,
                "estado":         "completa",
                "reglas_aplicadas": "RE3_resorte,P_RESORTE_tommy",
                "observaciones":  None,
            }
            if tiene_asterisco: reg = _aplicar_asterisco(reg)
            regs_tommy.append(reg)

        # Generar registro(s) de la otra marca
        reg_otra = parsear_venta(bloque_otra, fecha)
        reg_otra["marca"] = primera_otra
        if color_de_otra_marca and not reg_otra.get("color"):
            reg_otra["color"] = color_de_otra_marca
        if not reg_otra.get("talla") and talla_global:
            reg_otra["talla"] = talla_global
        if primera_otra in DEFAULTS_MARCA:
            d = DEFAULTS_MARCA[primera_otra]
            if not reg_otra.get("tipo_cuello"): reg_otra["tipo_cuello"] = d.get("tipo_cuello")
            if not reg_otra.get("estampado"):   reg_otra["estampado"]   = d.get("estampado")
        reg_otra["reglas_aplicadas"] = (reg_otra.get("reglas_aplicadas") or "") + ",P_RESORTE_otra_marca"
        if tiene_asterisco: reg_otra = _aplicar_asterisco(reg_otra)
        regs_otras.append(reg_otra)
    else:
        # Solo Tommy/resorte con múltiples colores, sin otra marca
        colores_tommy = _colores_en(linea)
        for color in colores_tommy:
            reg = {
                "linea_original": linea,
                "fecha":          fecha,
                "cantidad":       1,
                "marca":          "Tommy",
                "tipo_cuello":    "mao",
                "manga":          "corta",
                "estampado":      "resorte",
                "color":          color,
                "talla":          talla_global,
                "precio":         base_global.get("precio"),
                "tipo_tela":      None,
                "nivel_confianza": 100.0,
                "estado":         "completa",
                "reglas_aplicadas": "RE3_resorte,P_RESORTE_solo_tommy",
                "observaciones":  None,
            }
            if tiene_asterisco: reg = _aplicar_asterisco(reg)
            regs_tommy.append(reg)

    todos = regs_tommy + regs_otras
    return todos if len(todos) >= 2 else None


# P4: Count == nº colores 

def _count_igual_colores(linea: str, fecha, tiene_asterisco: bool) -> Optional[list]:
    """
    Ej: "4 ch vino, blanca, negra, azul marino Zara manga corta"
    count=4, colores detectados=4 → 4 registros, 1 por color.
    El 'resorte' y otros no-colores se ignoran en el conteo.
    """
    cant, _ = extraer_numero_inicial(linea)
    if not cant or cant < 2:
        return None

    colores = _colores_en(linea)
    if len(colores) != cant:
        return None

    # Construir base sin los colores para heredar el resto de atributos
    base = parsear_venta(linea, fecha)

    regs = []
    for color in colores:
        reg = dict(base)
        reg["color"] = color
        reg["cantidad"] = 1
        reg["reglas_aplicadas"] = (base.get("reglas_aplicadas") or "") + ",P4_count_colores"
        if tiene_asterisco: reg = _aplicar_asterisco(reg)
        regs.append(reg)

    logger.debug(f"P4: cant={cant} colores={colores}")
    return regs


# P5: Count == n marcas 

def _count_igual_marcas(linea: str, fecha, tiene_asterisco: bool) -> Optional[list]:
    """
    Ej: "3 xg Zara, Tommy, Calvin" → 3 registros, 1 por marca.
    """
    cant, _ = extraer_numero_inicial(linea)
    if not cant or cant < 2:
        return None

    marcas = _marcas_en(linea)
    if len(marcas) != cant:
        return None

    base = parsear_venta(linea, fecha)
    regs = []
    for marca in marcas:
        reg = dict(base)
        reg["marca"] = marca
        reg["cantidad"] = 1
        if marca in DEFAULTS_MARCA:
            d = DEFAULTS_MARCA[marca]
            if not reg.get("tipo_cuello"): reg["tipo_cuello"] = d.get("tipo_cuello")
            if not reg.get("manga"):       reg["manga"]       = d.get("manga")
            if not reg.get("estampado"):   reg["estampado"]   = d.get("estampado")
        reg["reglas_aplicadas"] = (base.get("reglas_aplicadas") or "") + ",P5_count_marcas"
        if tiene_asterisco: reg = _aplicar_asterisco(reg)
        regs.append(reg)
    return regs


# P6: Segmentos mixtos 

def _segmentos_mixtos(linea: str, fecha, tiene_asterisco: bool) -> Optional[list]:
    """
    Maneja: "4, 2 Zara palo de rosa vino, manga corta blanca m, y mao azul cielo"
    El número inicial es el total declarado; lo que sigue son segmentos separados
    por comas y 'y', cada uno pudiendo tener su propio sub-count/marca/color.
    """
    # Extraer número inicial incluso cuando va seguido de coma: "4, ..." o "4 ..."
    m = re.match(r'^(\d+)[,\s]\s*(.*)', linea.strip(), re.DOTALL)
    if not m:
        return None
    cant_total = int(m.group(1))
    resto = m.group(2).strip()
    if cant_total < 2:
        return None

    partes = _split_inteligente(resto)
    if len(partes) < 2:
        return None

    base = parsear_venta(linea, fecha)  # contexto global
    regs = []

    for parte in partes:
        parte = parte.strip(' ,')
        if not parte or len(parte) < 2:
            continue

        # ¿Tiene sub-count al inicio? "2 Zara palo de rosa vino"
        sub_cant, sub_resto = extraer_numero_inicial(parte)

        if sub_cant and sub_cant >= 2:
            # Sub-segmento con múltiples unidades → desdoblar por colores o parsear N veces
            sub_colores = _colores_en(sub_resto)
            if len(sub_colores) == sub_cant:
                # "2 Zara palo de rosa vino" → Zara/palo de rosa + Zara/vino
                sub_base = parsear_venta(parte, fecha)
                for col in sub_colores:
                    reg = dict(sub_base)
                    reg["color"] = col
                    reg["cantidad"] = 1
                    _heredar(reg, base)
                    reg["reglas_aplicadas"] = (reg.get("reglas_aplicadas") or "") + ",P6_sub_colores"
                    if tiene_asterisco: reg = _aplicar_asterisco(reg)
                    regs.append(reg)
            else:
                # Parsear el segmento N veces con cantidad 1
                sub_reg = parsear_venta(sub_resto, fecha)
                _heredar(sub_reg, base)
                sub_reg["cantidad"] = 1
                sub_reg["reglas_aplicadas"] = (sub_reg.get("reglas_aplicadas") or "") + ",P6_sub_mult"
                if tiene_asterisco: sub_reg = _aplicar_asterisco(sub_reg)
                for _ in range(sub_cant):
                    regs.append(dict(sub_reg))
        else:
            # Segmento simple: "manga corta blanca m", "mao azul cielo"
            reg = parsear_venta(parte, fecha)
            _heredar(reg, base)
            reg["cantidad"] = 1
            reg["reglas_aplicadas"] = (reg.get("reglas_aplicadas") or "") + ",P6_seg_simple"
            if tiene_asterisco: reg = _aplicar_asterisco(reg)
            regs.append(reg)

    # Validar que el total de registros coincida con cant_total
    total_calc = sum(r.get("cantidad", 1) for r in regs)
    if total_calc != cant_total and len(regs) > 0:
        logger.debug(f"P6: cant_total={cant_total} pero generados={total_calc} ({len(regs)} regs)")

    return regs if len(regs) >= 2 else None


#  P7: Split genérico y/coma 

def _separadores(linea: str, fecha, tiene_asterisco: bool) -> Optional[list]:
    cant_inicial, _ = extraer_numero_inicial(linea)
    partes = _split_inteligente(linea)
    if len(partes) <= 1:
        return None

    base = parsear_venta(linea, fecha)
    regs = []
    for parte in partes:
        parte = parte.strip(' ,')
        if not parte or len(parte) < 2:
            continue
        sub = parsear_venta(parte, fecha)
        _heredar(sub, base)
        sub["cantidad"] = 1
        sub["fecha"] = fecha
        sub["reglas_aplicadas"] = (sub.get("reglas_aplicadas") or "") + ",P7_sep"
        if tiene_asterisco: sub = _aplicar_asterisco(sub)
        regs.append(sub)

    if len(regs) < 2:
        return None
    if cant_inicial and cant_inicial > 1:
        por = max(1, cant_inicial // len(regs))
        for r in regs:
            r["cantidad"] = por
    return regs


# Helpers 

def _heredar(reg: dict, base: dict):
    """Hereda atributos del base si el registro no los tiene."""
    for attr in ['marca', 'tipo_cuello', 'manga', 'estampado', 'talla', 'precio', 'tipo_tela']:
        if reg.get(attr) is None and base.get(attr) is not None:
            reg[attr] = base[attr]


def _aplicar_asterisco(reg: dict) -> dict:
    """
    Regla de negocio *: siempre sobreescribe con atributos especiales del negocio.
    blanca / larga / con cuello / textura cuadrada
    """
    r = dict(reg)
    for campo, valor in ASTERISCO_OVERRIDES.items():
        r[campo] = valor
    obs = r.get("observaciones") or ""
    r["observaciones"] = (obs + "; asterisco_aplicado").lstrip("; ")
    return r


def _split_inteligente(texto: str) -> list:
    """Split por 'y' y ',' protegiendo colores compuestos."""
    marcadores = {}
    t = texto
    for i, col in enumerate(COLORES_COMPUESTOS):
        mk = f"__C{i}__"
        pat = re.compile(re.escape(col), re.IGNORECASE)
        if pat.search(t):
            t = pat.sub(mk, t)
            marcadores[mk] = col
    partes = re.split(r'\s+y\s+|\s*,\s*', t, flags=re.IGNORECASE)
    resultado = []
    for p in partes:
        for mk, col in marcadores.items():
            p = p.replace(mk, col)
        resultado.append(p.strip())
    return [p for p in resultado if p]


def _orig(regs: list, linea: str) -> list:
    for r in regs:
        r["linea_original"] = linea
    return regs
