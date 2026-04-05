"""
normalizers/atributo_normalizer.py — Normaliza atributos de ventas_candidatas
contra los diccionarios del negocio. Aplica los diccionarios de utils/text_utils.py.
"""

import logging
from utils.text_utils import (
    DICT_MARCAS, DICT_TALLAS, DICT_COLORES, DICT_ESTAMPADOS,
    DICT_CUELLO, DICT_MANGA, DICT_TELA, DEFAULTS_MARCA,
    lower_sin_acento
)

logger = logging.getLogger("etl_camisas.atributo_normalizer")

# Valores válidos del catálogo (R6-R9)
MARCAS_VALIDAS    = {'Armani','Burberry','Calvin','Dolce','Levis',
                     'Manchini','Polo','Tommy','Versace','Zara'}
TALLAS_VALIDAS    = {'CH','M','G','XG'}
MANGAS_VALIDAS    = {'corta','larga'}
CUELLOS_VALIDOS   = {'botones','con cuello','mao'}
ESTAMPADOS_VALIDOS = {'cuadros','figuras','flores','lisa','micro cuadro',
                      'micro figuras','puntos','rayas delgadas','resorte',
                      'textura cuadrada'}
COLORES_VALIDOS   = {'azul celeste','azul cielo','azul marino','azul primario',
                     'azul rey','beige','blanca','gris','lila','melon','mostaza',
                     'negra','palo de rosa','puntos','roja','verde agua',
                     'verde fuerte','verde militar','vino'}


def _normalizar_campo(valor, diccionario: dict, valores_validos: set) -> tuple:
    """
    Normaliza un campo. Retorna (valor_normalizado, fue_modificado).
    """
    if valor is None:
        return None, False
    v = str(valor).strip()
    if v in valores_validos:
        return v, False   # ya válido
    # Buscar en diccionario
    k = lower_sin_acento(v)
    norm = None
    # Intentar matcheo directo
    for clave, val_norm in diccionario.items():
        if lower_sin_acento(clave) == k:
            norm = val_norm
            break
    if norm and norm in valores_validos:
        return norm, True
    return v, False   # devolver original aunque no sea válido


def normalizar_registro(registro: dict) -> dict:
    """
    Aplica normalización a todos los atributos de un registro.
    Retorna el registro con atributos normalizados y lista de correcciones.
    """
    r = dict(registro)
    correcciones = []

    # Marca
    marca_norm, mod = _normalizar_campo(r.get("marca"), DICT_MARCAS, MARCAS_VALIDAS)
    if mod:
        correcciones.append(f"marca:{r['marca']}→{marca_norm}")
    r["marca"] = marca_norm

    # Talla
    talla_norm, mod = _normalizar_campo(r.get("talla"), DICT_TALLAS, TALLAS_VALIDAS)
    if mod:
        correcciones.append(f"talla:{r['talla']}→{talla_norm}")
    r["talla"] = talla_norm

    # Manga
    manga_norm, mod = _normalizar_campo(r.get("manga"), DICT_MANGA, MANGAS_VALIDAS)
    if mod:
        correcciones.append(f"manga:{r['manga']}→{manga_norm}")
    r["manga"] = manga_norm

    # Cuello
    cuello_norm, mod = _normalizar_campo(r.get("tipo_cuello"), DICT_CUELLO, CUELLOS_VALIDOS)
    if mod:
        correcciones.append(f"cuello:{r['tipo_cuello']}→{cuello_norm}")
    r["tipo_cuello"] = cuello_norm

    # Estampado
    estampado_norm, mod = _normalizar_campo(r.get("estampado"), DICT_ESTAMPADOS, ESTAMPADOS_VALIDOS)
    if mod:
        correcciones.append(f"estampado:{r['estampado']}→{estampado_norm}")
    r["estampado"] = estampado_norm

    # Color
    color_norm, mod = _normalizar_campo(r.get("color"), DICT_COLORES, COLORES_VALIDOS)
    if mod:
        correcciones.append(f"color:{r['color']}→{color_norm}")
    r["color"] = color_norm

    if correcciones:
        logger.debug(f"Correcciones aplicadas: {correcciones}")
        reglas = r.get("reglas_aplicadas", "") or ""
        r["reglas_aplicadas"] = reglas + ",NORM:" + "|".join(correcciones)

    return r


def validar_registro(registro: dict) -> tuple[bool, list[str]]:
    """
    Valida que los atributos estén dentro de valores válidos del catálogo.
    Retorna (es_valido, lista_errores).
    """
    errores = []

    if registro.get("marca") and registro["marca"] not in MARCAS_VALIDAS:
        errores.append(f"marca_invalida: {registro['marca']}")
    if registro.get("talla") and registro["talla"] not in TALLAS_VALIDAS:
        errores.append(f"talla_invalida: {registro['talla']}")
    if registro.get("manga") and registro["manga"] not in MANGAS_VALIDAS:
        errores.append(f"manga_invalida: {registro['manga']}")
    if registro.get("tipo_cuello") and registro["tipo_cuello"] not in CUELLOS_VALIDOS:
        errores.append(f"cuello_invalido: {registro['tipo_cuello']}")
    if registro.get("estampado") and registro["estampado"] not in ESTAMPADOS_VALIDOS:
        errores.append(f"estampado_invalido: {registro['estampado']}")
    if registro.get("color") and registro["color"] not in COLORES_VALIDOS:
        errores.append(f"color_invalido: {registro['color']}")

    return len(errores) == 0, errores
