"""
utils/text_utils.py — Utilidades de limpieza de texto y diccionarios de normalización.
Todos los diccionarios reflejan las reglas de negocio R1-R14 del Prompt Maestro.
"""

import re
import unicodedata
from typing import Optional

# ─────────────────────────────────────
# DICCIONARIOS DE NORMALIZACIÓN
# ─────────────────────────────────────

DICT_MARCAS = {
    'zara': 'Zara',
    'tommy': 'Tommy', "tommy's": 'Tommy', 'tommys': 'Tommy',
    'calvin': 'Calvin', 'calvin klein': 'Calvin',
    'polo': 'Polo',
    'dolce': 'Dolce', 'dolce gabbana': 'Dolce', 'doce': 'Dolce',
    'armani': 'Armani',
    'versace': 'Versace',
    'levis': 'Levis', 'leviz': 'Levis',
    'burberry': 'Burberry', 'bulberry': 'Burberry',
    'bilberry': 'Burberry', 'bulbery': 'Burberry', 'bilbery': 'Burberry',
    'manchini': 'Manchini', 'machini': 'Manchini',
}

DICT_TALLAS = {
    'g': 'G', 'grande': 'G',
    'm': 'M', 'mediana': 'M', 'mediano': 'M',
    'ch': 'CH', 'chica': 'CH', 'chico': 'CH',
    'xg': 'XG', 'x g': 'XG', 'xl': 'XG',
}

DICT_COLORES = {
    'negra': 'negra', 'negro': 'negra',
    'blanca': 'blanca', 'blanco': 'blanca',
    'azul marino': 'azul marino', 'marino': 'azul marino',
    'azul cielo': 'azul cielo', 'cielo': 'azul cielo',
    'azul celeste': 'azul celeste', 'celeste': 'azul celeste',
    'azul rey': 'azul rey', 'rey': 'azul rey',
    'azul primario': 'azul primario', 'primario': 'azul primario',
    'vino': 'vino',
    'rosa chillon': 'rosa chillon', 'rosa chillón': 'rosa chillon',
    'rosa chilona': 'rosa chillon', 'rosa claro': 'rosa chillon',
    'rosa': 'palo de rosa',   # ambiguo → default palo de rosa
    'palo de rosa': 'palo de rosa', 'palo rosa': 'palo de rosa', 'palo': 'palo de rosa',
    'lila': 'lila',
    'roja': 'roja', 'rojo': 'roja', 'rojas': 'roja',
    'verde fuerte': 'verde fuerte',
    'verde militar': 'verde militar',
    'verde agua': 'verde agua',
    'verde': 'verde fuerte',   # cuando aparece solo → verde fuerte
    'gris': 'gris',
    'besh': 'beige', 'beige': 'beige',
    'melon': 'melon', 'melón': 'melon', 'azul melón': 'melon',
    'mostaza': 'mostaza',
    'azul': 'azul marino',    # cuando aparece solo marino (R9 default)
    'puntos': 'puntos',       # color puntos (existe en catálogo)
}

DICT_ESTAMPADOS = {
    'micro cuadro': 'micro cuadro', 'micro cuadros': 'micro cuadro',
    'cuadritos': 'micro cuadro', 'micro': 'micro cuadro',
    'micro cuadrado': 'micro cuadro',
    'mq': 'micro cuadro',      # mq = manga corta + micro cuadro (ver R7/R8)
    'cuadros': 'cuadros', 'cuadrado': 'cuadros', 'cuadro': 'cuadros',
    'rayas': 'rayas delgadas', 'rayas delgadas': 'rayas delgadas',
    'rayas polo': 'rayas delgadas', 'rayas nueva': 'rayas delgadas',
    'rayas nuevas': 'rayas delgadas',
    'puntos': 'puntos', 'punto': 'puntos',
    'flores': 'flores', 'floral': 'flores',
    'palmas': 'flores', 'palmeras': 'flores',
    'playa': 'flores', 'tucanes': 'flores', 'pelicanos': 'flores',
    'pelícanos': 'flores',
    'figuras': 'micro figuras', 'de figuras': 'micro figuras',
    'micro figuras': 'micro figuras', 'cruces': 'micro figuras',
    'resorte': 'resorte', 'resort': 'resorte', 'rezrte': 'resorte',
    'textura cuadrada': 'textura cuadrada',
    'lisa': 'lisa',
}

DICT_CUELLO = {
    'mao': 'mao', 'mao corta': 'mao',
    'con cuello': 'con cuello', 'cuello': 'con cuello',
    'botones': 'botones', 'con botones': 'botones',
    'resorte': 'mao',   # resorte - cuello=mao + estampado=resorte
    'resort': 'mao',
    'rezrte': 'mao',
}

DICT_MANGA = {
    'manga corta': 'corta', 'maga corta': 'corta',
    'corta': 'corta', 'mc': 'corta',
    'manga larga': 'larga', 'larga': 'larga',
}

DICT_TELA = {
    'pana': 'Gruesa',
    'franela': 'Gruesa',
    'mezclilla': 'Tipo mezclilla', 'mezclila': 'Tipo mezclilla',
    'buchona': 'Terciopelo',
    'lino': 'Tipo lino',
    'tela fresca': 'Ligera', 'fresca': 'Ligera',
    'tela suave': 'Suave', 'suave': 'Suave',
    'elastica': 'Elástica', 'elástica': 'Elástica',
    'satinada': 'Satinada',
    'poliester': 'Poliester', 'poliéster': 'Poliester',
    'algodon': 'Algodon', 'algodón': 'Algodon',
}

# Defaults por marca (R12)
DEFAULTS_MARCA = {
    'Zara':     {'tipo_cuello': 'con cuello', 'manga': 'larga',  'estampado': 'lisa'},
    'Tommy':    {'tipo_cuello': 'con cuello', 'manga': 'larga',  'estampado': 'lisa'},
    'Calvin':   {'tipo_cuello': 'con cuello', 'manga': 'larga',  'estampado': 'lisa'},
    'Polo':     {'tipo_cuello': 'con cuello', 'manga': 'larga',  'estampado': 'micro cuadro'},
    'Dolce':    {'tipo_cuello': 'mao',        'manga': 'larga',  'estampado': 'lisa'},
    'Armani':   {'tipo_cuello': 'botones',    'manga': 'larga',  'estampado': 'cuadros'},
    'Burberry': {'tipo_cuello': 'mao',        'manga': 'corta',  'estampado': 'lisa'},
}

# Palabras que indican RUIDO (R2)
PALABRAS_RUIDO = {
    'mama', 'mamá', 'cambio', 'contabilidad', 'correo',
    'contraseña', 'transferencia', 'pantalón', 'pantalon', 'playera',
    'pato', 'vecino', 'helados', 'excel', 'sin contar',
}

# Nombres de meses en español (R1)
MESES_ES = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
    'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
    'septiembre': 9, 'sep': 9, 'octubre': 10, 'oct': 10,
    'noviembre': 11, 'nov': 11, 'diciembre': 12, 'dic': 12,
}

DIAS_SEMANA = {'lunes', 'martes', 'miércoles', 'miercoles',
               'jueves', 'viernes', 'sábado', 'sabado', 'domingo'}

# ─────────────────────────────────────
# FUNCIONES DE UTILIDAD
# ─────────────────────────────────────

def limpiar_texto(texto: str) -> str:
    """Normaliza el texto: strip, lowercase, elimina caracteres de control."""
    if not texto:
        return ""
    texto = texto.strip()
    # Eliminar caracteres de control excepto espacios
    texto = re.sub(r'[\x00-\x08\x0b-\x1f\x7f]', '', texto)
    # Colapsar espacios múltiples
    texto = re.sub(r'\s+', ' ', texto)
    return texto


def normalizar_unicode(texto: str) -> str:
    """Convierte caracteres acentuados para búsqueda insensible a acentos."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    ).lower()


def lower_sin_acento(texto: str) -> str:
    """Lowercase + elimina acentos. Útil para matching de diccionarios."""
    return normalizar_unicode(texto.lower())


def buscar_en_dict(texto: str, diccionario: dict) -> Optional[str]:
    """
    Busca el texto (en lowercase) en el diccionario.
    Primero búsqueda exacta, luego búsqueda normalizada sin acentos.
    """
    t = texto.lower().strip()
    if t in diccionario:
        return diccionario[t]
    t_norm = lower_sin_acento(t)
    for clave, valor in diccionario.items():
        if lower_sin_acento(clave) == t_norm:
            return valor
    return None


def buscar_token_en_dict(tokens: list[str], diccionario: dict) -> tuple[Optional[str], Optional[str]]:
    """
    Busca tokens (de más largo a más corto) en el diccionario.
    Retorna (valor_encontrado, token_original_matched).
    Prueba primero bigramas, luego unigramas.
    """
    # Prueba bigramas
    for i in range(len(tokens) - 1):
        bigrama = f"{tokens[i]} {tokens[i+1]}"
        val = buscar_en_dict(bigrama, diccionario)
        if val:
            return val, bigrama
    # Prueba unigramas
    for tok in tokens:
        val = buscar_en_dict(tok, diccionario)
        if val:
            return val, tok
    return None, None


def es_numero(texto: str) -> bool:
    """True si el texto es un número entero."""
    return bool(re.match(r'^\d+$', texto.strip()))


def es_precio(texto: str) -> bool:
    """True si el texto parece un precio ($170, 170, $1,000)."""
    t = texto.strip()
    return bool(re.match(r'^\$?\d{1,4}$', t)) and int(re.sub(r'[$,]', '', t)) >= 100


def extraer_numero_inicial(texto: str) -> tuple[Optional[int], str]:
    """
    Extrae número entero al inicio de la línea (R3).
    Retorna (cantidad, resto_de_linea).
    Si no hay número → cantidad=None, linea completa.
    """
    match = re.match(r'^(\d+)\s+(.*)', texto.strip())
    if match:
        num = int(match.group(1))
        resto = match.group(2).strip()
        # Validar que no sea un precio suelto (>= 100 sin producto)
        if num >= 100 and not resto:
            return None, texto
        return num, resto
    return None, texto


def calcular_nivel_confianza(registro: dict, atributos_requeridos: list) -> float:
    """
    Calcula nivel de confianza como porcentaje de atributos requeridos presentes.
    """
    presentes = sum(
        1 for attr in atributos_requeridos
        if registro.get(attr) is not None and registro.get(attr) != ""
    )
    return round((presentes / len(atributos_requeridos)) * 100, 2)


def construir_id_camisa(marca: str, cuello: str, manga: str,
                        estampado: str, color: str, talla: str) -> Optional[str]:
    """Construye el ID único de camisa según el patrón del negocio."""
    partes = [marca, cuello, manga, estampado, color, talla]
    if any(p is None or p == "" for p in partes):
        return None
    return "-".join(p.upper().replace(" ", "_") for p in partes)


def separar_por_conjuncion(texto: str) -> list[str]:
    """Divide texto por 'y', ',' para desdoblamiento (R11)."""
    # Primero por ' y '
    partes = re.split(r'\s+y\s+', texto, flags=re.IGNORECASE)
    resultado = []
    for p in partes:
        # Luego por coma
        resultado.extend([s.strip() for s in p.split(',') if s.strip()])
    return resultado
