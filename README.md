# ETL Pipeline — Sistema de Ventas de Camisas 2025

> Pipeline de datos completo que transforma registros de ventas escritos a mano en texto libre hacia una base de datos analítica estructurada, con arquitectura en capas `raw → bronze → silver → gold`.

---

## Contexto del problema

Un negocio de venta de camisas en mercado registra sus ventas manualmente en un archivo `.txt` sin estructura formal, usando lenguaje coloquial, abreviaciones y errores ortográficos. Por ejemplo:

```
Miércoles agosto 13
1 g rayas polo azul cielo
2 Xg Zara blanca y negra manga corta
4 ch resorte, vino, blanca, negra y azul marino Zara manga corta
3 dolce y 2 Zara manga corta
1 m dolce *
```

El reto: convertir estas anotaciones informales en datos limpios, normalizados y analizables.

---

## Objetivos del proyecto

- Diseñar un pipeline ETL robusto que procese datos sucios del mundo real
- Aplicar reglas de negocio específicas del dominio para parseo e interpretación
- Normalizar atributos contra un catálogo maestro
- Construir una arquitectura de datos en capas siguiendo estándares profesionales
- Medir y reportar la calidad del proceso con KPIs automatizados

---

## Arquitectura

```
ventas_2025.txt  ──►  raw  ──►  bronze  ──►  silver  ──►  gold
                      │           │             │            │
                 archivos_    lineas_txt    candidatas   hecho_ventas
                 cargados    (crudas +     homologadas  metricas_
                             clasific.)    rechazadas   calidad
```

### Capas del pipeline

| Capa | Tabla | Descripción |
|------|-------|-------------|
| **raw** | `archivos_cargados` | Registro de metadatos del archivo fuente |
| **bronze** | `lineas_txt` | Líneas crudas con clasificación inicial |
| **silver** | `ventas_candidatas` | Registros parseados (pueden tener nulls) |
| **silver** | `ventas_homologadas` | Registros normalizados y válidos |
| **silver** | `ventas_rechazadas` | Registros no procesables con motivo |
| **gold** | `hecho_ventas` | Tabla analítica final con dimensiones de fecha |
| **gold** | `metricas_calidad` | KPIs del proceso ETL |
| **ref** | `catalogo_camisas` | Catálogo maestro de productos |

---

## Desafíos técnicos resueltos

### 1. Parseo de lenguaje natural no estructurado
El orden de los tokens en cada línea no es fijo. El parser detecta atributos (marca, talla, color, manga, estampado, cuello) sin importar en qué posición aparezcan, usando diccionarios de sinónimos y reglas de negocio:

```python
# La misma información expresada de formas distintas:
"1 g rayas polo azul cielo"       # talla primero
"tommy larga azul marino 2 g"     # talla al final
"azul marino G Tommy"              # sin cantidad explícita
```

### 2. Desdoblamiento de líneas compuestas
Una sola línea puede contener múltiples camisas. El sistema detecta 7 patrones distintos y genera los registros correspondientes:

```
"2 rayas polo azul cielo G/M"
→ Polo/G/azul cielo + Polo/M/azul cielo

"4 ch resorte, vino, blanca, negra y azul marino Zara manga corta"
→ Tommy/CH/mao/vino + Tommy/CH/mao/blanca + Tommy/CH/mao/negra + Zara/CH/corta/azul marino

"3 dolce y 2 Zara manga corta"
→ 3× Dolce (defaults) + 2× Zara manga corta
```

### 3. Reglas de negocio implícitas del dominio
El sistema codifica conocimiento experto del negocio:

- `resorte` → implica marca=Tommy, cuello=mao, manga=corta, estampado=resorte
- `mao + blanca` → implica marca=Calvin
- `*` al final → color=blanca, manga=larga, cuello=con cuello, estampado=textura cuadrada
- Defaults por marca: Polo→micro cuadro, Armani→cuadros, Burberry→corta

### 4. Fuzzy matching de marcas
Errores ortográficos se resuelven automáticamente:

```python
"machini"   → Manchini
"bilberry"  → Burberry
"tommys"    → Tommy
"leviz"     → Levis
```

---

## Resultados del pipeline

| Métrica | Resultado | Meta |
|---------|-----------|------|
| % Recuperación | **97.3%** ✅ | ≥ 85% |
| % Homologación | **~82%** ✅ | ≥ 80% |
| % Rechazo | **~12%** ✅ | ≤ 15% |
| % ID_camisa válido | **99.5%** ✅ | ≥ 95% |

*Procesados: 1,309 líneas → 902 ventas identificadas → 1,151 candidatas (tras desdoblamiento)*

---

## Stack tecnológico

| Herramienta | Uso |
|-------------|-----|
| **Python 3.11** | Pipeline ETL principal |
| **PostgreSQL 15** | Base de datos analítica |
| **psycopg2** | Conexión Python ↔ PostgreSQL |
| **pandas** | Procesamiento tabular |
| **openpyxl** | Lectura del catálogo Excel |
| **difflib** | Fuzzy matching de marcas |
| **SQL** | Limpieza post-ETL, procedures, análisis |

---

## 📁 Estructura del proyecto

```
etl_camisas/
├── main.py                       # Orquestador — flags: --solo-bronze, --desde-silver
├── config.py                     # Conexión DB, rutas, parámetros
├── sql/
│   └── schema.sql                # DDL completo (9 esquemas, 10 tablas)
├── data/
│   ├── ventas_2025.txt           # Fuente: notas de ventas manuales
│   └── Camisas_inventario.xlsx   # Catálogo maestro
├── loaders/
│   ├── raw_loader.py             # Registro y hash MD5 del archivo
│   ├── bronze_loader.py          # Clasificación y carga de líneas
│   ├── silver_loader.py          # Inserción en candidatas/homologadas/rechazadas
│   └── gold_loader.py            # Hechos de venta + métricas de calidad
├── parsers/
│   ├── fecha_parser.py           # Parseo de fechas en español informal
│   ├── linea_classifier.py       # Clasificador: fecha/venta/ruido/desconocida
│   ├── venta_parser.py           # Extracción de atributos + reglas empíricas
│   └── desdoblador.py            # 7 patrones de desdoblamiento de líneas
├── normalizers/
│   ├── atributo_normalizer.py    # Diccionarios de normalización
│   └── homologador.py            # Homologación contra catálogo
└── utils/
    └── text_utils.py             # Helpers de texto, diccionarios de negocio
```

---

### Verificar resultados

```sql

--verificacion de resultados comparando conel .txt por bloques de ventas por fecha
SELECT SUM(H.cantidad) OVER() AS SUMA_REGISTROS, 
		C.linea_original, 
		H.id_camisa,
		H.marca, 
		H.tipo_cuello, 
		H.manga,
		H.estampado, 
		H.color, 
		H.talla, 
		H.fecha,
		H.cantidad,
		H.nivel_confianza
FROM silver.ventas_homologadas H
JOIN silver.ventas_candidatas C
ON H.id_candidata = C.id_candidata
where H.fecha = '2025-07-12';

--verificacion de resultados con una marca especifica para verificacion de resultados

WITH bad_Zara AS(

SELECT *
FROM silver.ventas_homologadas
WHERE marca = 'Zara' 
AND tipo_cuello = 'mao'

UNION ALL

SELECT *
FROM silver.ventas_homologadas
WHERE marca = 'Zara' 
AND estampado <> 'lisa'

)

SELECT SUM(Z.cantidad) OVER() AS Numero_inconsistencias, Z.*, C.linea_original -- Variable que contiene el texto de la linea original 
FROM silver.ventas_candidatas C
JOIN bad_Zara Z
ON Z.id_candidata = C.id_candidata;
```

---

## Decisiones de diseño destacadas

**¿Por qué arquitectura en capas (medallion)?**
Permite auditoría completa: siempre se preserva la línea original en bronze. Si una regla de parseo falla, se puede re-procesar desde bronze sin volver a leer el archivo fuente.

**¿Por qué priorizar recuperación sobre precisión?**
Un registro `parcial` con marca+talla pero sin color es más valioso que un registro rechazado. El nivel de confianza (0-100%) cuantifica esta decisión y permite filtrado posterior.

**¿Por qué el desdoblador tiene 7 patrones en orden de prioridad?**
Los patrones más específicos (slash-talla, cant+marca explícito, resorte-split) se evalúan antes que los genéricos (split por 'y'). Esto evita que una línea compleja caiga en el fallback y pierda información.

---

## Consultas analíticas de ejemplo

```sql
--Evolucion por ventas
SELECT anio, semana, SUM(cantidad) as piezas
FROM gold.hecho_ventas
GROUP BY anio, semana ORDER BY  semana DESC;

-- Combinaciones más vendidas (top SKUs)
SELECT id_camisa, marca, color, talla, SUM(cantidad) as total
FROM gold.hecho_ventas
GROUP BY id_camisa, marca, color, talla
ORDER BY total DESC LIMIT 10;

-- Proporción manga corta vs larga por marca
SELECT  marca, 
		manga, 
		SUM(cantidad) as total,
		ROUND(SUM(cantidad) * 100.0 / SUM(SUM(cantidad)) OVER (PARTITION BY marca), 1) as pct
FROM gold.hecho_ventas
GROUP BY marca, manga 
ORDER BY marca, manga;
```
## Agregacion de datos en SQL
```sql
--se complemento la tabla ventas_candidatas en columnas que habian null con informacion conocida
WITH NO_HOMOLOGADOS AS(
SELECT C.*
FROM silver.ventas_candidatas C
WHERE NOT EXISTS(
	SELECT *
	FROM silver.ventas_homologadas H
	WHERE C.id_candidata = H.id_candidata
)
	AND linea_original LIKE '%mao%' 
	AND linea_original LIKE '1%' 
	AND manga = 'larga'
)

UPDATE silver.ventas_candidatas C
SET 
	precio = 170,
	tipo_tela = 'Algodon',
	observaciones = '1'
FROM NO_HOMOLOGADOS H
WHERE C.id_candidata = H.id_candidata;
--en base a informacion agregada en 'silver.ventas_candidatas' se agregan mas registros a 'silver.ventas_homologadas'
INSERT INTO silver.ventas_homologadas
    (id_candidata, id_camisa, fecha, cantidad, marca, tipo_cuello,
     manga, estampado, color, talla, precio, tipo_tela,
     nivel_confianza, reglas_aplicadas, observaciones)
SELECT
    vc.id_candidata,
    UPPER(REPLACE(vc.marca,' ','_')) || '-' ||
    UPPER(REPLACE(vc.tipo_cuello,' ','_')) || '-' ||
    UPPER(vc.manga) || '-' ||
    UPPER(REPLACE(vc.estampado,' ','_')) || '-' ||
    UPPER(REPLACE(vc.color,' ','_')) || '-' ||
    UPPER(vc.talla) AS id,
    vc.fecha, vc.cantidad, vc.marca, vc.tipo_cuello,
    vc.manga, vc.estampado, vc.color, vc.talla,
    vc.precio, vc.tipo_tela,
    vc.nivel_confianza,
    vc.reglas_aplicadas || ',SQL_REHOMOLOG',
    vc.observaciones
FROM silver.ventas_candidatas vc


```

---

## Habilidades demostradas

`ETL / ELT` · `Python` · `PostgreSQL` · `Arquitectura Medallion ` · `Reglas de negocio` · `Normalización de datos`  · `Métricas de calidad de datos` · `Diseño de esquemas relacionales` 

---

## Autor

**Jose Luis** 

