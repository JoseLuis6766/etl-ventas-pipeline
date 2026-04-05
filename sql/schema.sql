-- ============================================================
-- ESQUEMA ETL CAMISAS 2025 — Arquitectura en capas
-- raw → bronze → silver → gold | ref
-- ============================================================

-- ─────────────────────────────────────
-- SCHEMAS
-- ─────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ref;

-- ─────────────────────────────────────
-- RAW — Registro de archivos cargados
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.archivos_cargados (
    id_archivo      SERIAL PRIMARY KEY,
    nombre_archivo  VARCHAR(255) NOT NULL,
    ruta_archivo    TEXT,
    fecha_carga     TIMESTAMP DEFAULT NOW(),
    total_lineas    INTEGER,
    encoding        VARCHAR(50),
    hash_md5        VARCHAR(64),
    estado          VARCHAR(20) DEFAULT 'cargado',   -- cargado | procesado | error
    observaciones   TEXT
);

-- ─────────────────────────────────────
-- BRONZE — Líneas crudas del .txt
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.lineas_txt (
    id_linea        SERIAL PRIMARY KEY,
    id_archivo      INTEGER REFERENCES raw.archivos_cargados(id_archivo),
    num_linea       INTEGER NOT NULL,
    contenido_raw   TEXT NOT NULL,
    tipo_linea      VARCHAR(20),     -- fecha | venta | ruido | desconocida
    bloque_fecha    INTEGER,         -- número de bloque de fecha
    fecha_contexto  DATE,            -- fecha parseada del bloque activo
    fecha_carga     TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────
-- SILVER — Ventas candidatas (parseadas, pueden tener NULLs)
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.ventas_candidatas (
    id_candidata        SERIAL PRIMARY KEY,
    id_linea            INTEGER REFERENCES bronze.lineas_txt(id_linea),
    id_archivo          INTEGER REFERENCES raw.archivos_cargados(id_archivo),
    linea_original      TEXT,
    fecha               DATE,
    cantidad            INTEGER,
    marca               VARCHAR(50),
    tipo_cuello         VARCHAR(50),
    manga               VARCHAR(10),
    estampado           VARCHAR(50),
    color               VARCHAR(50),
    talla               VARCHAR(5),
    precio              NUMERIC(10,2),
    tipo_tela           VARCHAR(50),
    nivel_confianza     NUMERIC(5,2),   -- 0-100
    estado              VARCHAR(20),    -- completa | parcial | fallida
    reglas_aplicadas    TEXT,           -- "R5_marca,R7_manga_default"
    observaciones       TEXT,
    fecha_proceso       TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────
-- SILVER — Ventas homologadas (normalizadas contra catálogo)
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.ventas_homologadas (
    id_homologada       SERIAL PRIMARY KEY,
    id_candidata        INTEGER REFERENCES silver.ventas_candidatas(id_candidata),
    id_camisa           VARCHAR(100),   -- MARCA-CUELLO-MANGA-ESTAMPADO-COLOR-TALLA
    fecha               DATE,
    cantidad            INTEGER,
    marca               VARCHAR(50),
    tipo_cuello         VARCHAR(50),
    manga               VARCHAR(10),
    estampado           VARCHAR(50),
    color               VARCHAR(50),
    talla               VARCHAR(5),
    precio              NUMERIC(10,2),
    tipo_tela           VARCHAR(50),
    nivel_confianza     NUMERIC(5,2),
    reglas_aplicadas    TEXT,
    observaciones       TEXT,
    fecha_proceso       TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────
-- SILVER — Ventas rechazadas
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.ventas_rechazadas (
    id_rechazada        SERIAL PRIMARY KEY,
    id_linea            INTEGER REFERENCES bronze.lineas_txt(id_linea),
    id_archivo          INTEGER REFERENCES raw.archivos_cargados(id_archivo),
    linea_original      TEXT,
    motivo_rechazo      VARCHAR(100),   -- ruido | no_es_camisa | sin_atributos | marca_desconocida | etc.
    detalle_rechazo     TEXT,
    nivel_confianza     NUMERIC(5,2),
    fecha_proceso       TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────
-- GOLD — Hecho ventas (tabla analítica final)
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.hecho_ventas (
    id_hecho            SERIAL PRIMARY KEY,
    id_homologada       INTEGER REFERENCES silver.ventas_homologadas(id_homologada),
    id_camisa           VARCHAR(100),
    fecha               DATE,
    anio                INTEGER,
    mes                 INTEGER,
    semana              INTEGER,
    dia_semana          VARCHAR(15),
    cantidad            INTEGER,
    precio_unitario     NUMERIC(10,2),
    precio_total        NUMERIC(10,2),
    marca               VARCHAR(50),
    tipo_cuello         VARCHAR(50),
    manga               VARCHAR(10),
    estampado           VARCHAR(50),
    color               VARCHAR(50),
    talla               VARCHAR(5),
    tipo_tela           VARCHAR(50),
    fecha_carga         TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────
-- GOLD — Métricas de calidad del ETL
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.metricas_calidad (
    id_metrica          SERIAL PRIMARY KEY,
    id_archivo          INTEGER REFERENCES raw.archivos_cargados(id_archivo),
    fecha_proceso       TIMESTAMP DEFAULT NOW(),
    total_lineas        INTEGER,
    lineas_fecha        INTEGER,
    lineas_ruido        INTEGER,
    lineas_venta        INTEGER,
    lineas_desconocida  INTEGER,
    candidatas_total    INTEGER,
    candidatas_completa INTEGER,
    candidatas_parcial  INTEGER,
    candidatas_fallida  INTEGER,
    homologadas_total   INTEGER,
    rechazadas_total    INTEGER,
    pct_recuperacion    NUMERIC(5,2),   -- lineas_venta / (total - ruido - fecha)
    pct_homologacion    NUMERIC(5,2),   -- homologadas / candidatas
    pct_rechazo         NUMERIC(5,2),   -- rechazadas / candidatas
    pct_completitud     NUMERIC(5,2),   -- campos llenos / campos requeridos
    pct_consistencia_id NUMERIC(5,2),   -- id_camisa válidos / total
    cumple_recuperacion BOOLEAN,
    cumple_homologacion BOOLEAN,
    cumple_rechazo      BOOLEAN,
    observaciones       TEXT
);

-- ─────────────────────────────────────
-- REF — Catálogo de camisas
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS ref.catalogo_camisas (
    id_camisa           VARCHAR(100) PRIMARY KEY,
    marca               VARCHAR(50),
    tipo_cuello         VARCHAR(50),
    manga               VARCHAR(10),
    estampado           VARCHAR(50),
    color               VARCHAR(50),
    talla               VARCHAR(5),
    tipo_tela           VARCHAR(50),
    precio_referencia   NUMERIC(10,2),
    activo              BOOLEAN DEFAULT TRUE,
    fecha_alta          TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────
-- REF — Catálogo de atributos válidos
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS ref.catalogo_atributos (
    id_atributo     SERIAL PRIMARY KEY,
    tipo_atributo   VARCHAR(50),    -- marca | talla | color | estampado | cuello | manga | tela
    valor_valido    VARCHAR(100),
    activo          BOOLEAN DEFAULT TRUE
);

-- Poblamos catálogo de atributos
INSERT INTO ref.catalogo_atributos (tipo_atributo, valor_valido) VALUES
-- Marcas
('marca','Armani'),('marca','Burberry'),('marca','Calvin'),('marca','Dolce'),
('marca','Levis'),('marca','Manchini'),('marca','Polo'),('marca','Tommy'),
('marca','Versace'),('marca','Zara'),
-- Tallas
('talla','CH'),('talla','M'),('talla','G'),('talla','XG'),
-- Manga
('manga','corta'),('manga','larga'),
-- Cuello
('cuello','botones'),('cuello','con cuello'),('cuello','mao'),
-- Estampados
('estampado','cuadros'),('estampado','figuras'),('estampado','flores'),
('estampado','lisa'),('estampado','micro cuadro'),('estampado','micro figuras'),
('estampado','puntos'),('estampado','rayas delgadas'),('estampado','resorte'),
('estampado','textura cuadrada'),
-- Colores
('color','azul celeste'),('color','azul cielo'),('color','azul marino'),
('color','azul primario'),('color','azul rey'),('color','beige'),
('color','blanca'),('color','gris'),('color','lila'),('color','melon'),
('color','mostaza'),('color','negra'),('color','palo de rosa'),
('color','puntos'),('color','roja'),('color','verde agua'),
('color','verde fuerte'),('color','verde militar'),('color','vino'),
-- Telas
('tela','Media'),('tela','Gruesa'),('tela','Elástica'),('tela','Suave'),
('tela','Tipo lino'),('tela','Tipo mezclilla'),('tela','Satinada'),
('tela','Poliester'),('tela','Algodon'),('tela','Microperforada'),
('tela','Texturizada'),('tela','Terciopelo'),('tela','Ligera')
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────
-- REF — Diccionario de normalización
-- ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS ref.diccionario_normalizacion (
    id_regla        SERIAL PRIMARY KEY,
    tipo_atributo   VARCHAR(50),
    texto_entrada   VARCHAR(200),
    valor_normalizado VARCHAR(100),
    activo          BOOLEAN DEFAULT TRUE
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_bronze_archivo   ON bronze.lineas_txt(id_archivo);
CREATE INDEX IF NOT EXISTS idx_bronze_tipo      ON bronze.lineas_txt(tipo_linea);
CREATE INDEX IF NOT EXISTS idx_silver_candidata ON silver.ventas_candidatas(id_linea);
CREATE INDEX IF NOT EXISTS idx_silver_homolog   ON silver.ventas_homologadas(id_candidata);
CREATE INDEX IF NOT EXISTS idx_gold_fecha       ON gold.hecho_ventas(fecha);
CREATE INDEX IF NOT EXISTS idx_gold_marca       ON gold.hecho_ventas(marca);
CREATE INDEX IF NOT EXISTS idx_gold_id_camisa   ON gold.hecho_ventas(id_camisa);
