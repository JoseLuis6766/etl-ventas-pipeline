-- Limpieza de datos en sql

--query para saber que registros no pudieron ser homologados

SELECT C.*
FROM silver.ventas_candidatas C
WHERE NOT EXISTS(
	SELECT *
	FROM silver.ventas_homologadas H
	WHERE C.id_candidata = H.id_candidata
)

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


UPDATE silver.ventas_candidatas
SET marca = 'Calvin'
WHERE linea_original LIKE '%mao%'

--se ocupara la columna observaciones para llevar un registro de cambios y ver que registros ya se modificaron; 1 modificado, 0 no modificado
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

-- segunda agregacion de datos

WITH NO_HOMOLOGADOS AS(
SELECT C.*
FROM silver.ventas_candidatas C
WHERE NOT EXISTS(
	SELECT *
	FROM silver.ventas_homologadas H
	WHERE C.id_candidata = H.id_candidata
)
	AND linea_original LIKE '%mao%' 
	AND linea_original NOT LIKE '1%' 
	AND manga = 'larga'

)
select * from NO_HOMOLOGADOS;
UPDATE silver.ventas_candidatas C
SET 
	nivel_confianza = 100
	
FROM NO_HOMOLOGADOS H
WHERE C.id_candidata = H.id_candidata;
	


--INSERT INTO silver.ventas_homologadas
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

