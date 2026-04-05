SELECT *
FROM silver.ventas_homologadas

SELECT *
FROM bronze.lineas_txt
WHERE tipo_linea = 'desconocida'

SELECT marca, SUM(cantidad) AS total
FROM gold.hecho_ventas
GROUP BY marca
ORDER BY total DESC; 

SELECT *
FROM silver.ventas_candidatas

-- Consultas de validacion de datos para validacion de scripts y encontrar inconsistenicas

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
where H.fecha = '2025-07-12'

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

SELECT *
FROM gold.hecho_ventas



