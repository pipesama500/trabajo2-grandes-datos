-- Trabajo 2, parte A - Consultas en Amazon Athena (región us-east-2)
-- Reemplace <BUCKET> por el nombre de su bucket.
-- Ejecute cada bloque por separado en el Query editor de Athena.

-- 1. Base de datos
CREATE DATABASE trabajo2;

-- 2. Tabla de la rama 1 (horas sol pico por departamento)
CREATE EXTERNAL TABLE trabajo2.agg1_hsp (
  ranking int,
  departamento string,
  departamento_nombre string,
  hsp_promedio double,
  hsp_dia_minimo double,
  hsp_dia_maximo double,
  num_estaciones bigint,
  num_dias_analizados bigint
)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/agg1/';

-- 3. Tabla de la rama 2 (radiación diaria promedio por departamento y mes)
CREATE EXTERNAL TABLE trabajo2.agg2_meses (
  departamento string,
  departamento_nombre string,
  mes int,
  mes_nombre string,
  radiacion_diaria_kwh_m2 double,
  ranking_mes int
)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/agg2/';

-- Pregunta 1: ¿Qué departamentos tienen más horas sol pico?
SELECT ranking, departamento_nombre, hsp_promedio, hsp_dia_minimo, hsp_dia_maximo, num_estaciones
FROM trabajo2.agg1_hsp
ORDER BY ranking;

-- Pregunta 2: ¿En qué mes hay más radiación en cada departamento?
SELECT departamento_nombre, mes_nombre, radiacion_diaria_kwh_m2
FROM trabajo2.agg2_meses
WHERE ranking_mes = 1
ORDER BY radiacion_diaria_kwh_m2 DESC;

-- Extra: los 12 meses de un departamento
SELECT mes_nombre, radiacion_diaria_kwh_m2, ranking_mes
FROM trabajo2.agg2_meses
WHERE departamento = 'ANTIOQUIA'
ORDER BY mes;
