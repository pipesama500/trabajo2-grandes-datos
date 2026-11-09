"""
Rama 1 - Trabajo 2 (Grandes Datos, EAFIT)

Pregunta: ¿Qué departamentos tienen más horas sol pico (HSP)?

HSP de un día = radiación total del día (Wh/m2) / 1000
  -> equivale a las horas de sol "perfecto" (1000 W/m2) de ese día
  -> numéricamente es igual a la radiación diaria en kWh/m2

Pasos:
  1. Suma la radiación de las 24 horas de cada día y estación -> HSP del día
  2. Promedia por departamento -> HSP promedio diaria
  3. Ordena de mayor a menor y escribe en Parquet

Parámetro del trabajo:  --BUCKET  nombre del bucket (sin s3://)
"""

import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET = args["BUCKET"]
RUTA_ENTRADA = f"s3://{BUCKET}/processed/"
RUTA_SALIDA = f"s3://{BUCKET}/agg1/"

# 1. Leer los datos limpios
datos = spark.read.parquet(RUTA_ENTRADA)

# 2. HSP de cada día, por estación
hsp_diaria = (datos
    .groupBy("departamento", "departamento_nombre", "codigo_estacion", "anio", "mes", "dia")
    .agg((F.sum("radiacion_wh_m2") / 1000).alias("hsp_dia"))
)

# 3. Promedio por departamento
resultado = (hsp_diaria
    .groupBy("departamento", "departamento_nombre")
    .agg(
        F.round(F.avg("hsp_dia"), 2).alias("hsp_promedio"),
        F.round(F.min("hsp_dia"), 2).alias("hsp_dia_minimo"),
        F.round(F.max("hsp_dia"), 2).alias("hsp_dia_maximo"),
        F.countDistinct("codigo_estacion").alias("num_estaciones"),
        F.count("*").alias("num_dias_analizados"),
    )
)

# 4. Ranking: 1 = departamento con más horas sol pico
ventana = Window.orderBy(F.col("hsp_promedio").desc())
resultado = (resultado
    .withColumn("ranking", F.row_number().over(ventana))
    .select("ranking", "departamento", "departamento_nombre", "hsp_promedio",
            "hsp_dia_minimo", "hsp_dia_maximo", "num_estaciones", "num_dias_analizados")
    .orderBy("ranking")
)

# 5. Escribir en Parquet (un solo archivo, son ~23 filas)
resultado.coalesce(1).write.mode("overwrite").parquet(RUTA_SALIDA)

print("\n===== RAMA 1: HORAS SOL PICO POR DEPARTAMENTO =====")
resultado.show(50, truncate=False)

job.commit()
