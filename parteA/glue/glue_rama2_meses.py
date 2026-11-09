"""
Rama 2 - Trabajo 2 (Grandes Datos, EAFIT)

Pregunta: ¿En qué meses hay más radiación en cada departamento?

Pasos:
  1. Suma la radiación de las 24 horas de cada día y estación -> radiación diaria (kWh/m2/día)
  2. Promedia por departamento y mes
  3. Ordena los meses de cada departamento (1 = mes con más radiación)
  4. Escribe en Parquet

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
RUTA_SALIDA = f"s3://{BUCKET}/agg2/"

MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
         "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

# 1. Leer los datos limpios
datos = spark.read.parquet(RUTA_ENTRADA)

# 2. Radiación de cada día, por estación (kWh/m2/día)
diaria = (datos
    .groupBy("departamento", "departamento_nombre", "codigo_estacion", "anio", "mes", "dia")
    .agg((F.sum("radiacion_wh_m2") / 1000).alias("radiacion_dia_kwh_m2"))
)

# 3. Promedio por departamento y mes
resultado = (diaria
    .groupBy("departamento", "departamento_nombre", "mes")
    .agg(F.round(F.avg("radiacion_dia_kwh_m2"), 2).alias("radiacion_diaria_kwh_m2"))
)

# Nombre del mes
nombre_mes = F.element_at(F.array(*[F.lit(m) for m in MESES]), F.col("mes"))
resultado = resultado.withColumn("mes_nombre", nombre_mes)

# 4. Ranking de meses dentro de cada departamento (1 = mes con más radiación)
ventana = Window.partitionBy("departamento").orderBy(F.col("radiacion_diaria_kwh_m2").desc())
resultado = (resultado
    .withColumn("ranking_mes", F.row_number().over(ventana))
    .select("departamento", "departamento_nombre", "mes", "mes_nombre",
            "radiacion_diaria_kwh_m2", "ranking_mes")
    .orderBy("departamento", "mes")
)

# 5. Escribir en Parquet (un solo archivo, son ~23 x 12 filas)
resultado.coalesce(1).write.mode("overwrite").parquet(RUTA_SALIDA)

print("\n===== RAMA 2: MES CON MÁS RADIACIÓN POR DEPARTAMENTO =====")
resultado.where(F.col("ranking_mes") == 1).orderBy(F.col("radiacion_diaria_kwh_m2").desc()).show(50, truncate=False)

job.commit()
