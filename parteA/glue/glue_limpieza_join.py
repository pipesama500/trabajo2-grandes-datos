"""
Trabajo de Glue inicial - Trabajo 2 (Grandes Datos, EAFIT)

1. Lee los datos horarios de NASA POWER (CSV) y el catálogo de estaciones del IDEAM (CSV) desde S3
2. Limpia: descarta fechas inválidas, valores vacíos, -999, fuera de rango,
   estaciones que no existen en el catálogo y duplicados
3. Une (join) NASA + catálogo por el código de estación
4. Escribe el resultado en Parquet, particionado por departamento
5. Escribe un reporte con cuántas filas se descartaron y por qué

Parámetro del trabajo:  --BUCKET  nombre del bucket (sin s3://)
"""

import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ---------------- Inicio del trabajo ----------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Fechas mal escritas -> nulo (en vez de error)
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

BUCKET = args["BUCKET"]
RUTA_NASA = f"s3://{BUCKET}/raw/nasa/"
RUTA_CATALOGO = f"s3://{BUCKET}/raw/catalogo/"
RUTA_SALIDA = f"s3://{BUCKET}/processed/"
RUTA_REPORTE = f"s3://{BUCKET}/reportes/limpieza/"

# Rangos válidos para Colombia
RAD_MIN, RAD_MAX = 0, 1400        # W/m2
TEMP_MIN, TEMP_MAX = -15, 50      # °C
VIENTO_MIN, VIENTO_MAX = 0, 40    # m/s

# ---------------- 1. Lectura ----------------
# Todo se lee como texto para poder detectar los valores mal escritos
esquema_nasa = StructType([
    StructField("codigo_estacion", StringType()),
    StructField("fecha_hora", StringType()),
    StructField("radiacion_wh_m2", StringType()),
    StructField("temperatura_c", StringType()),
    StructField("viento_m_s", StringType()),
])
nasa = spark.read.csv(RUTA_NASA, header=True, schema=esquema_nasa)

catalogo = spark.read.csv(RUTA_CATALOGO, header=True, inferSchema=False,
                          quote='"', escape='"', multiLine=True, encoding="UTF-8")
# Quitar BOM y espacios de los nombres de columna
for c in catalogo.columns:
    catalogo = catalogo.withColumnRenamed(c, c.replace("﻿", "").strip())

total_leidas = nasa.count()
print(f"Filas leídas de NASA: {total_leidas:,}")

# ---------------- 2. Preparar el catálogo ----------------
def a_numero(col):
    """El catálogo usa coma decimal: '6,2246' -> 6.2246"""
    return F.regexp_replace(F.col(col), ",", ".").cast("double")

def normalizar(col):
    """'Norte De Santander' -> 'NORTE_DE_SANTANDER' (sirve como nombre de partición)"""
    sin_tildes = F.translate(F.upper(F.trim(F.col(col))), "ÁÉÍÓÚÜÑ", "AEIOUUN")
    return F.regexp_replace(sin_tildes, "[^A-Z0-9]+", "_")

catalogo = (catalogo
    .select(
        F.col("Codigo").cast("long").alias("codigo"),
        F.col("Nombre").alias("nombre_estacion"),
        F.col("Categoria").alias("categoria"),
        F.col("Departamento").alias("departamento_nombre"),
        normalizar("Departamento").alias("departamento"),
        F.col("Municipio").alias("municipio"),
        a_numero("Altitud").alias("altitud_m"),
        a_numero("LATITUD").alias("latitud"),
        a_numero("LONGITUD").alias("longitud"),
    )
    .where(F.col("codigo").isNotNull())
    .dropDuplicates(["codigo"])
)

# ---------------- 3. Convertir tipos ----------------
nasa = (nasa
    .withColumn("codigo", F.col("codigo_estacion").cast("long"))
    .withColumn("fecha", F.to_timestamp("fecha_hora", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("radiacion", F.col("radiacion_wh_m2").cast("double"))
    .withColumn("temperatura", F.col("temperatura_c").cast("double"))
    .withColumn("viento", F.col("viento_m_s").cast("double"))
)

# Join con el catálogo (left para poder marcar las estaciones que no existen)
unido = nasa.join(F.broadcast(catalogo), on="codigo", how="left")

# ---------------- 4. Marcar el motivo de descarte ----------------
vacio = (F.col("radiacion").isNull() | F.col("temperatura").isNull() | F.col("viento").isNull())
menos_999 = ((F.col("radiacion") == -999) | (F.col("temperatura") == -999) | (F.col("viento") == -999))
fuera_rango = (
    ~F.col("radiacion").between(RAD_MIN, RAD_MAX)
    | ~F.col("temperatura").between(TEMP_MIN, TEMP_MAX)
    | ~F.col("viento").between(VIENTO_MIN, VIENTO_MAX)
)

unido = unido.withColumn(
    "motivo",
    F.when(F.col("codigo").isNull(), "codigo_invalido")
     .when(F.col("fecha").isNull(), "fecha_invalida")
     .when(vacio, "valor_vacio")
     .when(menos_999, "valor_menos_999")
     .when(fuera_rango, "fuera_de_rango")
     .when(F.col("departamento").isNull(), "estacion_inexistente")
     .otherwise(None)
).cache()

# ---------------- 5. Filtrar válidos y quitar duplicados ----------------
validos = unido.where(F.col("motivo").isNull())
n_validos_con_dup = validos.count()

limpio = (validos
    .dropDuplicates(["codigo", "fecha", "radiacion", "temperatura", "viento"])
    .select(
        F.col("codigo").alias("codigo_estacion"),
        "nombre_estacion", "categoria", "departamento_nombre", "municipio",
        "altitud_m", "latitud", "longitud",
        "fecha",
        F.year("fecha").alias("anio"),
        F.month("fecha").alias("mes"),
        F.dayofmonth("fecha").alias("dia"),
        F.hour("fecha").alias("hora"),
        F.col("radiacion").alias("radiacion_wh_m2"),
        F.col("temperatura").alias("temperatura_c"),
        F.col("viento").alias("viento_m_s"),
        "departamento",        # columna de partición (va al final)
    )
)

# ---------------- 6. Escribir Parquet particionado ----------------
(limpio.write
    .mode("overwrite")
    .partitionBy("departamento")
    .parquet(RUTA_SALIDA))

n_limpias = spark.read.parquet(RUTA_SALIDA).count()
n_duplicados = n_validos_con_dup - n_limpias

# ---------------- 7. Reporte de limpieza ----------------
descartes = (unido.where(F.col("motivo").isNotNull())
             .groupBy("motivo").count()
             .collect())
filas = [(r["motivo"], r["count"]) for r in descartes]
filas.append(("duplicado", n_duplicados))
filas.append(("total_descartadas", total_leidas - n_limpias))
filas.append(("total_leidas", total_leidas))
filas.append(("total_limpias", n_limpias))

reporte = spark.createDataFrame(filas, ["concepto", "filas"])
reporte.coalesce(1).write.mode("overwrite").option("header", True).csv(RUTA_REPORTE)

print("\n===== REPORTE DE LIMPIEZA =====")
for concepto, n in filas:
    print(f"{concepto:<22} {n:>12,}")

job.commit()
