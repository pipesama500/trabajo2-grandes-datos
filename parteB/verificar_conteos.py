"""
Verificación de conteos - Trabajo 2, parte B (Grandes Datos, EAFIT)

Compara lo que publicó el productor (resumen_productor.json) con lo que quedó en AWS:
  - eventos_vistos      -> únicos procesados y repetidos descartados
  - eventos_invalidos   -> inválidos descartados
  - ventanas_radiacion  -> suma de los conteos de todas las ventanas
  - S3 crudos/          -> todos los eventos tal como llegaron

Y responde la pregunta del proyecto:
  ¿Qué departamentos recibieron más radiación promedio durante la simulación?

Uso:  python verificar_conteos.py
"""

import json
import os
from collections import defaultdict

import boto3

REGION = "us-east-1"
BUCKET = "felipe-trabajo2-streaming"
PREFIJO_CRUDOS = "crudos/"
CARPETA = os.path.dirname(os.path.abspath(__file__))
RESUMEN = os.path.join(CARPETA, "resumen_productor.json")

dynamodb = boto3.resource("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def leer_tabla(nombre):
    """Lee todos los ítems de una tabla de DynamoDB (con paginación)."""
    tabla = dynamodb.Table(nombre)
    items, kwargs = [], {}
    while True:
        resp = tabla.scan(**kwargs)
        items.extend(resp["Items"])
        if "LastEvaluatedKey" not in resp:
            return items
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def contar_crudos():
    """Cuenta los eventos (líneas) de todos los archivos en S3 crudos/."""
    total, archivos = 0, 0
    paginador = s3.get_paginator("list_objects_v2")
    for pagina in paginador.paginate(Bucket=BUCKET, Prefix=PREFIJO_CRUDOS):
        for obj in pagina.get("Contents", []):
            cuerpo = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read().decode("utf-8")
            total += sum(1 for linea in cuerpo.splitlines() if linea.strip())
            archivos += 1
    return total, archivos


def titulo(texto):
    print("\n" + "=" * 55)
    print(texto)
    print("=" * 55)


# ---------------- Lo que dice el productor ----------------
with open(RESUMEN, encoding="utf-8") as f:
    prod = json.load(f)

# ---------------- Lo que quedó en AWS ----------------
vistos = leer_tabla("eventos_vistos")
invalidos = leer_tabla("eventos_invalidos")
ventanas = leer_tabla("ventanas_radiacion")
crudos, n_archivos = contar_crudos()

unicos = len(vistos)
repetidos = sum(int(v.get("repeticiones", 0)) for v in vistos)
n_invalidos = len(invalidos)
suma_ventanas = sum(int(v["conteo_eventos"]) for v in ventanas)

titulo("CONTEOS: PRODUCTOR vs AWS")
print(f"{'':<28}{'Productor':>12}{'AWS':>12}")
filas = [
    ("Publicados / crudos en S3", prod["publicados"], crudos),
    ("Únicos procesados", prod["unicos_validos"], unicos),
    ("Repetidos descartados", prod["repetidos"], repetidos),
    ("Inválidos descartados", prod["invalidos"], n_invalidos),
]
for nombre, a, b in filas:
    marca = "OK" if a == b else "NO CUADRA"
    print(f"{nombre:<28}{a:>12}{b:>12}   {marca}")

titulo("COMPROBACIÓN")
suma = unicos + repetidos + n_invalidos
print(f"Únicos + repetidos + inválidos = {unicos} + {repetidos} + {n_invalidos} = {suma}")
print(f"Publicados por el productor    = {prod['publicados']}")
print("Cuadra" if suma == prod["publicados"] else "NO cuadra")
print(f"\nSuma de conteos de todas las ventanas = {suma_ventanas} "
      f"({'igual' if suma_ventanas == unicos else 'distinto'} a los únicos procesados)")
print(f"Archivos en S3 crudos/: {n_archivos}")

titulo("MOTIVOS DE LOS INVÁLIDOS")
motivos = defaultdict(int)
for i in invalidos:
    motivo = i["motivo"].split(" (")[0]          # agrupa "radiacion fuera de rango (...)"
    motivos[motivo] += 1
for motivo, n in sorted(motivos.items(), key=lambda x: -x[1]):
    print(f"{motivo:<40}{n:>5}")

titulo("PREGUNTA: ¿Qué departamentos recibieron más radiación?")
acum = defaultdict(lambda: [0, 0.0])                 # departamento -> [eventos, suma]
for v in ventanas:
    n = int(v["conteo_eventos"])
    acum[v["departamento"]][0] += n
    acum[v["departamento"]][1] += float(v["radiacion_promedio_w_m2"]) * n
ranking = sorted(acum.items(), key=lambda x: -(x[1][1] / x[1][0]))
print(f"{'#':<4}{'Departamento':<24}{'Eventos':>8}{'Promedio W/m2':>16}")
for i, (dep, (n, s)) in enumerate(ranking, start=1):
    print(f"{i:<4}{dep:<24}{n:>8}{s / n:>16.2f}")
print(f"\nVentanas guardadas en DynamoDB: {len(ventanas)}")
