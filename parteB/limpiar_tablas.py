"""
Limpieza antes de grabar - Trabajo 2, parte B (Grandes Datos, EAFIT)

Borra los resultados de las pruebas para que la corrida del video empiece en cero:
  - todos los ítems de ventanas_radiacion, eventos_invalidos y eventos_vistos
  - todos los archivos de s3://BUCKET/crudos/
  - resumen_productor.json (si existe)

Las tablas y el bucket NO se borran, solo su contenido.

Uso:  python limpiar_tablas.py
"""

import os

import boto3

REGION = "us-east-1"
BUCKET = "felipe-trabajo2-streaming"
PREFIJO_CRUDOS = "crudos/"
TABLAS = {
    "ventanas_radiacion": ["departamento", "ventana_inicio"],
    "eventos_invalidos": ["id_evento"],
    "eventos_vistos": ["id_evento"],
}
CARPETA = os.path.dirname(os.path.abspath(__file__))

dynamodb = boto3.resource("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

respuesta = input("Esto borra los resultados de las tablas y de S3 crudos/. ¿Seguir? (s/n): ")
if respuesta.strip().lower() != "s":
    raise SystemExit("Cancelado.")

# 1. Vaciar las tablas de DynamoDB
for nombre, llaves in TABLAS.items():
    tabla = dynamodb.Table(nombre)
    borrados, kwargs = 0, {"ProjectionExpression": ", ".join(f"#k{i}" for i in range(len(llaves))),
                           "ExpressionAttributeNames": {f"#k{i}": k for i, k in enumerate(llaves)}}
    with tabla.batch_writer() as lote:
        while True:
            resp = tabla.scan(**kwargs)
            for item in resp["Items"]:
                lote.delete_item(Key={k: item[k] for k in llaves})
                borrados += 1
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    print(f"{nombre}: {borrados} ítems borrados")

# 2. Vaciar s3://BUCKET/crudos/
borrados = 0
paginador = s3.get_paginator("list_objects_v2")
for pagina in paginador.paginate(Bucket=BUCKET, Prefix=PREFIJO_CRUDOS):
    objetos = [{"Key": o["Key"]} for o in pagina.get("Contents", [])]
    if objetos:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objetos})
        borrados += len(objetos)
print(f"s3://{BUCKET}/{PREFIJO_CRUDOS}: {borrados} archivos borrados")

# 3. Resumen anterior del productor
resumen = os.path.join(CARPETA, "resumen_productor.json")
if os.path.exists(resumen):
    os.remove(resumen)
    print("resumen_productor.json borrado")

print("\nListo: todo en cero para la corrida del video.")
