"""
Lambda de eventos crudos - Trabajo 2, parte B (Grandes Datos, EAFIT)

Segundo consumidor del stream "stream-radiacion".
Guarda en S3 TODOS los eventos tal como llegan (válidos, repetidos e inválidos),
un archivo .jsonl por cada lote que entrega Kinesis (un evento por línea).

Variable de entorno obligatoria:  BUCKET  (nombre del bucket, sin s3://)
"""

import base64
import os
from datetime import datetime, timedelta, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
HORA_COLOMBIA = timezone(timedelta(hours=-5))


def lambda_handler(event, context):
    registros = event.get("Records", [])
    if not registros:
        return {"guardados": 0}

    # El texto de cada evento, sin modificarlo
    lineas = [base64.b64decode(r["kinesis"]["data"]).decode("utf-8") for r in registros]

    ahora = datetime.now(HORA_COLOMBIA)
    primera_secuencia = registros[0]["kinesis"]["sequenceNumber"]
    llave = (f"crudos/fecha={ahora:%Y-%m-%d}/hora={ahora:%H}/"
             f"{ahora:%H%M%S}_{primera_secuencia}.jsonl")

    s3.put_object(Bucket=BUCKET, Key=llave, Body="\n".join(lineas) + "\n")
    print(f"Guardados {len(lineas)} eventos en s3://{BUCKET}/{llave}")
    return {"guardados": len(lineas)}
