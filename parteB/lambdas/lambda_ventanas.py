"""
Lambda de ventanas - Trabajo 2, parte B (Grandes Datos, EAFIT)

Lee el stream de Kinesis "stream-radiacion" con ventanas de 1 minuto (tumbling window)
y, por cada evento:
  1. Si es inválido  -> lo guarda en la tabla "eventos_invalidos" con su motivo
  2. Si es repetido  -> no lo cuenta; suma 1 a "repeticiones" en "eventos_vistos"
  3. Si es válido    -> lo registra en "eventos_vistos" y lo suma a la ventana
Al cerrar cada ventana guarda, por departamento, el conteo y la radiación promedio
en la tabla "ventanas_radiacion".

Un evento es REPETIDO cuando su "id_evento" ya se había recibido antes.
"""

import base64
import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
TABLA_VENTANAS = dynamodb.Table(os.environ.get("TABLA_VENTANAS", "ventanas_radiacion"))
TABLA_INVALIDOS = dynamodb.Table(os.environ.get("TABLA_INVALIDOS", "eventos_invalidos"))
TABLA_VISTOS = dynamodb.Table(os.environ.get("TABLA_VISTOS", "eventos_vistos"))

HORA_COLOMBIA = timezone(timedelta(hours=-5))
RADIACION_MIN, RADIACION_MAX = 0, 1400          # W/m2
CAMPOS_OBLIGATORIOS = ["id_evento", "codigo_estacion", "departamento",
                       "radiacion_w_m2", "fecha_hora"]


def hora_local(texto_utc):
    """'2026-11-05T19:30:00Z' (UTC) -> '2026-11-05 14:30:00' (hora Colombia)"""
    fecha = datetime.fromisoformat(texto_utc.replace("Z", "+00:00"))
    return fecha.astimezone(HORA_COLOMBIA).strftime("%Y-%m-%d %H:%M:%S")


def ahora_local():
    return datetime.now(HORA_COLOMBIA).strftime("%Y-%m-%d %H:%M:%S")


def motivo_invalido(evento):
    """Devuelve el motivo si el evento es inválido, o None si es válido."""
    for campo in CAMPOS_OBLIGATORIOS:
        if evento.get(campo) in (None, ""):
            return f"falta el campo {campo}"
    try:
        radiacion = float(evento["radiacion_w_m2"])
    except (TypeError, ValueError):
        return "radiacion no numerica"
    if not RADIACION_MIN <= radiacion <= RADIACION_MAX:
        return f"radiacion fuera de rango ({radiacion} no esta entre {RADIACION_MIN} y {RADIACION_MAX})"
    return None


def guardar_invalido(id_evento, texto_original, motivo):
    TABLA_INVALIDOS.put_item(Item={
        "id_evento": id_evento,
        "motivo": motivo,
        "evento_original": texto_original,
        "recibido": ahora_local(),
    })


def es_primera_vez(evento):
    """Registra el evento en 'eventos_vistos'. Devuelve False si ya existía (repetido)."""
    try:
        TABLA_VISTOS.put_item(
            Item={
                "id_evento": evento["id_evento"],
                "departamento": evento["departamento"],
                "repeticiones": 0,
                "recibido": ahora_local(),
            },
            ConditionExpression="attribute_not_exists(id_evento)",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        # Ya existía: es repetido -> se cuenta la repetición, no el evento
        TABLA_VISTOS.update_item(
            Key={"id_evento": evento["id_evento"]},
            UpdateExpression="ADD repeticiones :uno",
            ExpressionAttributeValues={":uno": 1},
        )
        return False


def lambda_handler(event, context):
    # "state" guarda lo acumulado de la ventana actual entre invocaciones
    estado = event.get("state") or {}

    for registro in event.get("Records", []):
        secuencia = registro["kinesis"]["sequenceNumber"]
        texto = base64.b64decode(registro["kinesis"]["data"]).decode("utf-8")

        # 1. Inválidos
        try:
            evento = json.loads(texto)
        except json.JSONDecodeError:
            guardar_invalido(f"sin_id_{secuencia}", texto, "no es un JSON valido")
            continue
        motivo = motivo_invalido(evento)
        if motivo:
            id_evento = evento.get("id_evento") or f"sin_id_{secuencia}"
            guardar_invalido(str(id_evento), texto, motivo)
            continue

        # 2. Repetidos
        if not es_primera_vez(evento):
            continue

        # 3. Válidos: se suman a la ventana
        dep = evento["departamento"]
        acumulado = estado.setdefault(dep, {"conteo": 0, "suma": 0.0})
        acumulado["conteo"] += 1
        acumulado["suma"] += float(evento["radiacion_w_m2"])

    # Al cerrar la ventana de 1 minuto se guardan los resultados
    if event.get("isFinalInvokeForWindow"):
        inicio = hora_local(event["window"]["start"])
        fin = hora_local(event["window"]["end"])
        for dep, acumulado in estado.items():
            promedio = round(acumulado["suma"] / acumulado["conteo"], 2)
            TABLA_VENTANAS.put_item(Item={
                "departamento": dep,
                "ventana_inicio": inicio,
                "ventana_fin": fin,
                "conteo_eventos": acumulado["conteo"],
                "radiacion_promedio_w_m2": Decimal(str(promedio)),
            })
        print(f"Ventana {inicio} - {fin}: {len(estado)} departamentos, "
              f"{sum(a['conteo'] for a in estado.values())} eventos")
        return {"state": {}}

    return {"state": estado}
