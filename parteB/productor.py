"""
Productor - Trabajo 2, parte B (Grandes Datos, EAFIT)

Simula lecturas de radiación en tiempo real de las 25 estaciones del proyecto
y las publica en el stream de Kinesis "stream-radiacion" durante DURACION_MIN minutos.

A propósito:
  - repite algunos eventos (mismo id_evento enviado otra vez)
  - envía eventos inválidos (campo faltante o radiación fuera de 0-1400 W/m2)

Al terminar guarda resumen_productor.json con los conteos.

Uso:
    pip install boto3
    (credenciales de AWS Academy en ~/.aws/credentials, ver instrucciones)
    python productor.py
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3

# ---------------- Configuración ----------------
STREAM = "stream-radiacion"
REGION = "us-east-1"
DURACION_MIN = 6              # el enunciado pide al menos 5
EVENTOS_POR_SEGUNDO = 2
PROB_REPETIDO = 0.06          # 6 % de los envíos son repeticiones
PROB_INVALIDO = 0.05          # 5 % de los envíos son inválidos
SEMILLA = 42

CARPETA = os.path.dirname(os.path.abspath(__file__))
RESUMEN = os.path.join(CARPETA, "resumen_productor.json")
HORA_COLOMBIA = timezone(timedelta(hours=-5))

# Las 25 estaciones de la parte A (código IDEAM -> departamento)
ESTACIONES = {
    48015050: "AMAZONAS", 27015070: "ANTIOQUIA", 12015110: "ANTIOQUIA",
    17015010: "SAN_ANDRES", 29035200: "ATLANTICO", 21206960: "BOGOTA",
    29015040: "BOLIVAR", 24035430: "BOYACA", 26155220: "CALDAS",
    44035040: "CAQUETA", 35215020: "CASANARE", 53045040: "CAUCA",
    21055070: "CAUCA", 23195240: "CESAR", 11155030: "CHOCO",
    13035501: "CORDOBA", 23065180: "CUNDINAMARCA", 31095030: "GUAINIA",
    21115170: "HUILA", 15065180: "LA_GUAJIRA", 15015120: "MAGDALENA",
    35185010: "META", 52045080: "NARINO", 16015501: "NORTE_DE_SANTANDER",
    44015060: "PUTUMAYO",
}

random.seed(SEMILLA)
# Radiación "típica" de cada departamento para que la simulación sea creíble
BASE_DEPTO = {d: random.uniform(450, 800) for d in sorted(set(ESTACIONES.values()))}

kinesis = boto3.client("kinesis", region_name=REGION)


def evento_valido():
    codigo = random.choice(list(ESTACIONES))
    depto = ESTACIONES[codigo]
    radiacion = min(max(random.gauss(BASE_DEPTO[depto], 120), 0), 1400)
    return {
        "id_evento": str(uuid.uuid4()),
        "codigo_estacion": codigo,
        "departamento": depto,
        "radiacion_w_m2": round(radiacion, 2),
        "fecha_hora": datetime.now(HORA_COLOMBIA).strftime("%Y-%m-%d %H:%M:%S"),
    }


def evento_invalido():
    evento = evento_valido()
    tipo = random.choice(["sin_departamento", "sin_radiacion", "radiacion_negativa", "radiacion_muy_alta"])
    if tipo == "sin_departamento":
        del evento["departamento"]
    elif tipo == "sin_radiacion":
        del evento["radiacion_w_m2"]
    elif tipo == "radiacion_negativa":
        evento["radiacion_w_m2"] = round(random.uniform(-500, -1), 2)
    else:
        evento["radiacion_w_m2"] = round(random.uniform(1500, 5000), 2)
    return evento


def publicar(evento):
    kinesis.put_record(
        StreamName=STREAM,
        Data=json.dumps(evento).encode("utf-8"),
        PartitionKey=evento.get("departamento", "sin_departamento"),
    )


def main():
    conteo = {"publicados": 0, "unicos_validos": 0, "repetidos": 0, "invalidos": 0}
    enviados_validos = []
    fin = time.time() + DURACION_MIN * 60
    pausa = 1 / EVENTOS_POR_SEGUNDO

    print(f"Publicando en '{STREAM}' durante {DURACION_MIN} minutos... (Ctrl+C para parar)")
    inicio = datetime.now(HORA_COLOMBIA)
    try:
        while time.time() < fin:
            azar = random.random()
            if azar < PROB_REPETIDO and enviados_validos:
                evento = random.choice(enviados_validos)       # mismo id -> repetido
                conteo["repetidos"] += 1
                tipo = "REPETIDO"
            elif azar < PROB_REPETIDO + PROB_INVALIDO:
                evento = evento_invalido()
                conteo["invalidos"] += 1
                tipo = "INVALIDO"
            else:
                evento = evento_valido()
                enviados_validos.append(evento)
                conteo["unicos_validos"] += 1
                tipo = "valido"

            publicar(evento)
            conteo["publicados"] += 1
            if tipo != "valido" or conteo["publicados"] % 20 == 0:
                print(f"[{conteo['publicados']:>4}] {tipo:<9} {json.dumps(evento)}")
            time.sleep(pausa)
    except KeyboardInterrupt:
        print("\nDetenido a mano.")

    conteo["inicio"] = inicio.strftime("%Y-%m-%d %H:%M:%S")
    conteo["fin"] = datetime.now(HORA_COLOMBIA).strftime("%Y-%m-%d %H:%M:%S")
    with open(RESUMEN, "w", encoding="utf-8") as f:
        json.dump(conteo, f, indent=2, ensure_ascii=False)

    print("\n===== RESUMEN DEL PRODUCTOR =====")
    for k in ["publicados", "unicos_validos", "repetidos", "invalidos"]:
        print(f"{k:<16} {conteo[k]:>6}")
    print(f"Comprobación: {conteo['unicos_validos']} + {conteo['repetidos']} + "
          f"{conteo['invalidos']} = {conteo['unicos_validos'] + conteo['repetidos'] + conteo['invalidos']}")
    print(f"Guardado en {RESUMEN}")


if __name__ == "__main__":
    main()
