"""
Descarga datos HORARIOS de NASA POWER para 25 estaciones del IDEAM (2001-2024).
Resultado: nasa_power_horario.csv (~5.259.600 filas)

Uso:
    pip install pandas requests
    python descargar_nasa_power.py

Necesita en la misma carpeta el catálogo del IDEAM:
    Catálogo_Nacional_de_Estaciones_del_IDEAM_20261024_1.csv
"""

import os
import glob
import time
import requests
import pandas as pd

# ---------------- Configuración ----------------
# Todo se busca y se guarda en la carpeta donde está este script
CARPETA = os.path.dirname(os.path.abspath(__file__))

# Busca el catálogo aunque el nombre tenga espacios o tildes distintas
_candidatos = glob.glob(os.path.join(CARPETA, "Cat*logo*IDEAM*.csv"))
if not _candidatos:
    raise FileNotFoundError(f"No encontré el catálogo del IDEAM en {CARPETA}")
CATALOGO = _candidatos[0]

CARPETA_TEMP = os.path.join(CARPETA, "nasa_por_estacion")   # un CSV por estación (permite reanudar)
SALIDA = os.path.join(CARPETA, "nasa_power_horario.csv")

ANIO_INICIO = 2001
ANIO_FIN = 2024
PARAMETROS = "ALLSKY_SFC_SW_DWN,T2M,WS2M"   # radiación, temperatura, viento
URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"

ESTACIONES = [
    48015050, 27015070, 12015110, 17015010, 29035200,
    21206960, 29015040, 24035430, 26155220, 44035040,
    35215020, 53045040, 21055070, 23195240, 11155030,
    13035501, 23065180, 31095030, 21115170, 15065180,
    15015120, 35185010, 52045080, 16015501, 44015060,
]


# ---------------- Funciones ----------------
def cargar_coordenadas():
    """Lee el catálogo y devuelve código, latitud y longitud de las 25 estaciones."""
    cat = pd.read_csv(CATALOGO)
    cat = cat[cat["Codigo"].isin(ESTACIONES)].copy()
    # El catálogo usa coma decimal: "6,2246389" -> 6.2246389
    cat["lat"] = cat["LATITUD"].astype(str).str.replace(",", ".").astype(float)
    cat["lon"] = cat["LONGITUD"].astype(str).str.replace(",", ".").astype(float)
    faltan = set(ESTACIONES) - set(cat["Codigo"])
    if faltan:
        raise ValueError(f"No se encontraron en el catálogo: {faltan}")
    return cat[["Codigo", "lat", "lon"]]


def descargar_anio(lat, lon, anio, intentos=5):
    """Pide a la API un año de datos horarios para un punto."""
    params = {
        "parameters": PARAMETROS,
        "community": "RE",
        "longitude": lon,
        "latitude": lat,
        "start": f"{anio}0101",
        "end": f"{anio}1231",
        "format": "JSON",
        "time-standard": "LST",   # hora local de Colombia
    }
    for i in range(intentos):
        try:
            r = requests.get(URL, params=params, timeout=120)
            if r.status_code == 200:
                return r.json()["properties"]["parameter"]
            print(f"    HTTP {r.status_code}, reintento {i + 1}/{intentos}")
        except Exception as e:
            print(f"    Error: {e}, reintento {i + 1}/{intentos}")
        time.sleep(10 * (i + 1))
    raise RuntimeError(f"No se pudo descargar {anio} para ({lat}, {lon})")


def descargar_estacion(codigo, lat, lon):
    """Descarga todos los años de una estación y la guarda en su propio CSV."""
    archivo = os.path.join(CARPETA_TEMP, f"{codigo}.csv")
    if os.path.exists(archivo):
        print(f"  {codigo}: ya existe, se omite")
        return

    partes = []
    for anio in range(ANIO_INICIO, ANIO_FIN + 1):
        datos = descargar_anio(lat, lon, anio)
        df = pd.DataFrame(datos)                  # índice = "YYYYMMDDHH"
        df.index.name = "fecha_hora"
        df = df.reset_index()
        partes.append(df)
        print(f"  {codigo} - {anio}: {len(df)} filas")
        time.sleep(1)                             # no saturar la API

    df = pd.concat(partes, ignore_index=True)
    df.insert(0, "codigo_estacion", codigo)
    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], format="%Y%m%d%H").dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df.rename(columns={
        "ALLSKY_SFC_SW_DWN": "radiacion_wh_m2",
        "T2M": "temperatura_c",
        "WS2M": "viento_m_s",
    })
    df.to_csv(archivo, index=False)


def unir_archivos():
    """Une los 25 CSV en uno solo."""
    archivos = [os.path.join(CARPETA_TEMP, f"{c}.csv") for c in ESTACIONES]
    with open(SALIDA, "w", encoding="utf-8") as out:
        for i, a in enumerate(archivos):
            with open(a, encoding="utf-8") as f:
                if i > 0:
                    next(f)                       # saltar encabezado repetido
                out.writelines(f)


# ---------------- Programa principal ----------------
if __name__ == "__main__":
    print(f"Catálogo: {CATALOGO}")
    os.makedirs(CARPETA_TEMP, exist_ok=True)
    coords = cargar_coordenadas()

    for n, fila in enumerate(coords.itertuples(index=False), start=1):
        print(f"[{n}/{len(coords)}] Estación {fila.Codigo} ({fila.lat}, {fila.lon})")
        descargar_estacion(fila.Codigo, fila.lat, fila.lon)

    unir_archivos()
    total = sum(1 for _ in open(SALIDA, encoding="utf-8")) - 1
    print(f"\nListo: {SALIDA} con {total:,} registros")
    print("Cumple 5.000.000" if total >= 5_000_000 else "NO llega a 5.000.000")
