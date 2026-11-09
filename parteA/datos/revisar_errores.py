"""
Revisa si nasa_power_horario.csv tiene errores antes de decidir si inyectarlos.

Uso:
    python revisar_errores.py

Debe estar en la misma carpeta que:
    nasa_power_horario.csv
    el catálogo del IDEAM (Catálogo...IDEAM...csv)
"""

import os
import glob
import pandas as pd

CARPETA = os.path.dirname(os.path.abspath(__file__))
ARCHIVO = os.path.join(CARPETA, "nasa_power_horario.csv")
_cat = glob.glob(os.path.join(CARPETA, "Cat*logo*IDEAM*.csv"))
CATALOGO = _cat[0] if _cat else None

HORAS_ESPERADAS = 210_384          # 2001-2024 en horas, por estación
COLUMNAS_NUM = ["radiacion_wh_m2", "temperatura_c", "viento_m_s"]

# Rangos razonables para Colombia
RANGOS = {
    "radiacion_wh_m2": (0, 1400),
    "temperatura_c": (-15, 50),
    "viento_m_s": (0, 40),
}


def titulo(texto):
    print("\n" + "=" * 60)
    print(texto)
    print("=" * 60)


df = pd.read_csv(ARCHIVO)

# 1. Tamaño general
titulo("1. Tamaño")
print(f"Filas totales: {len(df):,}")
print(f"Columnas: {list(df.columns)}")
print(f"Cumple 5.000.000: {'SÍ' if len(df) >= 5_000_000 else 'NO'}")

# 2. Filas por estación
titulo("2. Filas por estación (esperadas: 210.384)")
conteo = df.groupby("codigo_estacion").size()
incompletas = conteo[conteo != HORAS_ESPERADAS]
print(f"Estaciones: {conteo.size}")
if incompletas.empty:
    print("Todas las estaciones tienen las filas esperadas")
else:
    print("Estaciones con un número de filas distinto:")
    print(incompletas.to_string())

# 3. Valores vacíos
titulo("3. Valores vacíos (nulos)")
print(df.isna().sum().to_string())

# 4. Código -999 de NASA (sin dato)
titulo("4. Valores -999 (NASA: sin dato)")
for c in COLUMNAS_NUM:
    print(f"{c}: {(df[c] == -999).sum():,}")

# 5. Fuera de rango (sin contar los -999)
titulo("5. Valores fuera de rango (sin contar -999)")
for c, (mn, mx) in RANGOS.items():
    fuera = df[(df[c] != -999) & ((df[c] < mn) | (df[c] > mx))]
    print(f"{c} fuera de [{mn}, {mx}]: {len(fuera):,}")
    if len(fuera):
        print(f"   ejemplos: {fuera[c].head(5).tolist()}")

# 6. Duplicados
titulo("6. Duplicados")
print(f"Filas duplicadas exactas: {df.duplicated().sum():,}")
print(f"Misma estación y misma hora: {df.duplicated(subset=['codigo_estacion', 'fecha_hora']).sum():,}")

# 7. Fechas inválidas
titulo("7. Fechas")
fechas = pd.to_datetime(df["fecha_hora"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
print(f"Fechas que no se pueden leer: {fechas.isna().sum():,}")
print(f"Rango: {fechas.min()}  ->  {fechas.max()}")

# 8. Radiación de noche (debería ser 0)
titulo("8. Radiación de noche (entre 8 pm y 4 am)")
hora = fechas.dt.hour
noche = df[(hora >= 20) | (hora <= 4)]
print(f"Horas nocturnas con radiación > 0: {(noche['radiacion_wh_m2'] > 0).sum():,}")

# 9. Códigos que no están en el catálogo (para el join)
titulo("9. Códigos vs catálogo del IDEAM")
if CATALOGO:
    cat = pd.read_csv(CATALOGO, usecols=["Codigo"])
    no_estan = set(df["codigo_estacion"].unique()) - set(cat["Codigo"])
    print(f"Códigos que no están en el catálogo: {no_estan if no_estan else 'ninguno'}")
    print(f"Códigos repetidos en el catálogo: {cat['Codigo'].duplicated().sum()}")
else:
    print("No encontré el catálogo en la carpeta, se omite")

# Resumen
titulo("RESUMEN")
total_errores = (
    df.isna().any(axis=1).sum()
    + sum((df[c] == -999).sum() for c in COLUMNAS_NUM)
    + df.duplicated().sum()
    + fechas.isna().sum()
)
print(f"Errores encontrados (aprox.): {total_errores:,} de {len(df):,} filas "
      f"({total_errores / len(df) * 100:.4f} %)")
