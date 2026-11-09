"""
Inyecta 100.000 filas con errores a propósito en los datos de NASA POWER
para probar la limpieza en AWS Glue.

Las filas con error se AGREGAN como filas extra (copias alteradas de filas reales),
así que el original no pierde nada:
    original:  5.259.600 filas
    sucio:     5.359.600 filas
    tras limpiar en Glue deben quedar 5.259.600

Uso:
    python inyectar_errores.py

Entrada:  nasa_power_horario.csv
Salida:   nasa_power_horario_sucio.csv
          errores_inyectados.txt  (resumen)
"""

import os
import numpy as np
import pandas as pd

CARPETA = os.path.dirname(os.path.abspath(__file__))
ENTRADA = os.path.join(CARPETA, "nasa_power_horario.csv")
SALIDA = os.path.join(CARPETA, "nasa_power_horario_sucio.csv")
RESUMEN = os.path.join(CARPETA, "errores_inyectados.txt")

SEMILLA = 42
rng = np.random.default_rng(SEMILLA)

# Cuántas filas de cada tipo de error (total 100.000)
CANTIDADES = {
    "duplicado_exacto": 30_000,
    "valor_vacio": 20_000,
    "valor_menos_999": 20_000,
    "fuera_de_rango": 15_000,
    "fecha_invalida": 10_000,
    "estacion_inexistente": 5_000,
}

COLUMNAS_NUM = ["radiacion_wh_m2", "temperatura_c", "viento_m_s"]

print("Leyendo datos originales...")
df = pd.read_csv(ENTRADA, dtype={"fecha_hora": str})
n_original = len(df)
print(f"Filas originales: {n_original:,}")


def muestra(n):
    """Copia n filas reales elegidas al azar."""
    idx = rng.choice(n_original, size=n, replace=False)
    return df.iloc[idx].copy().reset_index(drop=True)


def columna_al_azar(n):
    return rng.choice(COLUMNAS_NUM, size=n)


partes = []

# 1. Duplicados exactos
dup = muestra(CANTIDADES["duplicado_exacto"])
partes.append(dup)

# 2. Valores vacíos en una columna numérica
vac = muestra(CANTIDADES["valor_vacio"])
cols = columna_al_azar(len(vac))
for c in COLUMNAS_NUM:
    vac.loc[cols == c, c] = np.nan
partes.append(vac)

# 3. -999 (código de NASA para "sin dato")
m999 = muestra(CANTIDADES["valor_menos_999"])
cols = columna_al_azar(len(m999))
for c in COLUMNAS_NUM:
    m999.loc[cols == c, c] = -999.0
partes.append(m999)

# 4. Fuera de rango (valores físicamente imposibles)
fr = muestra(CANTIDADES["fuera_de_rango"])
cols = columna_al_azar(len(fr))


def alto_o_bajo(n, bajo, alto):
    """Mitad de los valores en el rango 'bajo', mitad en el rango 'alto'."""
    es_alto = rng.random(n) < 0.5
    return np.where(es_alto, rng.uniform(*alto, n), rng.uniform(*bajo, n)).round(2)


m = cols == "radiacion_wh_m2"       # válido: 0 a 1400
fr.loc[m, "radiacion_wh_m2"] = alto_o_bajo(m.sum(), (-500, -1), (1500, 5000))
m = cols == "temperatura_c"         # válido: -15 a 50
fr.loc[m, "temperatura_c"] = alto_o_bajo(m.sum(), (-60, -30), (60, 90))
m = cols == "viento_m_s"            # válido: 0 a 40
fr.loc[m, "viento_m_s"] = alto_o_bajo(m.sum(), (-20, -1), (60, 150))
partes.append(fr)

# 5. Fechas mal escritas
fi = muestra(CANTIDADES["fecha_invalida"])
malas = np.array(["2015-13-45 25:00:00", "fecha_invalida", "00/00/0000", "2010-02-30 99:99:99", "N/A"])
fi["fecha_hora"] = rng.choice(malas, len(fi))
partes.append(fi)

# 6. Código de estación que no existe en el catálogo del IDEAM
ei = muestra(CANTIDADES["estacion_inexistente"])
ei["codigo_estacion"] = rng.choice([99999999, 11111111, 12345678], len(ei))
partes.append(ei)

# Unir original + errores y mezclar el orden
sucio = pd.concat([df] + partes, ignore_index=True)
sucio = sucio.sample(frac=1, random_state=SEMILLA).reset_index(drop=True)

print("Guardando archivo sucio...")
sucio.to_csv(SALIDA, index=False)

total_err = sum(CANTIDADES.values())
lineas = [
    "ERRORES INYECTADOS A PROPÓSITO (semilla 42)",
    "=" * 45,
    *[f"{k:<22} {v:>10,}" for k, v in CANTIDADES.items()],
    "-" * 45,
    f"{'Total errores':<22} {total_err:>10,}",
    f"{'Filas originales':<22} {n_original:>10,}",
    f"{'Filas archivo sucio':<22} {len(sucio):>10,}",
    f"{'Esperadas tras limpiar':<22} {n_original:>10,}",
]
texto = "\n".join(lineas)
with open(RESUMEN, "w", encoding="utf-8") as f:
    f.write(texto)
print("\n" + texto)
print(f"\nArchivo: {SALIDA}")
