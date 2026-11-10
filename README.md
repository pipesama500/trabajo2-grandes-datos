# Trabajo 2: pipeline batch y streaming en AWS

**Curso:** Almacenamiento y Procesamiento de Grandes Datos — Universidad EAFIT
**Autor:** Felipe Martínez (trabajo individual)
**Video:** https://youtu.be/BJRjzby34to

Proyecto integrador de referencia: plataforma para evaluar el potencial de paneles solares residenciales en Colombia, con datos de radiación de **NASA POWER** y estaciones del **IDEAM**.

- **Parte A (batch):** pipeline con AWS Glue (PySpark), orquestado por AWS Step Functions e iniciado por Amazon EventBridge Scheduler. Resultados consultados en Amazon Athena.
- **Parte B (streaming):** flujo con Amazon Kinesis Data Streams, dos funciones AWS Lambda, Amazon DynamoDB y Amazon S3, con ventanas de 1 minuto.

Todos los recursos se crearon **manualmente desde la consola web de AWS**, sin infraestructura como código.

---

## Estructura del repositorio

```
trabajo2-grandes-datos/
├── README.md
├── parteA/
│   ├── datos/
│   │   ├── descargar_nasa_power.py     # descarga datos horarios de NASA POWER (25 estaciones)
│   │   ├── revisar_errores.py          # revisa la calidad de los datos descargados
│   │   └── inyectar_errores.py         # agrega 100.000 filas con errores para probar la limpieza
│   ├── glue/
│   │   ├── glue_limpieza_join.py       # trabajo inicial: limpieza + join + Parquet particionado
│   │   ├── glue_rama1_hsp.py           # rama 1: horas sol pico por departamento
│   │   └── glue_rama2_meses.py         # rama 2: radiación por departamento y mes
│   ├── step_functions/
│   │   └── maquina_estados.json        # definición de la máquina de estados
│   └── athena/
│       └── consultas.sql               # tablas y consultas de las dos agrupaciones
└── parteB/
    ├── productor.py                    # publica eventos en Kinesis durante 6 minutos
    ├── lambdas/
    │   ├── lambda_ventanas.py          # consumidor 1: ventanas de 1 minuto -> DynamoDB
    │   └── lambda_crudos.py            # consumidor 2: eventos crudos -> S3
    ├── verificar_conteos.py            # comprueba que los conteos cuadren
    ├── limpiar_tablas.py               # deja tablas y S3 en cero antes de una corrida
    └── consultas_dynamodb.sql          # consultas PartiQL
```

Los archivos de datos (`*.csv`) no se suben al repositorio por su tamaño; se generan con los scripts de `parteA/datos/`.

---

# Parte A: pipeline batch

## Arquitectura

```
EventBridge Scheduler (horario)
        │
        ▼
Step Functions: pipeline-nasa-trabajo2
        │
        ├─ 1. Glue: glue-limpieza-join ──► s3://<BUCKET>/processed/  (Parquet, partición departamento)
        │
        └─ 2. Parallel
              ├─ Glue: glue-rama1-hsp   ──► s3://<BUCKET>/agg1/
              └─ Glue: glue-rama2-meses ──► s3://<BUCKET>/agg2/
                                                   │
                                                   ▼
                                           Amazon Athena
```

**Región:** `us-east-2` (Ohio).

## Datos

| Conjunto | Fuente | Registros | Formato |
|---|---|---|---|
| Radiación, temperatura y viento horarios | NASA POWER (API), 25 estaciones del IDEAM, 2001–2024 | 5.259.600 reales + 100.000 con errores = **5.359.600** | CSV |
| Catálogo Nacional de Estaciones del IDEAM | datos.gov.co | ~9.900 estaciones | CSV |

**Columna en común para el join:** código de la estación (`codigo_estacion` en NASA = `Codigo` en el catálogo). NASA POWER entrega datos por coordenadas; los datos se descargaron en las coordenadas de cada estación del IDEAM y a cada fila se le agregó el código de su estación.

Columnas del CSV de NASA: `codigo_estacion, fecha_hora, radiacion_wh_m2, temperatura_c, viento_m_s`.

## Paso 1: preparar los datos (en el PC)

Requisitos: Python 3.10+.

```bash
pip install pandas requests
```

Copie el catálogo del IDEAM (`Catálogo_Nacional_de_Estaciones_del_IDEAM_*.csv`) en `parteA/datos/` y ejecute en esa carpeta:

```bash
# 1. Descargar los datos horarios (600 peticiones a la API, 30-60 min; se puede reanudar)
python descargar_nasa_power.py
#    -> nasa_power_horario.csv (5.259.600 filas)

# 2. Revisar la calidad (resultado obtenido: 0 errores)
python revisar_errores.py

# 3. Agregar 100.000 filas con errores para probar la limpieza (semilla fija 42)
python inyectar_errores.py
#    -> nasa_power_horario_sucio.csv (5.359.600 filas) y errores_inyectados.txt
```

Errores inyectados a propósito:

| Error | Filas |
|---|---|
| Duplicados exactos | 30.000 |
| Valores vacíos | 20.000 |
| Valor `-999` (código de NASA para "sin dato") | 20.000 |
| Fuera de rango | 15.000 |
| Fecha mal escrita | 10.000 |
| Estación inexistente en el catálogo | 5.000 |

## Paso 2: S3 (consola web)

```
S3 -> Create bucket
   Bucket name: <BUCKET>          (ej. felipe-trabajo2-nasa)
   Región: us-east-2
   Resto por defecto -> Create bucket
```

Crear las carpetas y subir los archivos:

```
raw/nasa/        -> nasa_power_horario_sucio.csv
raw/catalogo/    -> catalogo_ideam.csv   (catálogo renombrado sin tildes ni espacios)
processed/
agg1/
agg2/
scripts/
```

## Paso 3: rol de IAM para Glue (consola web)

```
IAM -> Roles -> Create role
   Trusted entity type: AWS service
   Use case: Glue
   Permissions: AWSGlueServiceRole, AmazonS3FullAccess
   Role name: GlueRoleTrabajo2
   Botón: Create role
```

## Paso 4: trabajos de AWS Glue (consola web)

Crear tres trabajos con la misma configuración:

```
AWS Glue -> ETL jobs -> Script editor
   Engine: Spark -> Upload script -> (seleccionar el .py)
   Pestaña Job details:
      IAM Role: GlueRoleTrabajo2
      Glue version: 5.0
      Worker type: G.1X
      Requested number of workers: 2
      Job bookmark: Disable
      Job timeout: 20
      Advanced properties:
         Script path: s3://<BUCKET>/scripts/
         Job parameters: --BUCKET = <BUCKET>
   Botón: Save
```

| Nombre del trabajo | Script |
|---|---|
| `glue-limpieza-join` | `parteA/glue/glue_limpieza_join.py` |
| `glue-rama1-hsp` | `parteA/glue/glue_rama1_hsp.py` |
| `glue-rama2-meses` | `parteA/glue/glue_rama2_meses.py` |

**Qué hace el trabajo inicial (`glue_limpieza_join.py`):**
1. Lee los dos CSV desde `raw/` (NASA como texto, para detectar valores mal escritos).
2. Prepara el catálogo: coma decimal -> punto, nombre de departamento sin tildes ni espacios, una fila por estación.
3. Hace un **left join** (con `broadcast` del catálogo) por el código de estación.
4. Marca el motivo de descarte de cada fila: código inválido, fecha inválida, valor vacío, `-999`, fuera de rango o estación inexistente. Luego elimina duplicados.
5. Escribe Parquet en `processed/`, **particionado por `departamento`**.
6. Escribe un reporte de limpieza en `reportes/limpieza/`.

**Justificación de la partición por departamento:** las preguntas del proyecto se hacen por región; una consulta sobre un departamento lee solo su carpeta; quedan 23 particiones de tamaño similar (21 de ~210.000 filas y 2 de ~420.000). Particionar por fecha generaría cientos de miles de archivos pequeños.

**Ramas:**

| Rama | Pregunta del proyecto | Cálculo | Salida |
|---|---|---|---|
| 1 | ¿Qué departamentos tienen más horas sol pico (HSP)? | Radiación de cada día ÷ 1000 = HSP del día; promedio por departamento; ranking | `agg1/` (23 filas) |
| 2 | ¿En qué meses hay más radiación en cada departamento? | Radiación diaria (kWh/m²) promedio por departamento y mes; ranking de meses por departamento | `agg2/` (276 filas) |

## Paso 5: máquina de estados en Step Functions (consola web)

```
Step Functions -> State machines -> Create state machine
   Blank -> pestaña Code -> pegar parteA/step_functions/maquina_estados.json
   Pestaña Config:
      State machine name: pipeline-nasa-trabajo2
      Type: Standard
      Permissions: Automatic permissions (crea el rol)
   Botón: Create
```

Prueba manual: `Start execution` con input `{}`. Cada paso usa `glue:startJobRun.sync`, por lo que la máquina espera a que cada trabajo de Glue termine; las dos ramas solo arrancan si el trabajo inicial termina con éxito.

## Paso 6: horario en EventBridge Scheduler (consola web)

```
Amazon EventBridge -> Scheduler -> Schedules -> Create schedule
   Schedule name: horario-pipeline-nasa
   Occurrence: One-time schedule
   Date and time: <hora deseada>
   Time zone: America/Bogota
   Flexible time window: Off
   Next -> Target: AWS Step Functions -> StartExecution
      State machine: pipeline-nasa-trabajo2
      Input: {}
   Next -> Action after schedule completion: NONE
      Permissions: Create new role for this schedule
   Botón: Create schedule
```

Verificar en `Step Functions -> pipeline-nasa-trabajo2 -> Executions` que aparece una ejecución con hora de inicio igual a la del horario y estado **Succeeded**.

## Paso 7: consultas en Athena (consola web)

```
Athena -> Query editor -> Settings -> Manage
   Query result location: s3://<BUCKET>/athena-results/
```

Ejecutar los bloques de `parteA/athena/consultas.sql` (reemplazando `<BUCKET>`).

## Resultados de la parte A

Reporte de limpieza (`s3://<BUCKET>/reportes/limpieza/`):

| Concepto | Filas |
|---|---|
| Total leídas | 5.359.600 |
| Descartadas (todos los motivos) | 100.000 |
| **Total limpias** | **5.259.600** |

Las filas limpias coinciden exactamente con los datos reales de NASA antes de inyectar errores.

---

# Parte B: streaming

## Arquitectura

```
productor.py (PC)
      │  put_record
      ▼
Kinesis Data Streams: stream-radiacion (1 shard)
      │
      ├──► Lambda: lambda-ventanas (tumbling window 60 s)
      │        ├─► DynamoDB: ventanas_radiacion   (conteo y promedio por departamento y minuto)
      │        ├─► DynamoDB: eventos_invalidos    (eventos descartados + motivo)
      │        └─► DynamoDB: eventos_vistos       (ids recibidos; detecta repetidos)
      │
      └──► Lambda: lambda-crudos
               └─► S3: s3://<BUCKET_STREAMING>/crudos/fecha=.../hora=.../*.jsonl
```

**Cuenta:** AWS Academy Learner Lab. **Región:** `us-east-1`.

> En `us-east-1`, CloudFormation muestra un único stack creado automáticamente por AWS Academy al iniciar el laboratorio (descripción: *"associate Learner Lab template (academy)"*). No se creó ningún stack para este trabajo.

## Decisión: AWS Lambda en lugar de Amazon Managed Service for Apache Flink

El enunciado sugiere Amazon Managed Service for Apache Flink para calcular las ventanas, pero también permite AWS Lambda. Se eligió **Lambda** porque el volumen de datos es pequeño:

- El productor publica unos **2 eventos por segundo** durante 6 minutos (~600 eventos por corrida), muy por debajo de lo que justifica un motor de streaming como Flink.
- El trigger de Kinesis en Lambda ya trae **ventanas tumbling integradas** (*Tumbling window duration* = 60 s), suficientes para calcular el conteo y el promedio por minuto.
- **Costo:** Lambda cobra solo por ejecución; Flink cobra por hora mientras la aplicación está encendida, aunque no lleguen datos.
- **Simplicidad:** el código es una función de Python que se crea y se pega en la consola, sin montar una aplicación Flink ni sus conectores.

Con volúmenes mucho mayores (miles de eventos por segundo), ventanas más complejas o procesamiento por tiempo del evento, Flink sería la opción adecuada.

## El evento

```json
{"id_evento": "3f2a9c1e-...", "codigo_estacion": 27015070, "departamento": "ANTIOQUIA",
 "radiacion_w_m2": 650.3, "fecha_hora": "2026-11-09 01:20:15"}
```

- **Categoría:** `departamento`. **Campo numérico:** `radiacion_w_m2` (válido entre 0 y 1400 W/m²).
- **Repetido:** evento con un `id_evento` ya recibido.
- **Inválido:** falta un campo obligatorio, o la radiación no es numérica o está fuera de rango.
- Datos simulados a partir de las 25 estaciones de la parte A.

## Paso 1: stream de Kinesis (consola web)

```
Kinesis -> Data streams -> Create data stream
   Data stream name: stream-radiacion
   Capacity mode: Provisioned
   Provisioned shards: 1
   Botón: Create data stream
```

## Paso 2: tablas de DynamoDB (consola web)

```
DynamoDB -> Tables -> Create table   (resto por defecto)
```

| Table name | Partition key | Sort key |
|---|---|---|
| `ventanas_radiacion` | `departamento` (String) | `ventana_inicio` (String) |
| `eventos_invalidos` | `id_evento` (String) | — |
| `eventos_vistos` | `id_evento` (String) | — |

## Paso 3: bucket de S3 (consola web)

```
S3 -> Create bucket
   Bucket name: <BUCKET_STREAMING>     (ej. felipe-trabajo2-streaming)
   Botón: Create bucket
```

## Paso 4: funciones Lambda (consola web)

**lambda-ventanas**

```
Lambda -> Create function -> Author from scratch
   Function name: lambda-ventanas
   Runtime: Python 3.12
   Additional settings -> Execution role: Use an existing role -> LabRole
   Botón: Create function
Code: pegar parteB/lambdas/lambda_ventanas.py en lambda_function.py -> Deploy
Configuration -> General configuration -> Timeout: 1 min
Add trigger: Kinesis
   Kinesis stream: stream-radiacion
   Batch size: 100
   Starting position: Latest
   Additional settings -> Tumbling window duration: 60
   Botón: Add
```

**lambda-crudos**

```
Lambda -> Create function -> Author from scratch
   Function name: lambda-crudos
   Runtime: Python 3.12
   Execution role: LabRole
Code: pegar parteB/lambdas/lambda_crudos.py -> Deploy
Configuration -> General configuration -> Timeout: 1 min
Configuration -> Environment variables: BUCKET = <BUCKET_STREAMING>
Add trigger: Kinesis
   Kinesis stream: stream-radiacion
   Batch size: 100
   Starting position: Latest
   (sin tumbling window)
```

**Cómo calcula las ventanas `lambda-ventanas`:** el trigger con *tumbling window* de 60 s agrupa los eventos en ventanas de 1 minuto. Entre invocaciones de la misma ventana, Lambda conserva el acumulado (conteo y suma por departamento) en `state`; en la invocación final (`isFinalInvokeForWindow`) calcula el promedio y lo guarda en `ventanas_radiacion`. Los repetidos se detectan con una escritura condicional (`attribute_not_exists(id_evento)`) en `eventos_vistos`; si el id ya existía, no se cuenta y se suma 1 a `repeticiones`.

## Paso 5: credenciales en el PC

```
AWS Academy -> AWS Details -> AWS CLI: Show -> copiar el bloque
Pegar en: ~/.aws/credentials   (Windows: C:\Users\<usuario>\.aws\credentials)
```

Las credenciales caducan al terminar la sesión del laboratorio. **No se suben al repositorio.**

## Paso 6: ejecución (en el PC, carpeta `parteB/`)

Si `<BUCKET_STREAMING>` es distinto de `felipe-trabajo2-streaming`, cambiar la variable `BUCKET` en `verificar_conteos.py` y `limpiar_tablas.py`.

```bash
pip install boto3

# Dejar tablas y S3 en cero (opcional, antes de cada corrida)
python limpiar_tablas.py

# Publicar eventos durante 6 minutos (~2 por segundo)
python productor.py
#    -> resumen_productor.json

# Esperar 1-2 minutos (cierre de la última ventana) y verificar
python verificar_conteos.py
```

Consultas PartiQL para la consola de DynamoDB: `parteB/consultas_dynamodb.sql`.

## Resultados de la parte B (corrida de prueba)

| Concepto | Productor | AWS |
|---|---|---|
| Publicados / eventos crudos en S3 | 593 | 593 |
| Únicos procesados | 535 | 535 |
| Repetidos descartados | 24 | 24 |
| Inválidos descartados | 34 | 34 |

**535 + 24 + 34 = 593.** La suma de los conteos de todas las ventanas es 535 (igual a los únicos), por lo que ningún repetido se contó dos veces.

## Limpieza de recursos

- **Kinesis:** borrar `stream-radiacion` al terminar (cobra por hora mientras existe).
- Glue, Step Functions, Lambda y Athena solo cobran al ejecutarse.
- El horario de EventBridge es de una sola vez y no vuelve a ejecutarse.

---

# Declaración de uso de inteligencia artificial

En este trabajo se usó **Claude (Anthropic)** de la siguiente forma:

- **Generación de código:** los scripts del repositorio (Python, PySpark, la definición de la máquina de estados y las consultas SQL) fueron generados con ayuda de IA, a partir de los requisitos y decisiones definidos por el autor. El autor los probó, ejecutó y verificó.
- **Redacción de este README:** redactado con apoyo de IA y revisado por el autor.
- **Herramienta de apoyo:** para resolver dudas durante el desarrollo y recibir explicaciones conceptuales que ayudaran a la toma de decisiones.

Las decisiones del proyecto, la creación manual de todos los recursos en la consola de AWS, la ejecución de los scripts y la grabación del video fueron realizadas por el autor, quien comprende y responde por todo el trabajo.
