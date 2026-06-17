# Ejemplos de DAGs — Apache Airflow 2.6

> 🌐 **Idioma:** Estás leyendo la versión en español. The English version is available in [`examples.md`](./examples.md).

Este directorio contiene DAGs de ejemplo que demuestran distintos patrones y casos de uso reales con Airflow. Sirven como punto de partida para construir flujos de producción.

---

## Índice

### DAGs de casos de uso reales

| Archivo | DAG ID | Propósito |
|---|---|---|
| `dag_oci_dataflow.py` | `oci_dataflow_job` | Lanzar y monitorear un job de OCI Data Flow |
| `dag_aws_glue.py` | `aws_glue_job` | Ejecutar un AWS Glue Job con sensor de espera |
| `dag_etl_pipeline.py` | `etl_pipeline_with_branching` | ETL completo con extracción paralela y branching |
| `dag_parallel_processing.py` | `parallel_regional_processing` | Procesamiento paralelo con TaskGroups |
| `dag_data_quality.py` | `data_quality_checks` | Batería de checks de calidad con decisión automática |
| `dag_utilities.py` | `maintenance_utilities` | Mantenimiento del sistema: disco, logs y conexiones |

### DAGs de complejidad progresiva (Niveles 1 → 5)

| Archivo | DAG ID | Nivel | Descripción |
|---|---|---|---|
| `dag_nivel1_hello_world.py` | `nivel1_hello_world` | ⭐ Básico | Una sola tarea, estructura mínima de un DAG |
| `dag_nivel2_secuencial.py` | `nivel2_cadena_secuencial` | ⭐⭐ Básico+ | Cadena lineal Bash → Python → Python con XCom |
| `dag_nivel3_intermedio.py` | `nivel3_shortcircuit_variables` | ⭐⭐⭐ Intermedio | ShortCircuit en días no hábiles + Variables + ramas paralelas |
| `dag_nivel4_avanzado.py` | `nivel4_dynamic_task_mapping` | ⭐⭐⭐⭐ Avanzado | Dynamic Task Mapping + TimeSensor + decorador @task |
| `dag_nivel5_complejo.py` | `nivel5_pipeline_data_lake` | ⭐⭐⭐⭐⭐ Complejo | Pipeline Data Lake E2E — todos los patrones avanzados |

---

## 1. OCI Dataflow Job (`dag_oci_dataflow.py`)

### Propósito
Envía una aplicación Spark a **OCI Data Flow**, espera a que complete mediante polling activo, y registra las métricas del run.

### Grafo de tareas
```
submit_dataflow_run → wait_for_dataflow_run → log_run_metrics
```

### Componentes clave

| Elemento | Detalle |
|---|---|
| Operador | `PythonOperator` con SDK `oci` |
| Autenticación | Perfil OCI cargado desde `~/.oci/config` (volumen montado) |
| Comunicación entre tareas | XCom — `submit` publica el `run_id`, `wait` y `metrics` lo consumen |
| Timeout de espera | 3 horas (`execution_timeout`) |
| Reintentos | 1 reintento con 5 min de espera |

### Variables de Airflow requeridas
Ve a **Admin > Variables** y crea:

| Variable | Ejemplo |
|---|---|
| `oci_compartment_id` | `ocid1.compartment.oc1..xxxxxxx` |
| `oci_dataflow_app_id` | `ocid1.dataflowapplication.oc1..xxxxxxx` |
| `oci_dataflow_bucket` | `mi-bucket-dataflow` |
| `oci_dataflow_ns` | `mi-namespace` |

### Programación
`0 6 * * *` — Diario a las 6:00 AM.

---

## 2. AWS Glue Job (`dag_aws_glue.py`)

### Propósito
Inicia un **AWS Glue Job** con argumentos dinámicos basados en la fecha de ejecución, espera su completación con un sensor dedicado, y verifica que el output exista en S3.

### Grafo de tareas
```
prepare_glue_arguments → run_glue_job → wait_for_glue_job → verify_glue_output
```

### Componentes clave

| Elemento | Detalle |
|---|---|
| Operador principal | `GlueJobOperator` (provider `apache-airflow-providers-amazon`) |
| Sensor de espera | `GlueJobSensor` — polling cada 30 seg, timeout 2 horas |
| Verificación | `boto3` — lista objetos en S3 para confirmar output |
| Credenciales AWS | Conexión `aws_default` + volumen `~/.aws/credentials` |

### Conexión requerida en Airflow
Ve a **Admin > Connections** y crea:

| Campo | Valor |
|---|---|
| Conn ID | `aws_default` |
| Conn Type | `Amazon Web Services` |
| Extra | `{"region_name": "us-east-1"}` |

### Variables de Airflow requeridas

| Variable | Ejemplo |
|---|---|
| `glue_job_name` | `mi-etl-job` |
| `glue_script_bucket` | `mi-bucket-scripts` |
| `glue_output_path` | `s3://mi-bucket/output/` |

### Programación
`0 7 * * 1-5` — Lunes a viernes a las 7:00 AM.

---

## 3. ETL Pipeline con Branching (`dag_etl_pipeline.py`)

### Propósito
Pipeline ETL completo que demuestra patrones esenciales: extracción paralela desde múltiples fuentes, validación de integridad y bifurcación condicional del flujo.

### Grafo de tareas
```
check_sources ─┬─ extract_api ─┬─ validate_data ─ branch_on_validation ─┬─ load_warehouse ─┬─ cleanup
               └─ extract_db  ─┘                                         └─ notify_failure  ─┘
```

### Componentes clave

| Elemento | Detalle |
|---|---|
| Extracción paralela | `extract_api` y `extract_db` corren en paralelo automáticamente |
| Validación | Regla: mínimo 1,000 registros combinados para continuar |
| `BranchPythonOperator` | Decide entre `load_warehouse` o `notify_failure` según validación |
| `TriggerRule.ALL_DONE` | `cleanup` corre **siempre**, sin importar qué rama se ejecutó |

### Patrones demostrados
- **Paralelismo natural**: dos tareas sin dependencias entre sí corren simultáneamente.
- **XCom**: cada tarea publica su resultado; la siguiente lo consume con `xcom_pull`.
- **BranchPythonOperator**: retorna el `task_id` de la siguiente tarea a ejecutar.
- **TriggerRule**: controla cuándo una tarea debe correr en función del estado de sus predecesoras.

### Programación
`0 2 * * *` — Diario a las 2:00 AM.

---

## 4. Procesamiento Paralelo Regional (`dag_parallel_processing.py`)

### Propósito
Procesa 4 regiones geográficas en paralelo usando **TaskGroups**, luego agrega los resultados y genera un reporte consolidado. Ideal para jobs particionados por región, cliente, fecha, etc.

### Grafo de tareas
```
prepare_partitions
      │
      ▼
┌─────────────────────────────────────────────────┐
│  TaskGroup: process_regions                     │
│  ┌──────────┐ ┌──────────┐ ┌─────────┐ ┌─────┐ │
│  │ norte    │ │ sur      │ │oriente  │ │occid│ │
│  └──────────┘ └──────────┘ └─────────┘ └─────┘ │
└─────────────────────────────────────────────────┘
      │
      ▼
aggregate_results → generate_report
```

### Componentes clave

| Elemento | Detalle |
|---|---|
| `TaskGroup` | Agrupa visualmente las 4 tareas paralelas en la UI de Airflow |
| Generación dinámica | Un bucle `for region in REGIONS` crea las tareas programáticamente |
| XCom con prefijo de grupo | `xcom_pull(task_ids="process_regions.process_norte")` |
| Escalabilidad | Agregar una región al array `REGIONS` crea la tarea automáticamente |

### Patrones demostrados
- **TaskGroup**: organización visual en la UI sin afectar la ejecución.
- **Generación dinámica de tareas**: una lista de valores genera N tareas en código.
- **Fan-out / Fan-in**: múltiples tareas paralelas que convergen en una sola.

### Programación
`0 3 * * *` — Diario a las 3:00 AM.

---

## 5. Data Quality Checks (`dag_data_quality.py`)

### Propósito
Ejecuta 5 validaciones de calidad **en paralelo** sobre un dataset. Al final, toma una decisión automática basada en el número de checks fallidos: aprobar, poner en cuarentena o rechazar.

### Grafo de tareas
```
start ─┬─ check_completeness ─┐
       ├─ check_uniqueness    ─┤
       ├─ check_freshness     ─┼─ evaluate_checks ─┬─ approve_dataset
       ├─ check_volume        ─┤                    ├─ quarantine_dataset
       └─ check_value_range   ─┘                    └─ reject_dataset
```

### Checks implementados

| Check | Qué valida | Umbral |
|---|---|---|
| `completeness` | % de nulos en columnas críticas | ≤ 5% |
| `uniqueness` | % de registros duplicados | ≤ 1% |
| `freshness` | Antigüedad de la data | ≤ 26 horas |
| `volume` | Cantidad de registros | 500 – 100,000 |
| `value_range` | Valores fuera de rango numérico | 0% |

### Lógica de decisión

| Checks fallidos | Acción |
|---|---|
| 0 | `approve_dataset` — publica en capa de consumo |
| 1 – 2 | `quarantine_dataset` — requiere revisión manual |
| 3 o más | `reject_dataset` — lanza excepción, DAG falla |

### Componentes clave

| Elemento | Detalle |
|---|---|
| `TriggerRule.ALL_DONE` en `evaluate_checks` | Espera a todos los checks aunque alguno falle |
| `BranchPythonOperator` | Retorna el `task_id` de la acción a tomar |
| `retries: 0` | Los checks de calidad no se reintentan — el dato o está bien o no |

### Programación
`0 4 * * *` — Diario a las 4:00 AM.

---

## 6. Mantenimiento del Sistema (`dag_utilities.py`)

### Propósito
DAG de operaciones que mantiene el entorno Airflow saludable: verifica espacio en disco, limpia logs antiguos, elimina temporales y revisa las conexiones registradas.

### Grafo de tareas
```
check_disk_space ─┬─ clean_airflow_logs        ─┐
                  ├─ clean_tmp_files (Bash)     ─┼─ maintenance_summary
                  └─ check_airflow_connections  ─┘
                              (ALL_DONE)
```

### Componentes clave

| Elemento | Detalle |
|---|---|
| `BashOperator` | Ejecuta comandos del sistema (`find`, `rm`) para limpiar temporales |
| `on_failure_callback` | Callback global definido en `default_args` — notifica cualquier fallo |
| `shutil.disk_usage` | Verifica espacio y lanza error si cae bajo el 10% |
| `provide_session` | Accede directamente a la BD de Airflow para listar conexiones |
| `TriggerRule.ALL_DONE` | El resumen se genera siempre, incluso si una tarea falla |

### Patrones demostrados
- **`on_failure_callback`**: hook para notificaciones externas (Slack, email, etc.).
- **`BashOperator`**: integración con el sistema operativo.
- **Acceso interno a Airflow**: consultas a la metabase con `provide_session`.
- **Tarea de cierre garantizado**: `summary` siempre corre gracias a `ALL_DONE`.

### Programación
`0 1 * * 0` — Domingos a la 1:00 AM.

---

## Conceptos de Airflow utilizados

### Operadores usados

| Operador | Descripción |
|---|---|
| `PythonOperator` | Ejecuta una función Python |
| `BashOperator` | Ejecuta comandos de shell |
| `BranchPythonOperator` | Decide qué rama del DAG ejecutar |
| `EmptyOperator` | Nodo de inicio/fin sin lógica (antes llamado `DummyOperator`) |
| `GlueJobOperator` | Inicia un AWS Glue Job (provider Amazon) |
| `GlueJobSensor` | Espera a que un Glue Job termine (provider Amazon) |

### Patrones demostrados

| Patrón | DAG que lo usa |
|---|---|
| XCom — pasar datos entre tareas | Todos |
| Paralelismo natural | ETL Pipeline, Data Quality, Mantenimiento |
| Branching condicional | ETL Pipeline, Data Quality, Nivel 5 |
| Fan-out / Fan-in | Procesamiento Paralelo, Nivel 5 |
| TaskGroup (agrupación visual) | Procesamiento Paralelo, Nivel 4, Nivel 5 |
| TaskGroups anidados | Nivel 5 |
| TriggerRule.ALL_DONE | ETL Pipeline, Data Quality, Mantenimiento, Nivel 5 |
| TriggerRule.ONE_SUCCESS | Nivel 5 |
| on_failure_callback | Mantenimiento, Nivel 5 |
| on_success_callback | Nivel 5 |
| sla_miss_callback + SLA por tarea | Nivel 5 |
| Generación dinámica de tareas | Procesamiento Paralelo, Nivel 4 |
| Sensor con polling | AWS Glue |
| TimeSensor (ventana horaria) | Nivel 4 |
| ShortCircuitOperator | Nivel 3, Nivel 5 |
| Dynamic Task Mapping (.expand()) | Nivel 4 |
| Decorador @task | Nivel 4 |
| retry_exponential_backoff | Nivel 5 |
| Airflow Variables | Nivel 3, Nivel 5 |
| Jinja templating ({{ ds }}) | Nivel 3 |
| SDK externo (OCI, boto3) | OCI Dataflow, AWS Glue |

---

## Niveles de complejidad — resumen técnico

### Nivel 1 — Hello World
Una función Python, un operador, sin dependencias. Estructura mínima obligatoria de todo DAG.

### Nivel 2 — Cadena Secuencial
Tres operadores distintos en línea recta. Introduce XCom para compartir datos y `BashOperator` para shell.

### Nivel 3 — Intermedio
`ShortCircuitOperator` para lógica condicional de alto nivel. Variables de Airflow para configuración externalizada. Jinja templating en `BashOperator`. Dos ramas paralelas convergiendo.

### Nivel 4 — Avanzado
`Dynamic Task Mapping` con `.expand()`: genera N tareas en runtime según los datos, sin necesidad de hardcodear. `TimeSensor` controla la ventana de ejecución. Decorador `@task` como sintaxis moderna.

### Nivel 5 — Muy Complejo
Pipeline de producción real con 6 etapas (Control → Ingesta → Calidad → Transform → Entrega → Auditoría). Combina todos los patrones anteriores más: SLA miss callback, retry con backoff exponencial, TaskGroups anidados de 3 capas, TriggerRule múltiples en distintas tareas, y callbacks globales de éxito/fallo.

---

## `schedule_interval` — Guía de programación

El argumento `schedule_interval` define **cuándo** y **con qué frecuencia** se ejecuta un DAG. Acepta tres formatos: expresiones cron, macros predefinidas de Airflow y objetos `timedelta`.

> **Nota importante:** `start_date` define la fecha desde la que Airflow empieza a contar. El primer run ocurre al finalizar el **primer intervalo** después de `start_date`, no en `start_date` mismo. Para evitar runs del pasado, usa siempre `catchup=False`.

---

### Formato 1 — Expresión Cron

```
┌─────────── minuto       (0 - 59)
│ ┌───────── hora         (0 - 23)
│ │ ┌─────── día del mes  (1 - 31)
│ │ │ ┌───── mes          (1 - 12)
│ │ │ │ ┌─── día semana   (0=Dom, 1=Lun … 6=Sáb)
│ │ │ │ │
* * * * *
```

#### Caracteres especiales

| Carácter | Significado | Ejemplo |
|---|---|---|
| `*` | Cualquier valor | `* * * * *` = cada minuto |
| `,` | Lista de valores | `0 6,18 * * *` = a las 6 AM y 6 PM |
| `-` | Rango | `0 9-17 * * 1-5` = cada hora de 9 a 17, lunes a viernes |
| `/` | Paso/intervalo | `*/15 * * * *` = cada 15 minutos |
| `L` | Último | `0 0 L * *` = último día del mes (no soportado en todas las versiones) |

---

### Ejemplos de cron por frecuencia

#### Cada N minutos / horas

| `schedule_interval` | Descripción |
|---|---|
| `"*/5 * * * *"` | Cada 5 minutos |
| `"*/15 * * * *"` | Cada 15 minutos |
| `"*/30 * * * *"` | Cada 30 minutos |
| `"0 * * * *"` | Cada hora (en punto) |
| `"0 */2 * * *"` | Cada 2 horas |
| `"0 */6 * * *"` | Cada 6 horas |
| `"0 */12 * * *"` | Cada 12 horas |

#### Diario

| `schedule_interval` | Descripción |
|---|---|
| `"0 0 * * *"` | Medianoche (00:00) |
| `"0 1 * * *"` | 1:00 AM |
| `"0 2 * * *"` | 2:00 AM — recomendado para ETLs nocturnos |
| `"0 6 * * *"` | 6:00 AM |
| `"30 7 * * *"` | 7:30 AM |
| `"0 20 * * *"` | 8:00 PM |
| `"59 23 * * *"` | 11:59 PM |

#### Solo días hábiles (lunes a viernes)

| `schedule_interval` | Descripción |
|---|---|
| `"0 6 * * 1-5"` | 6:00 AM, lun–vie |
| `"0 7 * * 1-5"` | 7:00 AM, lun–vie |
| `"30 8 * * 1-5"` | 8:30 AM, lun–vie |
| `"0 18 * * 1-5"` | 6:00 PM, lun–vie (cierre) |
| `"0 0 * * 1-5"` | Medianoche, solo días hábiles |

#### Semanal

| `schedule_interval` | Descripción |
|---|---|
| `"0 1 * * 0"` | Domingos a la 1:00 AM |
| `"0 1 * * 1"` | Lunes a la 1:00 AM (inicio de semana) |
| `"0 6 * * 5"` | Viernes a las 6:00 AM |
| `"0 8 * * 1,3,5"` | Lunes, miércoles y viernes a las 8:00 AM |
| `"0 8 * * 2,4"` | Martes y jueves a las 8:00 AM |

#### Mensual

| `schedule_interval` | Descripción |
|---|---|
| `"0 0 1 * *"` | Primer día de cada mes a medianoche |
| `"0 6 1 * *"` | Primer día del mes a las 6:00 AM |
| `"0 0 15 * *"` | Día 15 de cada mes a medianoche |
| `"0 0 1,15 * *"` | Días 1 y 15 de cada mes |
| `"0 6 28-31 * *"` | Últimos días del mes (aprox.) a las 6:00 AM |

#### Anual / trimestral

| `schedule_interval` | Descripción |
|---|---|
| `"0 0 1 1 *"` | 1 de enero a medianoche (anual) |
| `"0 0 1 1,4,7,10 *"` | Inicio de cada trimestre |
| `"0 0 1 */3 *"` | Cada 3 meses (trimestral) |
| `"0 0 1 */6 *"` | Cada 6 meses (semestral) |

---

### Formato 2 — Macros predefinidas de Airflow

Airflow incluye atajos para los casos más comunes. Son equivalentes exactos de la expresión cron que aparece al lado.

| Macro | Equivalente cron | Descripción |
|---|---|---|
| `"@once"` | _(sin cron)_ | Corre una sola vez y nunca más |
| `"@hourly"` | `"0 * * * *"` | Cada hora en punto |
| `"@daily"` | `"0 0 * * *"` | Una vez al día a medianoche |
| `"@midnight"` | `"0 0 * * *"` | Igual que `@daily` |
| `"@weekly"` | `"0 0 * * 0"` | Una vez a la semana (domingos a medianoche) |
| `"@monthly"` | `"0 0 1 * *"` | Una vez al mes (día 1 a medianoche) |
| `"@quarterly"` | `"0 0 1 */3 *"` | Una vez por trimestre |
| `"@yearly"` | `"0 0 1 1 *"` | Una vez al año (1 de enero) |
| `"@annually"` | `"0 0 1 1 *"` | Igual que `@yearly` |
| `None` | _(sin cron)_ | El DAG solo corre si se dispara manualmente |

---

### Formato 3 — `timedelta` (intervalo relativo)

Útil cuando el intervalo no encaja limpiamente en cron (ej. "cada 90 minutos", "cada 3 días").

```python
from datetime import timedelta

schedule_interval=timedelta(minutes=30)    # Cada 30 minutos
schedule_interval=timedelta(hours=4)       # Cada 4 horas
schedule_interval=timedelta(days=1)        # Cada 24 horas exactas
schedule_interval=timedelta(days=3)        # Cada 3 días
schedule_interval=timedelta(weeks=1)       # Cada 7 días
schedule_interval=timedelta(hours=1, minutes=30)  # Cada 90 minutos
```

> **Diferencia clave con cron:** `timedelta` calcula el intervalo desde la ejecución anterior, por lo que el horario puede derivar con el tiempo. Cron siempre ejecuta a una hora fija del reloj.

---

### Variables de plantilla relacionadas con la fecha

Dentro de las tareas se puede acceder a fechas de ejecución vía **Jinja templating** o via `context`:

| Variable Jinja | `context` equivalente | Descripción |
|---|---|---|
| `{{ ds }}` | `context["ds"]` | Fecha lógica: `2026-06-12` |
| `{{ ds_nodash }}` | `context["ds_nodash"]` | Sin guiones: `20260612` |
| `{{ ts }}` | `context["ts"]` | Timestamp ISO: `2026-06-12T06:00:00+00:00` |
| `{{ ts_nodash }}` | `context["ts_nodash"]` | `20260612T060000` |
| `{{ prev_ds }}` | `context["prev_ds"]` | Fecha del run anterior |
| `{{ next_ds }}` | `context["next_ds"]` | Fecha del próximo run programado |
| `{{ execution_date }}` | `context["execution_date"]` | Objeto `datetime` completo |
| `{{ dag_run.id }}` | `context["run_id"]` | ID único del run |
| `{{ macros.ds_add(ds, 7) }}` | — | Suma 7 días a `ds` |
| `{{ macros.ds_add(ds, -1) }}` | — | Resta 1 día a `ds` |

**Ejemplo de uso en `BashOperator`:**
```python
BashOperator(
    task_id="export",
    bash_command="python export.py --date {{ ds }} --output s3://bucket/{{ ds_nodash }}/",
)
```

**Ejemplo de uso en `PythonOperator`:**
```python
def _procesar(**context):
    fecha_ayer = context["prev_ds"]         # "2026-06-11"
    fecha_hoy  = context["ds"]              # "2026-06-12"
    run_id     = context["run_id"]          # "scheduled__2026-06-12T06:00:00+00:00"
```

---

### Herramienta para validar expresiones cron

Antes de usar una expresión cron en producción, valídala en **[crontab.guru](https://crontab.guru)** — muestra en lenguaje natural qué significa cada expresión y cuándo será el próximo disparo.
