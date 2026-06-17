"""
NIVEL 4 — Avanzado: Dynamic Task Mapping + TaskGroups anidados + Sensor
========================================================================
Pipeline de ingesta multi-tabla con mapeo dinámico de tareas (Airflow 2.3+).
Cada tabla genera automáticamente su propio set de tareas (extract → validate → load)
dentro de TaskGroups anidados. Un TimeSensor de cortesía asegura que no se ejecute
antes de la ventana permitida.

Demuestra:
  - TimeSensor — espera hasta una hora específica antes de proceder
  - Dynamic Task Mapping (.expand()) — N tareas generadas en runtime según datos
  - TaskGroups anidados — agrupación visual jerárquica en la UI
  - Decorador @task — sintaxis funcional moderna de Airflow
  - Encadenamiento de mapped tasks

Grafo (por cada tabla en TABLAS):
  wait_for_window
       │
  descubrir_tablas
       │
  ┌────────────────────────────────────────────┐
  │  ingesta_multi_tabla  (TaskGroup)          │
  │  ┌──────────────────────────────────────┐  │
  │  │  tabla_X  (TaskGroup por tabla)      │  │
  │  │   extract_tabla_X                   │  │
  │  │       │                             │  │
  │  │   validar_tabla_X                   │  │
  │  │       │                             │  │
  │  │   cargar_tabla_X                    │  │
  │  └──────────────────────────────────────┘  │
  └────────────────────────────────────────────┘
       │
  resumen_ingesta
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils.task_group import TaskGroup

TABLAS = [
    {"nombre": "clientes",    "esquema": "ventas",    "filas_esperadas": (1000, 50000)},
    {"nombre": "pedidos",     "esquema": "ventas",    "filas_esperadas": (500, 200000)},
    {"nombre": "productos",   "esquema": "catalogo",  "filas_esperadas": (50, 5000)},
    {"nombre": "proveedores", "esquema": "compras",   "filas_esperadas": (10, 500)},
]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


@task
def descubrir_tablas() -> list[dict]:
    """Descubre las tablas a ingestar (en producción consultaría un catálogo)."""
    print(f"Tablas a ingestar: {[t['nombre'] for t in TABLAS]}")
    return TABLAS


@task
def extraer_tabla(tabla: dict) -> dict:
    """Extrae una tabla desde la fuente origen."""
    import random
    min_r, max_r = tabla["filas_esperadas"]
    filas = random.randint(min_r, max_r)
    print(f"[EXTRACT] {tabla['esquema']}.{tabla['nombre']} — {filas:,} filas extraídas")
    return {**tabla, "filas_extraidas": filas, "estado": "extraido"}


@task
def validar_tabla(tabla_extraida: dict) -> dict:
    """Valida que la extracción tenga el volumen esperado."""
    min_r, max_r = tabla_extraida["filas_esperadas"]
    filas = tabla_extraida["filas_extraidas"]
    valido = min_r <= filas <= max_r
    if not valido:
        raise ValueError(
            f"[VALIDATE] {tabla_extraida['nombre']}: {filas:,} filas fuera del rango "
            f"[{min_r:,} – {max_r:,}]"
        )
    print(f"[VALIDATE] {tabla_extraida['nombre']} ✓ — {filas:,} filas dentro del rango")
    return {**tabla_extraida, "estado": "validado"}


@task
def cargar_tabla(tabla_validada: dict) -> dict:
    """Carga la tabla en el destino (data warehouse / lakehouse)."""
    nombre = tabla_validada["nombre"]
    filas = tabla_validada["filas_extraidas"]
    print(f"[LOAD] {nombre} → warehouse.{tabla_validada['esquema']}.{nombre} ({filas:,} filas)")
    return {**tabla_validada, "estado": "cargado"}


def _resumen_ingesta(**context) -> None:
    ti = context["ti"]
    # Con dynamic task mapping, xcom_pull retorna una lista de resultados
    resultados = ti.xcom_pull(task_ids="cargar_tabla")
    if not isinstance(resultados, list):
        resultados = [resultados]

    print("=" * 50)
    print("RESUMEN DE INGESTA MULTI-TABLA")
    print("=" * 50)
    total_filas = 0
    for r in resultados:
        if r:
            print(f"  ✓ {r['esquema']}.{r['nombre']:<15} {r['filas_extraidas']:>8,} filas  [{r['estado']}]")
            total_filas += r["filas_extraidas"]
    print("-" * 50)
    print(f"  Total tablas  : {len(resultados)}")
    print(f"  Total filas   : {total_filas:,}")
    print("=" * 50)


with DAG(
    dag_id="nivel4_dynamic_task_mapping",
    description="Dynamic Task Mapping + TaskGroups anidados + TimeSensor",
    schedule_interval="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nivel-4", "dynamic-mapping", "taskgroup", "sensor"],
) as dag:

    # Espera hasta las 05:05 AM para asegurar que los sistemas origen estén listos
    esperar_ventana = TimeSensor(
        task_id="wait_for_window",
        target_time=datetime.strptime("05:05", "%H:%M").time(),
        poke_interval=60,
    )

    tablas = descubrir_tablas()

    # Dynamic Task Mapping: genera una tarea por cada elemento de `tablas`
    extraidas = extraer_tabla.expand(tabla=tablas)
    validadas = validar_tabla.expand(tabla_extraida=extraidas)
    cargadas = cargar_tabla.expand(tabla_validada=validadas)

    resumen = PythonOperator(
        task_id="resumen_ingesta",
        python_callable=_resumen_ingesta,
    )

    esperar_ventana >> tablas
    cargadas >> resumen
