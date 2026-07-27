"""
DATASETS — NIVEL 1 (Básico): Productor y Consumidor
===================================================
El patrón mínimo de Datasets: un DAG "productor" escribe un archivo
y declara que actualizó un Dataset (outlet). Un DAG "consumidor" no
tiene cron: se dispara automáticamente cada vez que el Dataset se
actualiza (data-aware scheduling, disponible desde Airflow 2.4).

Demuestra:
  - Dataset — URI lógica que representa un dato
  - outlets=[...] — la tarea anuncia que actualizó el Dataset
  - schedule=[dataset] — el DAG consumidor se programa por datos, no por tiempo
  - La pestaña "Datasets" de la UI muestra el grafo productor → consumidor

Flujo:
  [dataset_n1_productor]                       [dataset_n1_consumidor]
  generar_ventas ──(actualiza)──▶ 📦 ventas.csv ──(dispara)──▶ leer_ventas

Nota: los DAGs con Datasets usan el parámetro `schedule` (no
`schedule_interval`, que no acepta listas de Datasets).
"""

from __future__ import annotations

import csv
import random
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

# La URI es un identificador lógico: Airflow NO valida que el archivo exista,
# solo conecta productores y consumidores que referencian la misma URI.
VENTAS_CSV = Dataset("file:///tmp/datasets_demo/ventas.csv")

RUTA_LOCAL = Path("/tmp/datasets_demo/ventas.csv")


def _generar_ventas(**context) -> None:
    """Escribe un CSV con ventas simuladas del día."""
    RUTA_LOCAL.parent.mkdir(parents=True, exist_ok=True)
    filas = [
        {"producto": p, "unidades": random.randint(1, 50), "fecha": context["ds"]}
        for p in ("teclado", "mouse", "monitor")
    ]
    with RUTA_LOCAL.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["producto", "unidades", "fecha"])
        writer.writeheader()
        writer.writerows(filas)
    print(f"Archivo generado: {RUTA_LOCAL} ({len(filas)} filas)")
    # Al terminar esta tarea con éxito, Airflow registra un DatasetEvent
    # porque la tarea declara outlets=[VENTAS_CSV].


def _leer_ventas(**context) -> None:
    """Lee el CSV que produjo el otro DAG."""
    print(f"Disparado por actualización del dataset — run_id: {context['run_id']}")
    with RUTA_LOCAL.open() as f:
        for fila in csv.DictReader(f):
            print(f"  {fila['producto']:<10} {fila['unidades']:>3} unidades ({fila['fecha']})")


# ─── DAG PRODUCTOR: corre por tiempo (@daily) ────────────────────────────────
with DAG(
    dag_id="dataset_n1_productor",
    description="Genera ventas.csv y actualiza el Dataset",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datasets", "nivel-1", "basico", "productor"],
) as dag_productor:

    PythonOperator(
        task_id="generar_ventas",
        python_callable=_generar_ventas,
        outlets=[VENTAS_CSV],  # ← anuncia la actualización del Dataset
    )


# ─── DAG CONSUMIDOR: corre cuando el Dataset se actualiza ────────────────────
with DAG(
    dag_id="dataset_n1_consumidor",
    description="Se dispara automáticamente cuando ventas.csv se actualiza",
    schedule=[VENTAS_CSV],  # ← sin cron: programación por datos
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datasets", "nivel-1", "basico", "consumidor"],
) as dag_consumidor:

    PythonOperator(
        task_id="leer_ventas",
        python_callable=_leer_ventas,
    )
