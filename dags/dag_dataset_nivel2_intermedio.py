"""
DATASETS — NIVEL 2 (Intermedio): Múltiples Datasets y lógica AND
================================================================
Dos DAGs productores independientes (ventas y clientes) actualizan
Datasets distintos. Los consumidores demuestran las dos formas de
depender de datos:

  - Fan-out : un mismo Dataset dispara a más de un consumidor
  - Lógica AND : schedule=[ds1, ds2] espera a que AMBOS se actualicen
    al menos una vez desde la última corrida (en Airflow 2.6 la lista
    siempre es AND; los operadores | y & llegaron en 2.9)

Demuestra:
  - Varios productores con calendarios distintos (@daily vs @hourly)
  - Dataset con metadata `extra` (documentación visible en el código)
  - Consumidor de un solo Dataset (reporte rápido de ventas)
  - Consumidor que hace "join" de dos fuentes (espera ventas Y clientes)

Grafo de datos:
  productor_ventas   ─▶ 📦 ventas.json   ─┬─▶ reporte_ventas   (1 dataset)
                                          └─▶ ┐
                                               ├─▶ join_ventas_clientes (AND)
  productor_clientes ─▶ 📦 clientes.json ────▶ ┘
"""

from __future__ import annotations

import json
import random
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

# `extra` es metadata informativa (equipo dueño, formato, etc.).
# No afecta el scheduling: Airflow identifica el Dataset SOLO por su URI.
DS_VENTAS = Dataset(
    "file:///tmp/datasets_demo/ventas.json",
    extra={"owner": "equipo-comercial", "formato": "json", "frecuencia": "diaria"},
)
DS_CLIENTES = Dataset(
    "file:///tmp/datasets_demo/clientes.json",
    extra={"owner": "equipo-crm", "formato": "json", "frecuencia": "horaria"},
)

DIR_DEMO = Path("/tmp/datasets_demo")


def _producir_ventas(**context) -> None:
    DIR_DEMO.mkdir(parents=True, exist_ok=True)
    ventas = [
        {"cliente_id": cid, "monto": round(random.uniform(100, 900), 2)}
        for cid in ("C001", "C002", "C003", "C002")
    ]
    (DIR_DEMO / "ventas.json").write_text(json.dumps({"fecha": context["ds"], "ventas": ventas}))
    print(f"ventas.json actualizado: {len(ventas)} transacciones")


def _producir_clientes(**context) -> None:
    DIR_DEMO.mkdir(parents=True, exist_ok=True)
    clientes = {
        "C001": {"nombre": "Acme SA", "segmento": "corporativo"},
        "C002": {"nombre": "Beta Ltda", "segmento": "pyme"},
        "C003": {"nombre": "Gamma Inc", "segmento": "corporativo"},
    }
    (DIR_DEMO / "clientes.json").write_text(json.dumps(clientes))
    print(f"clientes.json actualizado: {len(clientes)} clientes")


def _reporte_ventas(**context) -> None:
    """Consumidor rápido: solo necesita ventas, no espera a clientes."""
    data = json.loads((DIR_DEMO / "ventas.json").read_text())
    total = sum(v["monto"] for v in data["ventas"])
    print(f"Reporte del {data['fecha']}: {len(data['ventas'])} ventas, total ${total:,.2f}")


def _join_ventas_clientes(**context) -> None:
    """Consumidor AND: aquí es seguro leer ambos archivos, los dos existen."""
    ventas = json.loads((DIR_DEMO / "ventas.json").read_text())["ventas"]
    clientes = json.loads((DIR_DEMO / "clientes.json").read_text())

    por_segmento: dict[str, float] = {}
    for v in ventas:
        segmento = clientes[v["cliente_id"]]["segmento"]
        por_segmento[segmento] = por_segmento.get(segmento, 0) + v["monto"]

    print("Ventas enriquecidas por segmento:")
    for segmento, monto in sorted(por_segmento.items()):
        print(f"  {segmento:<12} ${monto:,.2f}")


# ─── PRODUCTORES (calendarios independientes) ────────────────────────────────
with DAG(
    dag_id="dataset_n2_productor_ventas",
    description="Actualiza ventas.json cada día",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datasets", "nivel-2", "intermedio", "productor"],
) as dag_ventas:

    PythonOperator(
        task_id="producir_ventas",
        python_callable=_producir_ventas,
        outlets=[DS_VENTAS],
    )

with DAG(
    dag_id="dataset_n2_productor_clientes",
    description="Actualiza clientes.json cada hora",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datasets", "nivel-2", "intermedio", "productor"],
) as dag_clientes:

    PythonOperator(
        task_id="producir_clientes",
        python_callable=_producir_clientes,
        outlets=[DS_CLIENTES],
    )


# ─── CONSUMIDOR 1: un solo Dataset (se dispara con cada venta nueva) ─────────
with DAG(
    dag_id="dataset_n2_reporte_ventas",
    description="Fan-out: reporte inmediato al actualizarse ventas.json",
    schedule=[DS_VENTAS],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datasets", "nivel-2", "intermedio", "consumidor"],
) as dag_reporte:

    PythonOperator(
        task_id="reporte_ventas",
        python_callable=_reporte_ventas,
    )


# ─── CONSUMIDOR 2: lógica AND (espera a que AMBOS datasets se actualicen) ────
# Aunque clientes.json se actualice 24 veces al día, este DAG corre solo
# cuando ventas.json TAMBIÉN tenga una actualización pendiente de consumir.
with DAG(
    dag_id="dataset_n2_join_ventas_clientes",
    description="AND: corre solo cuando ventas.json Y clientes.json se actualizaron",
    schedule=[DS_VENTAS, DS_CLIENTES],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datasets", "nivel-2", "intermedio", "consumidor"],
) as dag_join:

    PythonOperator(
        task_id="join_ventas_clientes",
        python_callable=_join_ventas_clientes,
    )
