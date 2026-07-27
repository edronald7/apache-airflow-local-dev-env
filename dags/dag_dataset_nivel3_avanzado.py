"""
DATASETS — NIVEL 3 (Avanzado): Pipeline encadenado con gate de calidad
======================================================================
Pipeline multicapa estilo data lake donde cada capa se dispara por
Datasets y un DAG puede ser consumidor Y productor a la vez. Un
ShortCircuitOperator actúa como "gate" de calidad: si los datos raw
no pasan la validación, la tarea con outlets nunca corre, el Dataset
staging NO se actualiza y las capas siguientes no se disparan —
la mala calidad no se propaga.

Demuestra:
  - Encadenamiento: raw → staging → analytics (consumidor + productor)
  - ShortCircuit como gate: el DatasetEvent solo se emite si la calidad pasa
  - context["triggering_dataset_events"] — inspeccionar QUÉ evento disparó el run
  - Auditoría del historial de DatasetEvent consultando la metadata DB (ORM)

Grafo de datos:
  [ingesta_raw @daily]      [transformacion]                [analytics]
  ingerir ─▶ 📦 raw.json ─▶ validar_calidad ─▶ transformar ─▶ 📦 staging.json ─▶ metricas ─▶ auditoria
                              │ (si falla: SKIP,
                              ▼  staging NO se actualiza)
                            [fin: capas siguientes no corren]
"""

from __future__ import annotations

import json
import random
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models.dataset import DatasetEvent, DatasetModel
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.session import provide_session

DS_RAW = Dataset("file:///tmp/datasets_demo/lake/raw.json")
DS_STAGING = Dataset("file:///tmp/datasets_demo/lake/staging.json")

DIR_LAKE = Path("/tmp/datasets_demo/lake")

DEFAULT_ARGS = {"owner": "data-team", "retries": 1}


# ─── Capa RAW ────────────────────────────────────────────────────────────────
def _ingerir_raw(**context) -> None:
    """Simula ingesta desde una fuente externa; ~20% de los registros llegan sucios."""
    DIR_LAKE.mkdir(parents=True, exist_ok=True)
    registros = [
        {"id": i, "valor": round(random.uniform(10, 500), 2) if random.random() > 0.2 else None}
        for i in range(1, 21)
    ]
    (DIR_LAKE / "raw.json").write_text(json.dumps({"fecha": context["ds"], "registros": registros}))
    nulos = sum(1 for r in registros if r["valor"] is None)
    print(f"Ingesta raw: {len(registros)} registros, {nulos} con valor nulo")


# ─── Capa STAGING ────────────────────────────────────────────────────────────
def _validar_calidad(**context) -> bool:
    """Gate: si más del 40% de registros son nulos, cortocircuita el DAG.

    La tarea `transformar` (que tiene el outlet) se salta, el DatasetEvent
    de staging no se emite y el DAG de analytics NO se dispara.
    """
    registros = json.loads((DIR_LAKE / "raw.json").read_text())["registros"]
    pct_nulos = sum(1 for r in registros if r["valor"] is None) / len(registros)
    pasa = pct_nulos <= 0.40
    print(f"Calidad raw: {pct_nulos:.0%} nulos — {'PASA ✓' if pasa else 'FALLA ✗ (se detiene la propagación)'}")
    return pasa


def _transformar(**context) -> None:
    """Limpia los datos y los promueve a staging (emite el DatasetEvent)."""
    # ¿Qué evento de dataset disparó este run? (clave de contexto de Airflow 2.6+)
    eventos = context["triggering_dataset_events"]
    for uri, lista in eventos.items():
        for ev in lista:
            print(f"Disparado por: {uri} (evento emitido {ev.timestamp}, DAG origen: {ev.source_dag_id})")

    data = json.loads((DIR_LAKE / "raw.json").read_text())
    limpios = [r for r in data["registros"] if r["valor"] is not None]
    (DIR_LAKE / "staging.json").write_text(json.dumps({"fecha": data["fecha"], "registros": limpios}))
    print(f"Staging actualizado: {len(limpios)} registros limpios de {len(data['registros'])}")


# ─── Capa ANALYTICS ──────────────────────────────────────────────────────────
def _calcular_metricas(**context) -> dict:
    data = json.loads((DIR_LAKE / "staging.json").read_text())
    valores = [r["valor"] for r in data["registros"]]
    metricas = {
        "fecha": data["fecha"],
        "n": len(valores),
        "total": round(sum(valores), 2),
        "promedio": round(sum(valores) / len(valores), 2),
        "maximo": max(valores),
    }
    print(f"Métricas analytics: {json.dumps(metricas, indent=2)}")
    return metricas


@provide_session
def _auditar_eventos(session=None, **context) -> None:
    """Consulta la metadata DB: historial de eventos por cada Dataset del pipeline.

    Útil para auditoría/debugging: ver cuántas veces y cuándo se actualizó
    cada dataset, y qué DAG/tarea emitió cada evento.
    """
    for uri in (DS_RAW.uri, DS_STAGING.uri):
        modelo = session.query(DatasetModel).filter(DatasetModel.uri == uri).first()
        if modelo is None:
            print(f"{uri}: aún sin registrar (ningún productor ha corrido)")
            continue
        eventos = (
            session.query(DatasetEvent)
            .filter(DatasetEvent.dataset_id == modelo.id)
            .order_by(DatasetEvent.timestamp.desc())
            .limit(5)
            .all()
        )
        print(f"{uri} — últimos {len(eventos)} eventos:")
        for ev in eventos:
            print(f"  {ev.timestamp} ← {ev.source_dag_id}.{ev.source_task_id} (run: {ev.source_run_id})")


# ─── DAG 1: ingesta por tiempo (única entrada cron del pipeline) ─────────────
with DAG(
    dag_id="dataset_n3_ingesta_raw",
    description="Capa raw: ingesta diaria, actualiza el dataset raw",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["datasets", "nivel-3", "avanzado", "productor"],
) as dag_ingesta:

    PythonOperator(
        task_id="ingerir_raw",
        python_callable=_ingerir_raw,
        outlets=[DS_RAW],
    )


# ─── DAG 2: consumidor de raw Y productor de staging ─────────────────────────
with DAG(
    dag_id="dataset_n3_transformacion",
    description="Capa staging: gate de calidad + transformación (consumidor y productor)",
    schedule=[DS_RAW],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["datasets", "nivel-3", "avanzado", "consumidor", "productor"],
) as dag_transformacion:

    validar = ShortCircuitOperator(
        task_id="validar_calidad",
        python_callable=_validar_calidad,
    )

    transformar = PythonOperator(
        task_id="transformar",
        python_callable=_transformar,
        outlets=[DS_STAGING],  # solo se emite si la validación pasó
    )

    validar >> transformar


# ─── DAG 3: capa final de consumo + auditoría ────────────────────────────────
with DAG(
    dag_id="dataset_n3_analytics",
    description="Capa analytics: métricas + auditoría del historial de DatasetEvents",
    schedule=[DS_STAGING],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["datasets", "nivel-3", "avanzado", "consumidor"],
) as dag_analytics:

    metricas = PythonOperator(
        task_id="calcular_metricas",
        python_callable=_calcular_metricas,
    )

    auditoria = PythonOperator(
        task_id="auditar_eventos",
        python_callable=_auditar_eventos,
    )

    metricas >> auditoria
