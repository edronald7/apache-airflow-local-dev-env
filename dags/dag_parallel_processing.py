"""
DAG: Procesamiento paralelo — Fan-out / Fan-in dinámico
========================================================
Muestra cómo procesar múltiples particiones en paralelo usando
TaskGroups y generación dinámica de tareas.

Patrón fan-out / fan-in:
  prepare → [process_region_A | process_region_B | process_region_C | process_region_D]
           → aggregate → report

También demuestra:
  - TaskGroup para agrupar tareas visualmente en la UI
  - Generación dinámica de tareas en un bucle
  - Paso de resultados entre grupos mediante XCom
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

REGIONS = ["norte", "sur", "oriente", "occidente"]

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def _prepare_partitions(**context) -> list[str]:
    """Define las particiones a procesar y las publica en XCom."""
    print(f"Preparando particiones para: {context['ds']}")
    print(f"  Regiones a procesar: {REGIONS}")
    return REGIONS


def _process_region(region: str, **context) -> dict:
    """Procesa los datos de una región específica."""
    import time
    print(f"[{region.upper()}] Iniciando procesamiento...")
    # Simula tiempo de procesamiento variable por región
    time.sleep(random.uniform(1, 3))
    records = random.randint(200, 800)
    errors = random.randint(0, 10)
    result = {"region": region, "records": records, "errors": errors}
    print(f"[{region.upper()}] Completado: {records} registros, {errors} errores")
    return result


def _aggregate_results(**context) -> dict:
    """Consolida los resultados de todas las regiones."""
    ti = context["ti"]
    total_records = 0
    total_errors = 0
    summary = []

    for region in REGIONS:
        result: dict = ti.xcom_pull(task_ids=f"process_regions.process_{region}")
        total_records += result["records"]
        total_errors += result["errors"]
        summary.append(result)

    aggregated = {
        "date": context["ds"],
        "regions": len(REGIONS),
        "total_records": total_records,
        "total_errors": total_errors,
        "error_rate": round(total_errors / total_records * 100, 2) if total_records else 0,
        "detail": summary,
    }
    print("=== Resumen agregado ===")
    for k, v in aggregated.items():
        print(f"  {k}: {v}")
    return aggregated


def _generate_report(**context) -> None:
    """Genera el reporte final con los resultados consolidados."""
    ti = context["ti"]
    data: dict = ti.xcom_pull(task_ids="aggregate_results")

    report_lines = [
        f"REPORTE DE PROCESAMIENTO — {data['date']}",
        "=" * 45,
        f"Regiones procesadas : {data['regions']}",
        f"Total de registros  : {data['total_records']:,}",
        f"Total de errores    : {data['total_errors']:,}",
        f"Tasa de error       : {data['error_rate']}%",
        "",
        "Detalle por región:",
    ]
    for item in data["detail"]:
        report_lines.append(
            f"  {item['region'].ljust(12)}: {item['records']:>5} registros, {item['errors']:>3} errores"
        )

    print("\n".join(report_lines))
    # En producción: guardar en S3, enviar por email, publicar en dashboard, etc.


with DAG(
    dag_id="parallel_regional_processing",
    description="Procesamiento paralelo de múltiples regiones con fan-out/fan-in",
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["paralelo", "taskgroup", "fan-out"],
) as dag:

    prepare = PythonOperator(
        task_id="prepare_partitions",
        python_callable=_prepare_partitions,
    )

    # TaskGroup agrupa visualmente las tareas paralelas en la UI de Airflow
    with TaskGroup(group_id="process_regions") as process_group:
        region_tasks = []
        for region in REGIONS:
            task = PythonOperator(
                task_id=f"process_{region}",
                python_callable=_process_region,
                op_kwargs={"region": region},
            )
            region_tasks.append(task)

    aggregate = PythonOperator(
        task_id="aggregate_results",
        python_callable=_aggregate_results,
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
    )

    prepare >> process_group >> aggregate >> report
