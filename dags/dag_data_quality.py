"""
DAG: Data Quality — Validaciones con múltiples checks y alertas
===============================================================
Ejecuta una batería de controles de calidad sobre una tabla/dataset.
Cada check es independiente y puede fallar sin bloquear los demás.
Al final se toma una decisión global (pasar / cuarentena / rechazar).

Checks implementados:
  1. Completitud   — porcentaje de nulos en columnas críticas
  2. Unicidad      — registros duplicados
  3. Frescura      — la data tiene menos de N horas de antigüedad
  4. Volumen       — cantidad de registros dentro de rango esperado
  5. Rango         — valores numéricos dentro de límites válidos

Grafo:
  start
    ├─ check_completeness ──┐
    ├─ check_uniqueness   ──┤
    ├─ check_freshness    ──┼─ evaluate_checks ─── [branch]
    ├─ check_volume       ──┤                         ├── approve_dataset
    └─ check_value_range  ──┘                         ├── quarantine_dataset
                                                       └── reject_dataset
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-quality-team",
    "retries": 0,               # Los checks de calidad no se reintentan
    "email_on_failure": False,
}

# Umbrales de calidad (en producción vendrían de Variables de Airflow)
THRESHOLDS = {
    "max_null_pct": 5.0,        # Máximo 5% de nulos permitidos
    "max_duplicate_pct": 1.0,   # Máximo 1% de duplicados
    "max_freshness_hours": 26,  # Data no debe tener más de 26h
    "min_records": 500,
    "max_records": 100_000,
    "value_min": 0,
    "value_max": 1_000_000,
}


def _check_completeness(**context) -> dict:
    """Verifica el porcentaje de valores nulos en columnas críticas."""
    null_pct = random.uniform(0, 8)     # Simula resultado
    passed = null_pct <= THRESHOLDS["max_null_pct"]
    result = {
        "check": "completeness",
        "value": round(null_pct, 2),
        "threshold": THRESHOLDS["max_null_pct"],
        "passed": passed,
    }
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"[Completitud] {status} — Nulos: {null_pct:.2f}% (máx {THRESHOLDS['max_null_pct']}%)")
    return result


def _check_uniqueness(**context) -> dict:
    """Verifica que no haya registros duplicados por encima del umbral."""
    dup_pct = random.uniform(0, 3)
    passed = dup_pct <= THRESHOLDS["max_duplicate_pct"]
    result = {
        "check": "uniqueness",
        "value": round(dup_pct, 2),
        "threshold": THRESHOLDS["max_duplicate_pct"],
        "passed": passed,
    }
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"[Unicidad]    {status} — Duplicados: {dup_pct:.2f}% (máx {THRESHOLDS['max_duplicate_pct']}%)")
    return result


def _check_freshness(**context) -> dict:
    """Verifica que la data no sea más antigua que el umbral definido."""
    hours_old = random.uniform(0, 30)
    passed = hours_old <= THRESHOLDS["max_freshness_hours"]
    result = {
        "check": "freshness",
        "value": round(hours_old, 1),
        "threshold": THRESHOLDS["max_freshness_hours"],
        "passed": passed,
    }
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"[Frescura]    {status} — Antigüedad: {hours_old:.1f}h (máx {THRESHOLDS['max_freshness_hours']}h)")
    return result


def _check_volume(**context) -> dict:
    """Verifica que el volumen de registros esté dentro del rango esperado."""
    records = random.randint(100, 110_000)
    passed = THRESHOLDS["min_records"] <= records <= THRESHOLDS["max_records"]
    result = {
        "check": "volume",
        "value": records,
        "threshold": f"{THRESHOLDS['min_records']} - {THRESHOLDS['max_records']}",
        "passed": passed,
    }
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"[Volumen]     {status} — Registros: {records:,} (rango {THRESHOLDS['min_records']:,}-{THRESHOLDS['max_records']:,})")
    return result


def _check_value_range(**context) -> dict:
    """Verifica que los valores numéricos estén dentro de límites válidos."""
    out_of_range_pct = random.uniform(0, 5)
    passed = out_of_range_pct == 0
    result = {
        "check": "value_range",
        "value": round(out_of_range_pct, 2),
        "threshold": 0,
        "passed": passed,
    }
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"[Rango]       {status} — Fuera de rango: {out_of_range_pct:.2f}%")
    return result


def _evaluate_checks(**context) -> str:
    """
    Consolida todos los resultados y decide la acción:
      - 0 fallos  → approve_dataset
      - 1-2 fallos → quarantine_dataset
      - 3+ fallos → reject_dataset
    """
    ti = context["ti"]
    check_tasks = [
        "check_completeness",
        "check_uniqueness",
        "check_freshness",
        "check_volume",
        "check_value_range",
    ]

    failed = []
    print("\n=== Resumen de checks de calidad ===")
    for task_id in check_tasks:
        result: dict = ti.xcom_pull(task_ids=task_id)
        status = "✓" if result["passed"] else "✗"
        print(f"  {status} {result['check']}: {result['value']} (umbral: {result['threshold']})")
        if not result["passed"]:
            failed.append(result["check"])

    print(f"\nChecks fallidos: {len(failed)}/{len(check_tasks)}")

    if len(failed) == 0:
        return "approve_dataset"
    elif len(failed) <= 2:
        return "quarantine_dataset"
    else:
        return "reject_dataset"


def _approve(**context) -> None:
    print(f"Dataset APROBADO para {context['ds']} — publicando en capa de consumo.")


def _quarantine(**context) -> None:
    print(f"Dataset en CUARENTENA para {context['ds']} — requiere revisión manual antes de publicar.")


def _reject(**context) -> None:
    print(f"Dataset RECHAZADO para {context['ds']} — demasiados errores de calidad.")
    raise ValueError("Dataset rechazado por calidad insuficiente.")


with DAG(
    dag_id="data_quality_checks",
    description="Batería de checks de calidad con decisión automática",
    schedule_interval="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["data-quality", "branching", "validacion"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_completeness = PythonOperator(
        task_id="check_completeness",
        python_callable=_check_completeness,
    )
    check_uniqueness = PythonOperator(
        task_id="check_uniqueness",
        python_callable=_check_uniqueness,
    )
    check_freshness = PythonOperator(
        task_id="check_freshness",
        python_callable=_check_freshness,
    )
    check_volume = PythonOperator(
        task_id="check_volume",
        python_callable=_check_volume,
    )
    check_value_range = PythonOperator(
        task_id="check_value_range",
        python_callable=_check_value_range,
    )

    evaluate = BranchPythonOperator(
        task_id="evaluate_checks",
        python_callable=_evaluate_checks,
        # Los checks corren en paralelo; evaluate espera a todos
        trigger_rule=TriggerRule.ALL_DONE,
    )

    approve = PythonOperator(task_id="approve_dataset", python_callable=_approve)
    quarantine = PythonOperator(task_id="quarantine_dataset", python_callable=_quarantine)
    reject = PythonOperator(task_id="reject_dataset", python_callable=_reject)

    checks = [check_completeness, check_uniqueness, check_freshness, check_volume, check_value_range]

    start >> checks >> evaluate >> [approve, quarantine, reject]
