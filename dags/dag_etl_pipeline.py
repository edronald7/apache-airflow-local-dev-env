"""
DAG: ETL Pipeline completo con branching y dependencias múltiples
=================================================================
Muestra un flujo ETL realista con:
  - Extracción desde dos fuentes independientes (paralelas)
  - Transformación y validación
  - Branching según resultado de validación
  - Carga condicional o notificación de error
  - Tarea final de limpieza (siempre corre)

Grafo de tareas:
                   ┌─ extract_api ──┐
  check_sources ───┤                ├─ validate ─── [branch] ─── load_warehouse
                   └─ extract_db  ──┘                       └─── notify_failure
                                                                       │
                                                              cleanup ◄─┘ (trigger_rule=ALL_DONE)
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def _check_sources(**context) -> None:
    """Verifica que las fuentes de datos estén disponibles antes de extraer."""
    print(f"Verificando disponibilidad de fuentes para la fecha: {context['ds']}")
    # En producción: ping a la API, check de conexión a BD, etc.
    print("✓ API externa: disponible")
    print("✓ Base de datos: disponible")


def _extract_from_api(**context) -> int:
    """Simula la extracción de registros desde una API REST."""
    print(f"Extrayendo datos de API para fecha: {context['ds']}")
    records = random.randint(500, 2000)
    print(f"  Registros extraídos de API: {records}")
    return records


def _extract_from_db(**context) -> int:
    """Simula la extracción desde una base de datos transaccional."""
    print(f"Extrayendo datos de BD para fecha: {context['ds']}")
    records = random.randint(1000, 5000)
    print(f"  Registros extraídos de BD: {records}")
    return records


def _validate_data(**context) -> dict:
    """Combina los resultados de extracción y valida integridad."""
    ti = context["ti"]
    api_records: int = ti.xcom_pull(task_ids="extract_api")
    db_records: int = ti.xcom_pull(task_ids="extract_db")
    total = api_records + db_records

    print(f"Total de registros: {total} (API: {api_records}, BD: {db_records})")

    # Regla de negocio: mínimo 1000 registros para proceder
    is_valid = total >= 1000
    result = {"total": total, "is_valid": is_valid}
    print(f"Resultado de validación: {result}")
    return result


def _branch_on_validation(**context) -> str:
    """Decide el siguiente paso según si la validación fue exitosa."""
    ti = context["ti"]
    result: dict = ti.xcom_pull(task_ids="validate_data")
    if result["is_valid"]:
        print("Validación exitosa → cargando al warehouse")
        return "load_warehouse"
    print("Validación fallida → notificando error")
    return "notify_failure"


def _load_warehouse(**context) -> None:
    """Carga los datos transformados al data warehouse."""
    ti = context["ti"]
    result: dict = ti.xcom_pull(task_ids="validate_data")
    print(f"Cargando {result['total']} registros al warehouse...")
    print(f"  Tabla destino: fact_transactions_{context['ds_nodash']}")
    print("  Carga completada exitosamente.")


def _notify_failure(**context) -> None:
    """Envía notificación cuando la validación falla."""
    ti = context["ti"]
    result: dict = ti.xcom_pull(task_ids="validate_data")
    msg = (
        f"[ALERTA] ETL fallido para {context['ds']}. "
        f"Registros recibidos: {result['total']} (mínimo requerido: 1000)"
    )
    print(msg)
    # En producción: enviar a Slack, email, PagerDuty, etc.


def _cleanup(**context) -> None:
    """Limpia archivos temporales. Corre siempre, independiente del resultado."""
    print("Limpiando archivos temporales del pipeline...")
    print("  /tmp/etl_staging/ → eliminado")
    print("Limpieza completada.")


with DAG(
    dag_id="etl_pipeline_with_branching",
    description="ETL completo con extracción paralela, validación y branching",
    schedule_interval="0 2 * * *",       # Diario a las 2 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "pipeline", "branching"],
) as dag:

    check_sources = PythonOperator(
        task_id="check_sources",
        python_callable=_check_sources,
    )

    # Extracción paralela desde dos fuentes
    extract_api = PythonOperator(
        task_id="extract_api",
        python_callable=_extract_from_api,
    )

    extract_db = PythonOperator(
        task_id="extract_db",
        python_callable=_extract_from_db,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_validation",
        python_callable=_branch_on_validation,
    )

    load = PythonOperator(
        task_id="load_warehouse",
        python_callable=_load_warehouse,
    )

    notify = PythonOperator(
        task_id="notify_failure",
        python_callable=_notify_failure,
    )

    # trigger_rule=ALL_DONE: corre aunque alguna rama anterior haya sido saltada
    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=_cleanup,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Grafo de dependencias
    check_sources >> [extract_api, extract_db] >> validate >> branch
    branch >> [load, notify] >> cleanup
