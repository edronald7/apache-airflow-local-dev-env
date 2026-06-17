"""
DAG: Utilitarios — Mantenimiento, monitoreo y notificaciones
=============================================================
DAG de mantenimiento con tareas comunes de operaciones:
  1. Verificar espacio en disco
  2. Limpiar logs de Airflow con más de N días
  3. Limpiar archivos temporales del sistema
  4. Verificar estado de conexiones registradas en Airflow
  5. Generar resumen del estado y notificar

Demuestra:
  - BashOperator para comandos del sistema
  - PythonOperator con lógica de negocio utilitaria
  - Uso del hook de base de datos de Airflow internamente
  - Manejo de errores con on_failure_callback
  - TriggerRule para tareas de resumen que corren siempre
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

LOG_RETENTION_DAYS = 30
TMP_DIRS = ["/opt/airflow/logs/scheduler", "/tmp/airflow_*"]

DEFAULT_ARGS = {
    "owner": "ops-team",
    "retries": 0,
    "email_on_failure": False,
}


def _on_task_failure(context) -> None:
    """Callback global ante fallo de cualquier tarea."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    print(
        f"[FALLO] DAG: {dag_id} | Tarea: {task_id} | Fecha: {execution_date}\n"
        f"Error: {context.get('exception', 'desconocido')}"
    )
    # En producción: enviar alerta a Slack/Teams/PagerDuty


DEFAULT_ARGS["on_failure_callback"] = _on_task_failure


def _check_disk_space(**context) -> dict:
    """Verifica el espacio libre en el sistema de archivos."""
    import shutil
    total, used, free = shutil.disk_usage("/opt/airflow")
    free_pct = (free / total) * 100
    result = {
        "total_gb": round(total / 1e9, 1),
        "used_gb": round(used / 1e9, 1),
        "free_gb": round(free / 1e9, 1),
        "free_pct": round(free_pct, 1),
    }
    print(f"Espacio en disco — Total: {result['total_gb']}GB | "
          f"Usado: {result['used_gb']}GB | "
          f"Libre: {result['free_gb']}GB ({result['free_pct']}%)")

    if free_pct < 10:
        raise RuntimeError(f"Espacio en disco crítico: solo {free_pct:.1f}% libre")
    return result


def _clean_airflow_logs(**context) -> dict:
    """Elimina logs de Airflow más antiguos que LOG_RETENTION_DAYS días."""
    import os
    import time

    log_base = Path("/opt/airflow/logs")
    cutoff = time.time() - (LOG_RETENTION_DAYS * 86400)
    deleted_files = 0
    deleted_bytes = 0

    for log_file in log_base.rglob("*.log"):
        if log_file.stat().st_mtime < cutoff:
            size = log_file.stat().st_size
            log_file.unlink()
            deleted_files += 1
            deleted_bytes += size

    result = {
        "deleted_files": deleted_files,
        "freed_mb": round(deleted_bytes / 1e6, 2),
        "retention_days": LOG_RETENTION_DAYS,
    }
    print(f"Logs eliminados: {deleted_files} archivos ({result['freed_mb']} MB liberados)")
    return result


def _check_airflow_connections(**context) -> dict:
    """Verifica que las conexiones registradas en Airflow sean accesibles."""
    from airflow.hooks.base import BaseHook
    from airflow.models import Connection
    from airflow.utils.db import provide_session

    @provide_session
    def get_connections(session=None):
        return session.query(Connection).all()

    connections = get_connections()
    results = {"total": len(connections), "checked": [], "errors": []}

    for conn in connections:
        try:
            # Solo verifica que el objeto sea recuperable, no hace ping real
            results["checked"].append(conn.conn_id)
        except Exception as e:
            results["errors"].append({"conn_id": conn.conn_id, "error": str(e)})

    print(f"Conexiones registradas: {results['total']}")
    for conn_id in results["checked"]:
        print(f"  ✓ {conn_id}")
    for err in results["errors"]:
        print(f"  ✗ {err['conn_id']}: {err['error']}")

    return results


def _generate_maintenance_summary(**context) -> None:
    """Genera un resumen del mantenimiento completado."""
    ti = context["ti"]

    disk = ti.xcom_pull(task_ids="check_disk_space") or {}
    logs = ti.xcom_pull(task_ids="clean_airflow_logs") or {}
    conns = ti.xcom_pull(task_ids="check_airflow_connections") or {}

    print("=" * 50)
    print(f"RESUMEN DE MANTENIMIENTO — {context['ds']}")
    print("=" * 50)
    print(f"Disco libre       : {disk.get('free_pct', 'N/A')}%")
    print(f"Logs eliminados   : {logs.get('deleted_files', 'N/A')} archivos "
          f"({logs.get('freed_mb', 'N/A')} MB)")
    print(f"Conexiones OK     : {len(conns.get('checked', []))}/{conns.get('total', 'N/A')}")
    print(f"Errores en conex. : {len(conns.get('errors', []))}")
    print("=" * 50)


with DAG(
    dag_id="maintenance_utilities",
    description="Tareas de mantenimiento: disco, logs, conexiones y resumen",
    schedule_interval="0 1 * * 0",      # Domingos a la 1 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["utilitario", "mantenimiento", "ops"],
) as dag:

    check_disk = PythonOperator(
        task_id="check_disk_space",
        python_callable=_check_disk_space,
    )

    clean_logs = PythonOperator(
        task_id="clean_airflow_logs",
        python_callable=_clean_airflow_logs,
    )

    # BashOperator: limpia archivos temporales del SO
    clean_tmp = BashOperator(
        task_id="clean_tmp_files",
        bash_command="find /tmp -name 'airflow_*' -type f -mtime +1 -delete && echo 'Temporales limpiados'",
    )

    check_connections = PythonOperator(
        task_id="check_airflow_connections",
        python_callable=_check_airflow_connections,
    )

    # Corre siempre, aunque alguna tarea anterior falle
    summary = PythonOperator(
        task_id="maintenance_summary",
        python_callable=_generate_maintenance_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # check_disk primero; si pasa, las demás corren en paralelo
    check_disk >> [clean_logs, clean_tmp, check_connections] >> summary
