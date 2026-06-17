"""
DAG: OCI Dataflow — Submit y monitoreo de un job
=================================================
Envía una aplicación a OCI Data Flow y espera a que termine.
Las credenciales OCI se cargan desde ~/.oci/config (montado como volumen).

Conexión requerida en Airflow:
  - Ninguna (usa el SDK de OCI directamente con el perfil del volumen)

Variables de Airflow requeridas (Admin > Variables):
  - oci_compartment_id   : OCID del compartment donde corre Data Flow
  - oci_dataflow_app_id  : OCID de la aplicación Data Flow a ejecutar
  - oci_dataflow_bucket  : Nombre del bucket OCI para logs del job
  - oci_dataflow_ns      : Namespace del bucket
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _submit_dataflow_run(**context) -> str:
    """Envía la ejecución a OCI Data Flow y retorna el run_id."""
    import oci

    config = oci.config.from_file("~/.oci/config", profile_name="DEFAULT")
    df_client = oci.data_flow.DataFlowClient(config)

    compartment_id = Variable.get("oci_compartment_id")
    application_id = Variable.get("oci_dataflow_app_id")
    logs_bucket = Variable.get("oci_dataflow_bucket")
    logs_ns = Variable.get("oci_dataflow_ns")

    run_details = oci.data_flow.models.CreateRunDetails(
        compartment_id=compartment_id,
        application_id=application_id,
        display_name=f"airflow-run-{context['ds_nodash']}",
        logs_bucket_uri=f"oci://{logs_bucket}@{logs_ns}/dataflow-logs/",
        # Parámetros opcionales de la aplicación Spark
        arguments=["--date", context["ds"]],
    )

    response = df_client.create_run(create_run_details=run_details)
    run_id = response.data.id
    print(f"OCI Data Flow run enviado: {run_id}")

    # Guardamos el run_id en XCom para la siguiente tarea
    return run_id


def _wait_for_dataflow_run(**context) -> None:
    """Espera polling hasta que el run termine (succeeded/failed/cancelled)."""
    import oci

    config = oci.config.from_file("~/.oci/config", profile_name="DEFAULT")
    df_client = oci.data_flow.DataFlowClient(config)

    run_id: str = context["ti"].xcom_pull(task_ids="submit_dataflow_run")
    terminal_states = {"SUCCEEDED", "FAILED", "CANCELED", "DELETED"}

    print(f"Esperando run: {run_id}")
    while True:
        run = df_client.get_run(run_id).data
        state = run.lifecycle_state
        print(f"  Estado actual: {state}")

        if state in terminal_states:
            if state != "SUCCEEDED":
                raise ValueError(f"OCI Data Flow run terminó con estado: {state}")
            print("Run completado exitosamente.")
            break

        time.sleep(30)


def _log_run_metrics(**context) -> None:
    """Imprime métricas básicas del run para auditoría."""
    import oci

    config = oci.config.from_file("~/.oci/config", profile_name="DEFAULT")
    df_client = oci.data_flow.DataFlowClient(config)

    run_id: str = context["ti"].xcom_pull(task_ids="submit_dataflow_run")
    run = df_client.get_run(run_id).data

    print("=== Métricas del run ===")
    print(f"  Run ID       : {run.id}")
    print(f"  Estado final : {run.lifecycle_state}")
    print(f"  Inicio       : {run.time_created}")
    print(f"  Fin          : {run.time_updated}")


with DAG(
    dag_id="oci_dataflow_job",
    description="Envía y monitorea un job de OCI Data Flow",
    schedule_interval="0 6 * * *",       # Diario a las 6 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["oci", "dataflow", "spark"],
) as dag:

    submit = PythonOperator(
        task_id="submit_dataflow_run",
        python_callable=_submit_dataflow_run,
    )

    wait = PythonOperator(
        task_id="wait_for_dataflow_run",
        python_callable=_wait_for_dataflow_run,
        execution_timeout=timedelta(hours=3),
    )

    metrics = PythonOperator(
        task_id="log_run_metrics",
        python_callable=_log_run_metrics,
    )

    submit >> wait >> metrics
