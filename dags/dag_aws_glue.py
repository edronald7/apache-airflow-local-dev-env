"""
DAG: AWS Glue — Ejecución y monitoreo de un Glue Job
=====================================================
Inicia un AWS Glue Job, espera a que complete y verifica el resultado.
Demuestra el uso de GlueJobOperator y GlueJobSensor del provider de Amazon.

Conexión requerida en Airflow (Admin > Connections):
  - Conn ID : aws_default
  - Conn Type: Amazon Web Services
  - Extra    : {"region_name": "us-east-1"}
  Las credenciales se toman del volumen montado en ~/.aws/credentials

Variables de Airflow requeridas (Admin > Variables):
  - glue_job_name        : Nombre del Glue Job en AWS
  - glue_script_bucket   : Bucket S3 donde vive el script del job
  - glue_output_path     : Ruta S3 de salida (ej. s3://mi-bucket/output/)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def _prepare_glue_arguments(**context) -> dict:
    """Construye los argumentos del job Glue con el contexto de ejecución."""
    args = {
        "--execution_date": context["ds"],
        "--output_path": Variable.get("glue_output_path"),
        "--env": "dev",
    }
    print(f"Argumentos para Glue: {args}")
    return args


def _verify_glue_output(**context) -> None:
    """Verifica que el output del job Glue exista en S3."""
    import boto3

    output_path: str = Variable.get("glue_output_path")
    # s3://bucket/prefix/  -> bucket, prefix/
    parts = output_path.replace("s3://", "").split("/", 1)
    bucket, prefix = parts[0], parts[1] if len(parts) > 1 else ""
    prefix = f"{prefix}{context['ds_nodash']}/"

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    count = response.get("KeyCount", 0)

    if count == 0:
        raise FileNotFoundError(
            f"El job Glue no generó archivos en s3://{bucket}/{prefix}"
        )
    print(f"Output verificado: {count} archivo(s) encontrado(s) en s3://{bucket}/{prefix}")


with DAG(
    dag_id="aws_glue_job",
    description="Ejecuta y monitorea un AWS Glue Job",
    schedule_interval="0 7 * * 1-5",    # Lunes a viernes a las 7 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["aws", "glue", "etl"],
) as dag:

    prepare_args = PythonOperator(
        task_id="prepare_glue_arguments",
        python_callable=_prepare_glue_arguments,
    )

    # GlueJobOperator: inicia el job y retorna el job_run_id en XCom
    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name=Variable.get("glue_job_name", default_var="my-glue-job"),
        script_location=f"s3://{ Variable.get('glue_script_bucket', default_var='my-bucket') }/scripts/etl_job.py",
        s3_bucket=Variable.get("glue_script_bucket", default_var="my-bucket"),
        iam_role_name="AWSGlueServiceRole",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        script_args={
            "--execution_date": "{{ ds }}",
            "--output_path": Variable.get("glue_output_path", default_var="s3://my-bucket/output/"),
        },
        # Número de DPUs asignados al job
        num_of_dpus=2,
        wait_for_completion=False,      # El sensor se encarga de esperar
    )

    # GlueJobSensor: hace polling hasta que el job termine
    wait_for_glue = GlueJobSensor(
        task_id="wait_for_glue_job",
        job_name=Variable.get("glue_job_name", default_var="my-glue-job"),
        run_id="{{ task_instance.xcom_pull('run_glue_job') }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=60 * 60 * 2,           # Timeout de 2 horas
    )

    verify_output = PythonOperator(
        task_id="verify_glue_output",
        python_callable=_verify_glue_output,
    )

    prepare_args >> run_glue_job >> wait_for_glue >> verify_output
