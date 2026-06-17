"""
NIVEL 1 — Hello World
=====================
El DAG más simple posible: una sola tarea que imprime un mensaje.
Punto de partida para entender la estructura mínima de un DAG.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _saludar(**context):
    print(f"Hola desde Airflow!")
    print(f"Fecha de ejecución : {context['ds']}")
    print(f"DAG ID             : {context['dag'].dag_id}")
    print(f"Run ID             : {context['run_id']}")


with DAG(
    dag_id="nivel1_hello_world",
    description="DAG mínimo — una sola tarea",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nivel-1", "basico"],
) as dag:

    PythonOperator(
        task_id="saludar",
        python_callable=_saludar,
    )
