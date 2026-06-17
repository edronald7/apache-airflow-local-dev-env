"""
NIVEL 2 — Cadena secuencial con múltiples operadores
=====================================================
Tres tareas en secuencia usando distintos operadores:
  BashOperator → PythonOperator → PythonOperator

Demuestra:
  - Pasar datos entre tareas con XCom
  - Uso de BashOperator para shell commands
  - Cadena lineal de dependencias con >>
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _procesar_fecha(**context) -> dict:
    ds = context["ds"]
    year, month, day = ds.split("-")
    result = {
        "año": int(year),
        "mes": int(month),
        "dia": int(day),
        "es_fin_de_semana": datetime.strptime(ds, "%Y-%m-%d").weekday() >= 5,
    }
    print(f"Fecha procesada: {result}")
    return result


def _generar_resumen(**context) -> None:
    ti = context["ti"]
    fecha: dict = ti.xcom_pull(task_ids="procesar_fecha")
    tipo = "fin de semana" if fecha["es_fin_de_semana"] else "día hábil"
    print("=" * 35)
    print(f"Resumen del {context['ds']}")
    print(f"  Año  : {fecha['año']}")
    print(f"  Mes  : {fecha['mes']}")
    print(f"  Día  : {fecha['dia']}")
    print(f"  Tipo : {tipo}")
    print("=" * 35)


with DAG(
    dag_id="nivel2_cadena_secuencial",
    description="Cadena lineal — Bash → Python → Python",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nivel-2", "secuencial", "xcom"],
) as dag:

    obtener_fecha = BashOperator(
        task_id="obtener_fecha_sistema",
        bash_command='echo "Fecha del sistema: $(date +%Y-%m-%d_%H:%M:%S)"',
    )

    procesar = PythonOperator(
        task_id="procesar_fecha",
        python_callable=_procesar_fecha,
    )

    resumen = PythonOperator(
        task_id="generar_resumen",
        python_callable=_generar_resumen,
    )

    obtener_fecha >> procesar >> resumen
