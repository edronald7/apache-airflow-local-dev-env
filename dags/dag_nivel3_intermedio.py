"""
NIVEL 3 — Intermedio: ShortCircuit, Variables y múltiples ramas
===============================================================
Pipeline con lógica de cortocircuito: si no es día hábil, el DAG
se detiene limpiamente sin marcar error. Si es día hábil, procesa
dos flujos independientes (ventas y inventario) antes de un cierre.

Demuestra:
  - ShortCircuitOperator — salta el resto si la condición es False
  - Airflow Variables — configuración externalizada
  - Dos ramas paralelas que convergen en una tarea final
  - Parámetros dinámicos con Jinja templating ({{ ds }})

Grafo:
  es_dia_habil? ──[False: SKIP]
        │ True
        ▼
  ┌─ procesar_ventas    ─┐
  │                      ├─ cierre_diario
  └─ procesar_inventario ┘
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _es_dia_habil(**context) -> bool:
    """Retorna True si es lunes-viernes; False si es finde (ShortCircuit lo salta todo)."""
    weekday = datetime.strptime(context["ds"], "%Y-%m-%d").weekday()
    es_habil = weekday < 5
    dias = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
    print(f"Hoy es {dias[weekday]} — {'día hábil ✓' if es_habil else 'fin de semana, omitiendo pipeline'}")
    return es_habil


def _procesar_ventas(**context) -> dict:
    umbral = float(Variable.get("ventas_umbral_minimo", default_var="1000"))
    ventas_simuladas = 4_532.75
    print(f"Procesando ventas del {context['ds']}:")
    print(f"  Umbral mínimo configurado : ${umbral:,.2f}")
    print(f"  Total ventas del día      : ${ventas_simuladas:,.2f}")
    alerta = ventas_simuladas < umbral
    if alerta:
        print("  ⚠ Ventas por debajo del umbral")
    return {"total": ventas_simuladas, "alerta": alerta}


def _procesar_inventario(**context) -> dict:
    productos = [
        {"sku": "P001", "stock": 45, "min_stock": 10},
        {"sku": "P002", "stock": 3,  "min_stock": 10},
        {"sku": "P003", "stock": 120, "min_stock": 20},
    ]
    bajo_stock = [p for p in productos if p["stock"] < p["min_stock"]]
    print(f"Inventario revisado: {len(productos)} productos, {len(bajo_stock)} con stock bajo")
    for p in bajo_stock:
        print(f"  ⚠ {p['sku']} tiene {p['stock']} unidades (mínimo: {p['min_stock']})")
    return {"total_productos": len(productos), "bajo_stock": len(bajo_stock)}


def _cierre_diario(**context) -> None:
    ti = context["ti"]
    ventas: dict = ti.xcom_pull(task_ids="procesar_ventas")
    inventario: dict = ti.xcom_pull(task_ids="procesar_inventario")

    print("=" * 40)
    print(f"CIERRE DIARIO — {context['ds']}")
    print(f"  Ventas totales   : ${ventas['total']:,.2f}  {'⚠' if ventas['alerta'] else '✓'}")
    print(f"  Productos OK     : {inventario['total_productos'] - inventario['bajo_stock']}")
    print(f"  Bajo stock       : {inventario['bajo_stock']}  {'⚠' if inventario['bajo_stock'] else '✓'}")
    print("=" * 40)


with DAG(
    dag_id="nivel3_shortcircuit_variables",
    description="ShortCircuit en días no hábiles + Variables + ramas paralelas",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nivel-3", "shortcircuit", "variables"],
) as dag:

    es_habil = ShortCircuitOperator(
        task_id="es_dia_habil",
        python_callable=_es_dia_habil,
    )

    ventas = PythonOperator(
        task_id="procesar_ventas",
        python_callable=_procesar_ventas,
    )

    inventario = PythonOperator(
        task_id="procesar_inventario",
        python_callable=_procesar_inventario,
    )

    # BashOperator con Jinja: {{ ds }} se reemplaza en tiempo de ejecución
    backup = BashOperator(
        task_id="backup_resultados",
        bash_command='echo "Backup del día {{ ds }} generado en /backups/{{ ds_nodash }}.tar.gz"',
    )

    cierre = PythonOperator(
        task_id="cierre_diario",
        python_callable=_cierre_diario,
    )

    es_habil >> [ventas, inventario] >> backup >> cierre
