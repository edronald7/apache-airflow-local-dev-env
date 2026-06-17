"""
NIVEL 5 — Muy Complejo: Pipeline de Data Lake End-to-End
=========================================================
Pipeline completo de producción que combina todos los patrones avanzados
de Airflow en un flujo coherente de extremo a extremo.

Etapas:
  1. CONTROL    — verifica ventana horaria y disponibilidad de sistemas
  2. INGESTA    — extrae desde múltiples fuentes (paralelo, con reintentos)
  3. CALIDAD    — batería de validaciones con decisión automática
  4. TRANSFORM  — procesa por capas (raw → staging → curated) con TaskGroups
  5. ENTREGA    — publica en destinos (paralelo) con confirmación
  6. AUDITORÍA  — registra métricas, SLA y genera alertas si aplica

Patrones usados:
  ✓ on_success_callback / on_failure_callback globales
  ✓ SLA miss callback
  ✓ ShortCircuitOperator (ventana de mantenimiento)
  ✓ BranchPythonOperator (calidad: OK / degradado / fallo)
  ✓ TaskGroup anidados (transform por capas)
  ✓ TriggerRule: ALL_DONE, ONE_SUCCESS, ALL_SUCCESS
  ✓ XCom entre grupos
  ✓ Variables de Airflow para configuración
  ✓ Lógica de reintento diferenciada por tarea
  ✓ Decorador @task mezclado con operadores clásicos

Grafo (simplificado):
  verificar_ventana ──[skip si mantenimiento]
         │
  ┌──────┴────────────────┐
  │  INGESTA (paralelo)   │
  │  ingestar_crm         │
  │  ingestar_erp         │
  │  ingestar_web_events  │
  └──────────────────────-┘
         │
  validar_calidad_global
         │ [branch]
         ├── calidad_ok ──────────────┐
         ├── calidad_degradada ───────┤
         └── calidad_critica ─────────┤ (falla el DAG)
                                      │
         ┌────────────────────────────┘ (ONE_SUCCESS)
         │
  ┌──────┴──────────────────────────────────────────┐
  │  TRANSFORM                                      │
  │  ┌─────────────┐  ┌─────────────┐  ┌─────────┐ │
  │  │  raw_layer  │→ │staging_layer│→ │curated  │ │
  │  └─────────────┘  └─────────────┘  └─────────┘ │
  └─────────────────────────────────────────────────┘
         │
  ┌──────┴───────────────────┐
  │  ENTREGA (paralelo)      │
  │  publicar_api            │
  │  publicar_dashboard      │
  │  exportar_s3             │
  └──────────────────────────┘
         │
  auditoria_y_sla   (ALL_DONE — siempre corre)
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ─── Callbacks globales ───────────────────────────────────────────────────────

def _dag_success(context) -> None:
    duration = (datetime.utcnow() - context["dag_run"].start_date.replace(tzinfo=None)).seconds
    print(f"[OK] DAG completado en {duration}s para {context['ds']}")


def _dag_failure(context) -> None:
    task_id = context["task_instance"].task_id
    print(f"[ERROR] Falló la tarea '{task_id}' en DAG run de {context['ds']}")
    # En producción: webhook Slack, PagerDuty, etc.


def _sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    print(f"[SLA MISS] El DAG {dag.dag_id} superó el tiempo esperado")
    # En producción: escalada automática de incidente


DEFAULT_ARGS = {
    "owner":             "data-platform",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": True,   # Backoff exponencial entre reintentos
    "email_on_failure":  False,
    "on_failure_callback": _dag_failure,
}

# ─── Funciones de las tareas ──────────────────────────────────────────────────

def _verificar_ventana(**context) -> bool:
    """ShortCircuit: True=proceder, False=omitir (ventana de mantenimiento activa)."""
    en_mantenimiento = Variable.get("mantenimiento_activo", default_var="false").lower() == "true"
    hora_actual = datetime.utcnow().hour
    fuera_de_ventana = not (0 <= hora_actual <= 3)   # Mantenimiento 00-03 UTC

    if en_mantenimiento:
        print("Variable 'mantenimiento_activo' = true → pipeline detenido")
        return False
    if not fuera_de_ventana:
        print(f"Hora actual {hora_actual}:00 UTC dentro de ventana de mantenimiento → detenido")
        return False

    print(f"Sistema disponible — hora {hora_actual}:00 UTC, sin mantenimiento activo")
    return True


def _ingestar_fuente(fuente: str, registros_min: int, registros_max: int, **context) -> dict:
    """Ingesta datos de una fuente específica."""
    demora = random.uniform(1, 4)
    time.sleep(demora)
    registros = random.randint(registros_min, registros_max)
    errores = random.randint(0, max(1, registros // 200))
    resultado = {
        "fuente": fuente,
        "registros": registros,
        "errores": errores,
        "tasa_error": round(errores / registros * 100, 3),
        "duracion_seg": round(demora, 1),
    }
    print(f"[{fuente.upper()}] {registros:,} registros | {errores} errores | {demora:.1f}s")
    return resultado


def _validar_calidad_global(**context) -> str:
    """Evalúa la calidad agregada de todas las fuentes y bifurca el flujo."""
    ti = context["ti"]
    fuentes = ["ingestar_crm", "ingestar_erp", "ingestar_web_events"]
    total_registros = 0
    max_tasa_error = 0.0
    fallos_criticos = 0

    print("\n── Calidad de ingesta ──")
    for task_id in fuentes:
        r: dict = ti.xcom_pull(task_ids=task_id)
        if not r:
            fallos_criticos += 1
            print(f"  ✗ {task_id}: sin datos")
            continue
        total_registros += r["registros"]
        max_tasa_error = max(max_tasa_error, r["tasa_error"])
        estado = "✓" if r["tasa_error"] < 2 else "⚠"
        print(f"  {estado} {r['fuente']:<20} {r['registros']:>8,} reg | {r['tasa_error']:.3f}% errores")

    print(f"\n  Total registros: {total_registros:,}")
    print(f"  Tasa error máx : {max_tasa_error:.3f}%")

    if fallos_criticos > 0 or max_tasa_error > 10:
        print("  → Calidad CRÍTICA")
        return "calidad_critica"
    elif max_tasa_error > 3:
        print("  → Calidad DEGRADADA (continuará con advertencia)")
        return "calidad_degradada"
    else:
        print("  → Calidad OK")
        return "calidad_ok"


def _procesar_capa(nombre_capa: str, capa_anterior: str | None = None, **context) -> dict:
    """Procesa una capa del data lake (raw / staging / curated)."""
    ti = context["ti"]
    if capa_anterior:
        prev: dict = ti.xcom_pull(task_ids=capa_anterior)
        registros_entrada = prev.get("registros_salida", 10000)
    else:
        # Capa raw: toma el total de las ingesta
        fuentes = ["ingestar_crm", "ingestar_erp", "ingestar_web_events"]
        registros_entrada = sum(
            (ti.xcom_pull(task_ids=t) or {}).get("registros", 0) for t in fuentes
        )

    descarte = random.uniform(0, 0.05)           # 0-5% de registros descartados
    registros_salida = int(registros_entrada * (1 - descarte))

    print(f"[{nombre_capa.upper()}] Entrada: {registros_entrada:,} → Salida: {registros_salida:,} "
          f"(descartados: {registros_entrada - registros_salida:,})")
    return {"capa": nombre_capa, "registros_entrada": registros_entrada, "registros_salida": registros_salida}


def _publicar_destino(destino: str, **context) -> dict:
    """Publica datos en un destino específico."""
    ti = context["ti"]
    curated: dict = ti.xcom_pull(task_ids="transform.curated_layer.capa_curated")
    registros = curated.get("registros_salida", 0) if curated else 0
    tiempo = random.uniform(0.5, 3.0)
    time.sleep(tiempo)
    print(f"[{destino.upper()}] {registros:,} registros publicados en {tiempo:.1f}s")
    return {"destino": destino, "registros": registros, "duracion_seg": round(tiempo, 1)}


def _auditoria_y_sla(**context) -> None:
    """Tarea final: siempre corre. Registra métricas y evalúa SLA."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    start = dag_run.start_date.replace(tzinfo=None) if dag_run.start_date else datetime.utcnow()
    duracion_total = (datetime.utcnow() - start).seconds

    destinos = ["publicar_api", "publicar_dashboard", "exportar_s3"]
    total_publicados = 0
    for task_id in destinos:
        r: dict = ti.xcom_pull(task_ids=f"entrega.{task_id}") or {}
        total_publicados += r.get("registros", 0)

    sla_objetivo = int(Variable.get("pipeline_sla_segundos", default_var="600"))
    sla_ok = duracion_total <= sla_objetivo

    print("=" * 55)
    print(f"AUDITORÍA DEL PIPELINE — {context['ds']}")
    print("=" * 55)
    print(f"  Duración total : {duracion_total}s (SLA: {sla_objetivo}s) {'✓' if sla_ok else '⚠ SLA MISS'}")
    print(f"  Registros publ.: {total_publicados:,}")
    print(f"  Estado final   : {'ÉXITO' if sla_ok else 'ÉXITO CON SLA MISS'}")
    print("=" * 55)

    if not sla_ok:
        print(f"[ALERTA] Pipeline excedió SLA en {duracion_total - sla_objetivo}s")


# ─── Definición del DAG ───────────────────────────────────────────────────────

with DAG(
    dag_id="nivel5_pipeline_data_lake",
    description="Pipeline Data Lake E2E — todos los patrones avanzados combinados",
    schedule_interval="0 6 * * 1-5",    # Lunes a viernes a las 6 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_success_callback=_dag_success,
    sla_miss_callback=_sla_miss,
    tags=["nivel-5", "complejo", "data-lake", "e2e"],
) as dag:

    # ── CONTROL ───────────────────────────────────────────────────────────────
    verificar = ShortCircuitOperator(
        task_id="verificar_ventana",
        python_callable=_verificar_ventana,
    )

    # ── INGESTA (paralelo) ────────────────────────────────────────────────────
    with TaskGroup(group_id="ingesta") as grp_ingesta:

        crm = PythonOperator(
            task_id="ingestar_crm",
            python_callable=_ingestar_fuente,
            op_kwargs={"fuente": "CRM", "registros_min": 5_000, "registros_max": 20_000},
            retries=3,
        )

        erp = PythonOperator(
            task_id="ingestar_erp",
            python_callable=_ingestar_fuente,
            op_kwargs={"fuente": "ERP", "registros_min": 10_000, "registros_max": 50_000},
            retries=3,
        )

        web = PythonOperator(
            task_id="ingestar_web_events",
            python_callable=_ingestar_fuente,
            op_kwargs={"fuente": "WEB_EVENTS", "registros_min": 50_000, "registros_max": 500_000},
            retries=1,      # Web events: menos reintentos, tolerable perder algo
        )

    # ── CALIDAD (branch) ──────────────────────────────────────────────────────
    validar_calidad = BranchPythonOperator(
        task_id="validar_calidad_global",
        python_callable=_validar_calidad_global,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    calidad_ok        = EmptyOperator(task_id="calidad_ok")
    calidad_degradada = EmptyOperator(task_id="calidad_degradada")
    def _abortar_por_calidad(**ctx):
        raise RuntimeError("Calidad de datos crítica — pipeline abortado")

    calidad_critica = PythonOperator(
        task_id="calidad_critica",
        python_callable=_abortar_por_calidad,
        retries=0,
    )

    # Punto de convergencia: continúa si al menos una rama de calidad OK pasó
    continuar = EmptyOperator(
        task_id="continuar_transform",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # ── TRANSFORM (capas anidadas) ────────────────────────────────────────────
    with TaskGroup(group_id="transform") as grp_transform:

        with TaskGroup(group_id="raw_layer") as grp_raw:
            capa_raw = PythonOperator(
                task_id="capa_raw",
                python_callable=_procesar_capa,
                op_kwargs={"nombre_capa": "raw"},
            )

        with TaskGroup(group_id="staging_layer") as grp_staging:
            capa_staging = PythonOperator(
                task_id="capa_staging",
                python_callable=_procesar_capa,
                op_kwargs={"nombre_capa": "staging", "capa_anterior": "transform.raw_layer.capa_raw"},
            )

        with TaskGroup(group_id="curated_layer") as grp_curated:
            capa_curated = PythonOperator(
                task_id="capa_curated",
                python_callable=_procesar_capa,
                op_kwargs={"nombre_capa": "curated", "capa_anterior": "transform.staging_layer.capa_staging"},
            )

        grp_raw >> grp_staging >> grp_curated

    # ── ENTREGA (paralelo) ────────────────────────────────────────────────────
    with TaskGroup(group_id="entrega") as grp_entrega:

        api = PythonOperator(
            task_id="publicar_api",
            python_callable=_publicar_destino,
            op_kwargs={"destino": "API_REST"},
        )

        dashboard = PythonOperator(
            task_id="publicar_dashboard",
            python_callable=_publicar_destino,
            op_kwargs={"destino": "DASHBOARD"},
        )

        s3 = PythonOperator(
            task_id="exportar_s3",
            python_callable=_publicar_destino,
            op_kwargs={"destino": "S3_LAKE"},
        )

    # ── AUDITORÍA (siempre corre) ─────────────────────────────────────────────
    auditoria = PythonOperator(
        task_id="auditoria_y_sla",
        python_callable=_auditoria_y_sla,
        trigger_rule=TriggerRule.ALL_DONE,
        sla=timedelta(minutes=20),
    )

    # ── GRAFO DE DEPENDENCIAS ─────────────────────────────────────────────────
    verificar >> grp_ingesta >> validar_calidad
    validar_calidad >> [calidad_ok, calidad_degradada, calidad_critica]
    [calidad_ok, calidad_degradada] >> continuar
    continuar >> grp_transform >> grp_entrega >> auditoria
