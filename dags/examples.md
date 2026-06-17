# DAG Examples — Apache Airflow 2.6

> 🌐 **Language:** You are reading the English version (default). La versión en español está disponible en [`examples_es.md`](./examples_es.md).

This directory contains example DAGs that demonstrate different patterns and real-world use cases with Airflow. They serve as a starting point for building production flows.

---

## Index

### Real-world use case DAGs

| File | DAG ID | Purpose |
|---|---|---|
| `dag_oci_dataflow.py` | `oci_dataflow_job` | Launch and monitor an OCI Data Flow job |
| `dag_aws_glue.py` | `aws_glue_job` | Run an AWS Glue Job with a waiting sensor |
| `dag_etl_pipeline.py` | `etl_pipeline_with_branching` | Full ETL with parallel extraction and branching |
| `dag_parallel_processing.py` | `parallel_regional_processing` | Parallel processing with TaskGroups |
| `dag_data_quality.py` | `data_quality_checks` | Battery of quality checks with automatic decision |
| `dag_utilities.py` | `maintenance_utilities` | System maintenance: disk, logs and connections |

### Progressive complexity DAGs (Levels 1 → 5)

| File | DAG ID | Level | Description |
|---|---|---|---|
| `dag_nivel1_hello_world.py` | `nivel1_hello_world` | ⭐ Basic | A single task, minimal DAG structure |
| `dag_nivel2_secuencial.py` | `nivel2_cadena_secuencial` | ⭐⭐ Basic+ | Linear chain Bash → Python → Python with XCom |
| `dag_nivel3_intermedio.py` | `nivel3_shortcircuit_variables` | ⭐⭐⭐ Intermediate | ShortCircuit on non-business days + Variables + parallel branches |
| `dag_nivel4_avanzado.py` | `nivel4_dynamic_task_mapping` | ⭐⭐⭐⭐ Advanced | Dynamic Task Mapping + TimeSensor + @task decorator |
| `dag_nivel5_complejo.py` | `nivel5_pipeline_data_lake` | ⭐⭐⭐⭐⭐ Complex | E2E Data Lake pipeline — all advanced patterns |

---

## 1. OCI Dataflow Job (`dag_oci_dataflow.py`)

### Purpose
Submits a Spark application to **OCI Data Flow**, waits for it to complete via active polling, and logs the run metrics.

### Task graph
```
submit_dataflow_run → wait_for_dataflow_run → log_run_metrics
```

### Key components

| Element | Detail |
|---|---|
| Operator | `PythonOperator` with the `oci` SDK |
| Authentication | OCI profile loaded from `~/.oci/config` (mounted volume) |
| Inter-task communication | XCom — `submit` publishes the `run_id`, `wait` and `metrics` consume it |
| Wait timeout | 3 hours (`execution_timeout`) |
| Retries | 1 retry with a 5 min delay |

### Required Airflow variables
Go to **Admin > Variables** and create:

| Variable | Example |
|---|---|
| `oci_compartment_id` | `ocid1.compartment.oc1..xxxxxxx` |
| `oci_dataflow_app_id` | `ocid1.dataflowapplication.oc1..xxxxxxx` |
| `oci_dataflow_bucket` | `my-dataflow-bucket` |
| `oci_dataflow_ns` | `my-namespace` |

### Schedule
`0 6 * * *` — Daily at 6:00 AM.

---

## 2. AWS Glue Job (`dag_aws_glue.py`)

### Purpose
Starts an **AWS Glue Job** with dynamic arguments based on the execution date, waits for its completion with a dedicated sensor, and verifies that the output exists in S3.

### Task graph
```
prepare_glue_arguments → run_glue_job → wait_for_glue_job → verify_glue_output
```

### Key components

| Element | Detail |
|---|---|
| Main operator | `GlueJobOperator` (`apache-airflow-providers-amazon` provider) |
| Waiting sensor | `GlueJobSensor` — polling every 30 sec, 2 hour timeout |
| Verification | `boto3` — lists objects in S3 to confirm the output |
| AWS credentials | `aws_default` connection + `~/.aws/credentials` volume |

### Required Airflow connection
Go to **Admin > Connections** and create:

| Field | Value |
|---|---|
| Conn ID | `aws_default` |
| Conn Type | `Amazon Web Services` |
| Extra | `{"region_name": "us-east-1"}` |

### Required Airflow variables

| Variable | Example |
|---|---|
| `glue_job_name` | `my-etl-job` |
| `glue_script_bucket` | `my-scripts-bucket` |
| `glue_output_path` | `s3://my-bucket/output/` |

### Schedule
`0 7 * * 1-5` — Monday to Friday at 7:00 AM.

---

## 3. ETL Pipeline with Branching (`dag_etl_pipeline.py`)

### Purpose
A full ETL pipeline that demonstrates essential patterns: parallel extraction from multiple sources, integrity validation and conditional branching of the flow.

### Task graph
```
check_sources ─┬─ extract_api ─┬─ validate_data ─ branch_on_validation ─┬─ load_warehouse ─┬─ cleanup
               └─ extract_db  ─┘                                         └─ notify_failure  ─┘
```

### Key components

| Element | Detail |
|---|---|
| Parallel extraction | `extract_api` and `extract_db` run in parallel automatically |
| Validation | Rule: a minimum of 1,000 combined records to continue |
| `BranchPythonOperator` | Chooses between `load_warehouse` or `notify_failure` based on validation |
| `TriggerRule.ALL_DONE` | `cleanup` **always** runs, regardless of which branch executed |

### Patterns demonstrated
- **Natural parallelism**: two tasks with no dependencies between them run simultaneously.
- **XCom**: each task publishes its result; the next one consumes it with `xcom_pull`.
- **BranchPythonOperator**: returns the `task_id` of the next task to run.
- **TriggerRule**: controls when a task should run based on the state of its predecessors.

### Schedule
`0 2 * * *` — Daily at 2:00 AM.

---

## 4. Regional Parallel Processing (`dag_parallel_processing.py`)

### Purpose
Processes 4 geographic regions in parallel using **TaskGroups**, then aggregates the results and generates a consolidated report. Ideal for jobs partitioned by region, customer, date, etc.

### Task graph
```
prepare_partitions
      │
      ▼
┌─────────────────────────────────────────────────┐
│  TaskGroup: process_regions                     │
│  ┌──────────┐ ┌──────────┐ ┌─────────┐ ┌─────┐ │
│  │ norte    │ │ sur      │ │oriente  │ │occid│ │
│  └──────────┘ └──────────┘ └─────────┘ └─────┘ │
└─────────────────────────────────────────────────┘
      │
      ▼
aggregate_results → generate_report
```

### Key components

| Element | Detail |
|---|---|
| `TaskGroup` | Visually groups the 4 parallel tasks in the Airflow UI |
| Dynamic generation | A `for region in REGIONS` loop creates the tasks programmatically |
| XCom with group prefix | `xcom_pull(task_ids="process_regions.process_norte")` |
| Scalability | Adding a region to the `REGIONS` array creates the task automatically |

### Patterns demonstrated
- **TaskGroup**: visual organization in the UI without affecting execution.
- **Dynamic task generation**: a list of values generates N tasks in code.
- **Fan-out / Fan-in**: multiple parallel tasks that converge into a single one.

### Schedule
`0 3 * * *` — Daily at 3:00 AM.

---

## 5. Data Quality Checks (`dag_data_quality.py`)

### Purpose
Runs 5 quality validations **in parallel** on a dataset. At the end, it makes an automatic decision based on the number of failed checks: approve, quarantine or reject.

### Task graph
```
start ─┬─ check_completeness ─┐
       ├─ check_uniqueness    ─┤
       ├─ check_freshness     ─┼─ evaluate_checks ─┬─ approve_dataset
       ├─ check_volume        ─┤                    ├─ quarantine_dataset
       └─ check_value_range   ─┘                    └─ reject_dataset
```

### Implemented checks

| Check | What it validates | Threshold |
|---|---|---|
| `completeness` | % of nulls in critical columns | ≤ 5% |
| `uniqueness` | % of duplicate records | ≤ 1% |
| `freshness` | Age of the data | ≤ 26 hours |
| `volume` | Number of records | 500 – 100,000 |
| `value_range` | Values out of numeric range | 0% |

### Decision logic

| Failed checks | Action |
|---|---|
| 0 | `approve_dataset` — publish to the consumption layer |
| 1 – 2 | `quarantine_dataset` — requires manual review |
| 3 or more | `reject_dataset` — raises an exception, the DAG fails |

### Key components

| Element | Detail |
|---|---|
| `TriggerRule.ALL_DONE` on `evaluate_checks` | Waits for all checks even if one fails |
| `BranchPythonOperator` | Returns the `task_id` of the action to take |
| `retries: 0` | Quality checks are not retried — the data is either fine or not |

### Schedule
`0 4 * * *` — Daily at 4:00 AM.

---

## 6. System Maintenance (`dag_utilities.py`)

### Purpose
An operations DAG that keeps the Airflow environment healthy: checks disk space, cleans old logs, removes temporary files and reviews the registered connections.

### Task graph
```
check_disk_space ─┬─ clean_airflow_logs        ─┐
                  ├─ clean_tmp_files (Bash)     ─┼─ maintenance_summary
                  └─ check_airflow_connections  ─┘
                              (ALL_DONE)
```

### Key components

| Element | Detail |
|---|---|
| `BashOperator` | Runs system commands (`find`, `rm`) to clean temporary files |
| `on_failure_callback` | Global callback defined in `default_args` — notifies any failure |
| `shutil.disk_usage` | Checks space and raises an error if it drops below 10% |
| `provide_session` | Accesses the Airflow DB directly to list connections |
| `TriggerRule.ALL_DONE` | The summary is always generated, even if a task fails |

### Patterns demonstrated
- **`on_failure_callback`**: hook for external notifications (Slack, email, etc.).
- **`BashOperator`**: integration with the operating system.
- **Internal Airflow access**: queries to the metadata database with `provide_session`.
- **Guaranteed closing task**: `summary` always runs thanks to `ALL_DONE`.

### Schedule
`0 1 * * 0` — Sundays at 1:00 AM.

---

## Airflow concepts used

### Operators used

| Operator | Description |
|---|---|
| `PythonOperator` | Runs a Python function |
| `BashOperator` | Runs shell commands |
| `BranchPythonOperator` | Decides which DAG branch to run |
| `EmptyOperator` | Start/end node with no logic (formerly `DummyOperator`) |
| `GlueJobOperator` | Starts an AWS Glue Job (Amazon provider) |
| `GlueJobSensor` | Waits for a Glue Job to finish (Amazon provider) |

### Patterns demonstrated

| Pattern | DAG that uses it |
|---|---|
| XCom — passing data between tasks | All |
| Natural parallelism | ETL Pipeline, Data Quality, Maintenance |
| Conditional branching | ETL Pipeline, Data Quality, Level 5 |
| Fan-out / Fan-in | Parallel Processing, Level 5 |
| TaskGroup (visual grouping) | Parallel Processing, Level 4, Level 5 |
| Nested TaskGroups | Level 5 |
| TriggerRule.ALL_DONE | ETL Pipeline, Data Quality, Maintenance, Level 5 |
| TriggerRule.ONE_SUCCESS | Level 5 |
| on_failure_callback | Maintenance, Level 5 |
| on_success_callback | Level 5 |
| sla_miss_callback + per-task SLA | Level 5 |
| Dynamic task generation | Parallel Processing, Level 4 |
| Sensor with polling | AWS Glue |
| TimeSensor (time window) | Level 4 |
| ShortCircuitOperator | Level 3, Level 5 |
| Dynamic Task Mapping (.expand()) | Level 4 |
| @task decorator | Level 4 |
| retry_exponential_backoff | Level 5 |
| Airflow Variables | Level 3, Level 5 |
| Jinja templating ({{ ds }}) | Level 3 |
| External SDK (OCI, boto3) | OCI Dataflow, AWS Glue |

---

## Complexity levels — technical summary

### Level 1 — Hello World
One Python function, one operator, no dependencies. The minimum mandatory structure of every DAG.

### Level 2 — Sequential Chain
Three different operators in a straight line. Introduces XCom to share data and `BashOperator` for shell commands.

### Level 3 — Intermediate
`ShortCircuitOperator` for high-level conditional logic. Airflow Variables for externalized configuration. Jinja templating in `BashOperator`. Two parallel branches converging.

### Level 4 — Advanced
`Dynamic Task Mapping` with `.expand()`: generates N tasks at runtime based on the data, without hardcoding. `TimeSensor` controls the execution window. The `@task` decorator as modern syntax.

### Level 5 — Very Complex
A real production pipeline with 6 stages (Control → Ingestion → Quality → Transform → Delivery → Audit). Combines all the previous patterns plus: SLA miss callback, retry with exponential backoff, 3-layer nested TaskGroups, multiple TriggerRules across different tasks, and global success/failure callbacks.

---

## `schedule_interval` — Scheduling guide

The `schedule_interval` argument defines **when** and **how often** a DAG runs. It accepts three formats: cron expressions, predefined Airflow macros and `timedelta` objects.

> **Important note:** `start_date` defines the date from which Airflow starts counting. The first run happens at the end of the **first interval** after `start_date`, not at `start_date` itself. To avoid past runs, always use `catchup=False`.

---

### Format 1 — Cron expression

```
┌─────────── minute       (0 - 59)
│ ┌───────── hour         (0 - 23)
│ │ ┌─────── day of month (1 - 31)
│ │ │ ┌───── month        (1 - 12)
│ │ │ │ ┌─── day of week  (0=Sun, 1=Mon … 6=Sat)
│ │ │ │ │
* * * * *
```

#### Special characters

| Character | Meaning | Example |
|---|---|---|
| `*` | Any value | `* * * * *` = every minute |
| `,` | List of values | `0 6,18 * * *` = at 6 AM and 6 PM |
| `-` | Range | `0 9-17 * * 1-5` = every hour from 9 to 17, Monday to Friday |
| `/` | Step/interval | `*/15 * * * *` = every 15 minutes |
| `L` | Last | `0 0 L * *` = last day of the month (not supported in all versions) |

---

### Cron examples by frequency

#### Every N minutes / hours

| `schedule_interval` | Description |
|---|---|
| `"*/5 * * * *"` | Every 5 minutes |
| `"*/15 * * * *"` | Every 15 minutes |
| `"*/30 * * * *"` | Every 30 minutes |
| `"0 * * * *"` | Every hour (on the hour) |
| `"0 */2 * * *"` | Every 2 hours |
| `"0 */6 * * *"` | Every 6 hours |
| `"0 */12 * * *"` | Every 12 hours |

#### Daily

| `schedule_interval` | Description |
|---|---|
| `"0 0 * * *"` | Midnight (00:00) |
| `"0 1 * * *"` | 1:00 AM |
| `"0 2 * * *"` | 2:00 AM — recommended for nightly ETLs |
| `"0 6 * * *"` | 6:00 AM |
| `"30 7 * * *"` | 7:30 AM |
| `"0 20 * * *"` | 8:00 PM |
| `"59 23 * * *"` | 11:59 PM |

#### Business days only (Monday to Friday)

| `schedule_interval` | Description |
|---|---|
| `"0 6 * * 1-5"` | 6:00 AM, Mon–Fri |
| `"0 7 * * 1-5"` | 7:00 AM, Mon–Fri |
| `"30 8 * * 1-5"` | 8:30 AM, Mon–Fri |
| `"0 18 * * 1-5"` | 6:00 PM, Mon–Fri (close of day) |
| `"0 0 * * 1-5"` | Midnight, business days only |

#### Weekly

| `schedule_interval` | Description |
|---|---|
| `"0 1 * * 0"` | Sundays at 1:00 AM |
| `"0 1 * * 1"` | Mondays at 1:00 AM (start of week) |
| `"0 6 * * 5"` | Fridays at 6:00 AM |
| `"0 8 * * 1,3,5"` | Monday, Wednesday and Friday at 8:00 AM |
| `"0 8 * * 2,4"` | Tuesday and Thursday at 8:00 AM |

#### Monthly

| `schedule_interval` | Description |
|---|---|
| `"0 0 1 * *"` | First day of every month at midnight |
| `"0 6 1 * *"` | First day of the month at 6:00 AM |
| `"0 0 15 * *"` | 15th of every month at midnight |
| `"0 0 1,15 * *"` | 1st and 15th of every month |
| `"0 6 28-31 * *"` | Last days of the month (approx.) at 6:00 AM |

#### Yearly / quarterly

| `schedule_interval` | Description |
|---|---|
| `"0 0 1 1 *"` | January 1 at midnight (yearly) |
| `"0 0 1 1,4,7,10 *"` | Start of each quarter |
| `"0 0 1 */3 *"` | Every 3 months (quarterly) |
| `"0 0 1 */6 *"` | Every 6 months (semi-annual) |

---

### Format 2 — Predefined Airflow macros

Airflow includes shortcuts for the most common cases. They are exact equivalents of the cron expression shown next to them.

| Macro | Cron equivalent | Description |
|---|---|---|
| `"@once"` | _(no cron)_ | Runs once and never again |
| `"@hourly"` | `"0 * * * *"` | Every hour on the hour |
| `"@daily"` | `"0 0 * * *"` | Once a day at midnight |
| `"@midnight"` | `"0 0 * * *"` | Same as `@daily` |
| `"@weekly"` | `"0 0 * * 0"` | Once a week (Sundays at midnight) |
| `"@monthly"` | `"0 0 1 * *"` | Once a month (day 1 at midnight) |
| `"@quarterly"` | `"0 0 1 */3 *"` | Once per quarter |
| `"@yearly"` | `"0 0 1 1 *"` | Once a year (January 1) |
| `"@annually"` | `"0 0 1 1 *"` | Same as `@yearly` |
| `None` | _(no cron)_ | The DAG only runs if triggered manually |

---

### Format 3 — `timedelta` (relative interval)

Useful when the interval does not fit cleanly into cron (e.g. "every 90 minutes", "every 3 days").

```python
from datetime import timedelta

schedule_interval=timedelta(minutes=30)    # Every 30 minutes
schedule_interval=timedelta(hours=4)       # Every 4 hours
schedule_interval=timedelta(days=1)        # Every exact 24 hours
schedule_interval=timedelta(days=3)        # Every 3 days
schedule_interval=timedelta(weeks=1)       # Every 7 days
schedule_interval=timedelta(hours=1, minutes=30)  # Every 90 minutes
```

> **Key difference with cron:** `timedelta` calculates the interval from the previous execution, so the schedule can drift over time. Cron always runs at a fixed clock time.

---

### Date-related template variables

Inside tasks you can access execution dates via **Jinja templating** or via `context`:

| Jinja variable | `context` equivalent | Description |
|---|---|---|
| `{{ ds }}` | `context["ds"]` | Logical date: `2026-06-12` |
| `{{ ds_nodash }}` | `context["ds_nodash"]` | Without dashes: `20260612` |
| `{{ ts }}` | `context["ts"]` | ISO timestamp: `2026-06-12T06:00:00+00:00` |
| `{{ ts_nodash }}` | `context["ts_nodash"]` | `20260612T060000` |
| `{{ prev_ds }}` | `context["prev_ds"]` | Date of the previous run |
| `{{ next_ds }}` | `context["next_ds"]` | Date of the next scheduled run |
| `{{ execution_date }}` | `context["execution_date"]` | Full `datetime` object |
| `{{ dag_run.id }}` | `context["run_id"]` | Unique ID of the run |
| `{{ macros.ds_add(ds, 7) }}` | — | Adds 7 days to `ds` |
| `{{ macros.ds_add(ds, -1) }}` | — | Subtracts 1 day from `ds` |

**Example usage in `BashOperator`:**
```python
BashOperator(
    task_id="export",
    bash_command="python export.py --date {{ ds }} --output s3://bucket/{{ ds_nodash }}/",
)
```

**Example usage in `PythonOperator`:**
```python
def _process(**context):
    yesterday = context["prev_ds"]          # "2026-06-11"
    today     = context["ds"]               # "2026-06-12"
    run_id    = context["run_id"]           # "scheduled__2026-06-12T06:00:00+00:00"
```

---

### Tool to validate cron expressions

Before using a cron expression in production, validate it on **[crontab.guru](https://crontab.guru)** — it shows in plain language what each expression means and when the next trigger will be.
