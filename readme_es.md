# Apache Airflow 2.6 — Entorno Docker Local

> 🌐 **Idioma:** Estás leyendo la versión en español. The English version is available in [`readme.md`](./readme.md).

## Necesidad
Tener Apache Airflow corriendo localmente con Docker para desarrollar y probar DAGs antes de pasar a producción. El objetivo final es crear DAGs que ejecuten procesos Dataflow en OCI y AWS.

## Versiones
| Componente   | Versión      |
|--------------|--------------|
| Airflow      | 2.6.x        |
| Python       | 3.11         |
| Base de datos| PostgreSQL   |

## Proveedores Airflow (providers)
- `apache-airflow-providers-oracle` (OCI)
- `apache-airflow-providers-amazon` (AWS)
- `apache-airflow-providers-google` (GCP)

## Arquitectura Docker
El entorno se levanta con **Docker Compose** y contiene los siguientes servicios:

| Servicio      | Descripción                                              |
|---------------|----------------------------------------------------------|
| `webserver`   | Interfaz web de Airflow — expuesto en el puerto **8090** |
| `scheduler`   | Programador de DAGs                                      |
| `triggerer`   | Soporte para sensores y operadores deferibles            |

Executor utilizado: **LocalExecutor** (no requiere workers ni Celery adicionales).

### Base de datos PostgreSQL (externa)
PostgreSQL **no forma parte de este docker-compose**. Es un servicio independiente gestionado en otro proyecto Docker. Este proyecto se conecta a él mediante las variables definidas en el archivo `.env`.

El contenedor de PostgreSQL expone el puerto **5432** al host, y Airflow se conecta a través de `host.docker.internal` (o la IP del host según el entorno).

> 💡 **¿Aún no tienes un servidor PostgreSQL?** Puedes clonar y levantar este repositorio listo para usar, que inicia PostgreSQL (con pgAdmin) mediante Docker; allí podrás crear la base de datos y las credenciales que Airflow necesita: [edronald7/postgresql-pgadmin-local-dev-env](https://github.com/edronald7/postgresql-pgadmin-local-dev-env).
>
> ```bash
> git clone https://github.com/edronald7/postgresql-pgadmin-local-dev-env.git
> cd postgresql-pgadmin-local-dev-env
> cp .env.example .env
> docker compose up -d
> ```
>
> Una vez corriendo, crea la base de datos `airflow` y el usuario/contraseña, y usa esos valores en el `.env` de este proyecto.

## Credenciales de acceso
- **Usuario**: `admin`
- **Contraseña**: `admin123`
- **URL local**: http://localhost:8090

## Variables de entorno (`.env`)
La conexión a la base de datos y la configuración sensible de Airflow se define en un archivo `.env` en la raíz del proyecto. Este archivo **no debe subirse a control de versiones**.

Se incluye un `.env.example` como referencia:

```ini
# Conexión a PostgreSQL externo
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow_password

# Airflow
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=          # Generar con: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__WEBSERVER__SECRET_KEY=     # Cadena aleatoria segura

# Usuario administrador inicial
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin123
```

## Persistencia (volúmenes locales)
Los siguientes directorios se montan como volúmenes para persistir datos entre reinicios:

| Carpeta local     | Ruta en contenedor     | Contenido                   |
|-------------------|------------------------|-----------------------------|
| `./dags`          | `/opt/airflow/dags`    | Archivos DAG Python         |
| `./logs`          | `/opt/airflow/logs`    | Logs de ejecución de tareas |
| `./plugins`       | `/opt/airflow/plugins` | Plugins custom de Airflow   |

## Credenciales Cloud (montaje en contenedores)
Las credenciales se almacenan localmente y se montan en los contenedores:

| Proveedor | Carpeta local         | Ruta en contenedor |
|-----------|-----------------------|--------------------|
| OCI       | `./credentials/oci`   | `/home/airflow/.oci` |
| AWS       | `./credentials/aws`   | `/home/airflow/.aws` |
| GCP       | `./credentials/gcp`   | `/home/airflow/.gcp` |

## Estructura de carpetas esperada
```
airflow-2.6/
├── dags/                    # DAGs de desarrollo + ejemplos (ver dags/examples_es.md)
├── logs/                    # Logs generados por Airflow
├── plugins/                 # Plugins custom de Airflow
├── credentials/
│   ├── oci/                 # Configuración y llaves OCI (~/.oci)
│   ├── aws/                 # Credenciales AWS (~/.aws)
│   └── gcp/                 # Service account JSON de GCP
├── .env                     # Variables de entorno (NO versionar)
├── .env.example             # Plantilla de variables (SÍ versionar)
├── .gitignore               # Excluye .env y credentials/
├── fix-env.sh               # Corrige POSTGRES_HOST / AIRFLOW_UID / permisos (ver Solución de problemas)
├── Dockerfile               # Imagen custom de Airflow con providers
├── docker-compose.yml       # Servicios: webserver, scheduler, triggerer
├── readme.md                # Documentación (inglés, por defecto)
└── readme_es.md             # Documentación (español)
```

## DAGs de ejemplo
La carpeta `dags/` incluye **varios DAGs de ejemplo listos para usar** que cubren casos de uso reales y niveles de complejidad progresiva (1 → 5). Son un excelente punto de partida para construir tus propios flujos de producción.

| Archivo | DAG ID | Propósito |
|---|---|---|
| `dags/dag_oci_dataflow.py` | `oci_dataflow_job` | Lanzar y monitorear un job de OCI Data Flow |
| `dags/dag_aws_glue.py` | `aws_glue_job` | Ejecutar un AWS Glue Job con sensor de espera |
| `dags/dag_etl_pipeline.py` | `etl_pipeline_with_branching` | ETL completo con extracción paralela y branching |
| `dags/dag_parallel_processing.py` | `parallel_regional_processing` | Procesamiento paralelo con TaskGroups |
| `dags/dag_data_quality.py` | `data_quality_checks` | Checks de calidad con decisión automática |
| `dags/dag_utilities.py` | `maintenance_utilities` | Mantenimiento del sistema: disco, logs y conexiones |
| `dags/dag_nivel1_hello_world.py` → `dag_nivel5_complejo.py` | `nivel1` … `nivel5` | Tutoriales progresivos desde una sola tarea hasta un pipeline Data Lake completo |

> 📚 Consulta la documentación completa de cada ejemplo (grafos de tareas, variables/conexiones requeridas, patrones y programación) en [`dags/examples_es.md`](./dags/examples_es.md).

## Uso — Levantar el entorno

### Requisitos previos
- Docker y Docker Compose instalados.
- El contenedor PostgreSQL externo debe estar corriendo y accesible en el puerto **5432**. Si no tienes uno, clona [edronald7/postgresql-pgadmin-local-dev-env](https://github.com/edronald7/postgresql-pgadmin-local-dev-env) y levántalo con `docker compose up -d`.
- La base de datos `airflow` y el usuario deben existir previamente en PostgreSQL.

### 1. Configurar variables de entorno
```bash
cp .env.example .env
```
Editar `.env` con los datos reales de conexión a PostgreSQL y generar las claves de seguridad:

```bash
# Generar FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Generar SECRET_KEY
python -c "import secrets; print(secrets.token_hex(32))"
```

### 2. (Opcional) Ejecutar el script fix-env.sh
Si es tu primera vez, `./logs` y `./credentials` aún no existen y pueden quedar como `root` al crearlos Docker, y `POSTGRES_HOST` puede no coincidir con tu entorno. Ejecuta:
```bash
./fix-env.sh
```
Ajusta `AIRFLOW_UID` a tu UID del host, cambia `POSTGRES_HOST` a `host.docker.internal` si quedó como `postgres` (no existe un servicio `postgres` en este compose — Postgres es externo), y corrige los permisos de `./logs` y `./credentials`. Se puede ejecutar varias veces sin problema. Ver [Solución de problemas](#solución-de-problemas) para más detalle de los errores que soluciona.

### 3. Construir la imagen y levantar los servicios
```bash
docker compose up --build -d
```
El servicio `airflow-init` correrá primero, ejecutará las migraciones de BD y creará el usuario admin. Los demás servicios esperarán a que termine.

### 4. Verificar que los servicios estén saludables
```bash
docker compose ps
```
Todos los servicios deben mostrar estado `healthy` o `running`.

### 5. Acceder a la interfaz web
Abrir en el navegador: **http://localhost:8090**

- **Usuario**: `admin`
- **Contraseña**: `admin123` (o el valor definido en `.env`)

### Comandos útiles

| Acción | Comando |
|---|---|
| Levantar en segundo plano | `docker compose up -d` |
| Ver logs en tiempo real | `docker compose logs -f` |
| Ver logs de un servicio | `docker compose logs -f webserver` |
| Detener sin destruir | `docker compose stop` |
| Detener y destruir contenedores | `docker compose down` |
| Reconstruir imagen | `docker compose build --no-cache` |
| Reiniciar un servicio | `docker compose restart scheduler` |

> **Nota:** `docker compose down` **no** elimina los DAGs ni los logs porque están montados como volúmenes locales. Los datos de PostgreSQL tampoco se ven afectados ya que viven en el contenedor externo.

## Solución de problemas

### `could not translate host name "postgres" to address`
`airflow-init` (y luego `webserver`/`scheduler`/`triggerer`) no pueden alcanzar la base de datos. Este compose **no tiene servicio `postgres`** — Postgres es externo — así que `POSTGRES_HOST=postgres` en `.env` no resuelve. Usa `POSTGRES_HOST=host.docker.internal` (ya configurado vía `extra_hosts` en `docker-compose.yml`). Ojo: el script de `airflow-init` tiene `|| true`, así que puede salir con código `0` aunque esto falle silenciosamente — revisa siempre `docker compose logs airflow-init` si los demás servicios nunca quedan `healthy`.

### Permission denied al escribir en `./logs`
`./logs` y `./credentials` están en `.gitignore`, así que no existen al clonar el repo. En el primer `docker compose up`, Docker los crea en el host como `root:root` (el daemon crea las carpetas de los bind-mounts que faltan antes de que el contenedor aplique su `user:`). Como los servicios corren como `${AIRFLOW_UID}:0`, ese usuario no puede escribir ahí. Corrige los permisos con `sudo chown -R $(id -u):0 logs credentials`, o simplemente ejecuta `./fix-env.sh`.

### Usa `./fix-env.sh`
El script [`fix-env.sh`](./fix-env.sh) automatiza ambas correcciones de arriba (y ajusta `AIRFLOW_UID` a tu usuario del host). Se puede volver a ejecutar en cualquier momento con `./fix-env.sh`.

> ⚠️ Ejecútalo como tu usuario normal, **no** con `sudo ./fix-env.sh` — el script pide sudo él solo, únicamente para el paso de `chown`. Si corres todo el script como root, toma el UID `0` en vez del tuyo, dejando `AIRFLOW_UID=0` en `.env` y `./logs`/`./credentials` siguen siendo de `root`.

## Consideraciones
- La imagen base es `apache/airflow:2.6.3-python3.11` con los tres providers instalados.
- PostgreSQL es externo a este proyecto; este compose solo contiene los servicios de Airflow.
- Los DAGs persisten en `./dags`; el contenedor puede destruirse y recrearse sin perder nada.
- Los archivos `.env` y `credentials/` **no deben subirse a control de versiones** (definidos en `.gitignore`).