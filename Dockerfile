FROM apache/airflow:2.6.3-python3.11

USER root

# Dependencias del sistema necesarias para los providers (OCI SDK, etc.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Providers: OCI y AWS (GCP desactivado temporalmente)
RUN pip install --no-cache-dir \
    "oci>=2.100.0" \
    "apache-airflow-providers-oracle==3.6.0" \
    "apache-airflow-providers-amazon==8.5.1"
