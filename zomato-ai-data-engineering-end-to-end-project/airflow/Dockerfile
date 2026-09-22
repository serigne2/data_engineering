FROM apache/airflow:3.0.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake \
    apache-airflow-providers-fab \
    boto3 \
    snowflake-connector-python

RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
       "dbt-snowflake==1.8.*"