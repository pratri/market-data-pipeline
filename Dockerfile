# Airflow image with the DAG dependencies baked in, so they don't get
# reinstalled (and possibly upgraded) on every container start.

FROM apache/airflow:3.0.2

# install as the airflow user, not root
USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt

RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
