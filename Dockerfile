# Extends the official Airflow image with the packages our DAGs need.
#
# Why a custom image instead of pip-installing at container startup:
# startup installs re-run on every restart, slow things down, and can
# silently pull a different version than you tested with. Baking deps
# into the image makes the environment reproducible.

FROM apache/airflow:3.0.2

# Switch to the airflow user before pip. The base image runs as root at
# build time, and pip installing as root triggers warnings and can put
# packages where the airflow user can't reach them.
USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt

RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
