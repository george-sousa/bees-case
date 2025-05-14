FROM  apache/airflow:3.0.0-python3.11 as base

USER root

# configuracoes do Airflow
USER airflow

COPY requirements.txt requirements.txt

# removi o --user pois 'ERROR: Can not perform a '--user' install. User site-packages are not visible in this virtualenv.'
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH /opt/airflow/plugins

