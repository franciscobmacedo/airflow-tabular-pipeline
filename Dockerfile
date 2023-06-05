FROM apache/airflow:2.6.1-python3.10
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" tabular_pipeline==0.2.3