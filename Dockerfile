FROM puckel/docker-airflow:1.10.9
USER root
ENV AIRFLOW_USER_HOME=/usr/local/airflow
COPY airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
USER airflow
