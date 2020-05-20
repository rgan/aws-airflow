FROM puckel/docker-airflow:1.10.9
ENV AIRFLOW_USER_HOME=/usr/local/airflow
COPY airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg