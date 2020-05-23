FROM puckel/docker-airflow:1.10.9
USER root
RUN apt update && apt-get install -y openssh-server
ENV AIRFLOW_USER_HOME=/usr/local/airflow
COPY airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
USER airflow
