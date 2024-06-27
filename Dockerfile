# Use the official Airflow image as the base image
FROM apache/airflow:2.6.2

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps wget && \
    apt-get clean;

USER airflow

# Copy DAGs into the image
COPY dags/ $AIRFLOW_HOME/dags/

# Copy the JSON file into the image
COPY start_airflow.sh /usr/local/bin/start_airflow.sh
# Copy your plugins if you have any
# COPY plugins/ $AIRFLOW_HOME/plugins/

WORKDIR $AIRFLOW_HOME/dags

# Copy custom requirements if you have any
COPY requirements.txt $AIRFLOW_HOME/requirements.txt
RUN pip install  -r $AIRFLOW_HOME/requirements.txt
ARG AIRFLOW_USERNAME="admin"
ARG AIRFLOW_PASSWORD="admin"
ARG AIRFLOW_FIRSTNAME="Admin"
ARG AIRFLOW_LASTNAME="User"
ARG AIRFLOW_EMAIL="admin@example.com"
RUN airflow db init
RUN airflow users create \
    --username $AIRFLOW_USERNAME \
    --firstname $AIRFLOW_FIRSTNAME \
    --lastname $AIRFLOW_LASTNAME \
    --role Admin \
    --email $AIRFLOW_EMAIL \
    --password $AIRFLOW_PASSWORD

# Expose the port for the webserver
EXPOSE 8080

# Start Airflow webserver and scheduler by default

# Set the entrypoint to the script
ENTRYPOINT ["/usr/local/bin/start_airflow.sh"]