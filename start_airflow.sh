#!/bin/bash

# Start the Airflow scheduler in the background
exec airflow scheduler &

# Start the Airflow webserver in the foreground
exec airflow webserver