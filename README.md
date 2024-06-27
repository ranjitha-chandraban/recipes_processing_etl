# HelloFresh
- ### The below command is used to clone the repo
```
https://github.com/ranjitha-chandraban/recipes_processing_etl.git
```
- ##### Step 1: Navigate to the dags directory
```
cd ./recipes_processing_etl
```
- ##### Step 2: Run Docker Compose
```
docker-compose -f docker_compose.yml up -d
```
This command will start the Docker containers defined in your docker_compose.yml file. The -d flag runs the containers in detached mode.
- ##### Step 3: Access Airflow Web UI. Open your web browser and go to the specified URL
```
http://localhost:8080
```
- ##### Step 4: Log In to Airflow Use the credentials:
Username: admin
Password: admin
- ##### Step 5: Find and Execute the DAG
5.1 On the Airflow web UI, find the DAG named "data_processing_dag" in the list of DAGs.
5.2 On the DAG details page, click the "Trigger DAG" button (typically represented by a play icon) to start the DAG execution. 
5.3 Click on the DAG name to open the DAG details page to monitor the progress of each task in the DAG graph view. If a task fails, it will be marked as failed, and you can click on the task to view the logs and diagnose the issue.








