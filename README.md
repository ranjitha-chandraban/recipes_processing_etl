# Clone the repository
- ### The below command is used to clone the repo
```
https://github.com/ranjitha-chandraban/recipes_processing_etl.git
```
- ### Step 1: Navigate to the dags directory
```
cd ./recipes_processing_etl
```
- ### Step 2: Run Docker Compose
```
docker-compose -f docker_compose.yml up -d
```
This command will start the Docker containers defined in your docker_compose.yml file. The -d flag runs the containers in detached mode.
- ### Step 3: Access Airflow Web UI. Open your web browser and go to the specified URL
```
http://localhost:8080
```
- ### Step 4: Log In to Airflow Use the credentials:
•	Username: admin
•	Password: admin
- ### Step 5: Navigate to the DAG
On the Airflow web UI, find the DAG named data_processing_dag in the list of DAGs. Click on the DAG name to open the DAG details page.



