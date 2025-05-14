# BEES Data Engineering – Breweries Case

This project demonstrates a modern data pipeline architecture built to extract, transform, and load (ETL) data from the [Open Brewery DB API](https://www.openbrewerydb.org/). The pipeline follows the **medallion architecture** (bronze → silver → gold) and is orchestrated using **Apache Airflow** in a Dockerized environment.

---

## 🧱 Data Architecture Overview

- **Bronze**: Raw JSON data fetched from the Open Brewery API
- **Silver**: Cleaned and normalized data stored in partitioned Parquet files (by state)
- **Gold**: Aggregated summary of breweries per state and type

All data is stored locally in a lake-like folder structure (`/datalake/bronze`, `/silver`, `/gold`).

---

## Prerequisites
In case you don't have Docker Compose installed, please follow the installation instructions:
- [Install Docker Compose](https://docs.docker.com/compose/install/)

## 🚀 How to Run

### 1. Clone repository:
```
git clone https://github.com/george-sousa/bees-case.git
```

### 2. Initialize Airflow and start services

```
# Build the Docker image we are going to use, that's based on Airflow 3.0
docker compose build

# Initialize the environment (use the '-d' flag to run the Docker containers in the backgroud)
docker compose up -d
```

### (Optional) If you want to receive an email when there is a problem with the pipeline, it'll be necessary to insert you email in these lines on 'airflow.cfg' and it's recommended to use Airflow connections to store your password for that. If more information is needed, acess this [link](https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html#email-configuration)

```
from_email = "Your Name <your.email@gmail.com>"
smtp_mail_from = your.email@gmail.com
```

### 3. Check the status of the containers
```
# It will take some minutes for every container to be online. Check their status and confirm that they are healthy. If a problem appears, check this [guide](https://bobcares.com/blog/docker-error-container-is-unhealthy/)
docker ps
```

### 4. Airflow Web Interface

To acess the Airflow UI, use this link:

http://localhost:8080

Login credentials:
```
Username: airflow  
Password: airflow
```

### 5. Running the Pipeline
#### Airflow Web Interface
You can trigger the pipeline directly from the Airflow Web Interface. You can acess it directly with this link:

http://localhost:8080/dags/breweries_pipeline_dag

Click the Trigger button in the top-right corner to start the DAG.

## ⚙️ Design Decisions & Notes
No Cloud Resources Used: Given the scope and evaluation criteria, all resources are local. In a real-world scenario, a cloud-based solution (e.g., AWS S3, Glue, Redshift) would provide better governance, security, and scalability, especially considering a team with limited know-how of infra or with time constraints to keep the data lake working with the best practices in mind. 

Why MAX_PAGES Exists (in the "extract_from_api" file): To avoid infinite loops caused by potential API errors or misbehavior.

Expensive Imports Scoped: Heavy imports are placed inside functions to improve Airflow DAG parse performance, as recommended by Airflow Best Practices.

No Parallelism: The dataset is small, so parallel processing was deemed unnecessary and over-kill for this specific project.

I opted to fetch the API using the highest possible value of data within a page, since that's much more efficient for data extraction (as seen on the following print).
![image](https://github.com/user-attachments/assets/56ac3621-3e9f-4208-9cc6-9d8248503480)

For this project, one of the tasks was "Transform the data to a columnar storage format such as parquet or delta, and partition it by brewery location." I had to make a decision about what "brewery location" should mean in this context, since there were no field with this specific name. After analyzing the relevants fields from the API data and considering best practices for partitioning (low cardinality, even distribution, etc), I came to the conclusion that “state” should be the field used for partitioning. Of couse, on a production environment the wisest decision would be asking a colleague, but since the purpose of this case is to evaluete my habilities, I thought that working like this in this specific project would make more sense.

## 🔔 Monitoring & Alerting

### Airflow-Level Monitoring

- **Task-Level Status**: All DAG tasks are monitored via the Airflow UI. Successes, failures, retries, and durations are visible per task.
- **Retries & Timeouts**: Tasks have retry logic and timeout settings to ensure resilience against transient issues.
- **Logs**: Detailed logs for each run (including progress updates, warnings, and error traces) are written and accessible via the Airflow UI.

### Future Enhancements

- **Monitoring Dashboard**: Export logs and metrics to Prometheus/Grafana or use Airflow’s REST API for building custom dashboards.
- **Schema validation** using tools like Great Expectations
- **Anomaly detection** on metrics over time (e.g., drop in brewery counts)
- **Centralized Logging**: Redirect Airflow logs to a centralized location (e.g., S3, Elasticsearch) for production observability.
- **Audit Trail**: Persist metadata about each run (record counts, states) in a metadata database for tracking.


