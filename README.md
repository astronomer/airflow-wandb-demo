
Overview
========
This demonstration shows an Airflow integration with Weights and Biases.    
    

This workflow includes:
- Python virtual environment creation using the [Astro Buildkit for Docker](https://github.com/astronomer/astro-provider-venv)
- data ingest to [Snowflake](https://www.snowflake.com) using the [Astro SDK](https://github.com/astronomer/astro-sdk)
- transformations and tests with [DBT](https://www.getdbt.com/) via Astronomer [Cosmos](https://github.com/astronomer/astronomer-cosmos), 
- feature engineering, model training and predictions with the [Astro SDK](https://github.com/astronomer/astro-sdk) and scikit-learn
- model management with [Weights and Biases](https://wandb.ai)
    
<img style="display: block; float: right; max-width: 80%; height: auto; margin: auto; float: none!important;" src="images/dag.png">  

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for the Airflow DAG. 
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains additional directories for the services that will be built in the demo. Services included in this demo include:
    - [minio](https://min.io/): Object storage which is used for ingest staging as well as stateful backups for other services.  As an alternative to S3 or GCS this service is controlled by the user and provides a local option keeping data inside Snowflake.
- packages.txt: Install OS-level packages needed for the project.
- requirements.txt: Install Python packages needed for the project.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

Prerequisites:
Docker Desktop or similar Docker services running locally.  
Snowflake account or [Trial Account](https://signup.snowflake.com/)
W&B account or [Trial Account](https://wandb.ai/signup)
  
1. Install [Astronomver CLI](https://github.com/astronomer/astro-cli).  The Astro CLI is a command-line interface for data orchestration. It allows you to get started with Apache Airflow quickly and it can be used with all Astronomer products. This will provide a local instance of Airflow if you don't have an existing service.
For MacOS  
```bash
brew install astro
```
  
For Linux
```bash
curl -sSL install.astronomer.io | sudo bash -s
```

2. Clone this repository.  
```bash
git clone https://github.com/astronomer/airflow-wandb-demo
cd airflow-wandb-demo
```
Edit the `.env` file and update the "AIRFLOW_CONN_SNOWFLAKE_DEFAULT" parameter with your Snowflake account information.  Update the "WANDB_API_KEY" and "WANDB_LICENSE_KEY" with your WANDB account information.
  
 3.  Start an Airflow instance..  
```bash
astro dev start
```
  
4. Run the Airflow DAG in the Airflow UI 
- Open [localhost:8080](http://localhost:8080) in a browser and login (username: `admin`, password: `admin`)
- Click the "Play" button for customer_analytics and select "Trigger DAG".
  
5. After testing in local dev mode update the .env file with S3 credentials/buckets and deploy to Astro Cloud.  
