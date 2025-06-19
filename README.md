YouTube Data Pipeline with Apache Airflow
This project implements an ETL (Extract, Transform, Load) data pipeline using Apache Airflow to collect statistics for a predefined list of YouTube channels. The pipeline extracts data using the YouTube Data API v3, transforms it using Pandas, and loads the processed data into a PostgreSQL database.

The entire Airflow environment, including the PostgreSQL database for Airflow's metadata and the target database for YouTube stats, is containerized using Docker Compose for easy setup and portability.

Features
Extraction: Fetches channel statistics (like subscriber count, view count, video count) and snippet details (like channel name, description, publish date) for a list of YouTube channels.
Transformation: Cleans and reshapes the raw API data into a structured format suitable for analysis using Pandas.
Loading: Loads the transformed data into a specified table in a PostgreSQL database.
Orchestration: Uses Apache Airflow for scheduling (e.g., daily runs) and monitoring the pipeline.
Containerized: Leverages Docker and Docker Compose for a consistent and isolated development and execution environment.
Project Structure
```text
.
├── dags/
│   └── youtube_pipeline_dag.py         # The main Airflow DAG file
├── plugins/
│   └── youtube/
│       ├── __init__.py
│       ├── data_transformation.py      # Handles data cleaning and transformation
│       ├── load_data.py                # Handles loading data to PostgreSQL
│       └── settings.py                 # Contains API key loading, channel list, and data extraction logic
├── .env.example
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

Prerequisites
Docker
Docker Compose
Git (for cloning the repository)
A YouTube Data API v3 Key. You can obtain one from the Google Cloud Console.
Setup and Configuration
Clone the Repository:

git clone https://github.com/YOUR_USERNAME/YOUR_REPOSITORY_NAME.git
cd YOUR_REPOSITORY_NAME
Configure Environment Variables:

Copy the example environment file:
cp .env.example .env
Edit the newly created .env file and add your YouTube Data API Key:
YT_API_KEY=YOUR_YOUTUBE_API_KEY_HERE
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-postgres>=5.0.0 psycopg2-binary google-api-python-client python-dotenv pandas sqlalchemy
AIRFLOW_UID=50000 # Or your desired user ID for file permissions
Note: The _PIP_ADDITIONAL_REQUIREMENTS line ensures necessary Python packages for the pipeline (like pandas, google-api-python-client, python-dotenv, sqlalchemy) and the Airflow Postgres provider are installed in the Airflow containers.
Build and Start Airflow Services: From the root directory of the project (where docker-compose.yml is located):

docker-compose up -d
This will build the images (if a Dockerfile is used) and start the Airflow webserver, scheduler, worker, PostgreSQL, and Redis containers. The -d flag runs them in detached mode. Wait a few minutes for all services to initialize.

Configure Airflow Connection:

Open the Airflow UI in your browser (usually http://localhost:8080). The default login is airflow / airflow.
Navigate to Admin -> Connections.
Click the + button to add a new connection.
Prepare Target Database and Table (First Time Only): The pipeline appends data. The target database and table need to exist.

Connect to the postgres Docker container:
docker exec -it <your_project_name>_postgres_1 psql -U airflow -d airflow
Replace <your_project_name>_postgres_1 with the actual container name (use `docker ps`)
The default user/db in the postgres container (from official Airflow docker-compose) is airflow/airflow.

If you are connecting to a different user/db for youtube_stats, adjust accordingly.


