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
<pre> ```text . ├── dags/ │ └── youtube_pipeline_dag.py # The main Airflow DAG file ├── plugins/ │ └── youtube/ # Custom Python modules for the pipeline │ ├── __init__.py │ ├── data_transformation.py # Handles data cleaning and transformation │ ├── load_data.py # Handles loading data to PostgreSQL │ └── settings.py # Contains API key loading, channel list, and data extraction logic ├── .env.example # Example environment file for configuration ├── docker-compose.yml # Docker Compose file to set up Airflow and PostgreSQL ├── requirements.txt # Python dependencies for custom plugins ├── .gitignore └── README.md ``` </pre>

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
# Replace <your_project_name>_postgres_1 with the actual container name (use `docker ps`)
# The default user/db in the postgres container (from official Airflow docker-compose) is airflow/airflow.
# If you are connecting to a different user/db for youtube_stats, adjust accordingly.
Inside psql, create the youtube_stats database if it doesn't exist (if your postgres_default connection is not using the airflow database):
-- If connecting as 'airflow' user to 'airflow' db, you might want to create a new user and DB for youtube_stats for separation
-- For simplicity, if using the 'postgres' superuser for 'postgres_default' connection:
-- First, connect as default 'postgres' superuser if not already:
-- docker exec -it <your_project_name>_postgres_1 psql -U postgres
CREATE DATABASE youtube_stats;
\c youtube_stats -- Connect to the new database
Create the target table (adjust columns and types based on your data_transformation.py output):
CREATE TABLE IF NOT EXISTS youtube_channel_stats (
    channel_name VARCHAR(255),
    channel_description TEXT,
    custom_url VARCHAR(255),
    publish_date TIMESTAMP,
    default_language VARCHAR(20),
    localized_title VARCHAR(255),
    local_description TEXT,
    country VARCHAR(10),
    content_detail_related_playlist_likes VARCHAR(255),
    view_count BIGINT,
    suscriber_count BIGINT,
    hidden_subscriber BOOLEAN,
    video_count INTEGER,
    contentdetails_relatedplaylists_uploads VARCHAR(255),
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_label VARCHAR(255), -- Added based on your settings.py
    date DATE -- Added based on your settings.py
);
Type \q to exit psql.
Running the Pipeline
In the Airflow UI (http://localhost:8080), find the DAG named youtube_data_pipeline (or youtube_data_pipeline_taskflow).
Unpause the DAG by toggling the switch to "On".
The DAG is scheduled to run daily by default. You can also trigger it manually by clicking the "play" button next to the DAG name.
Customization
Channel List: Modify the channels dictionary in plugins/youtube/settings.py to change or add YouTube channels to track.
Schedule Interval: Change the schedule parameter in the @dag decorator in dags/youtube_pipeline_dag.py.
Transformations: Update the logic in plugins/youtube/data_transformation.py.
