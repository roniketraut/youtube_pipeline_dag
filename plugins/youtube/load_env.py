from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Verify the variable has been loaded
airflow_home = os.getenv("AIRFLOW_HOME")
print(f"AIRFLOW_HOME is set to: {airflow_home}")