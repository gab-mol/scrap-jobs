import os, psycopg2
from dotenv import load_dotenv
import pathlib

ENV_PATH = pathlib.Path("docker/.db.env")

if ENV_PATH.exists(): 
    load_dotenv(ENV_PATH)
else:
    load_dotenv("/opt/airflow/.env")

def get_connection():
    try:
        return psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
    except KeyError as e:
        raise RuntimeError(f"Environment variable missing: {e}")