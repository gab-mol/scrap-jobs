import os, psycopg2
from dotenv import load_dotenv
import pathlib

ENV_PATH = pathlib.Path("docker/.db.env")

load_dotenv(ENV_PATH)

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )