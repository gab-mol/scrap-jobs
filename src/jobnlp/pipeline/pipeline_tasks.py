import os
import time
from docker import DockerClient
from dotenv import load_dotenv

from jobnlp.pipeline import fetch_raw, clean_text, nlp_extract, entity_count

def load_env():
    load_dotenv()

def start_docker_compose():
    load_env()
    client = DockerClient.from_env()
    client.containers.run(
        "postgres:16",
        name="postgres_jobsnlp",
        environment={
            "POSTGRES_DB": os.getenv("DB_NAME"),
            "POSTGRES_USER": os.getenv("DB_USER"),
            "POSTGRES_PASSWORD": os.getenv("DB_PASS"),
        },
        ports={f"{os.getenv('DB_PORT', '5432')}:5432"},
        volumes=["postgres_jobsnlp_data:/var/lib/postgresql/data"],
        detach=True,
    )
    while True:
        result = client.containers.get("postgres_jobsnlp").exec_run(
            ["pg_isready", "-U", os.getenv("DB_USER", "postgres"), "-d", os.getenv("DB_NAME", "postgres")]
        )
        if result.exit_code == 0:
            break
        time.sleep(2)

def stop_docker_compose():
    client = DockerClient.from_env()
    container = client.containers.get("postgres_jobsnlp")
    container.stop()
    container.remove()