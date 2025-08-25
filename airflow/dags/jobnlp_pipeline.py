from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from jobnlp.pipeline import pipeline_tasks

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='jobnlp_pipeline_python',
    default_args=default_args,
    description='Pipeline diario con PythonOperator',
    schedule_interval='@daily',
    start_date=datetime(2025, 8, 13),
    catchup=False,
) as dag:
    load_env = PythonOperator(
        task_id='load_env',
        python_callable=pipeline_tasks.load_env,
    )

    start_docker = PythonOperator(
        task_id='start_docker',
        python_callable=pipeline_tasks.start_docker_compose,
    )

    fetch_raw = PythonOperator(
        task_id='fetch_raw',
        python_callable=pipeline_tasks.fetch_raw.main,
    )

    clean_text = PythonOperator(
        task_id='clean_text',
        python_callable=pipeline_tasks.clean_text.main,
    )

    nlp_extract = PythonOperator(
        task_id='nlp_extract',
        python_callable=pipeline_tasks.nlp_extract.main,
    )

    entity_count = PythonOperator(
        task_id='entity_count',
        python_callable=pipeline_tasks.entity_count.main,
    )

    stop_docker = PythonOperator(
        task_id='stop_docker',
        python_callable=pipeline_tasks.stop_docker_compose,
    )

    load_env >> start_docker >> fetch_raw >> clean_text >> nlp_extract >> entity_count >> stop_docker