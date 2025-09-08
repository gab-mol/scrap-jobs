from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import jobnlp.pipeline as pipeline_tasks

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='jobnlp_pipeline_python',
    default_args=default_args,
    description='Pipeline diario con PythonOperator',
    schedule='@daily',
    start_date=datetime(2025, 8, 13),
    catchup=False,
) as dag:
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

    fetch_raw >> clean_text >> nlp_extract >> entity_count