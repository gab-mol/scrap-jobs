FROM apache/airflow:3.0.5

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY setup.py /project/setup.py
COPY src /project/src
WORKDIR /project
RUN pip install -e .

WORKDIR /opt/airflow
