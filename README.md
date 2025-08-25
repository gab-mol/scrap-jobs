# JobNLP Lakehouse Pipeline
A modular data pipeline for scraping, cleaning, and storing online job advertisements using a PostgreSQL-based lakehouse architecture. This project is designed to be flexible, reproducible, and easy to extend.

>**Purpose**  
>The goal of this pipeline is to extract and normalize unstructured job advertisement data from online sources, store raw and cleaned versions in a structured format, and lay the foundation for further NLP-based analysis.

## Features

- **Web scraping** with Python and BeautifulSoup.
- **Text preprocessing** including HTML cleanup and normalization.
- **Layered data storage** using a lakehouse-inspired model (`raw` → `bronze` → `silver`).
- **Conflict-safe inserts** with deduplication via content hashes.
- **Dockerized environment** for consistency.
- **Airflow DAGs** for scheduling and orchestrating pipeline tasks (coming soon).

## Roadmap (work in progress)

1. ☑ Web scraping and preprocessing (raw layer)
2. ☑ Deduplication logic
3. ☑ Basic Docker support
4. ☑ Extracting named entities from ads using spacy's NLP model and rules
5. ☐ Airflow orchestration
6. ☐ Example of data visualization with Dash

## Configuration and use

### Prerequisites

- Docker
- Python 3.11+

### Run locally

Create a Python virtual environment (`venv`) for the project. Activate it. Install dependencies (`pip install -r requirements.txt`). Then, install the project (`jobnlp`) locally, as a package: 

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e src/
```

Make sure a `docker/.db.env` file is present with the following variables:

```env
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
POSTGRES_PORT=
```

To start the PostgreSQL service via Docker:

```bash
docker compose --env-file docker/.db.env -f docker/docker-compose-db.yaml up
```

#### Available CLI tasks
> Tip: Run the tasks inside the Python virtual environment where `jobnlp` is installed.

| Task        | Description                                | Entry point                          |
|-------------|--------------------------------------------|--------------------------------------|
| `fetch_raw` | Scrape job ads and store in raw layer      | `jobnlp.pipeline.fetch_raw:main`     |
| `clean_text`| Preprocess and normalize, store in bronze  | `jobnlp.pipeline.clean_text:main`    |
| `nlp_extract`| Tokenization and named entity extraction  | `jobnlp.pipeline.nlp_extract:main`   |
| `entity_count`| count of stored entities by date and ad  | `jobnlp.pipeline.entity_count:main`  |

### Orchestration with Airflow

Airflow *DAGs for this pipeline are currently under development* and will allow scheduled, repeatable execution of all steps, fro2m data collection to enrichment.

#### Airflow setup

Create the required sub-directories inside `airflow/`:
```bash
mkdir -p ./airflow/{logs,dags,plugins,config}
```
Create the environment file at `airflow/config/.airflow.env`:

```env
AIRFLOW_UID=$(id -u)
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=user:admin
AIRFLOW_PROJ_DIR=/path/to/proyect
JWT_SECRET="rIyQnG8nGGAbCfwNZac6aQ=="
FERNET_KEY="rQdow8Su_9nWTcS7QSPpkEzjtzt5PvGDuOLldGVVpCU="
```
>**Important**: Generate your own secure values for`JWT_SECRET` and `FERNET_KEY`.

Buid the Airflow image:
```bash
docker build -f docker/Dockerfile.airflow .
```
Start Airflow with Docker Compose:
```bash
docker compose --env-file airflow/config/.airflow.env -f docker/docker-compose.yaml up -d
```
The password for the users defined in `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` will be stored in:
```
airflow/config/simple_auth_passwords.json
```

## Ethical note
This project uses a custom `User-Agent` header during scraping:

```
scrap-jobs/0.1.0 (+https://github.com/gab-mol/scrap-jobs.git)
```

This is intended to clearly identify the source and purpose of the requests. The scraper respects the site's `robots.txt` rules, and includes retry logic and exponential backoff to avoid server overload.

Scraping targets are defined in a YAML config file under `src/jobnlp/scraper/config/scraper.yml`. 