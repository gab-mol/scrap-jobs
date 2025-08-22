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

Make sure a `.env` file is present in the project root (`./.env`) with the following variables:

```env
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
POSTGRES_PORT=
```

To start the PostgreSQL service via Docker:

```bash
docker compose --env-file .env -f docker/docker-compose.dev.yml up
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

#### Airflow configuration
Create `airflow/config/.env` with:

```env
AIRFLOW_UID=50000
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=user:admin
```
Set the password for this user in `airflow/config/simple_auth_passwords.json`:

```json
{"user" : "password"}
```

Start with:
```bash
docker compose --env-file airflow/config/.env -f airflow/docker-compose.yaml up -d
```

## Ethical note
This project uses a custom `User-Agent` header during scraping:

```
scrap-jobs/0.1.0 (+https://github.com/gab-mol/scrap-jobs.git)
```

This is intended to clearly identify the source and purpose of the requests. The scraper respects the site's `robots.txt` rules, and includes retry logic and exponential backoff to avoid server overload.

Scraping targets are defined in a YAML config file under `src/jobnlp/scraper/config/scraper.yml`. 