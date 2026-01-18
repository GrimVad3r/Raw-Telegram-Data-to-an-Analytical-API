# medical-telegram-warehouse

End-to-end data product that scrapes public Ethiopian medical Telegram channels, lands raw data into a data lake, loads it into PostgreSQL, transforms it with dbt into a star-schema warehouse, enriches images with YOLOv8, exposes analytics via FastAPI, and orchestrates the pipeline with Dagster.
***

## Project Overview

This project is designed for the “Shipping a Data Product: From Raw Telegram Data to an Analytical API” Week 8 challenge of the 10 Academy Artificial Intelligence Mastery program.

You will:
- Scrape messages and images from public Telegram channels about medical/pharma products.
- Build a layered ELT pipeline (raw → staging → marts) on PostgreSQL with dbt.
- Enrich messages with object-detection metadata from YOLOv8.
- Serve analytical insights through a REST API built with FastAPI.
- Orchestrate all steps with Dagster.
- Containerize Postgres, the API, and dbt with Docker Compose. 
***

## 🚀 Production-Grade Features

To meet industry standards for data reliability and maintainability, this project implements:

- **Idempotent Pipelines:** Loaders use `UPSERT` (ON CONFLICT) logic to prevent data duplication and allow for safe re-runs.
- **Incremental Processing:** YOLO detection tracks state to avoid re-processing images, saving significant compute costs.
- **Systematic Error Handling:** - **Scraper:** Implements exponential backoff for Telegram FloodWait errors.
    - **Database:** Transactional integrity with batch inserts and automated rollbacks on failure.
    - **Orchestration:** Integrated Dagster health checks and pipeline verification ops.
- **Data Quality Gates:** dbt "Source Freshness" tests and custom schema assertions (e.g., non-negative views, no future dates).

## 🤝 Development & PR Workflow

This project adheres to a professional **Pull Request (PR) based workflow**:
1. **Branching Strategy:** Main branch protection is simulated. All features are developed in `feature/` or `fix/` branches.
2. **CI/CD Integration:** Every Pull Request triggers a GitHub Action (`unittests.yml`) that:
    - Lints Python code for PEP8 compliance.
    - Runs unit tests for core logic (scrapers/detectors).
    - Builds a temporary Postgres environment to validate dbt models and run data quality tests.
3. **Automated Verification:** Merges to `main` are blocked if dbt tests or Python unit tests fail, ensuring the FastAPI production endpoint always serves verified data.

## Repository Structure

```text
medical-telegram-warehouse/
├── .vscode/
│   └── settings.json
├── .github/
│   └── workflows/
│       └── unittests.yml
├── .env                  # Local secrets (not committed)
├── .gitignore
├── docker-compose.yml    # Postgres + API + dbt
├── Dockerfile            # App image (API + dbt + scripts)
├── requirements.txt
├── README.md
├── data/
│   └── raw/
│       ├── telegram_messages
│       └── images
│   └── processed/
├── medical_warehouse/    # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── Dbt_packages/
│   ├── logs/
│   ├── target/
│   ├── models/
│   │   ├── staging/
│   │       ├── sources.yml
│   │   │   └── stg_telegram_messages.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── dim_dates.sql
│   │       ├── dim_channels.sql
│   │       ├── fct_messages.sql
│   │       └── fct_image_detections.sql
│   └── tests/
│       ├── assert_positive_views.sql
│       └── assert_no_future_messages.sql
├── src/
│   ├── scraper.py                # Task 1: Telegram scraping + data lake
│   ├── load_raw_to_postgres.py   # Task 2: Raw → Postgres
│   ├── yolo_detect.py            # Task 3: YOLOv8 detection
│   └── load_yolo_to_postgres.py  # Task 3: YOLO results → Postgres
├── api/
│   ├── __init__.py
│   ├── main.py           # Task 4: FastAPI app
│   ├── database.py       # SQLAlchemy engine/session
│   └── schemas.py        # Pydantic models
├── notebooks/
│   └── __init__.py
├── tests/
│   ├── __init__.py
│   └── tests_scrapper.py      
└── pipeline.py        # Task 5: Dagster job
```

This layout follows the challenge guideline and separates scraping, transformation, enrichment, API, and orchestration components.

***

## 1. Setup & Installation

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Git
- A Telegram account and API credentials (for scraping)
- Optional: local PostgreSQL if you want to run outside Docker. 
### Clone the repository

```bash
git clone <your-repo-url> medical-telegram-warehouse
cd medical-telegram-warehouse
```

### Python dependencies (for local runs)

```bash
pip install -r requirements.txt
pip install dbt-postgres
```

Requirements should include (non-exhaustive):
- telethon
- python-dotenv
- fastapi
- uvicorn
- sqlalchemy
- psycopg2-binary
- ultralytics
- dagster, dagster-webserver

### Environment variables

Create `.env` in the project root (do not commit):

```env
# Telegram
API_ID=your_api_id
API_HASH=your_api_hash
PHONE_NUMBER=+2519xxxxxxx
SESSION_NAME=telegram_session

# Postgres
PG_HOST=localhost
PG_PORT=5432
PG_DB=telegram_warehouse
PG_USER=postgres
PG_PASSWORD=postgres
```

Docker Compose will also set/override the database variables when running containers. 

***

## 2. Running with Docker

The simplest way to bring up Postgres + dbt + FastAPI is via Docker Compose. [github](https://github.com/caarmen/fastapi-postgres-docker-example)

### Build and start services

```bash
docker compose up --build
```

This will:
- Start a `postgres:16` container (`db` service).
- Build an app image from `Dockerfile`.
- Run dbt models/tests, then start the FastAPI app on port 8000. [github](https://github.com/caarmen/fastapi-postgres-docker-example)

### Access endpoints

- FastAPI docs: `http://localhost:8000/docs`
- Postgres: `localhost:5432` (user `postgres`, password `postgres`, db `telegram_warehouse`). [github](https://github.com/testdrivenio/fastapi-tdd-docker/blob/main/docker-compose.yml)

### Running dbt manually in Docker

```bash
docker compose run --rm dbt run
docker compose run --rm dbt test
```

The `dbt` service uses the same image but starts with `dbt` as entrypoint for ad hoc commands. [docs.getdbt](https://docs.getdbt.com/docs/core/docker-install)

***

## 3. Task-by-Task Guide

### Task 1 – Data Scraping & Data Lake

Goal: Use Telethon to scrape messages and images from Telegram channels and store them as partitioned JSON + images in `data/raw`. 

Main script: `src/scraper.py`  
Key behavior:
- Connects to Telegram via API credentials.
- Iterates over channels such as:
  - `https://t.me/CheMed123`
  - `https://t.me/lobelia4cosmetics`
  - `https://t.me/tikvahpharma`
- For each message, stores:
  - `message_id`, `channel_name`, `message_date`
  - `message_text`, `views`, `forwards`
  - `has_media`, `image_path` if photo present
- Writes line-delimited JSON to `data/raw/telegram_messages/YYYY-MM-DD/channel_name.json`.
- Downloads images to `data/raw/images/{channel_name}/{message_id}.jpg`.
- Logs activity to `logs/scraper.log`. 

Run locally:

```bash
python src/scraper.py
```

This is the first step of the ELT pipeline and populates the data lake. 

***

### Task 2 – Data Modeling & dbt

Goal: Load raw JSON into Postgres, model it with dbt in a star schema (dim_channels, dim_dates, fct_messages) and add tests/docs. 

#### 2.1 Load raw JSON into Postgres

Script: `src/load_raw_to_postgres.py`  

Responsibilities:
- Create `raw.telegram_messages` schema/table.
- Iterate over `data/raw/telegram_messages/**.json`.
- Insert raw records into Postgres with correct types. 

Run:

```bash
python src/load_raw_to_postgres.py
```

#### 2.2 dbt project

Location: `medical_warehouse/`  

Main elements:
- `models/staging/stg_telegram_messages.sql`  
  - Cleans column names, types, nulls.
  - Adds `message_length`, `has_image` flag.
- `models/marts/dim_dates.sql` – calendar dimension.
- `models/marts/dim_channels.sql` – channel attributes (type, first/last post, total posts, avg views).
- `models/marts/fct_messages.sql` – one row per message with foreign keys to dims. 

Testing:
- `models/staging/schema.yml` and `models/marts/schema.yml` define `not_null`, `unique`, and `relationships` tests.
- `tests/assert_no_future_messages.sql` ensures no future-dated messages. 

Run dbt (local):

```bash
cd medical_warehouse
dbt run
dbt test
```

Generate docs:

```bash
dbt docs generate
dbt docs serve
```

This turns raw Telegram data into reliable analytical tables. 

***

### Task 3 – Data Enrichment with YOLO

Goal: Detect objects in message images, classify image types, and integrate results into the warehouse as `fct_image_detections`. 

#### 3.1 YOLO detection

Script: `src/yolo_detect.py`  

Responsibilities:
- Scan `data/raw/images/**.jpg`.
- Run YOLOv8 nano (`yolov8n.pt`) detections.
- For each image, capture:
  - `channel_name`, `message_id`
  - `detected_class`, `confidence`
  - `image_category` (promotional, product_display, lifestyle, other) based on detected objects. 
- Save results to `data/yolo_detections.csv`.

Run:

```bash
python src/yolo_detect.py
```

#### 3.2 Load YOLO results to Postgres

Script: `src/load_yolo_to_postgres.py`  

Responsibilities:
- Create `raw.yolo_detections`.
- Load `data/yolo_detections.csv` into Postgres. 

Run:

```bash
python src/load_yolo_to_postgres.py
```

#### 3.3 dbt model for detections

Model: `medical_warehouse/models/marts/fct_image_detections.sql`  

Joins:
- `raw.yolo_detections` with `stg_telegram_messages`, `dim_channels`, `dim_dates`.
- Produces: `message_id`, `channel_key`, `date_key`, `detected_class`, `confidence`, `image_category`. 

Run dbt:

```bash
cd medical_warehouse
dbt run --select fct_image_detections
```

This enables analyses such as “Do promotional posts get more views than product_display posts?” and “Which channels use more visual content?”. 

***

### Task 4 – Analytical API (FastAPI)

Goal: Expose key analytics through a REST API backed by the dbt marts. 

Location: `api/`

Key files:
- `database.py` – SQLAlchemy engine and session, reading DB config from env.
- `schemas.py` – Pydantic models for responses (TopProduct, ChannelActivity, MessageItem, VisualContentStats).
- `main.py` – FastAPI app and endpoints. 

Core endpoints:

1. **Top products / terms**  
   `GET /api/reports/top-products?limit=10`  
   - Tokenizes `message_text` into terms.
   - Returns most frequent terms across all messages. 

2. **Channel activity**  
   `GET /api/channels/{channel_name}/activity`  
   - Aggregates daily message count and average views for a given channel. 

3. **Message search**  
   `GET /api/search/messages?query=paracetamol&limit=20`  
   - Searches `message_text` with `ILIKE` and orders by view count. 

4. **Visual content stats**  
   `GET /api/reports/visual-content`  
   - Aggregates `fct_image_detections` per channel by `image_category`. 

Run locally:

```bash
uvicorn api.main:app --reload
```

Docs: `http://localhost:8000/docs` (OpenAPI/Swagger UI). [fastapi.tiangolo](https://fastapi.tiangolo.com/tutorial/)

***

### Task 5 – Pipeline Orchestration (Dagster)

Goal: Convert scripts into a orchestrated pipeline with clear dependencies and scheduling. 

Location: `scripts/pipeline.py`  

Contents:
- `@op` definitions:
  - `scrape_telegram_data` → `src/scraper.py`
  - `load_raw_to_postgres` → `src/load_raw_to_postgres.py`
  - `run_dbt_transformations` → `dbt run` + `dbt test`
  - `run_yolo_enrichment` → `src/yolo_detect.py`, `src/load_yolo_to_postgres.py`, `dbt run --select fct_image_detections`. 
- `@job telegram_analytics_job` that wires ops in order:
  - `scrape_telegram_data` → `load_raw_to_postgres` → `run_dbt_transformations` → `run_yolo_enrichment`. 

Run Dagster UI:

```bash
dagster dev -f scripts/pipeline.py
```

Open `http://localhost:3000` to run jobs and inspect logs. [docs.dagster](https://docs.dagster.io/guides/automate/schedules/defining-schedules)

You can add a schedule in the same file to run the job daily and configure failure alerts according to Dagster docs. [docs.dagster](https://docs.dagster.io/guides/automate/schedules)

***

## Development & Testing

### Unit tests

- Put Python tests under `tests/` and configure `pytest` (or your preferred framework).
- CI workflow (`.github/workflows/unittests.yml`) can run tests and possibly `dbt test` on pushes. 

### Linting & formatting

- Optional but recommended: `black`, `isort`, `flake8`.  
- Configure in `pyproject.toml` or via VS Code settings in `.vscode/settings.json`. 

***

## Reports & Deliverables

The challenge requires: 

- **Interim report**:  
  - Describe data lake structure.  
  - Diagram the star schema.  
  - Summarize data quality issues and fixes.

- **Final report (blog style)**:  
  - Visual diagram of the full pipeline (scraper → data lake → Postgres → dbt → YOLO → API → Dagster).  
  - Star schema diagram with explanation of each table and grain.  
  - Screenshots of:
    - dbt docs
    - Working API endpoints
    - Dagster UI
  - Reflections and possible improvements.


***

## References

Helpful resources used in this project design (see challenge guideline): 

- Telethon docs – Telegram scraping basics. [youtube](https://www.youtube.com/watch?v=VirndPTeRaw)
- dbt docs – project structure, testing, and best practices. [docs.getdbt](https://docs.getdbt.com/docs/core/docker-install)
- FastAPI docs – building and documenting the API. [fastapi.tiangolo](https://fastapi.tiangolo.com/tutorial/)
- Ultralytics YOLOv8 docs – running detection models. [github](https://github.com/z6601821/TelegramChannelScraper)
- Dagster docs – defining jobs and schedules. [docs.dagster](https://docs.dagster.io/guides/automate/schedules)