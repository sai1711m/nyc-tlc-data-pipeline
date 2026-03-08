# NYC TLC Data Engineering Pipeline

AWS Glue-based ETL pipeline for processing NYC Taxi & Limousine Commission (TLC) High Volume For-Hire Vehicle (HVFHV) trip data.

## Project Structure

```
deh_hof_labs/
├── src/
│   ├── jobs/              # AWS Glue job scripts
│   │   ├── 01_ingestion.py
│   │   ├── 02_raw_to_curated.py
│   │   └── 03_curated_to_aggregated.py
│   └── notebooks/         # Jupyter notebooks for development
│       ├── raw_to_curated.ipynb
│       └── curated_to_aggregated.ipynb
├── data/                  # Local sample data
├── docs/                  # Documentation
├── config/                # Configuration files
├── tests/                 # Test files
└── README.md

```

## Pipeline Overview

### 1. Ingestion (01_ingestion.py)
- Downloads HVFHV trip data from NYC TLC
- Supports historical and incremental modes
- Uploads to S3 raw layer with partitioning (year/month)
- Creates `.done` file for event-driven orchestration

### 2. Raw to Curated (02_raw_to_curated.py)
- Transforms raw data with type casting and validation
- Derives business metrics (trip duration, total fare, etc.)
- Applies data quality rules
- Writes to S3 curated layer partitioned by year/month/day

### 3. Curated to Aggregated (03_curated_to_aggregated.py)
- Creates three aggregation tables:
  - **daily_borough**: Borough-level daily summaries
  - **daily_zone**: Zone-level route analysis
  - **daily_service**: Service type metrics (shared rides, WAV)
- Enriches with zone lookup data
- Writes to S3 aggregated layer

## S3 Data Layout

```
s3://mission-deh-hof-nyc-tlc-{account_id}/
├── raw/
│   └── hvfhv/year=YYYY/month=MM/
├── curated/
│   └── hvfhv/year=YYYY/month=MM/day=DD/
└── aggregated/
    ├── daily_borough/year=YYYY/month=MM/
    ├── daily_zone/year=YYYY/month=MM/day=DD/
    └── daily_service/year=YYYY/month=MM/
```

## Usage

Run jobs with AWS Glue:
```bash
# Ingestion (historical mode)
aws glue start-job-run --job-name nyc-tlc-ingestion --arguments '{"--jobtype":"historical"}'

# Ingestion (incremental mode)
aws glue start-job-run --job-name nyc-tlc-ingestion --arguments '{"--jobtype":"current"}'

# Raw to Curated transformation
aws glue start-job-run --job-name nyc-tlc-raw-to-curated

# Curated to Aggregated
aws glue start-job-run --job-name nyc-tlc-curated-to-aggregated
```

## Requirements

- AWS Glue 4.0
- Python 3.10
- PySpark 3.3
- boto3
