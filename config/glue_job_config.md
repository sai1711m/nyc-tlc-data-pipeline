# AWS Glue Job Configuration Template

## Job Parameters

### 01_ingestion.py
```json
{
  "--jobtype": "historical",  // or "current"
  "--enable-metrics": "",
  "--enable-continuous-cloudwatch-log": "true"
}
```

### 02_raw_to_curated.py
```json
{
  "--enable-metrics": "",
  "--enable-continuous-cloudwatch-log": "true",
  "--enable-spark-ui": "true"
}
```

### 03_curated_to_aggregated.py
```json
{
  "--enable-metrics": "",
  "--enable-continuous-cloudwatch-log": "true",
  "--enable-spark-ui": "true"
}
```

## Glue Job Settings

- **Glue Version**: 4.0
- **Language**: Python 3
- **Worker Type**: G.1X (recommended) or G.2X for large datasets
- **Number of Workers**: 2-10 (depends on data volume)
- **Job Timeout**: 60 minutes
- **Max Retries**: 1
- **IAM Role**: Requires S3 read/write and Glue permissions

## S3 Bucket Naming

Format: `mission-deh-hof-nyc-tlc-{AWS_ACCOUNT_ID}`

The account ID is automatically detected at runtime.
