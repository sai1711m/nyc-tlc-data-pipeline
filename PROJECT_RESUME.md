# NYC TLC Data Engineering Pipeline - Resume Description

## PROJECT DESCRIPTION

**NYC Taxi & Limousine Commission Big Data ETL Pipeline**

Designed and implemented a scalable AWS-based ETL pipeline processing 19+ million NYC taxi trip records monthly using AWS Glue, PySpark, and S3. Built a three-layer data architecture (Raw → Curated → Aggregated) enabling business analytics for route optimization, revenue analysis, and regulatory compliance. Orchestrated end-to-end workflow using AWS Step Functions with automated error handling, retry logic, and SNS notifications.

**Technologies:** AWS Glue, PySpark, Python, S3, AWS Step Functions, SNS, Parquet, Boto3, AWS Glue Data Catalog

**Data Volume:** 19M+ records/month, 2+ GB monthly processing

---

## ROLES & RESPONSIBILITIES

### 1. ETL Pipeline Development & Orchestration
- Developed 3-stage AWS Glue ETL pipeline: data ingestion, transformation, and aggregation
- Built automated data ingestion job downloading NYC TLC trip data from public APIs to S3
- Implemented dual-mode ingestion: historical backfill (12 months) and incremental daily loads
- Created PySpark transformation jobs processing 19M+ records with complex business logic
- Orchestrated sequential workflow using AWS Step Functions with state machine definition
- Implemented retry logic (2 attempts, exponential backoff) and error handling for each pipeline stage
- Configured SNS notifications for pipeline success/failure alerts with stage-specific error messages

### 2. Data Architecture & Modeling
- Architected 3-layer data lake on S3: Raw (source data), Curated (validated), Aggregated (analytics)
- Implemented Hive-style partitioning (year/month/day) for query optimization
- Designed star schema with 3 aggregation tables: borough-level, zone-level, and service-level metrics
- Performed data enrichment joining 19M trip records with 265 NYC taxi zone lookup tables
- Achieved 99%+ data compression through intelligent aggregations

### 3. Data Quality & Validation
- Built comprehensive data quality framework with validation rules achieving 99%+ pass rate
- Implemented validation checks: null handling, range validation (0-500 miles, $0-$10K fares)
- Created temporal consistency checks (dropoff > pickup time, pickup >= request time)
- Validated regulatory compliance with license number verification (HV0002-HV0005)
- Tracked rejection rates (<1%) and data quality metrics per job run

### 4. Performance & Cost Optimization
- Optimized storage using Parquet format with Snappy compression, reducing costs by 70%
- Configured distributed processing with appropriate Glue worker types (G.1X/G.2X)
- Implemented partition pruning and predicate pushdown for faster queries
- Achieved sub-hour execution times for 19M+ record processing
- Applied incremental processing patterns to avoid full table scans

### 5. Development & Documentation
- Created reusable PySpark functions for transformations: fare calculations, speed metrics, flag conversions
- Developed Jupyter notebooks for iterative development and testing in AWS Glue Studio
- Documented pipeline architecture, data transformations, and validation rules
- Implemented logging and error handling for production monitoring
- Maintained version control and comprehensive project documentation

---

## KEY ACHIEVEMENTS

✅ Processed 19M+ records monthly with 99%+ data quality rate
✅ Reduced storage costs by 70% through Parquet compression and partitioning
✅ Achieved sub-hour execution times for large-scale data processing
✅ Built fully automated pipeline with zero manual intervention using Step Functions orchestration
✅ Implemented robust error handling with 2-retry logic and SNS alerting
✅ Enabled analytics for route optimization and revenue analysis

---

## RESUME BULLET POINTS

• Architected and deployed AWS Glue ETL pipeline processing 19M+ monthly taxi trip records with 3-layer data architecture (Raw → Curated → Aggregated), orchestrated via AWS Step Functions with automated error handling and SNS notifications

• Developed PySpark transformation jobs with comprehensive data quality framework achieving 99%+ validation rate across 24 source columns, including null handling, business rules, and temporal consistency checks

• Designed star schema aggregation layer with 3 fact tables (borough, zone, service-level) achieving 99%+ data compression while preserving analytical value for business stakeholders

• Orchestrated end-to-end pipeline workflow using AWS Step Functions state machine with sequential job execution, retry logic (2 attempts, exponential backoff), and SNS alerting for success/failure notifications

• Implemented dual-mode ingestion strategy (historical + incremental) with automated latest-month detection, reducing manual intervention to zero

• Optimized data storage using Parquet format with Snappy compression and multi-level partitioning, reducing storage costs by 70% and improving query performance by 5x

• Built data enrichment pipeline joining 19M trip records with 265 NYC taxi zones, denormalizing lookup tables for improved downstream query performance

• Established logging, monitoring, and documentation standards for 3 production ETL jobs, enabling efficient debugging and knowledge transfer across data engineering team

• Created reusable PySpark functions for complex transformations (fare calculations, speed metrics, flag conversions), processing 2+ GB data with sub-hour execution times
