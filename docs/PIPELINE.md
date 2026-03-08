# Pipeline Architecture

## Data Flow

```
NYC TLC API → Raw Layer → Curated Layer → Aggregated Layer
```

## Layers

### Raw Layer
- **Purpose**: Store unmodified source data
- **Format**: Parquet
- **Partitioning**: year/month
- **Retention**: Indefinite

### Curated Layer
- **Purpose**: Cleaned, validated, and enriched data
- **Transformations**:
  - Type casting (timestamps, decimals, booleans)
  - Null handling with coalesce
  - Derived fields (trip_date, day_of_week, is_weekend)
  - Calculated metrics (total_fare, total_amount, avg_speed_mph)
- **Validation Rules**:
  - Non-null pickup/dropoff times and locations
  - Trip miles: 0-500
  - Fare: 0-10,000
  - Dropoff > Pickup time
  - Valid license numbers (HV0002-HV0005)
- **Format**: Parquet
- **Partitioning**: year/month/day

### Aggregated Layer
- **Purpose**: Pre-aggregated analytics tables
- **Tables**:
  1. **daily_borough**: Borough-level metrics
  2. **daily_zone**: Zone-level route analysis
  3. **daily_service**: Service type analysis
- **Format**: Parquet
- **Partitioning**: Varies by table

## Job Dependencies

```
01_ingestion.py
    ↓
02_raw_to_curated.py
    ↓
03_curated_to_aggregated.py
```

## Data Quality

- Rejection rate typically < 1%
- Invalid records logged but not stored
- Validation metrics tracked per job run
