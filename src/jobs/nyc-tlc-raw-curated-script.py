
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get AWS account ID and set paths
account_id = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET = f"mission-deh-hof-nyc-tlc-{account_id}"
RAW_PATH = f"s3://{S3_BUCKET}/raw/hvfhv/"
CURATED_PATH = f"s3://{S3_BUCKET}/curated/hvfhv/"

print(f"Loading all data from: {RAW_PATH}")

# Load raw data
try:
    raw_df = spark.read.parquet(RAW_PATH)
    print("✅ Data loaded successfully!")
    
    # Basic data exploration
    print(f"\n📊 Dataset Overview:")
    print(f"Total records: {raw_df.count():,}")
    print(f"Total columns: {len(raw_df.columns)}")
    
    # Show schema
    print(f"\n📋 Schema:")
    raw_df.printSchema()
    
    # Show sample data
    print(f"\n🔍 Sample Data (first 5 rows):")
    raw_df.show(5, truncate=False)
    
    # Column analysis
    print(f"\n📈 Column Analysis:")
    for col_name in raw_df.columns:
        null_count = raw_df.filter(col(col_name).isNull()).count()
        null_percentage = (null_count / raw_df.count()) * 100
        print(f"{col_name}: {null_count:,} nulls ({null_percentage:.2f}%)")
    
    # Check unique license numbers
    license_counts = raw_df.groupBy("hvfhs_license_num").count().orderBy(desc("count"))
    print(f"\n🚗 License Distribution:")
    license_counts.show()
    
    print(f"\n✅ Data loading completed! Ready for transformations...")
    
except Exception as e:
    print(f"❌ Error loading data: {str(e)}")
    raise

# Core transformation function
def transform_raw_to_curated(raw_df):
    """
    Transform raw HVFHV data to curated layer per mapping requirements
    """
    
    print("🔄 Starting core transformations...")
    
    # Apply all transformations from mapping sheet
    curated_df = raw_df.select(
        
        # Direct passthrough fields (no transformation)
        col("hvfhs_license_num"),
        col("dispatching_base_num"), 
        col("originating_base_num"),
        
        # Timestamp fields - cast to TIMESTAMP (already correct type)
        col("request_datetime").cast("timestamp").alias("request_datetime"),
        col("on_scene_datetime").cast("timestamp").alias("on_scene_datetime"), 
        col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        
        # Integer fields - cast to INTEGER (already correct type)
        col("PULocationID").cast("integer").alias("PULocationID"),
        col("DOLocationID").cast("integer").alias("DOLocationID"),
        
        # Decimal fields - cast to DECIMAL(10,2) and handle nulls
        coalesce(col("trip_miles"), lit(0.0)).cast("decimal(10,2)").alias("trip_miles"),
        col("trip_time").cast("integer").alias("trip_time_seconds"),  # Rename per mapping
        coalesce(col("base_passenger_fare"), lit(0.0)).cast("decimal(10,2)").alias("base_passenger_fare"),
        coalesce(col("tolls"), lit(0.0)).cast("decimal(10,2)").alias("tolls"),
        coalesce(col("bcf"), lit(0.0)).cast("decimal(10,2)").alias("bcf"),
        coalesce(col("sales_tax"), lit(0.0)).cast("decimal(10,2)").alias("sales_tax"),
        coalesce(col("congestion_surcharge"), lit(0.0)).cast("decimal(10,2)").alias("congestion_surcharge"),
        coalesce(col("airport_fee"), lit(0.0)).cast("decimal(10,2)").alias("airport_fee"),
        coalesce(col("tips"), lit(0.0)).cast("decimal(10,2)").alias("tips"),
        coalesce(col("driver_pay"), lit(0.0)).cast("decimal(10,2)").alias("driver_pay"),
        
        # Boolean flag conversions (Y/N to TRUE/FALSE)
        when(col("shared_request_flag") == "Y", True).otherwise(False).alias("shared_request_flag"),
        when(col("shared_match_flag") == "Y", True).otherwise(False).alias("shared_match_flag"),
        when(col("access_a_ride_flag") == "Y", True).otherwise(False).alias("access_a_ride_flag"),
        when(col("wav_request_flag") == "Y", True).otherwise(False).alias("wav_request_flag"),
        when(col("wav_match_flag") == "Y", True).otherwise(False).alias("wav_match_flag"),
        
        # Missing field - add cbd_congestion_fee (new field starting Jan 5, 2025)
        lit(0.0).cast("decimal(10,2)").alias("cbd_congestion_fee"),
        
        # Derived fields from mapping sheet
        col("pickup_datetime").cast("date").alias("trip_date"),
        hour("pickup_datetime").alias("pickup_hour"),
        
        # Day of week as string
        when(dayofweek("pickup_datetime") == 1, "Sunday")
        .when(dayofweek("pickup_datetime") == 2, "Monday") 
        .when(dayofweek("pickup_datetime") == 3, "Tuesday")
        .when(dayofweek("pickup_datetime") == 4, "Wednesday")
        .when(dayofweek("pickup_datetime") == 5, "Thursday")
        .when(dayofweek("pickup_datetime") == 6, "Friday")
        .when(dayofweek("pickup_datetime") == 7, "Saturday")
        .otherwise("Unknown")
        .alias("day_of_week"),
        
        # Weekend flag
        when(dayofweek("pickup_datetime").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        
        # Trip duration in minutes
        (col("trip_time") / 60.0).cast("decimal(10,2)").alias("trip_duration_minutes"),
        
        # Total fare calculation
        (coalesce(col("base_passenger_fare"), lit(0.0)) + 
         coalesce(col("tolls"), lit(0.0)) + 
         coalesce(col("bcf"), lit(0.0)) + 
         coalesce(col("sales_tax"), lit(0.0)) + 
         coalesce(col("congestion_surcharge"), lit(0.0)) + 
         coalesce(col("airport_fee"), lit(0.0)) + 
         lit(0.0)  # cbd_congestion_fee
        ).cast("decimal(10,2)").alias("total_fare"),
        
        # Total amount (total_fare + tips)
        (coalesce(col("base_passenger_fare"), lit(0.0)) + 
         coalesce(col("tolls"), lit(0.0)) + 
         coalesce(col("bcf"), lit(0.0)) + 
         coalesce(col("sales_tax"), lit(0.0)) + 
         coalesce(col("congestion_surcharge"), lit(0.0)) + 
         coalesce(col("airport_fee"), lit(0.0)) + 
         lit(0.0) +  # cbd_congestion_fee
         coalesce(col("tips"), lit(0.0))
        ).cast("decimal(10,2)").alias("total_amount"),
        
        # Average speed calculation (handle division by zero)
        when(col("trip_time") > 0, 
             (col("trip_miles") / (col("trip_time") / 3600.0))
        ).otherwise(None).cast("decimal(10,2)").alias("avg_speed_mph"),
        
        # Airport trip flag
        when(coalesce(col("airport_fee"), lit(0.0)) > 0, True).otherwise(False).alias("is_airport_trip"),
        
        # Partition columns
        year("pickup_datetime").alias("year"),
        month("pickup_datetime").alias("month"), 
        dayofmonth("pickup_datetime").alias("day"),
        
        # Processing timestamp
        current_timestamp().alias("processing_timestamp")
    )
    
    print("✅ Core transformations completed!")
    return curated_df

# Apply transformations
curated_df = transform_raw_to_curated(raw_df)

# Show results
print(f"\n📊 Curated Dataset Overview:")
print(f"Total records: {curated_df.count():,}")
print(f"Total columns: {len(curated_df.columns)}")

print(f"\n📋 Curated Schema:")
curated_df.printSchema()

print(f"\n🔍 Sample Transformed Data:")
curated_df.show(5, truncate=False)

# Step 3: Data Validation
def validate_curated_data(curated_df):
    print("🔍 Starting data quality validation...")
    
    initial_count = curated_df.count()
    print(f"Initial record count: {initial_count:,}")
    
    # Apply validation filters
    validated_df = curated_df.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("PULocationID").isNotNull() &
        col("DOLocationID").isNotNull() &
        (col("trip_miles") >= 0) & (col("trip_miles") <= 500) &
        (col("base_passenger_fare") >= 0) & (col("base_passenger_fare") <= 10000) &
        (col("dropoff_datetime") > col("pickup_datetime")) &
        (col("pickup_datetime") >= col("request_datetime")) &
        col("hvfhs_license_num").isin(['HV0002', 'HV0003', 'HV0004', 'HV0005'])
    )
    
    validated_count = validated_df.count()
    rejected_count = initial_count - validated_count
    rejection_rate = (rejected_count / initial_count) * 100
    
    print(f"✅ Validation completed!")
    print(f"Valid records: {validated_count:,}")
    print(f"Rejected records: {rejected_count:,}")
    print(f"Rejection rate: {rejection_rate:.2f}%")
    
    return validated_df

# Apply validation
validated_df = validate_curated_data(curated_df)

# Show sample validated data
print(f"\n🔍 Sample Validated Data:")
validated_df.show(5, truncate=False)

# Step 4: Write Validated Data to S3
output_path = f"s3://mission-deh-hof-nyc-tlc-{account_id}/curated/hvfhv/"

print(f"💾 Writing validated data to: {output_path}")

validated_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)

print("✅ Data successfully written to S3 curated layer!")
print(f"📊 Final count: {validated_df.count():,} records")

job.commit()