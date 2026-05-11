from pyspark.sql.functions import col, hour, dayofweek, month, year, round as spark_round, unix_timestamp
df_taxi = spark.read.format("parquet").load(
    "abfss://CourseProject@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/bronze/nyc_taxi/*.parquet"
)
display(df_taxi.limit(5))
df_taxi.schema


df_taxi = df_taxi.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "fare_amount"])
df_taxi = df_taxi.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0) &
    (col("passenger_count") > 0) &
    (col("tpep_pickup_datetime") >= "2024-01-01") &
    (col("tpep_pickup_datetime") <  "2025-01-01") &
    (col("total_amount") > 0)
)


df_taxi = df_taxi.withColumnRenamed("VendorID", "vendor_id") \
       .withColumnRenamed("RatecodeID", "ratecode_id") \
       .withColumnRenamed("PULocationID","pu_location_id") \
       .withColumnRenamed("DOLocationID","do_location_id") \
       .withColumnRenamed("Airport_fee", "airport_fee")


df_taxi = df_taxi.withColumn("pickup_year", year("tpep_pickup_datetime")) \
       .withColumn("pickup_month", month("tpep_pickup_datetime")) \
       .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
       .withColumn("pickup_weekday", dayofweek("tpep_pickup_datetime")) \
       .withColumn("trip_duration_minutes",
           spark_round(
               (unix_timestamp(col("tpep_dropoff_datetime")) -
                unix_timestamp(col("tpep_pickup_datetime"))) / 60, 2
           )
       ) \
       .filter(col("trip_duration_minutes") > 0)


df_silver_taxi = df_taxi.select(
    "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "pickup_year", "pickup_month", "pickup_hour", "pickup_weekday",
    "trip_duration_minutes", "passenger_count", "trip_distance",
    "ratecode_id", "pu_location_id", "do_location_id", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "airport_fee",
    "total_amount", "store_and_fwd_flag"
)

print(f"Rows to write: {df_silver_taxi.count():,}")


df_silver_taxi.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("nyc_taxi_silver")

print("NYC Taxi Silver Delta table saved successfully!")