from pyspark.sql.functions import col, to_timestamp, year, month, hour, first

df_pm25 = spark.read.format("csv").option("header", "true").option("encoding", "UTF-8").load("abfss://CourseProject@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/bronze/air_quality/openaq_pm25.csv")
display(df_pm25.limit(5))
df_pm25.schema

df_no2 = spark.read.format("csv").option("header", "true").option("encoding", "UTF-8").load("abfss://CourseProject@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/bronze/air_quality/openaq_no2.csv")
display(df_no2.limit(5))
df_no2.schema

df_o3 = spark.read.format("csv").option("header", "true").option("encoding", "UTF-8").load("abfss://CourseProject@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/bronze/air_quality/openaq_o3.csv")
display(df_o3.limit(5))
df_o3.schema

df = df_pm25.union(df_no2).union(df_o3)
display(df.limit(5))


DATETIME_FMT = "M/d/yyyy h:mm:ss a"

df = df.withColumn("location_id", col("location_id").cast("integer")) \
       .withColumn("sensor_id", col("sensor_id").cast("integer")) \
       .withColumn("parameter_id", col("parameter_id").cast("integer")) \
       .withColumn("value", col("value").cast("double")) \
       .withColumn("datetime_utc", to_timestamp(col("datetime_utc"),    DATETIME_FMT)) \
       .withColumn("datetime_to_utc", to_timestamp(col("datetime_to_utc"), DATETIME_FMT))

display(df.limit(5))


df_cleaned = df.dropna(subset=["location_id", "sensor_id", "value", "datetime_utc"])
df_cleaned = df_cleaned.filter(col("value") >= 0)

df_silver = df_cleaned.withColumn("year", year("datetime_utc")) \
       .withColumn("month", month("datetime_utc")) \
       .withColumn("hour", hour("datetime_utc")) \
       .filter(col("year") == 2024) \
       .select(
           "location_id", "locality", "timezone",
           "sensor_id", "parameter_id", "parameter_name", "parameter_units",
           "value", "datetime_utc", "datetime_to_utc",
           "year", "month", "hour"
       )

print(f"Total rows: {df_silver.count():,}")
display(df_silver.limit(5))


df_silver.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("openaq_silver")

print("OpenAQ Silver Delta table saved!")