from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

df = spark.read.json(
    "abfss://CourseProject@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/bronze/weather/nyc_weather_2024/*.json"
)
display(df.limit(5))
df.schema

df_silver = df \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("date_key", date_format("date", "yyyyMMdd").cast(IntegerType())) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("day_of_week", dayofweek("date")) \
    .withColumn("day_name", date_format("date", "EEEE")) \
    .withColumn("max_temp_c", round(col("max_temp_c"), 2)) \
    .withColumn("min_temp_c", round(col("min_temp_c"), 2)) \
    .withColumn("avg_temp_c", round(col("avg_temp_c"), 2)) \
    .withColumn("max_wind_kph", round(col("max_wind_kph"), 2)) \
    .withColumn("total_precip_mm", round(col("total_precip_mm"), 2)) \
    .withColumn("avg_humidity", round(col("avg_humidity"), 2)) \
    .withColumn("uv_index", round(col("uv_index"), 2)) \
    .filter(col("date").isNotNull()) \
    .select(
        "date_key", "date", "year", "month", "day",
        "day_of_week", "day_name", "city",
        "max_temp_c", "min_temp_c", "avg_temp_c",
        "max_wind_kph", "total_precip_mm",
        "avg_humidity", "condition", "uv_index"
    )

print(f"Silver rows: {df_silver.count()}")
display(df_silver.limit(5))

df_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"weather_silver")

print("weather_silver written")