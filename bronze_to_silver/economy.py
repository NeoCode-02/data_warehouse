from pyspark.sql.functions import col, to_date, year


df_gdp = spark.read.format("csv").option("header","true").load(
    "abfss://CourseProject@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/bronze/economy/world_gdp.csv"
)
display(df_gdp.limit(5))
df_gdp.schema

df_gdp_silver = df_gdp.select(
    col("countryiso3code").alias("country_code"),
    col("value").cast("double").alias("gdp_usd"),
    year(to_date(col("date"), "M/d/yyyy")).alias("year")
) \
.filter(col("gdp_usd").isNotNull()) \
.filter(col("country_code") == "USA")

print(f"GDP rows: {df_gdp_silver.count():,}")
display(df_gdp_silver.limit(5))

df_gdp_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("world_gdp_silver")

print("World GDP Silver saved successfully!")



df_fx = spark.read.format("csv").option("header","true").load(
    "abfss://CourseProject@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/bronze/economy/ecb_usd_eur_2024.csv"
)
display(df_fx.limit(5))
print(df_fx.schema)

from pyspark.sql.functions import col, to_date, year

df_fx_silver = df_fx.select(
    col("CURRENCY").alias("currency"),
    col("CURRENCY_DENOM").alias("currency_denom"),
    to_date(col("TIME_PERIOD"), "M/d/yyyy").alias("date"),   
    col("OBS_VALUE").cast("double").alias("exchange_rate")
) \
.withColumn("year", year("date")) \
.filter(col("date").isNotNull() & col("exchange_rate").isNotNull()) \
.filter(
    (col("date") >= "2024-01-01") &
    (col("date") <= "2024-12-31")
)

print(f"FX rows: {df_fx_silver.count():,}")
display(df_fx_silver.limit(5))

df_fx_silver.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("ecb_fx_silver")

print("ECB FX Silver saved!")
