from pyspark.sql.functions import col, to_date, year


df_gdp = spark.read.format("csv").option("header","true").load(
    "abfss://CourseProject@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/bronze/economy/world_gdp.csv"
)
display(df_gdp.limit(5))
df_gdp.schema

df_gdp_silver = df_gdp.select(
    col("countryiso3code").alias("country_code"),
    col("date").alias("date_str"),
    col("value").alias("gdp_usd")
) \
.withColumn("gdp_usd", col("gdp_usd").cast("double")) \
.withColumn("year", year(to_date(col("date_str"), "M/d/yyyy"))) \
.drop("date_str") \
.filter(col("gdp_usd").isNotNull()) \
.filter(col("country_code") == "USA")

print(f"GDP rows: {df_gdp_silver.count():,}")
display(df_gdp_silver.limit(5))

df_gdp_silver.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("world_gdp_silver")

print("World GDP Silver saved!")



df_fx = spark.read.format("csv").option("header","true").load(
    "abfss://CourseProject@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/bronze/economy/ecb_usd_eur_2024.csv"
)
display(df_fx.limit(5))
print(df_fx.schema)

df_fx_silver = df_fx.select(
    col("CURRENCY").alias("currency"),
    col("CURRENCY_DENOM").alias("currency_denom"),
    col("TIME_PERIOD").alias("date"),   
    col("OBS_VALUE").alias("exchange_rate")
) \
.withColumn("date", to_date(col("date"), "M/d/yyyy")) \
.withColumn("exchange_rate", col("exchange_rate").cast("double")) \
.filter(col("date").isNotNull() & col("exchange_rate").isNotNull()) \
.filter(
    (col("date") >= "2024-01-01") &
    (col("date") <= "2024-12-31")
) \
.withColumn("year", year("date"))

print(f"FX rows: {df_fx_silver.count()}")
display(df_fx_silver.limit(5))

df_fx_silver.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("ecb_fx_silver")

print("ECB FX Silver saved!")
