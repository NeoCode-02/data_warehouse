fact_aq = spark.read.table(f"lh_silver.dbo.openaq_silver") \
    .filter(
        col("datetime_utc").isNotNull() &
        col("value").isNotNull() &
        (col("value") >= 0)
    ) \
    .withColumn("reading_date", to_date("datetime_utc")) \
    .withColumn("date_key", date_format("reading_date", "yyyyMMdd").cast(IntegerType())) \
    .groupBy(
        "date_key", "reading_date",
        "year", "month",
        "location_id", "locality", "timezone",
        "parameter_name", "parameter_units"
    ).agg(
        count("*").alias("reading_count"),
        round(avg("value"), 4).alias("avg_value"),
        round(min("value"), 4).alias("min_value"),
        round(max("value"), 4).alias("max_value")
    ) \
    .withColumnRenamed("parameter_name",  "pollutant") \
    .withColumnRenamed("parameter_units", "unit")

display(fact_aq.limit(5))


fact_aq.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"FactAirQualityDaily")

print(f"FactAirQualityDaily done — {fact_aq.count()} rows")