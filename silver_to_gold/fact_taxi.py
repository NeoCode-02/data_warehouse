fact_taxi = spark.read.table(f"lh_silver.dbo.nyc_taxi_silver") \
    .filter(
        col("tpep_pickup_datetime").isNotNull() &
        (col("fare_amount") > 0) &
        (col("trip_distance") > 0)
    ) \
    .withColumn("pickup_date", to_date("tpep_pickup_datetime")) \
    .withColumn("date_key", date_format("pickup_date", "yyyyMMdd").cast(IntegerType())) \
    .groupBy(
        "date_key", "pickup_date",
        "pickup_year", "pickup_month", "pickup_weekday",
        "pu_location_id", "do_location_id", "payment_type"
    ).agg(
        count("*").alias("trip_count"),
        round(sum("fare_amount"), 2).alias("total_fare_usd"),
        round(avg("fare_amount"), 2).alias("avg_fare_usd"),
        round(avg("trip_distance"), 3).alias("avg_distance_miles"),
        round(avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        round(avg("passenger_count"), 2).alias("avg_passengers"),
        round(sum("total_amount"), 2).alias("total_revenue_usd"),
        round(sum("tip_amount"), 2).alias("total_tips_usd"),
        round(sum("congestion_surcharge"), 2).alias("total_congestion_usd")
    ) \
    .withColumnRenamed("pu_location_id", "pickup_zone_id") \
    .withColumnRenamed("do_location_id", "dropoff_zone_id")

fx = spark.read.table("lh_gold.dbo.DimFX").select("date", "exchange_rate")

fact_taxi = fact_taxi \
    .join(fx, fact_taxi.pickup_date == fx.date, "left") \
    .withColumn("total_fare_eur", round(col("total_fare_usd") * col("exchange_rate"), 2)) \
    .withColumn("total_revenue_eur", round(col("total_revenue_usd") * col("exchange_rate"), 2)) \
    .drop("date", "exchange_rate")

display(fact_taxi.limit(5))


fact_taxi.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"FactTaxiDaily")

print(f"FactTaxiDaily done — {fact_taxi.count()} rows")