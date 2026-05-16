fact_weather = spark.read.table("lh_silver.dbo.weather_silver") \
    .select(
        col("date_key"),
        col("date"),
        col("year"),
        col("month"),
        col("day"),
        col("day_of_week"),
        col("day_name"),
        col("city"),
        col("max_temp_c"),
        col("min_temp_c"),
        col("avg_temp_c"),
        col("max_wind_kph"),
        col("total_precip_mm"),
        col("avg_humidity"),
        col("condition"),
        col("uv_index")
    )

fact_weather.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("FactWeatherDaily")

print(f"FactWeatherDaily done — {fact_weather.count()} rows")