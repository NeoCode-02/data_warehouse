dim_date = spark.range(1).select(
    explode(
        sequence(to_date(lit("2019-01-01")), to_date(lit("2026-12-31")))
    ).alias("date")).select(
        date_format("date", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
        col("date"),
        year("date").alias("year"),
        quarter("date").alias("quarter"),
        month("date").alias("month"),
        date_format("date", "MMMM").alias("month_name"),
        dayofmonth("date").alias("day"),
        dayofweek("date").alias("day_of_week"),
        date_format("date", "EEEE").alias("day_name"),
        weekofyear("date").alias("week_of_year"),
        when(dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend")
)
display(dim_date.limit(5))

dim_date.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("DimDate")

print(f"DimDate done — {dim_date.count()} rows")