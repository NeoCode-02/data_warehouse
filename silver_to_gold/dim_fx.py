dim_fx = spark.read.table(f"lh_silver.dbo.ecb_fx_silver") \
    .filter(col("date").isNotNull() & col("exchange_rate").isNotNull()) \
    .select(
        date_format("date", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
        col("date"),
        col("currency"),
        col("currency_denom"),
        col("exchange_rate")
    )

display(dim_fx.limit(5))


dim_fx.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"DimFX")

print(f"DimFX done — {dim_fx.count()} rows")