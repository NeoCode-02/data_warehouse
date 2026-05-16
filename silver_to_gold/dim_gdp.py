dim_gdp = spark.read.table(f"lh_silver.dbo.world_gdp_silver") \
    .filter(col("year").isNotNull() & col("gdp_usd").isNotNull()) \
    .select(
        col("country_code"),
        col("gdp_usd"),
        col("year")
    )

display(dim_gdp.limit(5))

dim_gdp.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("DimGDP")

print(f"DimGDP done — {dim_gdp.count()} rows")