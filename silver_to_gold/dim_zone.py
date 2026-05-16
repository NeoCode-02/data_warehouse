dim_zone = spark.read.table(f"lh_silver.dbo.nyc_taxi_silver") \
    .filter(col("pu_location_id").isNotNull()) \
    .select(col("pu_location_id").alias("zone_id")) \
    .distinct() \
    .withColumn("city", lit("NYC"))

display(dim_zone.limit(5))


dim_zone.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("DimZone")

print(f"DimZone done — {dim_zone.count()} rows")