# Runs the real fact_taxi.py (up to its display) against tiny in-memory tables.
import os, sys
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
import datetime as dt
from types import SimpleNamespace
from pyspark.sql import SparkSession
from pyspark.sql.functions import *  # same as the notebook's first cell
from pyspark.sql.types import IntegerType

s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
d = dt.date
trip = lambda day, fare: (day, 2024, 1, 2, 1, 1, 1, fare, 1.0, 10.0, 1.0, fare, 0.0, 0.0)
cols = ["tpep_pickup_datetime", "pickup_year", "pickup_month", "pickup_weekday", "pu_location_id",
        "do_location_id", "payment_type", "fare_amount", "trip_distance", "trip_duration_minutes",
        "passenger_count", "total_amount", "tip_amount", "congestion_surcharge"]
tables = {
    "lh_silver.dbo.nyc_taxi_silver": s.createDataFrame([
        trip(dt.datetime(2024, 1, 1, 9), 10.956),   # holiday before first rate -> next rate (Jan 2)
        trip(dt.datetime(2024, 1, 2, 9), 10.956),   # business day
        trip(dt.datetime(2024, 1, 6, 9), 10.921),   # Saturday -> Friday Jan 5 rate
    ], cols),
    "lh_gold.dbo.DimDate": s.createDataFrame([(d(2023, 12, 31), 2023)] + [(d(2024, 1, i), 2024) for i in range(1, 8)], ["date", "year"]),
    "lh_gold.dbo.DimFX": s.createDataFrame([(d(2024, 1, 2), 1.0956), (d(2024, 1, 5), 1.0921)], ["date", "exchange_rate"]),
}
spark = SimpleNamespace(read=SimpleNamespace(table=tables.__getitem__))
src = open(os.path.join(os.path.dirname(__file__), "..", "silver_to_gold", "fact_taxi.py"), encoding="utf-8").read().split("display(fact_taxi")[0]
exec(src)
got = {r.pickup_date: r.total_fare_eur for r in fact_taxi.collect()}
print(got)
assert got == {d(2024, 1, 1): 10.0, d(2024, 1, 2): 10.0, d(2024, 1, 6): 10.0}, got
print("OK")
