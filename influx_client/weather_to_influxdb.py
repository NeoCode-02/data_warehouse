import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import dotenv

dotenv.load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

df = pd.read_csv(os.path.join(os.path.dirname(__file__), "factweatherdaily.csv"))
df["date"] = pd.to_datetime(df["date"])  

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

for _, row in df.iterrows():
    point = (
        Point("weather_daily")
        .tag("city", row["city"])
        .tag("condition", row.get("condition", "unknown"))
        .field("avg_temp_c", float(row["avg_temp_c"]))
        .field("max_temp_c", float(row["max_temp_c"]))
        .field("min_temp_c", float(row["min_temp_c"]))
        .field("avg_humidity", float(row["avg_humidity"]))
        .field("total_precip_mm", float(row["total_precip_mm"]))
        .field("max_wind_kph", float(row["max_wind_kph"]))
        .time(row["date"])  
    )
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

print(f"Written {len(df)} rows to InfluxDB")
client.close()