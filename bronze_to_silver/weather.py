import requests
import json
from datetime import date, timedelta
from pyspark.sql import Row
import os
import dotenv
 
dotenv.load_dotenv()
 
API_KEY = os.getenv("API_KEY")
CITY = "New York"
BRONZE_PATH = "Files/bronze/weather"
 
def fetch_weather_for_date(api_key, city, target_date):
    url = "http://api.weatherapi.com/v1/history.json"
    params = {
        "key": api_key,
        "q":   city,
        "dt":  target_date.strftime("%Y-%m-%d")
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()
 
start_date = date(2024, 1, 1)
end_date   = date(2024, 12, 31)
 
all_records = []
current  = start_date
 
while current <= end_date:
    try:
        day_data = fetch_weather_for_date(API_KEY, CITY, current)
        day      = day_data["forecast"]["forecastday"][0]["day"]
        record   = {
            "date":            current.strftime("%Y-%m-%d"),
            "city":            CITY,
            "max_temp_c":      day["maxtemp_c"],
            "min_temp_c":      day["mintemp_c"],
            "avg_temp_c":      day["avgtemp_c"],
            "max_wind_kph":    day["maxwind_kph"],
            "total_precip_mm": day["totalprecip_mm"],
            "avg_humidity":    day["avghumidity"],
            "condition":       day["condition"]["text"],
            "uv_index":        day["uv"]
        }
        all_records.append(record)
        print(f"  Fetched: {current}")
    except Exception as e:
        print(f"  Error on {current}: {e}")
    current += timedelta(days=1)
 
print(f"\nTotal days fetched: {len(all_records)}")
 
df_bronze = spark.createDataFrame([Row(**r) for r in all_records])

df_bronze.write.mode("overwrite").json(
    "Files/bronze/weather/nyc_weather_2024"
)

print(f"Saved to Bronze: Files/bronze/weather/nyc_weather_2024/")
print(f"Total rows: {df_bronze.count()}")
print("Bronze ingestion complete")