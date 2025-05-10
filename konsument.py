
import json
from kafka import KafkaConsumer
import pandas as pd

SERVER = "broker:9092"
TOPIC = "sensor_data"

file_path = r"data.json"
df = pd.read_json(file_path)
df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

if __name__ == "__main__":
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[SERVER],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='temperature_filter_group'
    )

    try:
        for message in consumer:
            data = message.value
            temp = data.get("temperature")
            lat = data.get("latitude")
            lon = data.get("longitude")

            if temp is not None and temp > 10 and lat is not None and lon is not None:
                try:
                    lat = float(lat)
                    lon = float(lon)
                except ValueError:
                    continue

                df["distance_score"] = abs(df["Latitude"] - lat) + abs(df["Longitude"] - lon)
                closest_row = df.loc[df["distance_score"].idxmin()]
                closest_city = closest_row["Name"]
                closest_district = closest_row["District"]

                print(f"🚨Anomalia: temperatura {temp}°C | Lokalizacja: {closest_city} ({closest_district})")

    except KeyboardInterrupt:
        print("Konsument zatrzymany.")
