import json
from kafka import KafkaConsumer
 
SERVER = "broker:9092"
TOPIC = "sensor_data"
 
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
            temp_comp = data.get("temperature_compensated")
            lat = data.get("latitude")
            lon = data.get("longitude")
             
            if temp is not None and isinstance(temp, (int, float)) and temp > 20:
                print(f"🚨Anomalia: temperatura {temp}°C | Lokalizacja: ({lat}, {lon})")
            #elif:
             #   print("temperatura prawidłowa")
 
    except KeyboardInterrupt:
        print("Konsument zatrzymany.")