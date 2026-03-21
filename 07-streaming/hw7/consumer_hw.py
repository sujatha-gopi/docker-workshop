import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-console',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name}...")

count = 0
i = 0
for message in consumer:
    ride = message.value
    #pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)
    #print(f"Received: PU={ride.PULocationID}, DO={ride.DOLocationID}, "
    #      f"distance={ride.trip_distance}, amount=${ride.total_amount:.2f}, "
    #      f"pickup={pickup_dt}")
    if ride.trip_distance > 5:
        count += 1
        if count > 8500:
            print("Count :", count)

    i += 1
    #count += 1
    if i >= 1000:
        print(f"\n... received {count} messages so far (Non stopping after 10 for demo)")
        i = 0
print("Count of trip > than 5 KM:", count)
consumer.close()