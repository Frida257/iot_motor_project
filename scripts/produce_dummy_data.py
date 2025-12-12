from kafka import KafkaProducer
import json
import time

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to determine topic based on sensor_type
def get_topic(sensor_type):
    if 'vibration' in sensor_type:
        return 'sensor_vibration_stream'
    elif sensor_type in ['current', 'voltage']:
        return 'electrical_stream'
    else:
        return 'sensor_vibration_stream'  # Default; customize as needed

# Read and produce dummy data
with open('D:/UNIV/S5/PID/iot_motor_project/scripts/data/dummy_sensor_data_fix.jsonl', 'r') as f:
    for line in f:
        event = json.loads(line)
        topic = get_topic(event['sensor_type'])
        # Use machine_id as key for partitioning (preserves order per machine)
        producer.send(topic, key=event['machine_id'].encode('utf-8'), value=event)
        print(f"Sent to {topic}: {event}")
        time.sleep(0.1)  # Simulate real-time delay (adjust for speed)

producer.flush()
producer.close()