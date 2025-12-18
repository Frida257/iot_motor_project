from kafka import KafkaProducer
import json
import time
from datetime import datetime

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Penyesuaian Topik berdasarkan Rancangan Proyek
def get_topic(sensor_type):
    # Sesuaikan dengan kunci di SENSORS generator Anda
    if 'vibration' in sensor_type:
        return 'sensor_vibration_stream'
    elif sensor_type in ['current', 'voltage']:
        return 'sensor_electrical_stream'
    elif 'temperature' in sensor_type:
        return 'sensor_temperature_stream'
    elif sensor_type == 'pressure':
        return 'sensor_pressure_stream'
    else:
        return 'sensor_vibration_stream'

# Simulasi pengiriman data
path_data = 'D:/UNIV/S5/PID/iot_motor_project/dummy_sensor_data_fix.jsonl'

try:
    with open(path_data, 'r') as f:
        for line in f:
            event = json.loads(line)
            
            # Memastikan field wajib ada untuk validasi schema di Consumer [cite: 141]
            if 'timestamp' not in event:
                event['timestamp'] = datetime.now().isoformat() + "Z"
            
            topic = get_topic(event['sensor_type'])
            
            # Pengiriman dengan Key (machine_id) agar ordering terjaga [cite: 109, 121]
            producer.send(
                topic, 
                key=event['machine_id'].encode('utf-8'), 
                value=event
            )
            
            print(f"Sent to {topic}: {event['machine_id']} - {event['sensor_type']}: {event['value']}")
            
            # Jeda 0.1 detik untuk simulasi real-time [cite: 118]
            time.sleep(0.1) 

except FileNotFoundError:
    print(f"File tidak ditemukan di: {path_data}")
finally:
    producer.flush()
    producer.close()