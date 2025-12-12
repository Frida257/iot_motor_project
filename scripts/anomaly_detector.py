from kafka import KafkaConsumer, KafkaProducer
import json
import statistics
from datetime import datetime, timezone # <<< PERBAIKAN 1: Import timezone
import io
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, WritePrecision, Point # <<< PERBAIKAN 2: Import Point
from minio import Minio

# --- CONFIGURATION & CLIENT INITIALIZATION ---

# Ganti dengan nilai yang benar
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "Edzs7GMbhUiK3FsQOBKbRoLps4CfxKsO6SIj34H5lex8I6XNRdMkjqQhtn1lNak-VIi0U5qO7XSHmd9VbaEvhQ=="
INFLUX_ORG = "motor_org"
INFLUX_BUCKET = "motor_bucket" # <<< PERBAIKAN 3: Definisikan bucket

KAFKA_BROKERS = ['localhost:9092']
MINIO_HOST = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "motor-data-lake"

# Consumers for input topics
consumer = KafkaConsumer(
    'sensor_vibration_stream', 'electrical_stream',
    bootstrap_servers=KAFKA_BROKERS,
    group_id='anomaly_detector',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Producer for output
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# MinIO Client (Initialized Once)
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Pastikan bucket MinIO ada
found = minio_client.bucket_exists(MINIO_BUCKET)
if not found:
    minio_client.make_bucket(MINIO_BUCKET)
    print(f"Bucket {MINIO_BUCKET} dibuat.")

# --- ANOMALY & VALIDATION LOGIC (UNCHANGED) ---
def detect_anomaly(event):
    anomalies = []
    # Mengasumsikan sensor_type ada, karena divalidasi oleh validate_event
    if event.get('sensor_type') == 'temperature_coil' and event.get('value', 0) > 120:
        anomalies.append('Thermal: Overheating')
    if event.get('sensor_type') == 'vibration_rotor' and event.get('value', 0) > 5:
        anomalies.append('Mechanical: High Vibration')
    return anomalies

def validate_event(event):
    required_fields = ['timestamp', 'machine_id', 'sensor_type', 'value']
    if not all(field in event for field in required_fields):
        return 'invalid: missing fields'
    if event['sensor_type'] == 'temperature_coil' and not (20 <= event['value'] <= 120):
        return 'suspect'
    return 'valid'

# --- MAIN STREAM PROCESSING LOOP ---
window_data = {} 
window_events = {}
WINDOW_SIZE = 5

with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
    write_api = client.write_api(write_options=SYNCHRONOUS, write_precision=WritePrecision.S)

    print("Memulai proses Kafka Consumer...")
    for message in consumer:
        event = message.value
        machine = event['machine_id']

        # --- VALIDASI & ANOMALI ---
        validity = validate_event(event)
        event['validity'] = validity

        anomalies = detect_anomaly(event)
        if anomalies:
            anomaly_event = {**event, 'anomalies': anomalies}
            producer.send('anomaly_events', value=anomaly_event)

        # --- DATA FUSION / WINDOWING ---
        if machine not in window_data:
            window_data[machine] = []
            window_events[machine] = [] # Inisialisasi list event

        window_data[machine].append(event['value'])
        window_events[machine].append(event) # Simpan event mentah

        if len(window_data[machine]) >= WINDOW_SIZE:  # Window is full
            avg = statistics.mean(window_data[machine])

            # Menggunakan timestamp dari event terakhir sebagai timestamp gabungan
            last_event = event 
            fused_event = {
                **last_event, # Mengambil semua data dari event terakhir
                'fused_avg': avg,
                'window_size': WINDOW_SIZE,
                'data_source': f"{machine}_fused_window"
            }

            # --- Load to InfluxDB (TSDB) ---
            try:
                # 1. Parsing timestamp
                timestamp_str = last_event['timestamp'].replace('Z', '+00:00')
                dt_object = datetime.fromisoformat(timestamp_str).astimezone(timezone.utc)
                
                # 2. Membuat Point (Perhatikan Indentasi yang Konsisten di Bawah)
                point = (Point("sensor_summary") # Mulai di sini
                    .tag("machine_id", machine)
                    .tag("sensor_type", last_event['sensor_type'])
                    .field("value", last_event['value']) 
                    .field("fused_avg", avg) # <<< PERBAIKAN: Indentasi disamakan
                    .field("window_size", WINDOW_SIZE)
                    .time(dt_object, WritePrecision.S)
                ) # Tutup parenthesis

                write_api.write(bucket=INFLUX_BUCKET, record=point)
                print(f"InfluxDB: Data gabungan untuk {machine} berhasil ditulis.")

            except Exception as e:
                print(f"ERROR InfluxDB: Gagal menulis data. {e}")


            # --- Load to Data Lake (MinIO) ---
            try:
                json_data_str = json.dumps(fused_event)
                json_data_bytes = json_data_str.encode('utf-8')
                data_stream = io.BytesIO(json_data_bytes)
                data_length = len(json_data_bytes)

                # Nama file unik berdasarkan machine_id dan timestamp
                minio_filename = f"{machine}/{last_event['timestamp']}_fused.json"
                
                minio_client.put_object(
                    MINIO_BUCKET,
                    minio_filename,
                    data_stream,
                    data_length,
                    content_type='application/json' # Best practice
                )
                print(f"MinIO: Data gabungan untuk {machine} berhasil diunggah ke {minio_filename}.")

            except Exception as e:
                print(f"ERROR MinIO: Gagal mengunggah data. {e}")

            # --- Reset Window ---
            window_data[machine] = []
            window_events[machine] = []