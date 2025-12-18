from kafka import KafkaConsumer, KafkaProducer
import json
import statistics
from datetime import datetime, timezone  # <<< PERBAIKAN 1: Import timezone
import io
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, WritePrecision, Point  # <<< PERBAIKAN 2: Import Point
from minio import Minio

# --- CONFIGURATION ---

# Ganti dengan nilai yang benar
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "Edzs7GMbhUiK3FsQOBKbRoLps4CfxKsO6SIj34H5lex8I6XNRdMkjqQhtn1lNak-VIi0U5qO7XSHmd9VbaEvhQ=="
INFLUX_ORG = "motor_org"
INFLUX_BUCKET = "motor_bucket"  # <<< PERBAIKAN 3: Definisikan bucket

KAFKA_BROKERS = ['localhost:9092']
MINIO_HOST = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "motor-data-lake"

WINDOW_SIZE = 1

# --- ANOMALY & VALIDATION LOGIC ---

def detect_anomaly(event):
    anomalies = []
    sensor = event.get('sensor_type')
    val = event.get('value', 0)

    # 1. Termal [cite: 68]
    if sensor == 'temperature_coil' and val > 120:
        anomalies.append('Thermal: Overheating')
    
    # 2. Mekanik [cite: 71]
    elif sensor == 'vibration_rotor' and val > 5:
        anomalies.append('Mechanical: High Vibration')
        
    # 3. Listrik [cite: 74]
    elif sensor == 'current' and (val > 50 or val < 0):
        anomalies.append('Electrical: Current Anomaly')

    # 4. Sistem Pendukung (Pressure) [cite: 80]
    elif sensor == 'pressure' and (val < 1000 or val > 5000):
        anomalies.append('Support: Pressure Unstable')

    return anomalies if anomalies else ["Normal"]

def validate_event(event):
    required = ['timestamp', 'machine_id', 'sensor_type', 'value']
    if not all(field in event for field in required):
        return 'invalid: missing fields'

    # 2. Validasi Tipe Data
    if not isinstance(event['value'], (int, float)):
        return 'invalid: wrong data type'
    
    s_type = event['sensor_type']
    val = event['value']

    # 3. Validasi Range Nilai Berdasarkan Spesifikasi [cite: 144, 145]
    # Termal (Batas normal dokumen: 20-120°C) [cite: 146]
    if s_type == 'temperature_coil':
        if not (0 <= val <= 200):  # Nilai di atas 200°C dianggap error sensor fisik
            return 'suspect'

    # Mekanik (Batas normal dokumen: <5 mm/s) [cite: 147]
    elif s_type == 'vibration_rotor':
        if val < 0 or val > 20:  # Getaran tidak mungkin negatif
            return 'suspect'

    # Listrik (Arus) [cite: 148]
    elif s_type == 'current':
        if val < -10 or val > 150:  # Arus negatif ekstrem atau lonjakan mustahil
            return 'suspect'

    # Sistem Pendukung (Pressure) [cite: 80, 81]
    elif s_type == 'pressure':
        if val < 0 or val > 10000:  # Tekanan sistem hidrolik/pneumatik [cite: 82]
            return 'suspect'

    # Kondisi Lingkungan [cite: 84, 85]
    elif s_type in ['temperature_env', 'humidity_env']:
        if s_type == 'humidity_env' and not (0 <= val <= 100):
            return 'suspect'

    return 'valid'

# --- CLIENT INITIALIZATION FUNCTION ---

def initialize_clients():
    # Consumers for input topics
    consumer = KafkaConsumer(
        'sensor_vibration_stream', 'sensor_electrical_stream', 'sensor_temperature_stream', 'sensor_pressure_stream',
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

    # InfluxDB Client
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS, write_precision=WritePrecision.S)

    return consumer, producer, minio_client, influx_client, write_api

# --- MAIN FUNCTION ---

def main():
    consumer, producer, minio_client, influx_client, write_api = initialize_clients()

    # Ubah window_data menjadi dict of dict: window_data[machine][sensor_type] = list of values
    window_data = {} 
    # window_events tidak digunakan lagi, dihapus untuk efisiensi

    print("Memulai proses Kafka Consumer...")
    for message in consumer:
        event = message.value
        machine = event['machine_id']
        sensor_type = event['sensor_type']

        # --- VALIDASI ---
        validity = validate_event(event)
        event['validity'] = validity

        # --- DETEKSI ANOMALI (dipanggil sekali) ---
        anomalies = detect_anomaly(event)
        anomaly_label = ",".join(anomalies) if anomalies != ["Normal"] else "Normal"

        # --- KIRIM ANOMALI KE KAFKA HANYA JIKA ADA ANOMALI NYATA ---
        if anomalies != ["Normal"]:
            anomaly_event = {**event, 'anomalies': anomalies}
            producer.send('anomaly_events', value=anomaly_event)

        # --- DATA FUSION / WINDOWING PER MACHINE DAN SENSOR_TYPE ---
        if machine not in window_data:
            window_data[machine] = {}
        if sensor_type not in window_data[machine]:
            window_data[machine][sensor_type] = []

        window_data[machine][sensor_type].append(event['value'])

        if len(window_data[machine][sensor_type]) >= WINDOW_SIZE:  # Window is full
            avg = statistics.mean(window_data[machine][sensor_type])

            # Menggunakan timestamp dari event terakhir sebagai timestamp gabungan
            last_event = event 
            fused_event = {
                **last_event,  # Mengambil semua data dari event terakhir
                'fused_avg': avg,
                'window_size': WINDOW_SIZE,
                'data_source': f"{machine}_{sensor_type}_fused_window",
                'anomalies': anomalies  # Tambahkan list anomalies ke fused_event
            }

            # --- Load to InfluxDB (TSDB) ---
            try:
                # 1. Parsing timestamp
                timestamp_str = last_event['timestamp'].replace('Z', '+00:00')
                dt_object = datetime.fromisoformat(timestamp_str).astimezone(timezone.utc)
                
                # 2. Membuat Point - Ubah anomaly_type menjadi field untuk fleksibilitas penyimpanan
                point = (Point("sensor_summary")
                    .tag("machine_id", machine)
                    .tag("sensor_type", sensor_type)  # Tag sensor_type untuk per jenis sensor
                    .tag("anomaly_status", "Anomalous" if anomaly_label != "Normal" else "Normal")  # Tag status
                    .field("anomaly_type", anomaly_label)  # Ubah ke field agar bisa menyimpan string panjang atau multiple
                    .time(dt_object, WritePrecision.S)
                )

                write_api.write(bucket=INFLUX_BUCKET, record=point)
                print(f"InfluxDB: Data gabungan untuk {machine} - {sensor_type} berhasil ditulis. Anomaly: {anomaly_label}")

            except Exception as e:
                print(f"ERROR InfluxDB: Gagal menulis data. {e}")

            # --- Load to Data Lake (MinIO) ---
            try:
                json_data_str = json.dumps(fused_event)
                json_data_bytes = json_data_str.encode('utf-8')
                data_stream = io.BytesIO(json_data_bytes)
                data_length = len(json_data_bytes)

                # Nama file unik berdasarkan machine_id, sensor_type, dan timestamp
                minio_filename = f"{machine}/{sensor_type}/{last_event['timestamp']}_fused.json"
                
                minio_client.put_object(
                    MINIO_BUCKET,
                    minio_filename,
                    data_stream,
                    data_length,
                    content_type='application/json'  # Best practice
                )
                print(f"MinIO: Data gabungan untuk {machine} - {sensor_type} berhasil diunggah ke {minio_filename}.")

            except Exception as e:
                print(f"ERROR MinIO: Gagal mengunggah data. {e}")

            # --- Reset Window ---
            window_data[machine][sensor_type] = []

    # Close clients if needed (though the loop runs indefinitely)
    influx_client.close()

if __name__ == "__main__":
    main()