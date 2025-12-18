# Arsitektur Deteksi Dini Anomali Mesin Pabrik Motor Listrik

Proyek memastikan implementasi sistem **deteksi dini anomali berbasis data streaming** untuk memonitor kondisi mesin motor listrik pada lingkungan pabrik secara **real-time**. Sistem ini dibangun menggunakan **Apache Kafka**, **InfluxDB**, **MinIO**, dan **Grafana**, dengan pendekatan **ELT (Extract, Load, Transform)**.

Proyek ini dikembangkan sebagai **Proyek Akhir Mata Kuliah Infrastruktur Data**.

---

## 📌 Tujuan Proyek

- Mensimulasikan arsitektur **IoT streaming** untuk lingkungan industri.
- Mendeteksi anomali mesin sejak dini berdasarkan data sensor.
- Menerapkan prinsip **ELT** untuk data berfrekuensi tinggi.
- Mengintegrasikan **hot storage** dan **cold storage**.
- Menyediakan **dashboard monitoring real-time** untuk operator.

---

## 🏗️ Arsitektur Sistem

```

Dummy Sensor Data
↓
Kafka Producer (produce_dummy_data.py)
↓
Apache Kafka (Streaming Layer)
↓
Kafka Consumer & Stream Processor (anomaly_detector.py)
↓
├── InfluxDB (Time-Series / Real-Time Storage)
├── MinIO (Data Lake / Cold Storage)
└── Kafka Topic (anomaly_events)
↓
Grafana Dashboard (Monitoring & Alert)

```

---

## 🔧 Teknologi yang Digunakan

| Komponen | Teknologi |
|-------|----------|
| Message Broker | Apache Kafka |
| Stream Processing | Kafka Consumer (Python) |
| Time-Series Database | InfluxDB 2.x |
| Data Lake | MinIO (S3-compatible) |
| Visualization | Grafana |
| Containerization | Docker & Docker Compose |
| Bahasa Pemrograman | Python |

---

## 📂 Struktur Folder

```

iot_motor_project/
│
├── docker-compose.yml
├── dummy_sensor_data_fix.jsonl
│
├── scripts/
│   ├── dummy_data_generate.py
│   ├── produce_dummy_data.py
│   └── anomaly_detector.py
│
└── README.md

````

---

## 📊 Jenis Data Sensor

| Sensor | Deskripsi | Unit |
|-----|----------|------|
| temperature_coil | Suhu kumparan motor | °C |
| vibration_rotor | Getaran rotor-stator | mm/s |
| current | Arus listrik | A |
| voltage | Tegangan listrik | V |
| pressure | Tekanan sistem pendukung | Pa |
| temperature_env | Suhu lingkungan | °C |
| humidity_env | Kelembapan lingkungan | % |

---

## ⚠️ Jenis Anomali yang Dideteksi

- **Anomali Termal** (Overheating)
- **Anomali Mekanik** (Getaran tinggi)
- **Anomali Listrik** (Arus abnormal)
- **Anomali Sistem Pendukung** (Tekanan tidak stabil)

Deteksi dilakukan menggunakan pendekatan **rule-based** yang mudah dijelaskan dan cocok untuk sistem peringatan dini.

---

## 🔄 Alur ELT (Extract – Load – Transform)

### Extract
Data sensor dihasilkan secara sintetis menggunakan simulator Python.

### Load
Data mentah dikirim ke Kafka melalui Kafka Producer tanpa transformasi awal.

### Transform
Transformasi, validasi, data fusion, dan deteksi anomali dilakukan di backend streaming consumer.

### Storage
- **InfluxDB** → data real-time & historis
- **MinIO** → data mentah dan hasil gabungan untuk analisis batch

---

## 🚀 Cara Menjalankan Proyek

### 1️⃣ Jalankan Infrastruktur
```bash
docker-compose up -d
````

### 2️⃣ Generate Data Dummy

```bash
python scripts/dummy_data_generate.py
```

### 3️⃣ Kirim Data ke Kafka

```bash
python scripts/produce_dummy_data.py
```

### 4️⃣ Jalankan Deteksi Anomali

```bash
python scripts/anomaly_detector.py
```

---

## 📈 Visualisasi Grafana

Akses Grafana melalui:

```
http://localhost:3000
```

Gunakan InfluxDB sebagai Data Source dan buat dashboard untuk:

* Status anomali per mesin
* Distribusi anomali per sensor
* Timeline kejadian anomali
* Monitoring real-time mesin

---

## 🔐 Catatan Keamanan

Proyek ini merupakan **simulasi akademik**, sehingga:

* Komunikasi Kafka menggunakan PLAINTEXT
* Kredensial disimpan secara hardcoded
* TLS dan autentikasi lanjutan tidak diaktifkan

Aspek keamanan dapat ditingkatkan pada implementasi produksi.

---

## 🎓 Konteks Akademik

Proyek ini dirancang untuk memenuhi kebutuhan pembelajaran pada mata kuliah **Infrastruktur Data**, dengan fokus pada:

* Streaming architecture
* Data validation
* Scalability
* Real-time monitoring
* Fault detection system

---

## 📌 Catatan Pengembangan Lanjutan

* Integrasi machine learning untuk deteksi anomali lanjutan
* Penerapan Kafka Streams atau Apache Flink
* Penerapan alerting otomatis berbasis threshold
* Implementasi security (TLS, SASL, secret management)


## 📄 Lisensi

Proyek ini dikembangkan untuk keperluan akademik dan pembelajaran.
