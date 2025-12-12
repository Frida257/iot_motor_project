import json
import random
from datetime import datetime, timedelta

# Define constants
START_TIME = datetime(2025, 12, 12, 17, 0, 0)  # Start of simulation
END_TIME = datetime(2025, 12, 12, 18, 0, 0)    # End of simulation
INTERVAL_SECONDS = 5  # Event frequency
NUM_MACHINES = 3  # e.g., LINE_01_MOTOR_01 to LINE_03_MOTOR_10 (adjust as needed)
MACHINES = [f"LINE_{i//10 + 1:02d}_MOTOR_{i%10 + 1:02d}" for i in range(NUM_MACHINES)]

# Sensor types and their normal ranges/units
SENSORS = {
    "temperature_coil": {"min": 20, "max": 120, "unit": "C"},
    "vibration_rotor": {"min": 0.1, "max": 5, "unit": "mm/s"},
    "current": {"min": 1, "max": 50, "unit": "A"},
    "voltage": {"min": 200, "max": 500, "unit": "V"},
    "pressure": {"min": 1000, "max": 5000, "unit": "Pa"},
    "temperature_env": {"min": 15, "max": 35, "unit": "C"},
    "humidity_env": {"min": 30, "max": 70, "unit": "%"}
}

# Function to generate a single event
def generate_event(timestamp, machine_id, sensor_type):
    sensor_info = SENSORS[sensor_type]
    # Normal value: random within range
    value = round(random.uniform(sensor_info["min"], sensor_info["max"]), 2)
    
    # Introduce anomalies randomly (10% chance)
    if random.random() < 0.1:
        if sensor_type == "temperature_coil":
            value = random.uniform(130, 150)  # Overheat
        elif sensor_type == "vibration_rotor":
            value = random.uniform(6, 10)  # High vibration
        elif sensor_type == "current":
            value = random.uniform(-5, 0) if random.random() < 0.5 else random.uniform(60, 80)  # Imbalance or spike
        elif sensor_type == "pressure":
            value = random.uniform(500, 900)  # Low pressure
        # Add more anomaly logic as needed
    
    return {
        "timestamp": timestamp.isoformat() + "Z",
        "machine_id": machine_id,
        "sensor_type": sensor_type,
        "value": value,
        "unit": sensor_info["unit"]
    }

# Generate all events
events = []
current_time = START_TIME
while current_time <= END_TIME:
    for machine in MACHINES:
        for sensor in SENSORS.keys():
            event = generate_event(current_time, machine, sensor)
            events.append(event)
    current_time += timedelta(seconds=INTERVAL_SECONDS)

# Output to JSON Lines file
with open("dummy_sensor_data_fix.jsonl", "w") as f:
    for event in events:
        f.write(json.dumps(event) + "\n")

print(f"Generated {len(events)} dummy events in 'dummy_sensor_data_fix.jsonl'")