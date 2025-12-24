from pycomm3 import LogixDriver
import psycopg2
import time
import csv
from datetime import datetime

# ========================
# CONFIGURATION
# ========================

PLC_IP = '141.141.141.138'
TAG_S1 = 'HMI_Hor_Sealer_Strk_1'
TAG_S2 = 'HMI_Hor_Sealer_Strk_2'

DB_CONFIG = {
    'dbname': 'short_data_hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': '192.168.1.149',
    'port': '5432'
}

CSV_FILE = 'stroke_pressure_log_18_2.csv'

S1_START, S1_END, S1_STEP = 45, 42, -0.2
S2_START, S2_END, S2_STEP = 5, 2, -0.2

STABILIZATION_TIME = 2  # seconds
PRESSURE_CAM_MIN, PRESSURE_CAM_MAX = 150, 190

# ========================
# HELPER FUNCTIONS
# ========================

def frange(start, stop, step):
    """Range function that works with floats."""
    while (step < 0 and start >= stop) or (step > 0 and start <= stop):
        yield round(start, 2)
        start += step

def get_status_and_data(conn):
    """Fetch latest status and data from DB."""
    cur = conn.cursor()
    cur.execute("""
        SELECT cam_position, spare1, timestamp, hor_pressure, status
        FROM mc18_short_data
        ORDER BY timestamp DESC
        LIMIT 2000;
    """)
    rows = cur.fetchall()
    latest_status = rows[0][4] if rows else 0
    return latest_status, rows

def get_avg_pressure(rows):
    """Calculate average pressure in desired cam range."""
    filtered = [r for r in rows if PRESSURE_CAM_MIN <= r[0] <= PRESSURE_CAM_MAX]
    if not filtered:
        return None, None
    pressures = [r[3] for r in filtered]
    avg_pressure = sum(pressures) / len(pressures)
    cycle_id = filtered[0][1]
    timestamp = filtered[0][2]
    return avg_pressure, (cycle_id, timestamp)

def write_csv_row(file, row):
    """Append a row to CSV."""
    with open(file, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(row)

# ========================
# MAIN PROGRAM
# ========================

with LogixDriver(PLC_IP) as plc, psycopg2.connect(**DB_CONFIG) as conn:

    # Read initial S1 & S2
    initial_s1 = plc.read(TAG_S1).value
    initial_s2 = plc.read(TAG_S2).value
    print(f"Initial S1={initial_s1}, S2={initial_s2}")

    # Prepare CSV header if file doesn't exist
    try:
        with open(CSV_FILE, 'x', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['cycle_id', 'stroke_1', 'stroke_2', 'avg_pressure', 'timestamp'])
    except FileExistsError:
        pass

    try:
        for s1 in frange(S1_START, S1_END, S1_STEP):
            for s2 in frange(S2_START, S2_END, S2_STEP):

                # Wait until status = 1
                while True:
                    status, _ = get_status_and_data(conn)
                    if status == 1:
                        break
                    else:
                        print("PAUSED - Resetting to initial values")
                        plc.write((TAG_S1, initial_s1))
                        plc.write((TAG_S2, initial_s2))
                        time.sleep(0.5)

                # Set new stroke values
                plc.write((TAG_S1, s1))
                plc.write((TAG_S2, s2))
                print(f"Testing S1={s1}, S2={s2}")

                # Wait for stabilization
                time.sleep(STABILIZATION_TIME)

                # Get pressure data
                _, rows = get_status_and_data(conn)
                avg_pressure, meta = get_avg_pressure(rows)

                if avg_pressure is not None:
                    cycle_id, timestamp = meta
                    write_csv_row(CSV_FILE, [cycle_id, s1, s2, avg_pressure, timestamp])
                    print(f"Logged: {cycle_id}, {s1}, {s2}, {avg_pressure:.2f}, {timestamp}")
                else:
                    print("No pressure data found in range!")

    finally:
        # Always reset on exit
        plc.write((TAG_S1, initial_s1))
        plc.write((TAG_S2, initial_s2))
        print("Reset S1 & S2 to initial values on exit.")

