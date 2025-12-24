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

CSV_FILE = 'stroke_pressure_log_18.csv'

S1_START, S1_END, S1_STEP = 54.8, 37, -0.2
S2_START, S2_END, S2_STEP = 5, 3, -0.2
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
        ORDER BY timestamp DESC LIMIT 50;
    """)
    rows = cur.fetchall()
    latest_status = rows[0][4] if rows else 0
    return latest_status, rows


def get_avg_pressure_grouped(rows):
    """
    Calculate avg pressure grouped by spare1, then average across those groups.
    """
    # Filter by cam range
    filtered = [r for r in rows if PRESSURE_CAM_MIN <= r[0] <= PRESSURE_CAM_MAX]
    if not filtered:
        return None, None

    # Group by spare1
    grouped = {}
    for cam, spare1, ts, pressure, status in filtered:
        grouped.setdefault(spare1, []).append(pressure)

    # Take average per spare1
    per_group_avg = [sum(vals) / len(vals) for vals in grouped.values()]
    overall_avg = sum(per_group_avg) / len(per_group_avg)

    # Use the first row's cycle_id + timestamp for logging
    first_row = filtered[0]
    return overall_avg, (first_row[1], first_row[2])


def write_csv_row(file, row):
    """Append a row to CSV."""
    with open(file, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(row)


def wait_for_ready_status(conn, plc, initial_s1, initial_s2):
    """Wait until machine status = 1, reset if needed."""
    while True:
        status, _ = get_status_and_data(conn)
        if status == 1:
            break
        else:
            print("PAUSED - Resetting to initial values")
            plc.write((TAG_S1, initial_s1))
            plc.write((TAG_S2, initial_s2))
            time.sleep(0.5)


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

    # Generate all parameter combinations
    param_combinations = []
    for s1 in frange(S1_START, S1_END, S1_STEP):
        for s2 in frange(S2_START, S2_END, S2_STEP):
            param_combinations.append((s1, s2))
    
    print(f"Total combinations to test: {len(param_combinations)}")

    try:
        previous_params = None  # Track previous parameter settings that need pressure calculation
        
        for i, (s1, s2) in enumerate(param_combinations):
            # Wait for machine to be ready
            wait_for_ready_status(conn, plc, initial_s1, initial_s2)
            
            # Set current parameters
            plc.write((TAG_S1, s1))
            plc.write((TAG_S2, s2))
            print(f"\nSetting S1={s1}, S2={s2} [{i+1}/{len(param_combinations)}]")
            
            # Wait for machine to stabilize with new settings
            time.sleep(STABILIZATION_TIME)
            
            # If this is NOT the first iteration, calculate pressure for PREVIOUS settings
            if previous_params is not None:
                prev_s1, prev_s2 = previous_params
                print(f"Calculating pressure for previous settings: S1={prev_s1}, S2={prev_s2}")
                
                # Fetch pressure data (this reflects the previous parameter settings)
                _, rows = get_status_and_data(conn)
                avg_pressure, meta = get_avg_pressure_grouped(rows)
                
                if avg_pressure is not None:
                    cycle_id, timestamp = meta
                    write_csv_row(CSV_FILE, [cycle_id, prev_s1, prev_s2, avg_pressure, timestamp])
                    print(f"Logged: {cycle_id}, {prev_s1}, {prev_s2}, {avg_pressure:.2f}, {timestamp}")
                else:
                    print(f"No valid pressure data found for S1={prev_s1}, S2={prev_s2}!")
            else:
                print("First iteration - no previous data to calculate")
            
            # Store current params as previous for next iteration
            previous_params = (s1, s2)
        
        # Handle the LAST parameter combination (no next iteration to calculate its pressure)
        if previous_params is not None:
            # Wait a bit more to ensure the last settings have taken effect
            print("\nWaiting for final settings to take effect...")
            time.sleep(STABILIZATION_TIME * 2)  # Wait a bit longer for final measurement
            
            prev_s1, prev_s2 = previous_params
            print(f"Calculating pressure for FINAL settings: S1={prev_s1}, S2={prev_s2}")
            
            # Calculate pressure for the last parameter combination
            _, rows = get_status_and_data(conn)
            avg_pressure, meta = get_avg_pressure_grouped(rows)
            
            if avg_pressure is not None:
                cycle_id, timestamp = meta
                write_csv_row(CSV_FILE, [cycle_id, prev_s1, prev_s2, avg_pressure, timestamp])
                print(f"Logged FINAL: {cycle_id}, {prev_s1}, {prev_s2}, {avg_pressure:.2f}, {timestamp}")
            else:
                print(f"No valid pressure data found for FINAL settings S1={prev_s1}, S2={prev_s2}!")

    finally:
        # Always reset on exit
        plc.write((TAG_S1, initial_s1))
        plc.write((TAG_S2, initial_s2))
        print(f"\nReset S1 & S2 to initial values ({initial_s1}, {initial_s2}) on exit.")
