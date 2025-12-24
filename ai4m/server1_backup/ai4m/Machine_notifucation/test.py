import psycopg2
from pycomm3 import LogixDriver
import time

# PLC Information
PLC_IP = "141.141.141.128"

# All tags to control
PLC_TAGS = [
    "MC17_DC_NOTIFICATION",
    "AENT:O.Data[7].0",
    "AENT:O.Data[7].1",
    "AENT:O.Data[7].2"
]

# Database Configuration
DB_CONFIG = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "192.168.1.168",
    "port": 5432
}

# ------------------------------------
# Function: Get "active" from database
# ------------------------------------
def get_active_count():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        query = """
            SELECT machine_status
            FROM field_overview_tp_status_l3
            WHERE machine_status->>'id' = 'MC 17';
        """

        cur.execute(query)
        result = cur.fetchone()

        cur.close()
        conn.close()

        if result is None:
            return None

        machine_status = result[0]
        return int(machine_status.get("active", 0))

    except Exception as e:
        print("DB Error:", e)
        return None


# ------------------------------------
# MAIN LOOP
# ------------------------------------
def main():
    last_state = None  # Avoid duplicate writes

    with LogixDriver(PLC_IP) as plc:

        while True:
            active = get_active_count()

            if active is None:
                print("⚠ Could not fetch active count.")
                time.sleep(2)
                continue

            print(f"Active count for MC17: {active}")

            # Determine PLC output
            desired_state = (active > 0)

            if desired_state != last_state:
                print(f"\n➡ Writing {desired_state} to ALL PLC tags...\n")

                for tag in PLC_TAGS:
                    try:
                        plc.write((tag, desired_state))
                        confirm = plc.read(tag).value
                        print(f"{tag} → {confirm}")
                    except Exception as e:
                        print(f"Error writing {tag}: {e}")

                last_state = desired_state
                print("\n✓ Update completed.\n")

            else:
                print("✓ No change → skipping PLC write.")

            time.sleep(2)


if __name__ == "__main__":
    main()

