import psycopg2
from pycomm3 import LogixDriver
import time

PLC_IP = "141.141.141.128"
TAG_NAME = "MC17_DC_NOTIFICATION"

DB_CONFIG = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "192.168.1.168",
    "port": 5432
}

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

        machine_status = result[0]   # JSON → dict
        active_val = int(machine_status.get("active", 0))
        return active_val

    except Exception as e:
        print("DB Error:", e)
        return None


def main():

    last_state = None

    with LogixDriver(PLC_IP) as plc:

        while True:
            active = get_active_count()

            if active is None:
                print("⚠ Could not fetch active count.")
                time.sleep(2)
                continue

            print(f"Active count for MC17: {active}")

            desired_state = (active > 0)

            if desired_state != last_state:
                print(f"➡ Writing {desired_state} to PLC...")
                plc.write((TAG_NAME, desired_state))

                read_back = plc.read(TAG_NAME).value
                print(f"🔍 PLC Confirmation: {read_back}")

                last_state = desired_state
            else:
                print("✓ No change detected")

            time.sleep(2)


if __name__ == "__main__":
    main()

