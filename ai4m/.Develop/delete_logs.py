#!/usr/bin/env python3
import shutil
import os
import glob

# List of log paths (use glob for wildcards like messages*)
LOG_FILES = [
    "/var/log/plc_reader.log",
    "/var/log/messages*"
]

THRESHOLD = 50  # Disk usage % threshold

def main():
    total, used, free = shutil.disk_usage("/")  # Checks root: /dev/mapper/rhel-root
    usage_percent = used / total * 100

    #print(f"Disk usage on /: {usage_percent:.2f}%")

    if usage_percent > THRESHOLD:
        for pattern in LOG_FILES:
            for log in glob.glob(pattern):
                if os.path.exists(log):
                    try:
                        os.remove(log)
                        print(f"[DELETED] {log}")
                    except Exception as e:
                        print(f"[ERROR] Could not delete {log}: {e}")
                else:
                    print(f"[SKIPPED] File not found: {log}")
    else:
        print(f"[OK] Disk usage is below {THRESHOLD}%, no logs deleted.")

if __name__ == "__main__":
    main()

