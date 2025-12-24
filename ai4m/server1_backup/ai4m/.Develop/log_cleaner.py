#!/usr/bin/env python3

import psutil
import os

LOG_FILE = "/var/log/plc_reader.log"  
THRESHOLD = 60 

def get_memory_usage_percent():
    return psutil.virtual_memory().percent

def clear_log_if_needed():
    mem_usage = get_memory_usage_percent()
    if mem_usage >= THRESHOLD:
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'w') as f:
                f.truncate()
            print(f"Memory usage is {mem_usage}%. Cleared log: {LOG_FILE}")
        else:
            print(f"Log file not found: {LOG_FILE}")

if __name__ == "__main__":
    clear_log_if_needed()

