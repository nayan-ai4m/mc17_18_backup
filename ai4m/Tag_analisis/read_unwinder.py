import csv
import time
from datetime import datetime
from pycomm3 import LogixDriver

# IP address of your PLC
PLC_IP = '141.141.141.128'

# Mapping of tags to read
tags_to_read = {
    'Output Frequency': 'Read_Data[1]',
    'Commanded Frequency': 'Read_Data[3]',
    'Output Current': 'Read_Data[5]',
    'Output Voltage': 'Read_Data[7]',
    'DC Bus Voltage': 'Read_Data[9]',
    'Output RPM': 'Read_Data[29]',
    'Output Speed': 'Read_Data[31]',
    'Output Power': 'Read_Data[33]',
    'Drive Temperature': 'Read_Data[53]'
}

# CSV file name
CSV_FILE = 'plc_data_log_10_5.csv'

def main():
    # Open PLC connection
    with LogixDriver(PLC_IP) as plc:
        print("Connected to PLC!")

        # Open CSV file for writing
        with open(CSV_FILE, mode='w', newline='') as file:
            writer = csv.writer(file)

            # Write header
            header = ['Timestamp'] + list(tags_to_read.keys())
            writer.writerow(header)

            start_time = time.time()

            while (time.time() - start_time) < 15 * 60* 60:  # Run for 5 minutes
                row = [datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]]  # Timestamp with milliseconds

                for name, tag in tags_to_read.items():
                    try:
                        value = plc.read(tag).value
                    except Exception as e:
                        value = 'Error'
                    row.append(value)

                writer.writerow(row)
                print(row)  # Optional: print each row to the console

                time.sleep(0.1)  # Sleep 0.1 seconds

    print("Finished logging data!")

if __name__ == "__main__":
    main()

