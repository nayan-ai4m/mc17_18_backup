import datetime
from pycomm3 import LogixDriver
import psycopg2
import time
import threading
import sys

class CycleTracker:
    def __init__(self):
        self.plc_ip = '141.141.141.128'  # MC17 IP
        self.insert_query = """
        INSERT INTO mc17(
            "timestamp", ver_sealer_front_1_temp, ver_sealer_front_2_temp, ver_sealer_front_3_temp, ver_sealer_front_4_temp,
            ver_sealer_front_5_temp, ver_sealer_front_6_temp, ver_sealer_front_7_temp, ver_sealer_front_8_temp,
            ver_sealer_front_9_temp, ver_sealer_front_10_temp, ver_sealer_front_11_temp, ver_sealer_front_12_temp,
            ver_sealer_front_13_temp, ver_sealer_rear_1_temp, ver_sealer_rear_2_temp, ver_sealer_rear_3_temp,
            ver_sealer_rear_4_temp, ver_sealer_rear_5_temp, ver_sealer_rear_6_temp, ver_sealer_rear_7_temp,
            ver_sealer_rear_8_temp, ver_sealer_rear_9_temp, ver_sealer_rear_10_temp, ver_sealer_rear_11_temp,
            ver_sealer_rear_12_temp, ver_sealer_rear_13_temp, hor_sealer_rear_1_temp, hor_sealer_front_1_temp,
            hor_sealer_rear_2_temp, hor_sealer_rear_3_temp, hor_sealer_rear_4_temp, hor_sealer_rear_5_temp,
            hor_sealer_rear_6_temp, hor_sealer_rear_7_temp, hor_sealer_rear_8_temp, hor_sealer_rear_9_temp,
            hopper_1_level, hopper_2_level, piston_stroke_length, hor_sealer_current, ver_sealer_current,
            rot_valve_1_current, fill_piston_1_current, fill_piston_2_current, rot_valve_2_current, web_puller_current,
            hor_sealer_position, ver_sealer_position, rot_valve_1_position, fill_piston_1_position, fill_piston_2_position,
            rot_valve_2_position, web_puller_position, sachet_count, cld_count, status, status_code, cam_position,
            pulling_servo_current, pulling_servo_position, hor_pressure, ver_pressure, eye_mark_count, spare1)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        self.db_settings = {
            'host': '192.168.1.149',
            'database': 'hul',
            'user': 'postgres',
            'password': 'ai4m2024'
        }

        self.plc = None
        self.cycle_id = 1
        self.last_cam_position = None
        self.current_day = None
        self.last_message_time = time.time()
        self.timeout = 30
        self.db_connection_attempts = 0
        self.max_db_connection_attempts = 5
        
        # Initialize connections
        self.connect_plc()
        self.connect_db()

    def connect_db(self):
        """Connect to PostgreSQL database with retry logic"""
        attempts = 0
        while attempts < self.max_db_connection_attempts:
            try:
                self.close_db_connection()  # Clean up any existing connection
                self.conn = psycopg2.connect(**self.db_settings)
                self.conn.autocommit = False
                self.cursor = self.conn.cursor()
                print("Successfully connected to PostgreSQL database")
                self.db_connection_attempts = 0
                return True
            except Exception as e:
                attempts += 1
                self.db_connection_attempts += 1
                print(f"Database connection error (attempt {attempts}): {e}")
                if attempts < self.max_db_connection_attempts:
                    time.sleep(5)
        
        print("Max database connection attempts reached. Service will exit.")
        sys.exit(1)

    def close_db_connection(self):
        """Safely close database connection"""
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
        except Exception as e:
            print(f"Error closing cursor: {e}")
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
        except Exception as e:
            print(f"Error closing connection: {e}")
        self.cursor = None
        self.conn = None

    def connect_plc(self):
        """Connect to the PLC using LogixDriver. Retry until successful."""
        while True:
            try:
                if self.plc:
                    self.plc.close()
                self.plc = LogixDriver(self.plc_ip)
                self.plc.open()
                print(f"Successfully connected to PLC at {self.plc_ip}")
                return
            except Exception as e:
                print(f"Failed to connect to PLC at {self.plc_ip}: {e}. Retrying in 5 seconds.")
                time.sleep(5)

    def reconnect(self):
        """Reconnect to both PLC and database"""
        print("Attempting to reconnect to PLC and database...")
        self.connect_plc()
        self.connect_db()
        self.last_message_time = time.time()

    def check_data_flow(self):
        """Check if data is received within the timeout period."""
        while True:
            if time.time() - self.last_message_time > self.timeout:
                print(f"No data received for {self.timeout} seconds. Reconnecting...")
                self.reconnect()
            time.sleep(5)

    def ensure_db_connection(self):
        """Ensure we have a valid database connection"""
        if self.conn is None or self.conn.closed != 0 or self.cursor is None or self.cursor.closed:
            print("Database connection lost. Reconnecting...")
            return self.connect_db()
        return True

    def read_and_insert(self):
        while True:
            # Read data from PLC
            try:
                if not self.plc or not self.plc.connected:
                    print("PLC not connected, reconnecting...")
                    self.connect_plc()
                
                tag_value = self.plc.read("MC17")
                if not tag_value:
                    print("No data read from PLC. Retrying.")
                    time.sleep(1)
                    continue
                
                data = tag_value.value
                self.last_message_time = time.time()
            except Exception as e:
                print(f"Error reading from PLC: {e}. Attempting to reconnect.")
                self.connect_plc()
                time.sleep(1)
                continue

            # Process data and insert into PostgreSQL
            try:
                if not self.ensure_db_connection():
                    print("Failed to establish database connection. Skipping this data point.")
                    continue

                timestamp = datetime.datetime(
                    data['Year'], data['Month'], data['Day'],
                    data['Hour'], data['Min'], data['Sec'],
                    data['Microsecond']
                )
                
                current_day = timestamp.date()
                if self.current_day is None:
                    self.current_day = current_day
                elif current_day != self.current_day:
                    self.cycle_id = 1
                    self.current_day = current_day

                current_cam_position = data['MC_Cam_Position']
                if self.last_cam_position is not None:
                    position_diff = abs(current_cam_position - self.last_cam_position)
                    if position_diff > 280:
                        self.cycle_id += 1
                self.last_cam_position = current_cam_position

                # Execute with new cursor each time to avoid "cursor already closed" errors
                with self.conn.cursor() as cursor:
                    cursor.execute(self.insert_query, (
                        str(timestamp),
                        data['MC_Ver_Sealer_Front_1_Temp'], data['MC_Ver_Sealer_Front_2_Temp'],
                        data['MC_Ver_Sealer_Front_3_Temp'], data['MC_Ver_Sealer_Front_4_Temp'],
                        data['MC_Ver_Sealer_Front_5_Temp'], data['MC_Ver_Sealer_Front_6_Temp'],
                        data['MC_Ver_Sealer_Front_7_Temp'], data['MC_Ver_Sealer_Front_8_Temp'],
                        data['MC_Ver_Sealer_Front_9_Temp'], data['MC_Ver_Sealer_Front_10_Temp'],
                        data['MC_Ver_Sealer_Front_11_Temp'], data['MC_Ver_Sealer_Front_12_Temp'],
                        data['MC_Ver_Sealer_Front_13_Temp'], data['MC_Ver_Sealer_Rear_1_Temp'],
                        data['MC_Ver_Sealer_Rear_2_Temp'], data['MC_Ver_Sealer_Rear_3_Temp'],
                        data['MC_Ver_Sealer_Rear_4_Temp'], data['MC_Ver_Sealer_Rear_5_Temp'],
                        data['MC_Ver_Sealer_Rear_6_Temp'], data['MC_Ver_Sealer_Rear_7_Temp'],
                        data['MC_Ver_Sealer_Rear_8_Temp'], data['MC_Ver_Sealer_Rear_9_Temp'],
                        data['MC_Ver_Sealer_Rear_10_Temp'], data['MC_Ver_Sealer_Rear_11_Temp'],
                        data['MC_Ver_Sealer_Rear_12_Temp'], data['MC_Ver_Sealer_Rear_13_Temp'],
                        data['MC_Hor_Sealer_Rear_1_Temp'], data['MC_Hor_Sealer_Front_1_Temp'],
                        data['MC_Hor_Sealer_Rear_2_Temp'], data['MC_Hor_Sealer_Rear_3_Temp'],
                        data['MC_Hor_Sealer_Rear_4_Temp'], data['MC_Hor_Sealer_Rear_5_Temp'],
                        data['MC_Hor_Sealer_Rear_6_Temp'], data['MC_Hor_Sealer_Rear_7_Temp'],
                        data['MC_Hor_Sealer_Rear_8_Temp'], data['MC_Hor_Sealer_Rear_9_Temp'],
                        data['MC_Hopper_1_Level'], data['MC_Hopper_2_Level'],
                        data['MC_Piston_Stroke_Length'], data['MC_Hor_Sealer_Current'],
                        data['MC_Ver_Sealer_Current'], data['MC_Rot_Valve_1_Current'],
                        data['MC_Fill_Piston_1_Current'], data['MC_Fill_Piston_2_Current'],
                        data['MC_Rot_Valve_2_Current'], data['MC_Web_Puller_Current'],
                        data['MC_Hor_Sealer_Position'], data['MC_Ver_Sealer_Position'],
                        data['MC_Rot_Valve_1_Position'], data['MC_Fill_Piston_1_Position'],
                        data['MC_Fill_Piston_2_Position'], data['MC_Rot_Valve_2_Position'],
                        data['MC_Web_Puller_Position'], data['MC_Sachet_Count'],
                        data['MC_CLD_Count'], data['MC_Status'], data['MC_Status_Code'],
                        data['MC_Cam_Position'], data['MC_Pulling_Servo_Current'],
                        data['MC_Pulling_Servo_Position'], data['MC_Hor_Pressure'],
                        data['MC_Ver_Pressure'], data['MC_Eye_Mark_Count'],
                        self.cycle_id
                    ))
                    self.conn.commit()
                    #print("Data successfully inserted into database",data)

            except psycopg2.InterfaceError as e:
                print(f"Database interface error: {e}. Will reconnect.")
                self.close_db_connection()
                time.sleep(1)
            except psycopg2.OperationalError as e:
                print(f"Database operational error: {e}. Will reconnect.")
                self.close_db_connection()
                time.sleep(5)
            except Exception as e:
                print(f"Error processing data: {e}")
                print(f"Data dump for debugging: {data}")
                # Don't exit on general errors, just continue
            
            time.sleep(0.03)

    def start(self):
        # Start data reading and flow check in separate threads
        read_thread = threading.Thread(target=self.read_and_insert, daemon=True)
        check_thread = threading.Thread(target=self.check_data_flow, daemon=True)
        read_thread.start()
        check_thread.start()
        
        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down...")
            self.close_db_connection()
            if self.plc:
                self.plc.close()
            sys.exit(0)

if __name__ == '__main__':
    reader = CycleTracker()
    reader.start()
