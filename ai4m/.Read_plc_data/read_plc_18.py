import datetime
from pycomm3 import LogixDriver
import psycopg2
import time
import threading
import sys
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

class CycleTracker:
    def __init__(self):
        # PLC configuration
        self.plc_ip = '141.141.141.138'  # MC18 IP
        
        # Database configuration
        self.insert_query = """
        INSERT INTO mc18(
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

        # Kafka configuration
        self.kafka_topic = 'l3_stoppage_code'
        self.kafka_bootstrap_servers = '192.168.1.149:9092'  # Update with your Kafka server address

        # Application state
        self.plc = None
        self.cycle_id = 1
        self.last_cam_position = None
        self.current_day = None
        self.last_message_time = time.time()
        self.timeout = 30
        self.producer = None
        self.status_code_queue = []
        self.queue_lock = threading.Lock()

        # Initialize connections
        self.connect_all()

    def connect_all(self):
        """Establish all required connections with continuous retry"""
        while True:
            try:
                self.connect_plc()
                self.connect_db()
                self.connect_kafka()
                return  # All connections successful
            except Exception as e:
                print(f"Connection error: {e}. Retrying all connections in 5 seconds...")
                time.sleep(5)

    def connect_db(self):
        """Connect to PostgreSQL database with continuous retry"""
        while True:
            try:
                self.close_db_connection()  # Clean up any existing connection
                self.conn = psycopg2.connect(**self.db_settings)
                self.conn.autocommit = False
                self.cursor = self.conn.cursor()
                #print("Successfully connected to PostgreSQL database")
                return
            except Exception as e:
                print(f"Database connection error: {e}. Retrying in 5 seconds...")
                time.sleep(5)

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
        """Connect to the PLC using LogixDriver with continuous retry"""
        while True:
            try:
                if self.plc:
                    self.plc.close()
                self.plc = LogixDriver(self.plc_ip)
                self.plc.open()
                #print(f"Successfully connected to PLC at {self.plc_ip}")
                return
            except Exception as e:
                print(f"Failed to connect to PLC at {self.plc_ip}: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def connect_kafka(self):
        """Connect to Kafka with continuous retry"""
        while True:
            try:
                if self.producer:
                    self.producer.close()
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5
                )
                #print(f"Successfully connected to Kafka at {self.kafka_bootstrap_servers}")
                return
            except Exception as e:
                print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def reconnect(self):
        """Reconnect to all services"""
        print("Attempting to reconnect to all services...")
        self.connect_all()
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
        try:
            if self.conn is None or self.conn.closed != 0 or self.cursor is None or self.cursor.closed:
                print("Database connection lost. Reconnecting...")
                self.connect_db()
            return True
        except Exception as e:
            print(f"Error checking database connection: {e}")
            return False

    def ensure_kafka_connection(self):
        """Ensure we have a valid Kafka connection"""
        try:
            if self.producer is None:
                print("Kafka connection lost. Reconnecting...")
                self.connect_kafka()
            return True
        except Exception as e:
            print(f"Error checking Kafka connection: {e}")
            return False

    def read_plc_data(self):
        """Read data from PLC and add status code to queue if available"""
        while True:
            try:
                if not self.plc or not self.plc.connected:
                    print("PLC not connected, reconnecting...")
                    self.connect_plc()
                
                tag_value = self.plc.read("MC18")
                if not tag_value:
                    print("No data read from PLC. Retrying.")
                    time.sleep(1)
                    continue
                
                data = tag_value.value
                self.last_message_time = time.time()

                # Add status code to queue for Kafka producer
                if 'MC_Status_Code' in data:
                    with self.queue_lock:
                        self.status_code_queue.append(data['MC_Status_Code'])

                return data

            except Exception as e:
                print(f"Error reading from PLC: {e}. Attempting to reconnect.")
                self.connect_plc()
                time.sleep(1)

    def process_data_to_db(self):
        """Process data and insert into PostgreSQL"""
        while True:
            data = self.read_plc_data()

            try:
                if not self.ensure_db_connection():
                    print("Failed to establish database connection. Will retry.")
                    time.sleep(1)
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
                    #print("Data inserted sucessfully:",data)

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
                #print(f"Data dump for debugging: {data}")
            
            time.sleep(0.03)

    def produce_kafka_messages(self):
        while True:
            try:
                if not self.ensure_kafka_connection():
                    continue
                message = None
                with self.queue_lock:
                    if self.status_code_queue:
                        message = {"mc18": self.status_code_queue.pop(0)}
                if message:
                    try:
                        self.producer.send(
                            self.kafka_topic, 
                            value=message,
                        ).add_errback(lambda e: self._handle_kafka_error(e, message))
                    except Exception as e:
                        self._handle_kafka_error(e, message)
                else:
                    time.sleep(0.001)  # Minimal sleep

            except Exception as e:
                print(f"Unexpected error: {e}")
                time.sleep(1)

    def _handle_kafka_error(self, error, message):
        with self.queue_lock:
            self.status_code_queue.insert(0, message['mc18'])
            print(f"Message failed: {error}")
            self.connect_kafka()  


    def start(self):
        # Start all threads
        threads = [
            threading.Thread(target=self.process_data_to_db, daemon=True),
            threading.Thread(target=self.produce_kafka_messages, daemon=True),
            threading.Thread(target=self.check_data_flow, daemon=True)
        ]
        
        for thread in threads:
            thread.start()
        
        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down...")
            self.close_db_connection()
            if self.plc:
                self.plc.close()
            if self.producer:
                self.producer.close()
            sys.exit(0)

if __name__ == '__main__':
    tracker = CycleTracker()
    tracker.start()
