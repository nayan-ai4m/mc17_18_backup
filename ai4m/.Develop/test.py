import datetime
import json
import time
import threading
import sys
from pycomm3 import LogixDriver
import psycopg2
from kafka import KafkaProducer
from kafka.errors import KafkaError

class CycleTracker:
    def __init__(self):
        # PLC configuration
        self.plc_ip = '141.141.141.128'  # MC17 IP
        
        # Load JSON configurations
        try:
            with open('high_speed.json', 'r') as f:
                high_speed_config = json.load(f)
                self.high_speed_tags = high_speed_config['tags']
        except FileNotFoundError:
            print("Error: high_speed.json not found.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error decoding high_speed.json: {e}")
            sys.exit(1)

        try:
            with open('low_speed.json', 'r') as f:
                low_speed_config = json.load(f)
                self.low_speed_tags = low_speed_config['tags']
        except FileNotFoundError:
            print("Error: low_speed.json not found.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error decoding low_speed.json: {e}")
            sys.exit(1)

        # Generate dynamic SQL queries
        high_speed_columns = ['"timestamp"'] + [tag.lower().replace('mc_', '') for tag in self.high_speed_tags] + ['spare1']
        self.high_speed_query = f"""
        INSERT INTO mc17 ({', '.join(high_speed_columns)})
        VALUES ({', '.join(['%s'] * len(high_speed_columns))});
        """

        low_speed_columns = ['"timestamp"'] + [tag.lower().replace('mc_', '') for tag in self.low_speed_tags] + ['spare1']
        self.low_speed_query = f"""
        INSERT INTO mc17_mid ({', '.join(low_speed_columns)})
        VALUES ({', '.join(['%s'] * len(low_speed_columns))});
        """

        self.db_settings = {
            'host': '100.64.84.30',
            'database': 'hul',
            'user': 'postgres',
            'password': 'ai4m2024'
        }

        # Kafka configuration
        self.kafka_topic = 'l3_stoppage_code'
        self.kafka_bootstrap_servers = '192.168.1.149:9092'

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
        self.last_low_speed_insert = 0
        self.low_speed_interval = 5  # 5 seconds for low-speed data
        self.low_speed_data_buffer = None

        # Initialize connections
        self.connect_all()

    def connect_all(self):
        """Establish all required connections with continuous retry"""
        while True:
            try:
                self.connect_plc()
                self.connect_db()
                self.connect_kafka()
                return
            except Exception as e:
                print(f"Connection error: {e}. Retrying all connections in 5 seconds...")
                time.sleep(5)

    def connect_db(self):
        """Connect to PostgreSQL database with continuous retry"""
        while True:
            try:
                self.close_db_connection()
                self.conn = psycopg2.connect(**self.db_settings)
                self.conn.autocommit = False
                self.cursor = self.conn.cursor()
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
                
                tag_value = self.plc.read("MC17")
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
        """Process data and insert into PostgreSQL tables"""
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

                # High-speed data (30ms) to mc17
                with self.conn.cursor() as cursor:
                    high_speed_values = [str(timestamp)] + [data[tag] for tag in self.high_speed_tags] + [self.cycle_id]
                    cursor.execute(self.high_speed_query, high_speed_values)
                    self.conn.commit()

                # Low-speed data (5s) to mc17_mid
                current_time = time.time()
                self.low_speed_data_buffer = data
                if current_time - self.last_low_speed_insert >= self.low_speed_interval:
                    with self.conn.cursor() as cursor:
                        low_speed_values = [str(timestamp)] + [data[tag] for tag in self.low_speed_tags] + [self.cycle_id]
                        cursor.execute(self.low_speed_query, low_speed_values)
                        self.conn.commit()
                        self.last_low_speed_insert = current_time

            except psycopg2.InterfaceError as e:
                print(f"Database interface error: {e}. Will reconnect.")
                self.close_db_connection()
                time.sleep(1)
            except psycopg2.OperationalError as e:
                print(f"Database operational error: {e}. Will reconnect.")
                self.close_db_connection()
                time.sleep(5)
            except KeyError as e:
                print(f"Key error: Tag {e} not found in PLC data.")
                time.sleep(1)
            except Exception as e:
                print(f"Error processing data: {e}")
            
            time.sleep(0.03)

    def produce_kafka_messages(self):
        while True:
            try:
                if not self.ensure_kafka_connection():
                    continue
                message = None
                with self.queue_lock:
                    if self.status_code_queue:
                        message = {"mc17": self.status_code_queue.pop(0)}
                if message:
                    try:
                        self.producer.send(
                            self.kafka_topic, 
                            value=message,
                        ).add_errback(lambda e: self._handle_kafka_error(e, message))
                    except Exception as e:
                        self._handle_kafka_error(e, message)
                else:
                    time.sleep(0.001)
            except Exception as e:
                print(f"Unexpected error: {e}")
                time.sleep(1)

    def _handle_kafka_error(self, error, message):
        with self.queue_lock:
            self.status_code_queue.insert(0, message['mc17'])
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
