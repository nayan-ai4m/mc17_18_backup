import asyncio
import json
import time
import nats
from pycomm3 import LogixDriver, CommError
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid
from datetime import datetime
import re

with open('config.json') as f:
    CONFIG = json.load(f)

with open('plc_tags.json') as f:
    PLC_TAGS = json.load(f)

class DatabaseManager:
    def __init__(self):
        self.connection_params = CONFIG['database']
        self.init_database()

    def get_connection(self):
        return psycopg2.connect(**self.connection_params)

    def init_database(self):
        try:
            conn = self.get_connection()
            conn.commit()
            print("Database initialized successfully")
        except Exception as e:
            print(f"Database initialization error: {e}")

    def insert_event(self, plc_id, tag_name, operation_type, previous_value=None, new_value=None):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            event_id = str(uuid.uuid4())
            timestamp = datetime.now()
            zone = "Control Panel"
            camera_id = f"MC{plc_id}"

            readable_name = self.get_readable_name_from_tag(tag_name)

            if operation_type == "TOGGLE":
                event_type = f"{readable_name} toggled"
            elif operation_type == "UPDATE" and previous_value is not None and new_value is not None:
                event_type = f"{readable_name} is changed from {previous_value} to {new_value}"
            else:
                event_type = f"{readable_name} is changed"

            alert_type = "Productivity"

            cursor.execute('''
                INSERT INTO event_table (timestamp, event_id, zone, camera_id, event_type, alert_type)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (timestamp, event_id, zone, camera_id, event_type, alert_type))

            conn.commit()
            conn.close()

            #print(f"Event logged: {event_type} for {camera_id} at {timestamp}")
            return True

        except psycopg2.IntegrityError as e:
            print(f"Database integrity error: {e}")
            return False
        except Exception as e:
            print(f"Database error: {e}")
            return False

    def get_readable_name_from_tag(self, tag_name):
        """Convert tag name like 'HMI_Rot_Valve_Open_Start_Deg' to 'Rotary Valve Open Start Degree'"""
        try:
            if tag_name.startswith('HMI_'):
                tag_name = tag_name[4:]

            prefix_mapping = {
                'Rot_': 'Rotary ',
                'Ver_': 'Vertical ',
                'VER_': 'Vertical ',
                'Hor_': 'Horizontal ',
                'HOZ_': 'Horizontal '
            }

            readable_name = tag_name
            for prefix, replacement in prefix_mapping.items():
                if tag_name.startswith(prefix):
                    readable_name = replacement + tag_name[len(prefix):]
                    break

            readable_name = readable_name.replace('_', ' ').title()

            readable_name = readable_name.replace('Deg', 'Degree')
            readable_name = readable_name.replace('Temp', 'Temperature')
            readable_name = readable_name.replace('Pos', 'Position')

            return readable_name
        except Exception as e:
            print(f"Error converting tag name to readable format: {e}")
            return tag_name

class PLC:
    def __init__(self, ip):
        self.ip = ip
        self.driver = None
        self.connect()

    def connect(self):
        try:
            self.driver = LogixDriver(self.ip)
            self.driver.open()
            #print(f"Connected to PLC {self.ip}")
        except Exception as e:
            print(f"Failed to connect to {self.ip}: {e}")
            self.driver = None

    def read(self, tag):
        if not self.driver:
            self.connect()

        if self.driver:
            try:
                result = self.driver.read(tag)
                return result.value if hasattr(result, 'value') else result
            except Exception as e:
                print(f"Read failed on {self.ip} for tag {tag}: {e}")
                return None
        return None

    def write(self, tag, value):
        if not self.driver:
            self.connect()

        if self.driver:
            try:
                self.driver.write(tag, value)
                print(self.driver.read(tag))
                #print(f"Successfully wrote {value} to {tag} on PLC {self.ip}")
                return True
            except CommError as e:
                print(f"Write failed on {self.ip}: {e}. Reconnecting...")
                self.connect()
                if self.driver:
                    try:
                        self.driver.write(tag, value)
                        return True
                    except Exception as e:
                        print(f"Retry failed on {self.ip}: {e}")
                        return False
            except Exception as e:
                print(f"Unexpected write error on {self.ip}: {e}")
                return False
        return False

class Server:
    def __init__(self):
        self.plcs = {plc_id: PLC(config['ip']) for plc_id, config in CONFIG['plcs'].items()}
        self.plc_topics = {plc_id: config['topic'] for plc_id, config in CONFIG['plcs'].items()}
        self.db_manager = DatabaseManager()

    def get_tag_info(self, plc_id, name):
        if plc_id == "17":
            for item in PLC_TAGS.get("MC17", []):
                if item["name"].lower() == name.lower():
                    return item
        elif plc_id == "18":
            for item in PLC_TAGS.get("MC18", []):
                if item["name"].lower() == name.lower():
                    return item
        return None

    async def message_handler(self, msg):
        try:
            req = json.loads(msg.data)
            #print("Request JSON:", req)
            plc_id = req.get("plc")
            plc = self.plcs.get(plc_id)

            if not plc:
                print(f"Invalid PLC ID: {plc_id}")
                return await msg.respond(json.dumps({"error": "Invalid PLC ID"}).encode())

            name = req.get("name")
            if not name:
                print("Missing name in request")
                return await msg.respond(json.dumps({"error": "Missing name"}).encode())

            command = req.get("command")
            if not command or command.upper() not in ["UPDATE", "TOGGLE"]:
                print("Invalid or missing command")
                return await msg.respond(json.dumps({"error": "Invalid command. Use UPDATE or TOGGLE"}).encode())

            tag_info = self.get_tag_info(plc_id, name)
            if not tag_info:
                print(f"Tag not found for name: {name}")
                return await msg.respond(json.dumps({"error": "Tag not found"}).encode())

            if tag_info["enable"] != 1:
                print(f"Tag {name} is not enabled for writing")
                return await msg.respond(json.dumps({"error": "Tag not enabled for writing"}).encode())

            if command.upper() == "TOGGLE":
                if name.lower() not in ["hmi_i_start","hmi_i_stop","hmi_i_reset"]:
                    print("TOGGLE command can only be used with start/stop/reset")
                    return await msg.respond(json.dumps({
                        "error": "TOGGLE command can only be used with start/stop/reset names"
                    }).encode())

                success1 = plc.write(tag_info["tag"], True)
                if success1:
                    #print(f"Wrote True to {tag_info['tag']} on PLC {plc_id}")
                    time.sleep(2)
                    success2 = plc.write(tag_info["tag"], False)
                    if success2:
                        #print(f"Wrote False to {tag_info['tag']} on PLC {plc_id}")

                        self.db_manager.insert_event(plc_id, tag_info["tag"], "TOGGLE")

                        return await msg.respond(json.dumps({
                            "plc": plc_id,
                            "ack": True,
                            "message": f"Toggled {name} successfully"
                        }).encode())
                    else:
                        return await msg.respond(json.dumps({
                            "error": f"Failed to complete toggle operation for {name}"
                        }).encode())
                else:
                    return await msg.respond(json.dumps({
                        "error": f"Failed to start toggle operation for {name}"
                    }).encode())

            elif command.upper() == "UPDATE":
                value = req.get("value")
                if value is None:
                    #print("Missing value for UPDATE command")
                    return await msg.respond(json.dumps({"error": "Missing value for UPDATE"}).encode())

                is_temp = name in ["HMI_I_Start","HMI_I_Stop","HMI_I_Reset","HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28"]
                #print(is_temp)
                write_tag = f"{tag_info['tag']}.SetValue" if is_temp else tag_info["tag"]

                previous_value = plc.read(write_tag)

                success = plc.write(write_tag, value)
                if success:
                    self.db_manager.insert_event(plc_id, tag_info["tag"], "UPDATE", previous_value, value)

                    return await msg.respond(json.dumps({
                        "plc": plc_id,
                        "ack": True,
                        "message": f"Updated {name} to {value}"
                    }).encode())
                else:
                    return await msg.respond(json.dumps({
                        "error": f"Failed to update {name}"
                    }).encode())
                is_temp = None

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            await msg.respond(json.dumps({"error": "Invalid JSON format"}).encode())
        except Exception as e:
            print(f"Error processing request: {e}")
            await msg.respond(json.dumps({"error": str(e)}).encode())

    async def run(self):
        try:
            nc = await nats.connect(CONFIG['nats']['server'])

            sub = await nc.subscribe(CONFIG['nats']['topic'], cb=self.message_handler)
            #print(f"Subscribed to NATS topic: {CONFIG['nats']['topic']}")

            #print("Server listening for PLC commands...")
            await asyncio.Event().wait()

        except OSError as e:
            print(f"Fatal NATS connection error: {e}")
            os._exit(1)

        except Exception as e:
            print(f"NATS connection error: {e}")
            os._exit(1)

        finally:
            if 'sub' in locals():
                await sub.unsubscribe()

if __name__ == "__main__":
    try:
        asyncio.run(Server().run())
    except KeyboardInterrupt:
        print("Server shutting down...")
    except Exception as e:
        print(f"Unexpected error: {e}")
