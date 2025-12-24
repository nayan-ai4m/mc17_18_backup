import asyncio
import json
import time
import nats
from pycomm3 import LogixDriver, CommError
import os
# Load the JSON configuration
with open('plc_tags_test.json') as f:
    PLC_TAGS = json.load(f)

class PLC:
    def __init__(self, ip):
        self.ip = ip
        self.driver = None
        self.connect()

    def connect(self):
        try:
            self.driver = LogixDriver(self.ip)
            self.driver.open()
            print(f"Connected to PLC {self.ip}")
        except Exception as e:
            print(f"Failed to connect to {self.ip}: {e}")
            self.driver = None

    def write(self, tag, value):
        if not self.driver:
            self.connect()
        
        if self.driver:
            try:
                self.driver.write(tag, value)
                print(self.driver.read(tag))
                print(f"Successfully wrote {value} to {tag} on PLC {self.ip}")
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
        self.plcs = {
            "17": PLC('141.141.141.128'),  # MC17
            "18": PLC('141.141.141.138')   # MC18
        }
        
        self.plc_topics = {
            "17": "adv.217",
            "18": "adv.217"
        }
        
    def get_tag_info(self, plc_id, name):
        if plc_id == "17":
            for item in PLC_TAGS.get("MC17_MC19_MC20_MC21_MC22", []):
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
            print("Request JSON:", req)
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
                if name.lower() not in ["start", "stop", "reset"]:
                    print("TOGGLE command can only be used with start/stop/reset")
                    return await msg.respond(json.dumps({
                        "error": "TOGGLE command can only be used with start/stop/reset names"
                    }).encode())
                
                plc.write(tag_info["tag"], True)
                print(f"Wrote True to {tag_info['tag']} on PLC {plc_id}")
                time.sleep(2)
                plc.write(tag_info["tag"], False)
                print(f"Wrote False to {tag_info['tag']} on PLC {plc_id}")
                
                return await msg.respond(json.dumps({
                    "plc": plc_id,
                    "ack": True,
                    "message": f"Toggled {name} successfully"
                }).encode())
            
            elif command.upper() == "UPDATE":
                value = req.get("value")
                if value is None:
                    print("Missing value for UPDATE command")
                    return await msg.respond(json.dumps({"error": "Missing value for UPDATE"}).encode())
                
                is_temp = name in ["HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28"]
                print(is_temp)
                write_tag = f"{tag_info['tag']}.SetValue" if is_temp else tag_info["tag"]
                
                print("{tag_info['tag']}.SetValue")
                success = plc.write(write_tag, value)
                if success:
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
            nc = await nats.connect("nats://192.168.1.149:4222")
            
            # Subscribe to the topic for machines 17 and 18
            sub = await nc.subscribe("adv.217", cb=self.message_handler)
            print("Subscribed to NATS topic: adv.217")
            
            print("Server listening for PLC commands (17/18)...")
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
