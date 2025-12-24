import asyncio
import json
import time
import nats
from pycomm3 import LogixDriver, CommError

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
                print(f"Successfully wrote {value} to {tag} on PLC {self.ip}")
            except CommError as e:
                print(f"Write failed on {self.ip}: {e}. Reconnecting...")
                self.connect()
                if self.driver:
                    try:
                        self.driver.write(tag, value)
                    except Exception as e:
                        print(f"Retry failed on {self.ip}: {e}")

class Server:
    def __init__(self):
        self.plcs = {"17": PLC('141.141.141.128'), "18": PLC('141.141.141.138')}

    async def message_handler(self, msg):
        try:
            req = json.loads(msg.data)
            print("Request JSON:", req)
            plc = self.plcs.get(req.get("plc"))
            
            if plc:
                tag, value = req.get("tag"), req.get("value")
                command = req.get("command")
                
                if command == "TOGGLE":
                    plc.write(tag, True)
                    print(f"Wrote True to {tag} on PLC {req['plc']}")
                    time.sleep(2)
                    plc.write(tag, False)
                    print(f"Wrote False to {tag} on PLC {req['plc']}")
                else:
                    plc.write(f"{tag}.SetValue" if command == "UPDATE_TEMP" else tag, value)
                    print(f"Wrote {value} to {tag} on PLC {req['plc']}")
                
                await msg.respond(json.dumps({"plc": req["plc"], "ack": True}).encode())
            else:
                print(f"Invalid PLC ID: {req.get('plc')}")
                await msg.respond(json.dumps({"error": "Invalid PLC ID"}).encode())
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            await msg.respond(json.dumps({"error": "Invalid JSON format"}).encode())
        except Exception as e:
            print(f"Error processing request: {e}")
            await msg.respond(json.dumps({"error": str(e)}).encode())

    async def run(self):
        try:
            nc = await nats.connect("nats://192.168.1.149:4222")
            await nc.subscribe("plc.217", cb=self.message_handler)
            print("Server listening...")
            await asyncio.Event().wait()
        except Exception as e:
            print(f"NATS connection error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(Server().run())
    except KeyboardInterrupt:
        print("Server shutting down...")
    except Exception as e:
        print(f"Unexpected error: {e}")

