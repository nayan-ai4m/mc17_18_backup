import asyncio
import nats
import json
import os
from dataclasses import asdict, dataclass
from pycomm3 import LogixDriver
import time


@dataclass
class Response:
    plc: str
    command:str
    ack:bool

@dataclass
class Request:
    plc:str
    tag:str
    value:float
    status:bool
    command:str

plc_17 = LogixDriver('141.141.141.128')
plc_17.open()
plc_18 = LogixDriver('141.141.141.138')
plc_18.open()

async def run():
    # Connect to the NATS server
    nc = await nats.connect("nats://192.168.1.149:4222")

    # Define the request handler
    async def message_handler(msg):
        request = Request(**json.loads(msg.data))
        print(request)
        if request.plc == "17":
            if request.command == "UPDATE_TEMP":
                tag = request.tag
                value = request.value
                print(tag+".SetValue",value)
                plc_17.write(tag+".SetValue",value)
                print("write for update on 17")

            if request.command == "UPDATE":
                tag = request.tag
                value = request.value
                print(tag,value)
                plc_17.write(tag,value)
                print("write for update on 17")


            if request.command == "TOGGLE":
                tag = request.tag
                value = request.value
                print(tag)
                plc_17.write(tag,True)
                print("written true")
                time.sleep(2)
                plc_17.write(tag,False)
                print("written false")
                



        if request.plc == "18":
            if request.command == "UPDATE_TEMP":
                tag = request.tag
                value = request.value
                print(tag+".SetValue",value)
                plc_18.write(tag+".SetValue",value)
                print("write of update on 18")

            if request.command == "UPDATE":
                tag = request.tag
                value = request.value
                print(tag,value)
                plc_18.write(tag,value)
                print("write for update on 18")

            if request.command == "TOGGLE":
                tag = request.tag
                value = request.value
                print(tag)
                plc_18.write(tag,True)
                print("written true")
                time.sleep(2)
                plc_18.write(tag,False)
                print("written false")

        payload = Response(plc=request.plc, command=request.command,ack=True)
        bytes_ = json.dumps(asdict(payload)).encode()
        await msg.respond(bytes_)

    # Subscribe to a subject and handle requests
    await nc.subscribe("plc.217", cb=message_handler)

    print("Server is listening for requests on 'greeting'...")
    
    # Keep the server running
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(run())
