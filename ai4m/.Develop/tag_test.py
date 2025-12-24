from pycomm3 import LogixDriver

PLC_IP = "141.141.141.138"
TAGS = ["HMI_Hor_Seal_Rear_35", "HMI_Hor_Seal_Rear_36"]

def read_two_tags(plc_ip, tags):
    try:
        # slot 0 is most common, change to /1 if your PLC is different
        with LogixDriver(f'ethernet/ip/{plc_ip}/0') as plc:
            if not plc.connected:
                print(f"❌ Failed to connect to PLC at {plc_ip}")
                return None

            response = plc.read(*tags)
            for tag in response:
                print(f"{tag.tag}: {tag.value}")

    except Exception as e:
        print(f"⚠️ Error reading from PLC {plc_ip}: {str(e)}")

if __name__ == "__main__":
    read_two_tags(PLC_IP, TAGS)

