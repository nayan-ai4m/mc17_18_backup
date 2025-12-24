from pycomm3 import LogixDriver

PLC_IP = '141.141.141.128'
TAG_NAME = 'MC17_DC_NOTIFICATION'   # Boolean tag

with LogixDriver(PLC_IP) as plc:

    # -------------------------------
    # Step 1: Write True
    # -------------------------------
    print("Writing True to the tag...")
    plc.write((TAG_NAME, True))

    # Confirm
    read_true = plc.read(TAG_NAME).value
    print(f"Confirmation after writing True: {read_true}")

    # -------------------------------
    # Step 2: Write False
    # -------------------------------
   # print("Writing False to the tag...")
    plc.write((TAG_NAME, False))

    # Confirm
    read_false = plc.read(TAG_NAME).value
    print(f"Confirmation after writing False: {read_false}")

print("Boolean write+confirm process completed.")

