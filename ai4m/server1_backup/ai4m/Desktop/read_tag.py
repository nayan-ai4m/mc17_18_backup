from pycomm3 import LogixDriver
import time

# PLC IP address
plc_ip = '141.141.141.128'

# Initialize PLC connection
with LogixDriver(plc_ip) as plc:
    total_time = 0  # Initialize total time tracker
    
    # Start the timer
    start_time = time.time()
    
    # Retrieve all tags from the PLC
    try:
        print(f"Connecting to PLC at {plc_ip}...")
        tags = plc.tags  # Retrieve all tags

        print(f"Found {len(tags)} tags in the PLC:")
        for tag_name, tag_info in tags.items():
            try:
                # Read each tag value
                value = plc.read(tag_name)
                print(f"Tag: {tag_name}, Value: {value.value}, Type: {tag_info['data_type']} \n ")
            except Exception as e:
                print(f"Error reading tag {tag_name}: {e}")
    except Exception as e:
        print(f"Error retrieving tags: {e}")
    
    # Stop the timer
    total_time = time.time() - start_time
    print(f"\nOperation completed in {total_time:.2f} seconds")


