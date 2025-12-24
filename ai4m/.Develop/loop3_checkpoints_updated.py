from pycomm3 import LogixDriver
import psycopg2
import json
from datetime import datetime
import time
import sys
import os

# Define the PLC IP address and database connection details
DB_PARAMS = {
    'host': '192.168.1.149',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Define tags for MC17 and MC18
mc_17_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC17_Hor_Torque", "MC17_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START","HMI_I_Start","HMI_I_Stop","HMI_I_Reset","HMI_Hopper_Low_Level","HMI_Hopper_High_Level","HMI_Hopper_Ex_Low_Level","Hopper_Level_Percentage","ROLL_END_SENSOR","LEAPING_SENSOR"]
mc_18_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_1_Level_Percentage", "Hopper_2_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Rear_35", "HMI_Hor_Seal_Rear_36", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC18_Hor_Torque", "MC18_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START","HMI_I_Start","HMI_I_Stop","HMI_I_Reset","HMI_Hopper_1_Low_Level","HMI_Hopper_1_High_Level","HMI_Hopper_1_Ex_Low_Level","Hopper_1_Level_Percentage","HMI_Hopper_2_Low_Level","HMI_Hopper_2_High_Level","HMI_Hopper_2_Ex_Low_Level","Hopper_2_Level_Percentage"]


# Function to read and insert tags into PostgreSQL
def read_tags(plc_ip, mc_tags):
    try:
        with LogixDriver(plc_ip) as plc:
            if not plc.connected:
                print(f"Failed to connect to PLC at {plc_ip}")
                return None

            plc.socket_timeout = 5.0  # 5 seconds timeout
            
            tag_data = {}
            response = plc.read(*mc_tags)
            for tags in response:
                tag_data[tags.tag] = tags.value
                print(f"\n {tags.tag}:{tags.value} ")
            return tag_data
            
    except Exception as e:
        print(f"Error reading from PLC {plc_ip}: {str(e)}")
        return None

# Run the function for each tag list and insert into corresponding table
db_connection = psycopg2.connect(**DB_PARAMS)

while True:
    try:
        results = {
            'mc17': read_tags('141.141.141.128', mc_17_tags),
            'mc18': read_tags('141.141.141.138', mc_18_tags),
        }

        # If both PLC reads fail, skip insertion
        if results['mc17'] is None and results['mc18'] is None:
            print("Both PLC reads failed, skipping database insert")
            continue

        # Replace failed reads with an empty JSON
        results['mc17'] = results['mc17'] if results['mc17'] is not None else {}
        results['mc18'] = results['mc18'] if results['mc18'] is not None else {}
        
        print(results['mc18'])

        insert_query = """INSERT INTO public.loop3_checkpoints(
            "timestamp", mc17, mc18)
            VALUES (%s, %s, %s);"""

        with db_connection.cursor() as cur:
            cur.execute(insert_query, (
                str(datetime.now()),
                json.dumps(results['mc17']),
                json.dumps(results['mc18']),
            ))
            db_connection.commit()
            print("Data inserted successfully")

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(f"Error at {fname} line {exc_tb.tb_lineno}: {str(e)}")

        # Attempt to reconnect to database if connection is lost
        try:
            if db_connection.closed:
                db_connection = psycopg2.connect(**DB_PARAMS)
        except:
            print("Failed to reconnect to database")

    time.sleep(5)

