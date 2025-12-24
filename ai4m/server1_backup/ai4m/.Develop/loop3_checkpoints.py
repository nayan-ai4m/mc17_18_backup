from pycomm3 import LogixDriver
import psycopg2
import json
from datetime import datetime
import time
import sys
import os
from datetime import timezone, timedelta

# ===== Database connection params =====
DB_PARAMS = {
    'host': '192.168.1.149',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# ===== Define tags =====
mc_17_tags = [
    "Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage",
    "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2",
    "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5",
    "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8",
    "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11",
    "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14",
    "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17",
    "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20",
    "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23",
    "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26",
    "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1",
    "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2",
    "Horizontal_Sealing_Servo_Torque_Running","Vertical_Sealing_Servo_Torque_Running",
    "MC17_Hor_Torque", "MC17_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg",
    "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg",
    "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg",
    "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END",
    "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START",
    "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END",
    "HMI_HOZ_OPEN_START", "HMI_I_Start","HMI_I_Stop","HMI_I_Pos", "HMI_I_Reset",
    "HMI_Hopper_Low_Level", "HMI_Hopper_High_Level",
    "HMI_Hopper_Ex_Low_Level", "Hopper_Level_Percentage",
    "ROLL_END_SENSOR", "LEAPING_SENSOR", "HMI_Filling_Start_Deg",
    "HMI_Puller_Start_Deg","HMI_Puller_Stop_Deg","HMI_Puller_Pos_Deg", "Sealer_Clean",
    "HMI_Pulling_ON_OFF", "HMI_Filling_ON_OFF", "I_Filling_ON_OFF_SS",
    "STOP_STS","Horizontal_Sealing_Servo_Torque_Running","Vertical_Sealing_Servo_Torque_Running"
]
mc_18_tags = [
    "Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_1_Level_Percentage",
    "Hopper_2_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1",
    "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4",
    "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7",
    "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10",
    "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13",
    "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16",
    "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19",
    "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22",
    "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25",
    "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Rear_35", "HMI_Hor_Seal_Rear_36",
    "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1",
    "HMI_Ver_Sealer_Strk_2", "MC18_Hor_Torque", "MC18_Ver_Torque",
    "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg",
    "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg",
    "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree",
    "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START",
    "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END",
    "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START",
    "HMI_I_Start","HMI_I_Stop", "HMI_I_Pos", "HMI_I_Reset", "HMI_Hopper_1_Low_Level",
    "HMI_Hopper_1_High_Level", "HMI_Hopper_1_Ex_Low_Level",
    "Hopper_1_Level_Percentage", "HMI_Hopper_2_Low_Level",
    "HMI_Hopper_2_High_Level", "HMI_Hopper_2_Ex_Low_Level",
    "Hopper_2_Level_Percentage", "HMI_Filling_Start_Deg",
    "HMI_Suction_Start_Deg1", "HMI_Suction_End_Degree1",
    "HMI_Filling_Stroke_Deg1", "HMI_Filling_ON_OFF", "HMI_Pulling_ON_OFF",
    "I_Filling_ON_OFF_SS","HMI_Rot_Valve_Open_Start_Deg1","HMI_Rot_Valve_Open_End_Deg1",
    "HMI_Rot_Valve_Close_Start_Deg1","HMI_Rot_Valve_Close_End_Deg1",
    "HMI_Filling_Start_Deg1","HMI_Suction_Start_Deg1","HMI_Suction_End_Degree1","HMI_Filling_Stroke_Deg1",
    "HMI_Puller_Start_Deg","HMI_Puller_Stop_Deg"
]

# Add main tags for timestamps
if "MC17" not in mc_17_tags:
    mc_17_tags.append("MC17")
if "MC18" not in mc_18_tags:
    mc_18_tags.append("MC18")

def read_tags(plc_ip, mc_tags):
    try:
        with LogixDriver(plc_ip) as plc:
            if not plc.connected:
                print(f"Failed to connect to PLC at {plc_ip}")
                return None

            plc.socket_timeout = 5.0
            tag_data = {}
            response = plc.read(*mc_tags)

            for tag in response:
                tag_data[tag.tag] = tag.value

            return tag_data

    except Exception as e:
        print(f"Error reading from PLC {plc_ip}: {str(e)}")
        return None

# Connect to DB
db_connection = psycopg2.connect(**DB_PARAMS)

# Define IST timezone (UTC+05:30)
ist_timezone = timezone(timedelta(hours=5, minutes=30))

while True:
    try:
        results = {
            'mc17': read_tags('141.141.141.128', mc_17_tags),
            'mc18': read_tags('141.141.141.138', mc_18_tags),
        }

        if results['mc17'] is None and results['mc18'] is None:
            print("Both PLC reads failed, skipping insert")
            continue

        results['mc17'] = results['mc17'] if results['mc17'] is not None else {}
        results['mc18'] = results['mc18'] if results['mc18'] is not None else {}

        # Decide timestamp priority: mc17 → mc18 → now()
        plc_timestamp = None
        if 'MC17' in results['mc17'] and results['mc17']['MC17']:
            ts_data = results['mc17']['MC17']
            try:
                plc_timestamp = datetime(
                    year=ts_data['Year'],
                    month=ts_data['Month'],
                    day=ts_data['Day'],
                    hour=ts_data['Hour'],
                    minute=ts_data['Min'],
                    second=ts_data['Sec'],
                    microsecond=ts_data.get('Microsecond', 0),
                    tzinfo=ist_timezone
                )
            except (KeyError, ValueError) as e:
                print(f"Error parsing mc17 timestamp: {str(e)}")
        elif 'MC18' in results['mc18'] and results['mc18']['MC18']:
            ts_data = results['mc18']['MC18']
            try:
                plc_timestamp = datetime(
                    year=ts_data['Year'],
                    month=ts_data['Month'],
                    day=ts_data['Day'],
                    hour=ts_data['Hour'],
                    minute=ts_data['Min'],
                    second=ts_data['Sec'],
                    microsecond=ts_data.get('Microsecond', 0),
                    tzinfo=ist_timezone
                )
            except (KeyError, ValueError) as e:
                print(f"Error parsing mc18 timestamp: {str(e)}")

        # If PLC timestamp is not valid, fallback to current time
        if plc_timestamp is None:
            plc_timestamp = datetime.now(ist_timezone)
            print("Using current time as fallback timestamp")

        insert_query = """INSERT INTO public.loop3_checkpoints(
            "timestamp", mc17, mc18)
            VALUES (%s, %s, %s);"""

        with db_connection.cursor() as cur:
            cur.execute(insert_query, (
                plc_timestamp,
                json.dumps(results['mc17']),
                json.dumps(results['mc18']),
            ))
            db_connection.commit()
            print(f"Inserted row at {plc_timestamp}")

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(f"Error at {fname} line {exc_tb.tb_lineno}: {str(e)}")

        try:
            if db_connection.closed:
                db_connection = psycopg2.connect(**DB_PARAMS)
        except:
            print("Failed to reconnect to DB")

    time.sleep(5)
