import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime,timedelta
import time
import math

# กำหนดข้อมูล PostgreSQL
db_user = 'postgres'
db_passwd = 'Automation01'
db_host = '100.82.151.125'
db_name = 'ISO-29110-DB'
db_port = '5432'

# ทำการเชื่อมต่อ
con = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_passwd
)

def calculate_duration_time(end_time, start_time):
    batch_duration = end_time - start_time

    # Extract total seconds from the timedelta object
    total_seconds = batch_duration.total_seconds()

    # Calculate hours, minutes, and seconds
    result_hours, remainder = divmod(total_seconds, 3600)
    result_minutes, result_seconds = divmod(remainder, 60)

    # Convert to integers
    result_hours = int(result_hours)
    result_minutes = int(result_minutes)
    result_seconds = int(result_seconds)

    # Format the result as a string
    batch_duration_result = f"{result_hours:02d}:{result_minutes:02d}:{result_seconds:02d}"

    return batch_duration_result

def insert_report_table(datetime_now, report_createby, device_id, report_volume, batch_duration, batch_id):
    insert_report = ("INSERT INTO device_report_table (device_report_type,device_report_create_at,device_report_create_by,device_report_status,device_id,device_report_volume,device_report_duration,device_batch_id ) "
                     "VALUES ('Batch report', %s, %s, true, %s, %s, %s, %s)")
    with con.cursor() as cursor:
        cursor.execute(insert_report,(datetime_now, report_createby, device_id, report_volume, batch_duration, batch_id))
        con.commit()

def update_report_status(report_status,device_batch_id):
    update_report_status = ("UPDATE device_batch_table SET device_report_status = %s WHERE device_batch_id = %s")
    with con.cursor() as cursor:
        cursor.execute(update_report_status,(report_status,device_batch_id))
        con.commit()

while True:
    try:
        print("------------------------ START ------------------------")
        datetime_now = datetime.now()
        batch_time_get = None
        query_batch = ("SELECT device_batch_id, device_batch_start_time, device_batch_end_time, device_batch_value, device_id, device_report_status "
                    "FROM device_batch_table ORDER BY device_batch_id DESC LIMIT 1"
                    )
        print("Log : select from device_batch_table")

        with con.cursor() as cursor:
            cursor.execute(query_batch)
            batch_data = cursor.fetchall()
            con.commit()

        for row in batch_data:
            batch_id = row[0]
            end_time = row[2]
            report_status = row[5]
            if end_time == None:
                print(f"Log : Batch_end_time is Null")
                if report_status==False:
                    print("Log : Batch in WORKING, do not REPORT.... wait for batch stop")
                    pass    
                elif report_status==True:
                    print("Log : Batch in WORKING, but REPORT_STATUS is TRUE")
                else:
                    raise Exception("device_report_status is NUll !!!!")
            else:
                print(f"Log : Batch_end_time is not Null")
                if report_status==False:
                    start_time = row[1]
                    batch_value = row[3]
                    device_id = row[4]
                    print("Log : Batch_duration CALCULATING")
                    batch_duration = calculate_duration_time(end_time, start_time)
                    print("Log : INSERTING report to REPORT_TABLE")
                    insert_report_table(datetime_now, batch_id, device_id, batch_value, batch_duration, batch_id)  
                    print("Log : UPDATING report status in BATCH_TABLE")
                    update_report_status(True, batch_id)
                elif report_status==True:
                    print("Log : Batch is STOP")
                    pass
                else:
                    raise Exception("device_report_status is NUll !!!!")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    except KeyboardInterrupt:
        print(f"Log : STOP WORKING")
    print("------------------------ END --------------------------")

    time.sleep(1.5)
    



