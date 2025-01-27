import warnings
warnings.filterwarnings("ignore", category=UserWarning)
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
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
batch_start_count = 0
batch_stop_count = 0


while True:
    try:
        # ใช้ pandas เพื่อดึงข้อมูลจาก PostgreSQL table
        query = f"SELECT * FROM device_raw_data INNER JOIN device_table ON device_table.device_id = device_raw_data.device_id order by raw_id desc limit 1"
        df = pd.read_sql_query(query, con)
        check_status_query = f"SELECT * FROM device_batch_table order by device_batch_id desc limit 1"
        df_check = pd.read_sql_query(check_status_query, con)     

        batch_status = df.at[0, 'device_batch_status']
        batch_status_check = df_check.at[0, 'device_batch_value_status']

        now_datetime = df.at[0, 'device_timestamp']
        now_batch_temp = 0
        now_status = False
        end_datetime = df.at[0, 'device_timestamp']
        end_volume = df.at[0,'device_tank_volume']
        batch_now_id = df_check.at[0, 'device_batch_id']

#--------------------------คำนวณ batch_value--------------------------------
        #batch value real-time = start_volume - real_time_volume *
        #batch value end batch = start_volume - end_volume *ดึงค่ามาคำนวณเหมือนกัน
        start_volume = df_check.at[0, 'device_batch_start_volume']
        now_batch_volume =  df.at[0,'device_tank_volume']
        now_batch_value = start_volume - now_batch_volume
#----------------------------------------------------------------------------
        #ตรวจสอบ batch_value ว่ามีค่าต่ำกว่า 0 ไหม
        if now_batch_value < 0:
            now_batch_value = 0
            now_batch_value = round(now_batch_value,2)
        else:
            now_batch_value = round(now_batch_value,2)
#----------------------------------------------------------------------------
        print("---------- START BATCH ----------")
        # ตรวจสอบการเปลี่ยนแปลงของ batch_status
        if batch_status_check == True:
            if batch_status == True: #update real time data
                batch_start_count += 1

                query_update = (
                    f"UPDATE device_batch_table "
                    f"SET device_batch_datetime = '{now_datetime}', "
                    f"    device_batch_value = {now_batch_value}, "
                    f"    device_batch_temp = {now_batch_temp} "
                    f"WHERE device_batch_id = {batch_now_id};"
                )
                #connect cursor และ execute คำสั่ง SQL
                with con.cursor() as cursor:
                    cursor.execute(query_update)
                con.commit()  # Commit the changes to the database
                print(f"Log : Update batchID = {batch_now_id}  complete")
            else: #update end batch data and end this batch
                query_update = (
                    f"UPDATE device_batch_table "
                    f"SET device_batch_datetime = '{now_datetime}', "
                    f"    device_batch_value = {now_batch_value}, "
                    f"    device_batch_temp = {now_batch_temp}, "
                    f"    device_batch_end_time = '{end_datetime}', "
                    f"    device_batch_end_volume = {end_volume}, "
                    f"    device_batch_value_status = {now_status} "
                    f"WHERE device_batch_id = {batch_now_id};"
                )
                #connect cursor และ execute คำสั่ง SQL
                with con.cursor() as cursor:
                    cursor.execute(query_update)
                con.commit()  # Commit the changes to the database
                print(f"Log : End batch with this ID: {batch_now_id}")

        elif batch_status_check == False:
            if batch_status == True:
                batch_start_count += 1
                insert_batch = (
                    f"INSERT INTO device_batch_table "
                    f"(device_batch_datetime,device_batch_value,device_batch_value_status,device_batch_start_time,device_batch_temp,device_batch_start_volume,device_id,device_report_status) "
                    f"VALUES ('{now_datetime}',0,{batch_status},'{now_datetime}',0,{end_volume},1,false)"
                )
                #connect cursor และ execute คำสั่ง SQL
                with con.cursor() as cursor:
                    cursor.execute(insert_batch)
                    con.commit()  # Commit the changes to the database
                print(f"Log : Create complete.")
            else:
                pass
        else:
            pass

        print(f"Log : Now batch status is {batch_status}")
        print("---------- END BATCH ----------")
        time.sleep(5)
    except KeyboardInterrupt:
        break

con.close()


