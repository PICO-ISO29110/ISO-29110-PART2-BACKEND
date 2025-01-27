import warnings
warnings.filterwarnings("ignore", category=UserWarning)
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime, timedelta
import requests

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

# -------------------------------- FUNCTION CODE AREA ---------------------------------------------------------------
# ---------------------------- Notification Function ------------------------------------
def send_line_notification(message, token):
    url = "https://notify-api.line.me/api/notify"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"message": message}

    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200:
        print("Log : Notification sent successfully.")
    else:
        print(f"Log : Failed to send notification. Status code: {response.status_code}.")
#---------------------------------------------------------------------------------------

# ------------------------------- volume record high-low function --------------------------------
def volume_high_record(device_volume,volume_high,volume_low,alarm_Message_High,alarm_ID,alarm_createby):
    # ตัวอย่าง SQL query ในการ insert ข้อมูลเข้า device_alarm_table
    volume_query = (
        f"INSERT INTO device_alarm_volume_record_table (volume_alarm_record,volume_alarm_volume_high_set,volume_alarm_volume_low_set,device_alarm_message,device_alarm_id,device_alarm_record_timestamp,alarm_createby)" 
        f"VALUES ({device_volume},{volume_high},{volume_low},'{alarm_Message_High}',{alarm_ID},'NOW()',{alarm_createby})"
    )
    with con.cursor() as cursor:
        cursor.execute(volume_query)
    con.commit()  # Commit the changes to the database
    print(f"Log : Volume too high recorded.")
def volume_low_record(device_volume,volume_high,volume_low,alarm_Message_Low,alarm_ID, alarm_createby):
    # ตัวอย่าง SQL query ในการ insert ข้อมูลเข้า device_alarm_table
    volume_query = (
    f"INSERT INTO device_alarm_volume_record_table (volume_alarm_record,volume_alarm_volume_high_set,volume_alarm_volume_low_set,device_alarm_message,device_alarm_id,device_alarm_record_timestamp,alarm_createby)" 
    f"VALUES ({device_volume},{volume_high},{volume_low},'{alarm_Message_Low}',{alarm_ID},'NOW()',{alarm_createby})"
    )
    with con.cursor() as cursor:
            cursor.execute(volume_query)
    con.commit()  # Commit the changes to the database
    print(f"Log : volume too low recorded.")
#-------------------------------------------------------------------------------------------------
    
# ------------------------------- level record high-low function --------------------------------
def level_high_record(device_level,level_alarm_set_high,level_alarm_set_low,alarm_Message_High,alarm_ID,alarm_createby):
    # ตัวอย่าง SQL query ในการ insert ข้อมูลเข้า device_alarm_table
    level_query = (
        f"INSERT INTO device_alarm_level_record_table (level_alarm_record, level_high_alarm_set, level_low_alarm_set, device_alarm_message, device_alarm_id, device_level_record_timestamp, alarm_createby)" 
        f"VALUES ({device_level},{level_alarm_set_high}, {level_alarm_set_low},'{alarm_Message_High}',{alarm_ID}, NOW(), {alarm_createby})"
    )
    with con.cursor() as cursor:
        cursor.execute(level_query)
    con.commit()  # Commit the changes to the database
    print(f"Log : Level too high recorded.")
def level_low_record(device_level,level_alarm_set_high,level_alarm_set_low,alarm_Message_Low,alarm_ID,alarm_createby):
    # ตัวอย่าง SQL query ในการ insert ข้อมูลเข้า device_alarm_table
    level_query = (
        f"INSERT INTO device_alarm_level_record_table (level_alarm_record, level_high_alarm_set, level_low_alarm_set, device_alarm_message, device_alarm_id, device_level_record_timestamp, alarm_createby)" 
        f"VALUES ({device_level},{level_alarm_set_high}, {level_alarm_set_low},'{alarm_Message_Low}',{alarm_ID}), NOW(), {alarm_createby}" 
    )
    with con.cursor() as cursor:
            cursor.execute(level_query)
    con.commit()  # Commit the changes to the database
    print(f"Log : Level too low recorded.")
#-------------------------------------------------------------------------------------------------

#------------------------------ record batchToolong fuction --------------------------------------
def batch_toolong_record(batch_value_set, batch_value, batch_msg, alarmID, batchID, alarm_createby):
    batch_query = (
        f"INSERT INTO device_alarm_batchtolong_record (alarm_batch_record_value_set, alarm_batch_value_record, alarm_batch_record_timestamp, alarm_batch_record_message, device_alarm_id, device_batch_id, alarm_createby)"
        f"VALUES ('{batch_value_set}', '{batch_value}', 'Now()', '{batch_msg}', {alarmID}, {batchID}, {alarm_createby})"
    )
    with con.cursor() as cursor:
        cursor.execute(batch_query)
    con.commit()  # Commit the changes to the database
    print(f"Log : Batch working too long.")

# ฟังก์ชันเสริม เปลี่ยน time เป็น str    
def timestamp_convertTo_str(result_time):
    # กำหนดค่าเริ่มต้นของ result_day
    # แปลง result_time เป็น string โดยให้แสดงเฉพาะชั่วโมงและนาที
    result_hours = (result_time.days * 24) + (result_time.seconds // 3600) # วันเป็นชั่วโมง + วินาทีเป็นชั่วโมง ไม่เอาเศษ
    result_minutes = (result_time.seconds % 3600) // 60 #เศษของวินาทีที่เหลือ แปลงเป็นนาที ด้วยการหาร 60
    result_time_str = f"{result_hours:02d}:{result_minutes:02d}"
    return result_time_str

# -------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------
first_process = []
batch_next_check = datetime.now()
volume_next_check = datetime.now()
level_next_check = datetime.now()

try:
    while True:
        DateTimeNow = datetime.now()
    # -------------------- SQL query เพื่อดึงข้อมูลจากตาราง device_alarm_table โดยเลือกเฉพาะแถวที่อยู่ในเวลาทำการ และสถานะ true -------------
        query_check_status = """
            SELECT * FROM device_alarm_table
            WHERE device_check_start_time <= NOW() AND NOW() <= device_check_stop_time
               OR device_alarm_status = True;
        """
        df = pd.read_sql(query_check_status, con)
    #------------------------------------------------------------------------------------------------------------------------------

        # ตั้งค่ารูปแบบของเวลา
        time_format = '%Y-%m-%d %H:%M:%S'

         #แปลงค่าในคอลัมน์ 'device_check_start_time' และ 'device_check_stop_time' เป็น datetime64
        df['device_check_start_time'] = pd.to_datetime(df['device_check_start_time'], format=time_format)
        df['device_check_stop_time'] = pd.to_datetime(df['device_check_stop_time'], format=time_format)


        # ดึงเวลาปัจจุบัน
        current_time = pd.to_datetime('now') + pd.Timedelta(hours=7)
        print("----------- START-LOOP -----------" ,current_time)
        # ทำการตรวจสอบและอัปเดตค่าใน DataFrame
        for index, row in df.iterrows():
            start_time = row['device_check_start_time']
            stop_time = row['device_check_stop_time']
            alarm_ID = df.at[index,'device_alarm_id']
            alarm_status = df.at[index, 'device_alarm_status']
            notify_ID = df.at[index, 'notify_id'] #สำหรับดึงโทเค็นข้อมูลจาก ตาราง notify มาและใช้แจ้งเตือน
            alarm_createby = df.at[index, 'device_alarm_create_by']

            # > เงื่อนไข 1: ตรวจสอบเวลาปัจจุบันหากตรงเงื่อนไขให้ทำการเปิด device_alarm_status เป็น True หากไม่ตรงให้เป็น False <
            if start_time <= current_time <= stop_time: #อยู่ในระว่างเวลาทำงาน
                # > เงื่อนไข 2.1: ตรวจสอบให้ device_alarm_status เป็น True แล้วทำงานต่อไป <
                if alarm_status == False or alarm_status is None: #if status เป็น false หรือ null ตอนนั้นให้เปลี่ยนเป็น true ให้ทำงานต่อเพื่อตรวจค่า volume และ level ต่อไป
                    query_update = (
                        f"UPDATE device_alarm_table "
                        f"SET device_alarm_status = True "
                        f"WHERE device_alarm_id = {alarm_ID};"
                    )
                    with con.cursor() as cursor:
                        cursor.execute(query_update)
                    con.commit()
                else: #เมื่อ status เป็น true ทำงานต่อไป เพื่อตรวจสอบค่า volume และ level ต่อไป
                    pass
            elif current_time > stop_time and current_time > start_time: #ไม่อยู่ในเวลาทำงานแล้ว
                # > เงื่อนไข 2.2: ตรวจสอบให้ device_alarm_status เป็น False แล้วหยุดทำงานตอนนี้ <
                if alarm_status == True or alarm_status is None: #if status เป็น true หรือ null ตอนนั้นให้เปลี่ยนเป็น false ไม่ทำงานต่อเพราะ status เปลี่ยนเป็น false แล้ว
                    query_update = (
                        f"UPDATE device_alarm_table "
                        f"SET device_alarm_status = False "
                        f"WHERE device_alarm_id = {alarm_ID};"
                    )
                    with con.cursor() as cursor:
                        cursor.execute(query_update)
                    con.commit()
                    print(f"Log : status เปลี่ยนเป็น false และไม่เริ่มการทำงานที่ Id: {alarm_ID}.")
                    time.sleep(0.5)
                    continue
                else: #เมื่อ status เป็น false เริ่มลูปต่อไปไม่ต้องทำงานต่อ
                    print(f"Log : status เป็น false ไม่ต้องทำงานที่ Id: {alarm_ID}.")
                    time.sleep(0.5)
                    continue
            else:
                continue
            
            # > เงื่อนไข 3: ทำการตรวจ alarm_type ว่าเป็น type ใด<
            print(f"Log : alarm_status เป็น True เริ่มการตรวจสอบ type ที่ Id: {alarm_ID}.")
            # ------------ ดึงข้อมูลจาก device_table มาก่อนเพื่อใช้ ดึงค่า Volume และ Level ปัจจุบัน --------------
            query_device_data = "SELECT * FROM device_table "
            df_check = pd.read_sql(query_device_data, con)
            alarm_type = df.at[index, 'device_alarm_type']

            query_device_data = "SELECT * FROM device_raw_data order by raw_id desc limit 1 "
            df_check2 = pd.read_sql(query_device_data, con)
            alarm_type = df.at[index, 'device_alarm_type']

            # ------------------------------------------------------------------------------------------
            # ------------------------------ทำการดึงข้อมูลจาก ตาราง notify มา ---------------------------------
            query_notify = f"SELECT * FROM device_notify_table WHERE notify_id={notify_ID};"
            df_notify = pd.read_sql(query_notify, con)
            Token = df_notify.at[0,'notify_token']
            # ----------------------------------------------------------------------------------------------

            # > เงื่อนไข 3.1: ตรวจสอบ type คือ alarmVolumeSet ทำการตรวจสอบค่า volume ใน row นี้ <
            if alarm_type == "alarmVolumeSet" :
                print("Log : alarmVolumeSet ทำการตรวจสอบ volume.")
                # > เงื่อนไข : ตรวจสอบค่า device_volume ว่ามากกว่าหรือน้อยกว่าค่าที่ตั้งไว้ใน device_table หรือไม่ <
                volume_high = df.at[index,'device_volume_high']  # ค่าสูงสุดที่กำหนดไว้ใน device_alarm_table
                volume_low = df.at[index,'device_volume_low']    # ค่าต่ำสุดที่กำหนดไว้ใน device_alarm_table

                for index, row in df_check2.iterrows():
                    device_volume = row['device_tank_volume']     # ค่า volume ปัจจุบัน ใน device_table
                    
                    #log เพื่อดูข้อมูลของ volume
                    print(f"Log : volume_high = {volume_high}.")
                    print(f"    : volume_low = {volume_low}.")
                    print(f"    : device_volume = {device_volume}.")

                    # --------------ทำการดึง volume ล่าสุดที่บันทึกไว้ โดยเลือกที่ alarm_id เดียวกับที่ตรวจสอบอยู่ในปัจจุบัน---------------------
                    query_last_volume = f"SELECT volume_alarm_record FROM device_alarm_volume_record_table WHERE device_alarm_id = {alarm_ID} ORDER BY device_volume_record_id DESC LIMIT 1;"
                    last_volume_df = pd.read_sql(query_last_volume, con)
                    # ---------------------------------------------------------------------------------------------------------

                    #คำนวณค่าต่างของ device_volume ปัจจุบัน และ volume สูงและต่ำที่เกิด alarm (ตอนนี้ไม่ใช้)
                    diference_higher = device_volume - volume_high
                    diference_lesser = volume_low - device_volume

                    #ประกาศตัวแปร Message
                    alarm_Message_High = (
                                          f"*Volume high alarm* \n"
                                          f"Actual volume: {device_volume} liters \n"
                                          f"High Limit: {volume_high} liters"
                                          )
                    alarm_Message_Low = (
                                          f"*Volume low alarm* \n"
                                          f"Actual volume: {device_volume} liters \n"
                                          f"Low Limit: {volume_low} liters"
                                          )

                    if device_volume > volume_high:
                        # เมื่อ volume สูงเกินไป ส่งข้อมูลแจ้งเตือนไปยัง device_alarm_record_table
                        try:
                            #ตรวจค่า volume ล่าสุดที่ได้มากับ volume ปัจจุบัน
                            if alarm_ID not in first_process:
                                volume_high_record(device_volume,volume_high,volume_low,alarm_Message_High,alarm_ID,alarm_createby)
                                send_line_notification(alarm_Message_High,Token)
                                first_process.append(alarm_ID)
                            elif volume_next_check <= DateTimeNow:
                                volume_high_record(device_volume,volume_high,volume_low,alarm_Message_High,alarm_ID,alarm_createby)
                                send_line_notification(alarm_Message_High,Token)
                                print(f"Log : First found volume too high in {alarm_ID}.")
                            else:
                                print("Log : volume isn't time for check.")
                                pass
                        except Exception as e:
                            print(f"An error occurred: {e}")

                    elif device_volume < volume_low:
                        # เมื่อ volume ต่ำ ส่งข้อมูลแจ้งเตือนไปยัง device_alarm_record_table
                        try:
                            #ตรวจค่า volume ล่าสุดที่ได้มากับ volume ปัจจุบัน
                            if alarm_ID not in first_process:
                                volume_low_record(device_volume,volume_high,volume_low,alarm_Message_Low,alarm_ID,alarm_createby)
                                send_line_notification(alarm_Message_Low,Token)
                                first_process.append(alarm_ID)
                                print(f"Log : First found volume too low in {alarm_ID}.")
                            elif volume_next_check <= DateTimeNow:
                                volume_high_record(device_volume,volume_high,volume_low,alarm_Message_Low,alarm_ID,alarm_createby)
                                send_line_notification(alarm_Message_Low,Token)
                                print(f"Log : Volume too low recorded.")
                            else:
                                print("Log : volume isn't time for check.")
                                pass
                        except Exception as e:
                            print(f"An error occurred: {e}")
                    else:
                        if alarm_ID in first_process:
                            first_process.remove(alarm_ID)
                            print(f"Log : Now volume at {alarm_ID} is not alarm. Back to first process.")
            # > เงื่อนไข 3.2: ตรวจสอบ type คือ batchToLong ทำการตรวจสอบ batch duration ใน row นี้ <
            elif alarm_type == "batchToLong" :
                batch_duration_set = df.at[index,'device_batch_duration_set'] # จะใช้ในการตรวจ batch_duration
                print("Log : batchTolong ทำการตรวจสอบ batch duration.")
                # --------- ดึงค่าจากตาราง batch_table มาใช้โดยเลือกดึงค่าจาก batch ที่สถานะ true -----------------
                query_batch_table = "SELECT * FROM device_batch_table WHERE device_batch_value_status = True "
                df_batch = pd.read_sql(query_batch_table,con)
                #------------------------------------------------------------------------------------------
                if batch_duration_set is not None:  # ตรวจสอบว่าไม่ใช่ None ก่อนที่จะใช้ split  
                    # แปลง batch_duration_set เป็น timedelta
                    hours, minutes = map(int, batch_duration_set.split(':'))
                    batch_duration_set_time = timedelta(hours=hours, minutes=minutes)
                    print(batch_duration_set_time)

                    for batch_index, batch_row in df_batch.iterrows():
                        batch_start_time = pd.to_datetime(batch_row['device_batch_start_time'], format=time_format)
                        batch_ID = df_batch.at[batch_index,'device_batch_id']
                        batch_current_time = DateTimeNow
                        batch_work_time = batch_current_time - batch_start_time

                        device_ID = df_batch.at[batch_index,'device_id']
                        
                        batch_record_time = timestamp_convertTo_str(batch_work_time)

                        alarm_Message_batch = (f"*Batch To Long alarm* \n" 
                                               f"Record Hours: {batch_record_time} hr\n"
                                               f"Hours Limit: {batch_duration_set} hr" 
                                            )

                        if batch_work_time > batch_duration_set_time :
                            try : 
                                #ตรวจค่า batch duration ล่าสุดที่คำนวณได้มากับ batch duration ที่ตั้งไว้ และทำการเซ็ตเวลาที่จะทำ alarm อีกครั้งใน alarm เดิม
                                if alarm_ID not in first_process:
                                    batch_toolong_record(batch_duration_set, batch_record_time, alarm_Message_batch, alarm_ID, batch_ID, alarm_createby)
                                    send_line_notification(alarm_Message_batch, Token)
                                    first_process.append(alarm_ID)
                                #ตรวจว่า เวลาตอนนี้พร้อมทำงานอีกครั้งหลังจากการทำงานครั้งแรกไหมจากที่เซ็ตไว้ใน batch_next_check
                                elif batch_next_check <= DateTimeNow:
                                    batch_toolong_record(batch_duration_set, batch_record_time, alarm_Message_batch, alarm_ID, batch_ID, alarm_createby)
                                    send_line_notification(alarm_Message_batch, Token)
                                else:
                                    print("Log: Batch isn't time for check.")
                                    pass
                            except Exception as e:
                                print(f"An error occurred: {e}")
                        elif batch_work_time < batch_duration_set_time :
                            try:
                                print("Log: Batch working in time.")
                                if alarm_ID in first_process:
                                    first_process.remove(alarm_ID)
                                    print(f"Log : Now batch is working in time at {alarm_ID}. Back to first process.")
                            except Exception as e:
                                print(f"An error occurred: {e}")
                        else :
                            try:
                                print("Log: Batch working on time. HOW A PERFECT TIME.")
                                if alarm_ID in first_process:
                                    first_process.remove(alarm_ID)
                                    print(f"Log : Now batch is working in time at {alarm_ID}. Back to first process.")
                            except Exception as e:
                                print(f"An error occurred: {e}")
                else:
                    print("Log: batch_duration_set is None, unable to calculate batch duration.")

            # > เงื่อนไข 3.3: ตรวจสอบ type คือ alarmLevelSet ทำการตรวจสอบค่า level(ระดับน้ำ) ใน row นี้ <
            elif alarm_type == "alarmLevelSet" :
                print("Log : alarmLevelSet ทำการตรวจสอบ level.")
                # > เงื่อนไข : ตรวจสอบค่า device_level ว่ามากกว่าหรือน้อยกว่าค่าที่ตั้งไว้ใน device_table หรือไม่ <
                level_high = df.at[index,'device_level_high_limit']  # ค่าสูงสุดที่กำหนดไว้ใน device_alarm_table
                level_low = df.at[index,'device_level_low_limit']    # ค่าต่ำสุดที่กำหนดไว้ใน device_alarm_table

                for index, row in df_check.iterrows():
                    # --------------ทำการดึงข้อมูล level โดยเลือกที่ distance ล่าสุดใน raw_data---------------------
                    query_distance = f"SELECT device_distance FROM device_raw_data ORDER BY raw_id DESC LIMIT 1;"
                    distance_df = pd.read_sql(query_distance, con)
                    # -------------------------------------------------------------------------------------------------
                    for index, row in distance_df.iterrows():
                        device_level = distance_df.at[index,'device_distance'] # ค่า level ปัจจุบัน คือ distance ใน raw_data
                        #log เพื่อดูข้อมูลของ level
                        print(f"Log : level_high = {level_high}.")
                        print(f"    : level_low = {level_low}.")
                        print(f"    : device_level = {device_level}.")
                        # --------------ทำการดึง level ล่าสุดที่บันทึกไว้ โดยเลือกที่ alarm_id เดียวกับที่ตรวจสอบอยู่ในปัจจุบัน---------------------
                        query_last_level = f"SELECT level_alarm_record FROM device_alarm_level_record_table WHERE device_alarm_id = {alarm_ID} ORDER BY device_level_record_id DESC LIMIT 1;"
                        last_level_df = pd.read_sql(query_last_level, con)
                        # ---------------------------------------------------------------------------------------------------------

                        #คำนวณค่าต่างของ device_level ปัจจุบัน และ level สูงและต่ำที่เกิด alarm
                        diference_higher = device_level - level_high
                        diference_lesser = level_low - device_level

                        #ประกาศตัวแปร Message
                        alarm_Message_High = (
                                          f"*Level high alarm* \n"
                                          f"Actual level: {device_volume} liters \n"
                                          f"High Limit: {level_high} liters"
                                          )
                        alarm_Message_Low = (
                                          f"*Level low alarm* \n"
                                          f"Actual level: {device_volume} liters \n"
                                          f"Low Limit: {level_low} liters"
                                          )
                        if device_level > level_high:
                            # เมื่อ level สูงเกินไป ส่งข้อมูลแจ้งเตือนไปยัง device_alarm_record_table
                            try:
                                #ตรวจค่า level ล่าสุดที่ได้มากับ level ปัจจุบัน
                                if alarm_ID not in first_process:
                                    level_high_record(device_level,level_high,level_low,alarm_Message_High,alarm_ID, alarm_createby)
                                    send_line_notification(alarm_Message_High,Token)
                                    first_process.append(alarm_ID)
                                elif level_next_check <= DateTimeNow:
                                    level_high_record(device_level,level_high,level_low,alarm_Message_High,alarm_ID,alarm_createby)
                                    send_line_notification(alarm_Message_High,Token)
                                    print(f"Log : Level too high recorded.")
                                else:
                                    print("Log: Level isn't time for check.")
                                    pass
                            except Exception as e:
                                print(f"An error occurred: {e}")
                        elif device_level < level_low:
                            # เมื่อ level สูงเกินไป ส่งข้อมูลแจ้งเตือนไปยัง device_alarm_record_table
                            try:
                                #ตรวจค่า level ล่าสุดที่ได้มากับ level ปัจจุบัน
                                if alarm_ID not in first_process:
                                    level_low_record(device_level,level_high,level_low,alarm_Message_Low,alarm_ID,alarm_createby)
                                    send_line_notification(alarm_Message_Low,Token)
                                    first_process.append(alarm_ID)
                                elif level_next_check <= DateTimeNow:
                                    level_low_record(device_level,level_high,level_low,alarm_Message_Low,alarm_ID,alarm_createby)
                                    send_line_notification(alarm_Message_Low,Token)
                                    print(f"Log : Level too low recorded.")
                                else:
                                    print("Log: Level isn't time for check.")
                                    pass
                            except Exception as e:
                                print(f"An error occurred: {e}")
                        else:
                            if alarm_ID in first_process:
                                first_process.remove(alarm_ID)
                                print(f"Log : Now level at {alarm_ID} is not alarm. Back to first process.")
            else :
                print("Log : Unknow type, do nothing.")
                pass

        # ทำงานภายในลูปเพื่อตรวจ alarm ทั้งหมด แล้วเซ็ตเวลาตรวจ alarm ของ batchTolong แล้วจากนั้นตรวจสอบว่า batch_next_check เหมือนการทำงานใน for เพื่อ set เวลาใหม่
        if batch_next_check <= DateTimeNow: 
            batch_next_check = DateTimeNow + timedelta(hours=1)
        if volume_next_check <= DateTimeNow:
            volume_next_check = DateTimeNow + timedelta(minutes=10)
        if level_next_check <= DateTimeNow:
            level_next_check = DateTimeNow + timedelta(minutes=10)
        print("----------- END-LOOP -----------")
        time.sleep(0.5)
except KeyboardInterrupt:
    print("exit this program")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # ปิดการเชื่อมต่อ PostgreSQL
    con.close()