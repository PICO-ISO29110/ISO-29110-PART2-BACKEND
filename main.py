from ast import List
from random import choice
from fastapi import FastAPI,  Cookie, Query ,Depends, HTTPException ,WebSocket, WebSocketDisconnect , Response ,Request
from pydantic import BaseModel
import psycopg2
import json  # ต้องนำเข้าไลบรารี json
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from fastapi.responses import JSONResponse
from typing import Optional , List ,Dict
import bcrypt
from datetime import timedelta
import asyncio
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select ,create_engine
from sqlalchemy.sql import text  # เพิ่มการ import นี้
import jwt
from fastapi import HTTPException, status
from psycopg2.extras import RealDictCursor
from fastapi.responses import FileResponse
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from fastapi.responses import StreamingResponse
import io
import csv
import pandas as pd

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SECRET_KEY = "Kakarot"
ALGORITHM = "HS256"

# รายละเอียดการเชื่อมต่อกับ PostgreSQL
db_params = {
    "database": "ISO-29110-DB",
    "host": "127.0.0.1",
    "user": "postgres",
    "password": "Automation01",
    "port": "5432"
}

origins = [
    "http://localhost",
    "http://localhost:5173",
    "http://127.0.0.1:5500",  # Adjust based on your frontend origin
    "http://100.82.151.125:8000",
    "http://100.86.153.116:3000/"
    
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # ให้เข้าถึงจาก React frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

conn = psycopg2.connect(database="ISO-29110-DB",
                        host="127.0.0.1",
                        user="postgres",
                        password="Automation01",
                        port="5432")
date_now = datetime.now()

# กำหนดชื่อตารางที่คุณต้องการดึงข้อมูล
table_name = "device_batch_table"

@app.get("/GetRealTime/")
def get_table_data():
    table_name = "device_batch_table"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT * ,TO_CHAR(device_batch_datetime, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_datetime,
        TO_CHAR(device_batch_start_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_start_time,
        TO_CHAR(device_batch_end_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_end_time
        FROM device_batch_table order by device_batch_id desc limit 1
        
        """
    get_to_cal_duration = f"SELECT device_batch_datetime, device_batch_start_time FROM device_batch_table ORDER BY device_batch_id DESC LIMIT 1 ;"

    with conn.cursor() as cursor:
        cursor.execute(get_to_cal_duration)
        get_duration = cursor.fetchone()
        cursor.execute(sql_str)
        data_start = cursor.fetchall()
        conn.commit()

    #cal to get realtime batch duration
    realtime, starttime = get_duration
    duration = realtime - starttime
    # คำนวณจำนวนชั่วโมงและนาที
    hours, remainder = divmod(int(duration.total_seconds() / 60), 60)
    minutes = remainder
    # สร้าง string ในรูปแบบที่ต้องการ
    duration_time = "{:02d} Hr {:02d} Min".format(hours, minutes)
    
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["device_batch_duration"] = duration_time
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data

class Item(BaseModel):
    name: str
    password: str
@app.post("/login/")
async def post_data(item: Item):
    # Access the data using the 'item' parameter
    print('name : ',item.name)
    print('password : ',hash(item.password))
    try:
        if item.name == "":
            raise HTTPException(status_code=400, detail=f"The User name entered is incorrect")

        query = "SELECT usere_mail, user_password, user_id, user_status, user_bancount FROM public.user_table WHERE usere_mail = %s;"
        with conn.cursor() as cursor:
            cursor.execute(query, (item.name,))
            user_data = cursor.fetchone()
            conn.commit

        if user_data:
            user_mail, user_password, user_id, user_status, ban_count = user_data
            input_pw = item.password
            hash_pw = user_password
            # ทำการอัปเดตข้อมูล (ตัวอย่างเท่านั้น คุณอาจต้องปรับเปลี่ยนตามความต้องการ)
            update_query = "UPDATE user_table SET user_lastlogin = CURRENT_TIMESTAMP WHERE user_id = %s;"
            with conn.cursor() as cursor:
                cursor.execute(update_query, (user_id,))
                conn.commit()
                cursor.execute("SELECT user_lastlogin, user_bannedtime FROM user_table WHERE user_id = %s;", (user_id,))
                last_login_timedata = cursor.fetchone()
                login_time, ban_time_at = last_login_timedata

            #สร้างช่วงเวลาที่จะปลดแบนตามต้องการ(เปลี่ยนได้ตามต้องการ)
            bantime = timedelta(days=1)

            # เช็ค ban_time_at เป็น null หรือไม่ หากไม่ให้แปลงค่าเตรียมใช้ในการตรวจสอบ
            if ban_time_at is not None:
                bantime_check = login_time - ban_time_at
                bantime_show = bantime - bantime_check

                # ตรวจสอบว่าเวลานับถอยหลังมีค่าติดลบหรือไม่
                if bantime_show.total_seconds() < 0:
                    bantime_show = timedelta(seconds=0)

                # แปลง bantime_check เป็นวินาที
                bantime_total_seconds = int(bantime_show.total_seconds())

                # คำนวณหาชั่วโมง, นาที, และวินาที
                hours, remainder = divmod(bantime_total_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)

                # แสดงผลเป็น H:M:S
                banned_time_show = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                print(banned_time_show)

            # ตรวจเวลาว่าเลยเวลาโดน ban แล้วหรือไม่
            if ban_time_at == None:
                pass
            elif (bantime_check > bantime) :
                user_status = True
                with conn.cursor() as cursor:
                    cursor.execute("UPDATE user_table SET user_status = %s WHERE user_id = %s", (True, user_id,))
                    conn.commit()
            else:
                pass

            if user_status :
                if bcrypt.checkpw(input_pw.encode('utf-8'), hash_pw.encode('utf-8')):
                    #----------- event -----------------
                    event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                                VALUES (%s, %s, %s, %s) """
                    with conn.cursor() as cursor:
                        cursor.execute(event_sql, ('User Login', 'User Has Logging In', datetime.now(), user_id,))
                        conn.commit()
                    #------------------------------------
                    with conn.cursor() as cursor:
                        cursor.execute("UPDATE user_table SET user_bancount = %s, user_bannedtime = NULL WHERE user_id = %s", (0, user_id,))
                        conn.commit()
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT * FROM user_table INNER JOIN user_role_table ON user_table.user_role_id=user_role_table.user_role_id where usere_mail = '"+user_mail+"' ;")
                        data_user = cursor.fetchall()
                        conn.commit()

                    table_name = "user_table"
                    data = {table_name: []}
                    for row in data_user:
                        row_data = {}
                        for i, column_name in enumerate(cursor.description):
                            row_data[column_name[0]] = row[i]
                        row_data["status"] = "ok"
                        data[table_name].append(row_data)
                    
                    return data
                else:
                    if (ban_count+1) > 2:
                        with conn.cursor() as cursor:
                            cursor.execute("UPDATE user_table SET user_status = %s, user_bancount = %s, user_bannedtime = CURRENT_TIMESTAMP WHERE user_id = %s", (False ,0 ,user_id,))
                            conn.commit()
                        raise HTTPException(status_code=400, detail="You are banned for 24 hours.")
                    else:
                        with conn.cursor() as cursor:
                            cursor.execute("UPDATE user_table SET user_bancount = %s WHERE user_id = %s", ((ban_count+1) ,user_id,))
                            conn.commit()
                        raise HTTPException(status_code=400, detail="Invalid username or password.")
            else:
                raise HTTPException(status_code=400, detail=f"Banned time left: {banned_time_show}.")
        else:
            raise HTTPException(status_code=400, detail="User not found.")
    finally:
        print("end")


class Userlogout(BaseModel):
    userid: int
    
@app.post("/logout/")
async def create_event(Userlogout: Userlogout):
    
    try:
            print(Userlogout.userid)
            insert_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                           VALUES (%s, %s, %s, %s) """

            with conn.cursor() as cursor:
                cursor.execute(insert_sql, ('User Logout', 'User Has Log Out', datetime.now(), Userlogout.userid,))
                conn.commit()

            return {"message": "logging update"}

    except Exception as e:
        # Log the error for debugging purposes
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="System error")


class Report(BaseModel):
    year: str
    month: str
    day: str
@app.post("/DailyReport/")
async def post_data(report: Report):      
    print('year : ',report.year)
    print('month : ',report.month)
    print('day : ',report.day)

    table_name = "device_batch_table"
    sql_str = f"""SELECT * , TO_CHAR(device_batch_datetime, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_datetime,
        TO_CHAR(device_batch_start_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_start_time,
        TO_CHAR(device_batch_end_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_end_time
        FROM {table_name} where 
        EXTRACT(YEAR FROM device_batch_datetime) = '{report.year}' and 
        EXTRACT(MONTH FROM device_batch_datetime) = '{report.month}' and 
        EXTRACT(DAY FROM device_batch_datetime) = '{report.day}' and 
        EXTRACT(MINUTE FROM device_batch_datetime) = '00'
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    # print(data_start)
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    print(data)
    return data


class MonthReport(BaseModel):
    year: str
    month: str
@app.post("/MonthlyReport/")
async def post_data(monthreport: MonthReport):

    table_name = "device_batch_table"
    sql_str = f"""SELECT * , TO_CHAR(device_batch_datetime, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_datetime,
        TO_CHAR(device_batch_start_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_start_time,
        TO_CHAR(device_batch_end_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_end_time
        FROM {table_name} where EXTRACT(YEAR FROM device_batch_datetime) = '{monthreport.year}' and 
        EXTRACT(MONTH FROM device_batch_datetime) = '{monthreport.month}' and 
        EXTRACT(HOUR FROM device_batch_datetime) = '00' and 
        EXTRACT(MINUTE FROM device_batch_datetime) = '00' 
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    print(sql_str)
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data
    

@app.get("/Device/")
async def post_data():

    sql_str = f"""SELECT * , 
        TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
        TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
        TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
        TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time 
        FROM device_table order by device_id DESC limit 1 
    """
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close


    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data

class DeviceReport(BaseModel):
    
    # Define the model fields here
    pass
    
@app.get("/device-values/")
async def post_data():
    table_name = "device-values"
    sql_str = f"""
        SELECT *
        FROM public.device_table 
        INNER JOIN user_table ON device_table.user_id = user_table.user_id
    """
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close


    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data

@app.get("/GetTankRealtimeData/")
async def post_data():
    table_name = "GetTankRealtimeData"
    sql_str = f"""
        SELECT *,
        TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
        TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
        TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
        TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time,
        TO_CHAR(device_timestamp::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp
        FROM 
            device_raw_data
        INNER JOIN 
            device_table ON device_table.device_id = device_raw_data.device_id 
        WHERE
            device_raw_data.device_timestamp <= NOW() 
        ORDER BY 
            device_raw_data.device_timestamp DESC
        LIMIT 1;

    """
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data




@app.get("/GetTankRealtimeData10data/")
async def post_data():
    table_name = "GetTankRealtimeData10data"
    sql_str = f"""
        SELECT *,
        TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
        TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
        TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
        TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time,
        TO_CHAR(device_timestamp::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp
        FROM 
            device_raw_data
        INNER JOIN 
            device_table ON device_table.device_id = device_raw_data.device_id 
        WHERE
            device_raw_data.device_timestamp <= NOW() 
        ORDER BY 
            device_raw_data.device_timestamp DESC
            OFFSET 1
        LIMIT 5;

    """
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data


@app.get("/GetTankCompareDataBarChart/")
async def post_data():
    table_name = "GetTankCompareDataBarChart"
    sql_str = f"""
       SELECT device_batch_id, device_batch_start_time, device_batch_end_time, device_batch_value 
    FROM device_batch_table 
    ORDER BY device_batch_id DESC 
   OFFSET 1
LIMIT 14;
        """
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data


@app.get("/GetTankData/")
async def post_data():
    table_name = "GetTankData"
    sql_str = f"""
        SELECT *,TO_CHAR(device_timestamp::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp,
        TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
        TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
        TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
        TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time
        FROM device_raw_data
        INNER JOIN device_table ON device_table.device_id = device_raw_data.device_id;
     """
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
 
# สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data


@app.get("/GetTankCompareDataPiechart/")
async def post_data():
    table_name = "GetTankCompareDataPiechart"
    sql_str = f"""
       SELECT 
    DATE(device_timestamp) AS day,
	 MAX(device_tank_volume) FILTER (WHERE EXTRACT(HOUR FROM device_timestamp) = 0 AND EXTRACT(MINUTE FROM device_timestamp) = 1 ) AS MIN_VALUE,
	  MIN(device_tank_volume) FILTER (WHERE EXTRACT(HOUR FROM device_timestamp) = 23 AND EXTRACT(MINUTE FROM device_timestamp) = 59 AND EXTRACT(SECOND FROM device_timestamp) = 58 ) AS MAX_VALUE,
    MAX(device_tank_volume) FILTER (WHERE EXTRACT(HOUR FROM device_timestamp) = 0 AND EXTRACT(MINUTE FROM device_timestamp) = 1) -
    MIN(device_tank_volume) FILTER (WHERE EXTRACT(HOUR FROM device_timestamp) = 23 AND EXTRACT(MINUTE FROM device_timestamp) = 59) AS average_volume
,
    TO_CHAR(date(device_timestamp), 'Day') as day_of_week
FROM 
    public.device_raw_data dr
WHERE 
    device_timestamp >= CURRENT_DATE - INTERVAL '7 days'
    AND device_timestamp < CURRENT_DATE
GROUP BY 
    day, day_of_week 
ORDER BY 
    day;

     """
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    
# สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data


class Report(BaseModel):
    year: str
    month: str
    day: str 
@app.post("/GetTankHistoryData_DAY/")
async def post_data(report: Report):          
    print('year:', report.year)
    print('month:', report.month)
    print('day:', report.day)
    
    sql_str = f"""SELECT * ,TO_CHAR(device_timestamp::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp,
        TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
        TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
        TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
        TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time
        FROM device_raw_data
        INNER JOIN device_table ON device_table.device_id = device_raw_data.device_id
        WHERE EXTRACT(YEAR FROM device_timestamp) = {report.year} AND 
        EXTRACT(MONTH FROM device_timestamp) = {report.month} AND
        EXTRACT(DAY FROM device_timestamp) = {report.day}; 
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
        # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetTankHistoryData_DAY": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetTankHistoryData_DAY"].append(row_data)
        return data


class MonthReport(BaseModel):
    year: str
    month: str
@app.post("/GetTankHistoryData_MONTH/")
async def post_data(MonthReport: MonthReport):      
    print('year : ',MonthReport.year)
    print('month : ',MonthReport.month)
    table_name ="GetTankHistoryData_MONTH"
    sql_str = f"""SELECT * ,TO_CHAR(device_timestamp::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp,
        TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
        TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
        TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
        TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time
        FROM device_raw_data
        INNER JOIN device_table ON device_table.device_id = device_raw_data.device_id
        where EXTRACT(YEAR FROM device_timestamp) = {MonthReport.year} and 
        EXTRACT(MONTH FROM device_timestamp) = {MonthReport.month}; 
        """
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
    # print(data_start)
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {table_name: []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data[table_name].append(row_data)
        return data

class Report(BaseModel):
    year: str
    month: str
    day: str 
@app.post("/GetBatchHistoryData_DAY/")
async def post_data(report: Report):          
    print('year:', report.year)
    print('month:', report.month)
    print('day:', report.day)
    
    sql_str = f"""SELECT *, TO_CHAR(device_batch_datetime, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_datetime,
        TO_CHAR(device_batch_start_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_start_time,
        TO_CHAR(device_batch_end_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_end_time,
        TO_CHAR(device_batch_history_createat::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_history_createat,
        TO_CHAR(device_batch_history_updateat::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_history_updateat,
        TO_CHAR(device_batch_history_start::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_history_start
        FROM public.device_batch_table
        INNER JOIN device_batch_history_table ON device_batch_table.device_id = device_batch_history_table.device_id
        WHERE EXTRACT(YEAR FROM device_batch_datetime) = { report.year} AND 
        EXTRACT(MONTH FROM device_batch_datetime) = {report.month} AND
        EXTRACT(DAY FROM device_batch_datetime) = {report.day}; 
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetBatchHistoryData_DAY": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetBatchHistoryData_DAY"].append(row_data)
        return data

class MonthReport(BaseModel):
    year: str
    month: str
    
@app.post("/GetBatchHistoryData_MONTH/")
async def post_data(MonthReport: MonthReport):          
    print('year:', MonthReport.year)
    print('month:', MonthReport.month)
    sql_str = f"""SELECT * , TO_CHAR(device_batch_datetime, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_datetime,
        TO_CHAR(device_batch_start_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_start_time,
        TO_CHAR(device_batch_end_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_end_time,
        TO_CHAR(device_batch_history_createat::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_history_createat,
        TO_CHAR(device_batch_history_updateat::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_history_updateat,
        TO_CHAR(device_batch_history_start::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_history_start
        FROM public.device_batch_table
        INNER JOIN device_batch_history_table ON device_batch_table.device_id = device_batch_history_table.device_id
        WHERE EXTRACT(YEAR FROM device_batch_datetime) = {MonthReport.year} AND 
        EXTRACT(MONTH FROM device_batch_datetime) = {MonthReport.month} ; 
    """
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetBatchHistoryData_MONTH": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetBatchHistoryData_MONTH"].append(row_data)
        return data


class Report(BaseModel):
    year: str
    month: str
    day: str 
    
@app.post("/GetEventHistory_DAY/")
async def post_data(report: Report):          
    print('year:', report.year)
    print('month:', report.month)
    print('day:', report.day)
    
    sql_str = f"""SELECT * , TO_CHAR(device_alarm_starttime, 'DD/MM/YYYY HH:MI:SS AM') as device_alarm_starttime,
        TO_CHAR(device_alarm_stoptime, 'DD/MM/YYYY HH:MI:SS AM') as device_alarm_stoptime
        FROM public.device_alarm_table
        WHERE EXTRACT(YEAR FROM device_alarm_starttime) = { report.year} AND 
        EXTRACT(MONTH FROM device_alarm_starttime) = {report.month} AND
        EXTRACT(DAY FROM device_alarm_starttime) = {report.day}; 
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetEventHistory_DAY": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetEventHistory_DAY"].append(row_data)
        return data

class MonthReport(BaseModel):
    year: str
    month: str
    
@app.post("/GetEventHistory_MONTH/")
async def post_data(MonthReport: MonthReport):          
    print('year:', MonthReport.year)
    print('month:', MonthReport.month)
    
    sql_str = f"""SELECT * , TO_CHAR(device_alarm_starttime, 'DD/MM/YYYY HH:MI:SS AM') as device_alarm_starttime,
        TO_CHAR(device_alarm_stoptime, 'DD/MM/YYYY HH:MI:SS AM') as device_alarm_stoptime
        FROM public.device_alarm_table
        WHERE EXTRACT(YEAR FROM device_alarm_starttime) = { MonthReport.year} AND 
        EXTRACT(MONTH FROM device_alarm_starttime) = {MonthReport.month}; 
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetEventHistory_MONTH": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetEventHistory_MONTH"].append(row_data)
        return data

class Report(BaseModel):
    year: str
    month: str
    day: str 
    
@app.post("/GetReportHistory_DAY/")
async def post_data(report: Report):          
    print('year:', report.year)
    print('month:', report.month)
    print('day:', report.day)
    
    sql_str = f"""SELECT * , TO_CHAR(device_report_create_at, 'DD/MM/YYYY HH:MI:SS AM') as device_report_create_at,
        TO_CHAR(device_report_update_at, 'DD/MM/YYYY HH:MI:SS AM') as device_report_update_at
        FROM public.device_report_table
        WHERE EXTRACT(YEAR FROM device_report_create_at) = { report.year} AND 
        EXTRACT(MONTH FROM device_report_create_at) = {report.month} AND
        EXTRACT(DAY FROM device_report_create_at) = {report.day}; 
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetReportHistory_DAY": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetReportHistory_DAY"].append(row_data)
        return data

@app.get("/ABCDEFG/")
async def abcde():          
    print('year:')
	
class MonthReport(BaseModel):
    year: str
    month: str
    
@app.post("/GetReportHistory_MONTH/")
async def post_data(MonthReport: MonthReport):          
    print('year:', MonthReport.year)
    print('month:', MonthReport.month)
    
    sql_str = f"""SELECT * , TO_CHAR(device_report_create_at, 'DD/MM/YYYY HH:MI:SS AM') as device_report_create_at,
        TO_CHAR(device_report_update_at, 'DD/MM/YYYY HH:MI:SS AM') as device_report_update_at
        FROM public.device_report_table
        WHERE EXTRACT(YEAR FROM device_report_create_at) = { MonthReport.year} AND 
        EXTRACT(MONTH FROM device_report_create_at) = {MonthReport.month}; 
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetReportHistory_MONTH": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetReportHistory_MONTH"].append(row_data)
        return data
    



class Alarm(BaseModel):
    eventName: str
    namedevice: int
    alarmtype:str
    devicelevellows: float
    devicelevelhighs: float
    devicevolumelows: float
    devicevolumehighs: float
    starttimes: str
    endtimes: str
    notifyselect: int
    selectBatchs :int
    batchDurationSets:str
    datausercreate:int
    # print(devicelevelhighs)
    

@app.post("/device_alarm/")
async def post_data(Alarm: Alarm):  
    print(Alarm.devicelevellows)    
    # เปลี่ยน start และ end time ให้ เป็น datetime ตอนรับค่ามา
    start_time = Alarm.starttimes
    end_time = Alarm.endtimes
    alarm_type = Alarm.alarmtype
    try:
        inputData = [Alarm.eventName,Alarm.namedevice,Alarm.alarmtype,Alarm.devicelevellows,
            Alarm.devicelevelhighs,
            Alarm.devicevolumelows,
            Alarm.devicevolumehighs,
            Alarm.starttimes,
            Alarm.endtimes,
            Alarm.notifyselect,
            Alarm.selectBatchs,
            Alarm.batchDurationSets,
            Alarm.datausercreate,]
        print(inputData)
        # ตรวจสอบค่าที่ input เข้ามาว่ามีค่าว่างไหมโดนเน้นตัวแปรเหล่านี้
        if Alarm.eventName == 'none' : #ชื่อห้ามเป็นค่าว่าง
            response_data = {"status": "error", "message": "Please input Alarm name!"}
            return JSONResponse(content=response_data, status_code=400)
        elif (Alarm.starttimes == 'none') or (Alarm.endtimes == 'none'): #ห้ามให้ช่วงเวลาเป็นค่าว่าง ต้องใส่
            response_data = {"status": "error", "message": "Please input Start time and End time!"}
            return JSONResponse(content=response_data, status_code=400)
        elif (Alarm.alarmtype == 'batchToLong') and (Alarm.batchDurationSets == 'none'): #ต้องใส่เวลาที่ batch ควรทำงานในการตรวจสอบเวลา batch
            response_data = {"status": "error", "message": "Please input Batch Duration!"}
            return JSONResponse(content=response_data, status_code=400)
        
        sql_check_1 = """
            SELECT * FROM device_alarm_table 
            WHERE (%s BETWEEN device_check_start_time and device_check_stop_time OR %s BETWEEN device_check_start_time and device_check_stop_time) AND device_alarm_type = %s
        """
        with conn.cursor() as cursor:
            cursor.execute(sql_check_1, (start_time,end_time,alarm_type))
            check_time = cursor.fetchall()
            conn.commit()

        sql_check_2 = """
            SELECT * FROM device_alarm_table 
            WHERE (device_check_start_time BETWEEN %s and %s OR device_check_stop_time BETWEEN %s and %s)  AND device_alarm_type = %s
        """
        with conn.cursor() as cursor:
            cursor.execute(sql_check_2, (start_time, end_time, start_time, end_time, alarm_type))
            check_time_2 = cursor.fetchall()
            conn.commit()
        
        # ถ้าข้อมูล start time หรือ end time อยู่ระหว่างช่วงเวลา check time 
        if check_time:
            # If the condition is met, return a response indicating the intersection
            response_data = {"status": "error", "message": "Your alarm times intersects with other alarm time range."}
            return JSONResponse(content=response_data, status_code=400)
        elif check_time_2:
                # If the condition is met, return a response indicating the intersection
            response_data = {"status": "error", "message": "Other alarm times intersects with your alarm time range."}
            return JSONResponse(content=response_data, status_code=400)
        else:
            # If the condition is not met, insert data into the database
            insert_table = "device_alarm_table"
            insert_sql = f"""INSERT INTO {insert_table} (device_alarm_type, device_id, device_alarm_name, device_check_start_time, device_check_stop_time, notify_id, device_alarm_status, device_level_low_limit, device_level_high_limit, device_volume_low, device_volume_high, device_batch_duration_set, device_batch_id, device_alarm_create_by)
                            VALUES ('{Alarm.alarmtype}', '{Alarm.namedevice}', '{Alarm.eventName}', '{Alarm.starttimes}', '{Alarm.endtimes}', '{Alarm.notifyselect}', 'True', '{Alarm.devicelevellows}', '{Alarm.devicelevelhighs}', '{Alarm.devicevolumelows}', '{Alarm.devicevolumehighs}', '{Alarm.batchDurationSets}', '{Alarm.selectBatchs}', '{Alarm.datausercreate}')
            """

            cursor = conn.cursor()
            cursor.execute(insert_sql)
            conn.commit()

#----------- event -----------------
            event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                           VALUES (%s, %s, %s, %s) """

            with conn.cursor() as cursor:
                cursor.execute(event_sql, ('Alarm Create', 'Alarm added successfully', datetime.now(), Alarm.datausercreate,))
                conn.commit()
#------------------------------------

            response_data = {"status": "success", "message": "Data successfully processed."}
            return JSONResponse(content=response_data, status_code=200)

    except Exception as e:
        response_data = {"status": "error", "message": str(e)}
        return JSONResponse(content=response_data, status_code=500)
    


class AlarmUpdate(BaseModel):
    alarm_id: int
    eventName: str
    starttimes: str
    endtimes: str
    alarmtype: str
    notifyselect: int
    devicelevellows: float
    devicelevelhighs: float
    devicevolumelows: float
    devicevolumehighs: float
    batchDurationSets: str
    selectBatchs: int
    datausercreate: int

# Assume you have a 'device_alarm_table' and 'device_event_table' set up in your database
# Replace these table names with your actual table names

@app.post("/UpdateAlarm/" , tags=['USERS MANAGEMENT'])
async def update_alarm(AlarmUpdate: AlarmUpdate):
    try:
        # ตรวจสอบค่าที่ input เข้ามาว่ามีค่าว่างไหมโดนเน้นตัวแปรเหล่านี้
        if AlarmUpdate.eventName == '' : #ชื่อห้ามเป็นค่าว่าง
            response_data = {"status": "error", "message": "Please input Alarm name!"}
            return JSONResponse(content=response_data, status_code=400)
        elif (AlarmUpdate.starttimes == '') or (AlarmUpdate.endtimes == ''): #ห้ามให้ช่วงเวลาเป็นค่าว่าง ต้องใส่
            response_data = {"status": "error", "message": "Please input Start time and End time!"}
            return JSONResponse(content=response_data, status_code=400)
        elif (AlarmUpdate.alarmtype == 'batchToLong') and (AlarmUpdate.batchDurationSets == ''): #ต้องใส่เวลาที่ batch ควรทำงานในการตรวจสอบเวลา batch
            response_data = {"status": "error", "message": "Please input Batch Duration!"}
            return JSONResponse(content=response_data, status_code=400)
        
        # Check if the alarm exists
        check_alarm_sql = """SELECT * FROM device_alarm_table WHERE device_alarm_id = %s"""
        with conn.cursor() as cursor:
            cursor.execute(check_alarm_sql, (AlarmUpdate.alarm_id,))
            existing_alarm = cursor.fetchone()

        if not existing_alarm:
            raise HTTPException(status_code=404, detail="Alarm not found")
        
        start_time = AlarmUpdate.starttimes
        end_time = AlarmUpdate.endtimes
        alarm_type = AlarmUpdate.alarmtype

        sql_check_1 = """
            SELECT * FROM device_alarm_table 
            WHERE ((%s BETWEEN device_check_start_time and device_check_stop_time OR %s BETWEEN device_check_start_time and device_check_stop_time) AND device_alarm_type = %s) AND device_alarm_id != %s
        """
        with conn.cursor() as cursor:
            cursor.execute(sql_check_1, (start_time, end_time, alarm_type,AlarmUpdate.alarm_id))
            check_time = cursor.fetchall()
            conn.commit()

        sql_check_2 = """
            SELECT * FROM device_alarm_table 
            WHERE ((device_check_start_time BETWEEN %s and %s OR device_check_stop_time BETWEEN %s and %s)  AND device_alarm_type = %s)  AND device_alarm_id != %s
        """
        with conn.cursor() as cursor:
            cursor.execute(sql_check_2, (start_time, end_time, start_time, end_time, alarm_type,AlarmUpdate.alarm_id))
            check_time_2 = cursor.fetchall()
            conn.commit()
        
        # ถ้าข้อมูล start time หรือ end time อยู่ระหว่างช่วงเวลา check time 
        if check_time:
            # If the condition is met, return a response indicating the intersection
            response_data = {"status": "error", "message": "Your alarm times intersect with other alarm time range."}
            return JSONResponse(content=response_data, status_code=400)
        elif check_time_2:
            # If the condition is met, return a response indicating the intersection
            response_data = {"status": "error", "message": "Other alarm times intersect with your alarm time range."}
            return JSONResponse(content=response_data, status_code=400)
        else:
            # Update the alarm data
            update_sql = f"""
                UPDATE device_alarm_table
                SET 
                    device_alarm_name = '{AlarmUpdate.eventName}',
                    device_check_start_time = '{AlarmUpdate.starttimes}',
                    device_check_stop_time = '{AlarmUpdate.endtimes}',
                    notify_id = '{AlarmUpdate.notifyselect}',
                    device_level_low_limit = '{AlarmUpdate.devicelevellows}',
                    device_level_high_limit = '{AlarmUpdate.devicelevelhighs}',
                    device_volume_low = '{AlarmUpdate.devicevolumelows}',
                    device_volume_high = '{AlarmUpdate.devicevolumehighs}',
                    device_batch_duration_set = '{AlarmUpdate.batchDurationSets}',
                    device_batch_id = '{AlarmUpdate.selectBatchs}'
                WHERE device_alarm_id = {AlarmUpdate.alarm_id}
            """

            with conn.cursor() as cursor:
                cursor.execute(update_sql)
                conn.commit()

            # Log the update event
            event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                        VALUES (%s, %s, %s, %s) """

            with conn.cursor() as cursor:
                cursor.execute(
                    event_sql,
                    ('Alarm Update', f'Alarm ID {AlarmUpdate.alarm_id} updated successfully', datetime.now(), AlarmUpdate.datausercreate,),
                )
                conn.commit()

            return {"status": "success", "message": f"Alarm ID {AlarmUpdate.alarm_id} updated successfully"}

    except Exception as e:
        # Log the error for debugging purposes
        print(f"Error updating alarm: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update alarm: {str(e)}",
        )

class User(BaseModel):
    usere_mail: str
    user_firstname: str
    user_lastname: str
    user_password: str
    user_status: bool
    user_role_id: int
    user_create: int

@app.post("/user/")
async def create_user(user: User):
    try:
        Input_Data = [user.usere_mail, user.user_firstname, user.user_lastname, user.user_password]
        print(Input_Data)
        if any(d == 'none' for d in Input_Data):
            response_data = {"status": "error", "message": "User input data isn't complete"}
            return JSONResponse(content=response_data, status_code=400)
        else:
            pass
        # Check if the email already exists
        sql_check_mail = """SELECT usere_mail FROM user_table WHERE usere_mail = %s"""
        with conn.cursor() as cursor:
            cursor.execute(sql_check_mail, (user.usere_mail,))
            existing_email = cursor.fetchone()

        if existing_email:
            response_data = {"status": "error", "message": "Your email is already exists."}
            return JSONResponse(content=response_data, status_code=400)
        else:
            # Hash password ----------------------
            # converting password to array of bytes 
            password_bytes = user.user_password.encode('utf-8') 
            # generating the salt 
            salt = bcrypt.gensalt() 
            # Hashing the password 
            hash_password = bcrypt.hashpw(password_bytes, salt) 
            hash_password_str = hash_password.decode('utf-8')
            # Insert the new user --------------------------
            insert_sql = """INSERT INTO user_table (usere_mail, user_password, user_firstname, user_lastname, user_status, user_role_id, user_createat, user_create_by)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

            with conn.cursor() as cursor:
                cursor.execute(insert_sql, (user.usere_mail, hash_password_str, user.user_firstname, user.user_lastname, user.user_status, user.user_role_id, datetime.now(), user.user_create))
                conn.commit()

#----------- event -----------------
            event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                           VALUES (%s, %s, %s, %s) """

            with conn.cursor() as cursor:
                cursor.execute(event_sql, ('User Create', 'User added successfully', datetime.now(), user.user_create,))
                conn.commit()
#------------------------------------
            return {"message": "User added successfully", "status": 'true'}

    except Exception as e:
        # Log the error for debugging purposes
        print(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create user")

class UserUpdate(BaseModel):
    user_id: int
    user_email: str
    user_firstname: str
    user_lastname: str
    user_password: str
    user_status: bool
    user_role_id: int
    user_create: int

@app.post("/UpdateUser/" , tags=['USERS MANAGEMENT'])  # Change the decorator to @app.post
async def update_user( updated_user: UserUpdate):
    try:
        # Check if the user exists
        Input_Data = [updated_user.user_email, updated_user.user_firstname, updated_user.user_lastname, updated_user.user_password]
        print(Input_Data)
        if any(d == '' for d in Input_Data):
            response_data = {"status": "error", "message": "User input data isn't complete"}
            return JSONResponse(content=response_data, status_code=400)
        else:
            pass
        sql_check_user = """SELECT * FROM user_table WHERE user_id = %s"""
        with conn.cursor() as cursor:
            cursor.execute(sql_check_user, (updated_user.user_id,))
            existing_user = cursor.fetchone()

        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
    # Hash password ----------------------
        # converting password to array of bytes 
        password_bytes = updated_user.user_password.encode('utf-8') 
        # generating the salt 
        salt = bcrypt.gensalt() 
        # Hashing the password 
        hash_password = bcrypt.hashpw(password_bytes, salt) 
        hash_password_str = hash_password.decode('utf-8')
    # Insert the new user --------------------------
        # Update the user data
        update_sql = """UPDATE user_table
                        SET usere_mail = %s, user_password = %s, user_firstname = %s, user_lastname = %s, user_status = %s, user_role_id = %s, user_updateat = %s
                        WHERE user_id = %s"""

        with conn.cursor() as cursor:
            cursor.execute(
                update_sql,
                (
                    updated_user.user_email,
                    hash_password_str,
                    updated_user.user_firstname,
                    updated_user.user_lastname,
                    updated_user.user_status,
                    updated_user.user_role_id,
                    datetime.now(),  # Assuming you have 'datetime.now()' defined somewhere
                    updated_user.user_id,
                ),
            )
            conn.commit()

        # Log the update event
        event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                       VALUES (%s, %s, %s, %s) """

        with conn.cursor() as cursor:
            cursor.execute(
                event_sql,
                ('User Update', f'User ID {updated_user.user_id} updated successfully', datetime.now(), updated_user.user_create,),
            )
            conn.commit()

        return {"message": f"User ID {updated_user.user_id} updated successfully", "status": 'true'}

    except Exception as e:
        # Log the error for debugging purposes
        print(f"Error updating user: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user",
        )





@app.get("/GetPermission/")
def get_table_data():
    table_name = "user_role_table"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT * 
        FROM user_role_table 
        
        """


    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data

@app.get("/GetUserTable/", tags=['Query_table'])
def get_table_data():
    table_name = "user_table"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""
        SELECT * 
        FROM user_table 
        LEFT JOIN user_role_table ON user_table.user_role_id = user_role_table.user_role_id
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close

    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]

        # ลบคีย์ 'user_password' ออกจากผลลัพธ์
        if "user_password" in row_data:
            del row_data["user_password"]

        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data



@app.get("/GetAlarmTable/" , tags = ['Query_table'])
def get_table_data():
    table_name = "device_alarm_table"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT * 
        FROM device_alarm_table 
        """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close

    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data


@app.get("/GetNotifyTable/" , tags = ['Query_table'])
def get_table_data():
    table_name = "device_notify_table"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT * 
        FROM device_notify_table 
        """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close

    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data



class Notify(BaseModel):
    notifyname: str
    linetoken: str
    notifystatus: bool
    usercreate: int

#return JSONResponse(content=response_data, status_code=400)
@app.post("/InsertNotify/")
async def create_user(Notify: Notify):
    try:
        if Notify.notifyname == 'none':
            response_data = {"status": "error", "message": "Please input your notify name."}
            return JSONResponse(content=response_data, status_code=400)  
        elif Notify.linetoken == '':
            response_data = {"status": "error", "message": "Please input your line token."}
            return JSONResponse(content=response_data, status_code=400)      

        sql_check_token = """SELECT notify_token FROM device_notify_table WHERE notify_token = %s"""
        with conn.cursor() as cursor:
            cursor.execute(sql_check_token, (Notify.linetoken,))
            existing_token = cursor.fetchone()

        if existing_token:
            response_data = {"status": "error", "message": "Your line token is already exists."}
            return JSONResponse(content=response_data, status_code=400)           
        else:
            insert_sql = """INSERT INTO device_notify_table (notify_name, notify_token, notify_status, notify_create_at)
                        VALUES (%s, %s, %s, %s) RETURNING notify_id"""

            with conn.cursor() as cursor:
                cursor.execute(insert_sql, (Notify.notifyname, Notify.linetoken, Notify.notifystatus, datetime.now()))
                notify_id = cursor.fetchone()[0]
                conn.commit()

#----------- event -----------------
            event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                           VALUES (%s, %s, %s, %s) """

            with conn.cursor() as cursor:
                cursor.execute(event_sql, ('Add Notify', 'Notify Token added successfully', datetime.now(), Notify.usercreate,))
                conn.commit()
#------------------------------------

            return {"message": "Notify added successfully", "status": 'true'}
    except Exception as e:
        # Log the error for debugging purposes
        print(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create Notify")




class UpdateNotify(BaseModel):
    notifyid: int
    notifyname: str
    linetoken: str
    notifystatus: bool
    usercreate: int

@app.post("/UpdateNotify/" , tags=['USERS MANAGEMENT'])
async def update_notify(UpdateNotify: UpdateNotify):
    try:
        # Check if the notify exists
        check_notify_sql = """SELECT * FROM device_notify_table WHERE notify_id = %s"""
        with conn.cursor() as cursor:
            cursor.execute(check_notify_sql, (UpdateNotify.notifyid,))
            existing_notify = cursor.fetchone()
        if not existing_notify:
            response_data = {"status": "error", "message": "Notify not found"}
            return JSONResponse(content=response_data, status_code=400)  

        if UpdateNotify.notifyname == 'none':
            response_data = {"status": "error", "message": "Please input your notify name."}
            return JSONResponse(content=response_data, status_code=400)  
        elif UpdateNotify.linetoken == '':
            response_data = {"status": "error", "message": "Please input your line token."}
            return JSONResponse(content=response_data, status_code=400)      

        # Update the notify data
        update_sql = f"""
            UPDATE device_notify_table
            SET notify_name = '{UpdateNotify.notifyname}',
                notify_token = '{UpdateNotify.linetoken}',
                notify_status = '{UpdateNotify.notifystatus}'
            WHERE notify_id = {UpdateNotify.notifyid}
        """

        with conn.cursor() as cursor:
            cursor.execute(update_sql)
            conn.commit()

        # Log the update event
        event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                       VALUES (%s, %s, %s, %s) """

        with conn.cursor() as cursor:
            cursor.execute(
                event_sql,
                ('Update Notify', f'Notify ID {UpdateNotify.notifyid} updated successfully', datetime.now(), UpdateNotify.usercreate,),
            )
            conn.commit()

        return {"message": f"Notify ID {UpdateNotify.notifyid} updated successfully ","status": 'true'}

    except Exception as e:
        # Log the error for debugging purposes
        print(f"Error updating notify: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to update notify")

@app.get("/GetBatch/")
def get_table_data():
    table_name = "device_batch_table"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT * 
        FROM device_batch_table 
        """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close

    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data



class eventReport(BaseModel):
    year: str
    month: str
    
@app.post("/GetSystemEvent/")
async def post_data(eventReport: eventReport):          
    print('year:', eventReport.year)
    print('month:', eventReport.month)
    
    sql_str = f"""SELECT * , TO_CHAR(event_timestamp, 'DD/MM/YYYY HH:MI:SS AM') as event_timestamp,
        event_type,
        event_message,
        event_createby
        FROM public.device_event_table
        INNER JOIN user_table ON device_event_table.event_createby = user_table.user_id
        WHERE EXTRACT(YEAR FROM event_timestamp) = {eventReport.year} AND 
        EXTRACT(MONTH FROM event_timestamp) = {eventReport.month}; 
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    
    if data_start == []:
        NoData = {"status": "No Data"}
        return NoData
    else:
        # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetSystemEvent": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetSystemEvent"].append(row_data)
        return data

class alarm_RecordReport(BaseModel):
    year: str
    month: str

@app.post("/GetAlarmRecord/")
async def post_data(alarm_RecordReport: alarm_RecordReport):
    print('year:', alarm_RecordReport.year)
    print('month:', alarm_RecordReport.month)
    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str_volume = f"""SELECT * 
            FROM device_alarm_volume_record_table 
            INNER JOIN device_alarm_batchtolong_record ON EXTRACT(YEAR FROM device_alarm_volume_record_table.device_alarm_record_timestamp) = {alarm_RecordReport.year} AND 
                                                EXTRACT(MONTH FROM device_alarm_volume_record_table.device_alarm_record_timestamp) = {alarm_RecordReport.month}
            INNER JOIN device_alarm_level_record_table ON (EXTRACT(YEAR FROM device_alarm_batchtolong_record.alarm_batch_record_timestamp) = {alarm_RecordReport.year} AND 
                                                EXTRACT(MONTH FROM device_alarm_batchtolong_record.alarm_batch_record_timestamp) = {alarm_RecordReport.month})
                                                AND (EXTRACT(YEAR FROM device_alarm_level_record_table.device_level_record_timestamp) = {alarm_RecordReport.year} AND 
                                                   EXTRACT(MONTH FROM device_alarm_level_record_table.device_level_record_timestamp) = {alarm_RecordReport.month})
            """
    with conn.cursor() as cursor:
        cursor.execute(sql_str_volume)
        combined_record = cursor.fetchall()
        conn.commit()

    if combined_record == []:
        NoData = {"status": "No Data"}
        return NoData
    else:
        # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetSystemEventAlarm": []}
        for row in combined_record:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetSystemEventAlarm"].append(row_data)
        return data


class Userdetail(BaseModel):
    userId : int
@app.post("/getUserdetail/")
async def post_data(Userdetail: Userdetail):

    table_name = "userData"
    sql_str = f"""SELECT * from user_table where user_id = {Userdetail.userId}
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    print(sql_str)
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data



class AlarmDetail(BaseModel):
    AlarmId : int
@app.post("/getAlarmDetail/")
async def post_data(AlarmDetail: AlarmDetail):

    table_name = "AlarmDetail"
    sql_str = f"""SELECT * from device_alarm_table where device_alarm_id = {AlarmDetail.AlarmId}
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    print(sql_str)
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data



class Notifydetail(BaseModel):
    notifyId : int
@app.post("/getNotifydetail/")
async def post_data(Notifydetail: Notifydetail):

    table_name = "notifyData"
    sql_str = f"""SELECT * from device_notify_table where notify_id = {Notifydetail.notifyId}
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    print(sql_str)
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data
    

class eventAlarmVolume(BaseModel):
    year: str
    month: str
    
@app.post("/GetSystemAlarmVolumeEvent/")
async def post_data(event_alarm_volume :eventAlarmVolume):
    print('year:', event_alarm_volume.year)
    print('month:', event_alarm_volume.month)

    # ใช้ parameterized queries เพื่อป้องกัน SQL injection
    sql_str = """SELECT * , TO_CHAR(device_alarm_record_timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_alarm_record_timestamp,
        volume_alarm_record,
        volume_alarm_volume_high_set,
        volume_alarm_volume_low_set,
        device_alarm_message,
        device_alarm_id
        FROM public.device_alarm_volume_record_table
        WHERE EXTRACT(YEAR FROM device_alarm_record_timestamp) = %s AND 
        EXTRACT(MONTH FROM device_alarm_record_timestamp) = %s; 
    """

    cursor = conn.cursor()
    cursor.execute(sql_str, (event_alarm_volume.year, event_alarm_volume.month))
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()

    if not data_start:
        no_data = {"status": "No Data"}
        return no_data
    else:
        # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetSystemAlarmVolumeEvent": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetSystemAlarmVolumeEvent"].append(row_data)
        return data
    





class EventLevelLevel(BaseModel):
    year: str
    month: str


@app.post("/GetSystemAlarmLevelEvent/")
async def post_data(event_level_volume: EventLevelLevel):
    try:
        print('year:', event_level_volume.year)
        print('month:', event_level_volume.month)

        # Use parameterized queries to prevent SQL injection
        sql_str = """SELECT * , TO_CHAR(device_level_record_timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_level_record_timestamp,
    level_alarm_record,
    level_low_alarm_set,
    device_alarm_message,
    device_alarm_id,
    level_high_alarm_set
FROM public.device_alarm_level_record_table
WHERE EXTRACT(YEAR FROM device_level_record_timestamp) = %s AND 
EXTRACT(MONTH FROM device_level_record_timestamp) = %s; 
        """

        cursor = conn.cursor()
        cursor.execute(sql_str, (event_level_volume.year, event_level_volume.month))
        data_start = cursor.fetchall()
        conn.commit()
        cursor.close()

        if not data_start:
            no_data = {"status": "No Data"}
            return no_data
        else:
            # Create JSON by combining table name and data
            data = {"GetSystemAlarmLevelEvent": []}
            for row in data_start:
                row_data = {}
                for i, column_name in enumerate(cursor.description):
                    row_data[column_name[0]] = row[i]
                row_data["status"] = "ok"
                data["GetSystemAlarmLevelEvent"].append(row_data)
            return data

    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    



class EventBatch(BaseModel):
    year: str
    month: str


@app.post("/GetSystemAlarmBatchEvent/")
async def post_data(event_Batch: EventBatch):
    try:
        print('year:', event_Batch.year)
        print('month:', event_Batch.month)

        # Use parameterized queries to prevent SQL injection
        sql_str = """SELECT * , TO_CHAR(alarm_batch_record_timestamp, 'DD/MM/YYYY HH:MI:SS AM') as alarm_batch_record_timestamp,
    alarm_batch_record_message,
    device_alarm_id,
    device_batch_id,
    alarm_batch_record_value_set,
    alarm_batch_value_record 
FROM public.device_alarm_batchtolong_record
WHERE EXTRACT(YEAR FROM alarm_batch_record_timestamp) = %s AND 
EXTRACT(MONTH FROM alarm_batch_record_timestamp) = %s; 
        """

        cursor = conn.cursor()
        cursor.execute(sql_str, (event_Batch.year, event_Batch.month))
        data_start = cursor.fetchall()
        conn.commit()
        cursor.close()

        if not data_start:
            no_data = {"status": "No Data"}
            return no_data
        else:
            # Create JSON by combining table name and data
            data = {"GetSystemAlarmBatchEvent": []}
            for row in data_start:
                row_data = {}
                for i, column_name in enumerate(cursor.description):
                    row_data[column_name[0]] = row[i]
                row_data["status"] = "ok"
                data["GetSystemAlarmBatchEvent"].append(row_data)
            return data

    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    
@app.get("/GetBatchCompareDataBarChart/")
def get_table_data():
    table_name = "GetBatchCompareDataBarChart"
    sql_str = """
    SELECT device_batch_id, device_batch_start_time, device_batch_end_time, device_batch_value 
    FROM device_batch_table 
    ORDER BY device_batch_id DESC 
    OFFSET 1 ROWS 
    FETCH FIRST 14 ROWS ONLY;
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    cursor.close()
    conn.close()
    
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data


class DeleteUser(BaseModel):
    userID: int
    UserDeleteBy: int

@app.post("/DeleteUserID/")
async def post_data(deleteUser: DeleteUser):
    try:
        print('user ID:', deleteUser.userID)

        sql_del_User = """
            DELETE FROM user_table
            WHERE user_id = %s;
        """

        cursor = conn.cursor()
        cursor.execute(sql_del_User, (deleteUser.userID,))
        conn.commit()
        cursor.close()

        event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                       VALUES (%s, %s, %s, %s) """

        with conn.cursor() as cursor:
            cursor.execute(
                event_sql,
                ('User Delete', f'User ID {deleteUser.userID} Delete successfully', datetime.now(), deleteUser.UserDeleteBy,),
            )
            conn.commit()

        return {"message": f"User ID{deleteUser.userID} Delete successfully", "status": 'true'}


        

    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    
class DeleteAlarm(BaseModel):
    alarmID: int
    AlarmDeleteID : int

@app.post("/DeleteAlarmID/")
async def post_data(deleteAlarm: DeleteAlarm):
    try:
        print('alarm ID:', deleteAlarm.alarmID)

        sql_del_Alarm = """
            DELETE FROM device_alarm_table
            WHERE device_alarm_id = %s;
        """

        cursor = conn.cursor()
        cursor.execute(sql_del_Alarm, (deleteAlarm.alarmID,))
        conn.commit()
        cursor.close()

        event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                       VALUES (%s, %s, %s, %s) """

        with conn.cursor() as cursor:
            cursor.execute(
                event_sql,
                ('Alarm Delete', f'Alarm ID {deleteAlarm.alarmID} Delete successfully', datetime.now(), deleteAlarm.AlarmDeleteID,),
            )
            conn.commit()

        return {"message": f"Alarm ID {deleteAlarm.alarmID} Delete successfully", "status": 'true'}


    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    
class DeleteNotify(BaseModel):
    notifyID: int
    notifyDelete: int

@app.post("/DeleteNotifyID/")
async def post_data(deleteNotify: DeleteNotify):
    try:
        print('notify ID:', deleteNotify.notifyID)

        sql_del_Notify = """
            DELETE FROM device_notify_table
            WHERE notify_id = %s;
        """

        cursor = conn.cursor()
        cursor.execute(sql_del_Notify, (deleteNotify.notifyID,))
        conn.commit()
        cursor.close()


        event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                       VALUES (%s, %s, %s, %s) """

        with conn.cursor() as cursor:
            cursor.execute(
                event_sql,
                ('Notify Delete', f'Notify ID {deleteNotify.notifyID} Delete successfully', date_now, deleteNotify.notifyDelete,),
            )
            conn.commit()

        return {"message": f"Notify {deleteNotify.notifyID} Delete successfully", "status": 'true'}

        

    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    


    
@app.get("/GetReportDataTable/")
def get_table_data():
    table_name = "device_report_table"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT * FROM device_report_table INNER JOIN device_batch_table ON device_report_table.device_batch_id=device_batch_table.device_batch_id
        """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close

    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data


@app.get("/GetAlarmCount/")
def get_table_data():
    table_name = "GetAlarmCount"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT SUM(total_records) AS total_count
FROM (
    SELECT COUNT(*) AS total_records
    FROM device_alarm_batchtolong_record where alarm_btl_saw = false
    UNION ALL
    SELECT COUNT(*) AS total_records
    FROM device_alarm_level_record_table where alarm_level_saw = false 
    UNION ALL
    SELECT COUNT(*) AS total_records
    FROM device_alarm_volume_record_table where alarm_volume_saw = false
) AS subquery;
        """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close

    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data





@app.get("/GetEventCount/")
def get_table_data():
    table_name = "GetEventCount"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT SUM(total_records) AS total_count
FROM (
    SELECT COUNT(*) AS total_records
    FROM device_event_table where event_saw = false
) AS subquery;
        """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close

    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data


class PostEvent(BaseModel):
    EventChange: bool

@app.post("/UpdateEvent/")
async def post_data(postevent: PostEvent):
    try:
        print('UpdateEvent:', postevent.EventChange)

        sql_update_notify = """
            UPDATE device_alarm_volume_record_table
            SET alarm_volume_saw = {};

            UPDATE device_alarm_level_record_table
            SET alarm_level_saw = {};

            UPDATE device_alarm_batchtolong_record
            SET alarm_btl_saw = {};
        """.format(postevent.EventChange, postevent.EventChange, postevent.EventChange)

        cursor = conn.cursor()
        cursor.execute(sql_update_notify)
        conn.commit()
        cursor.close()

    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    




    
class PostEventSystem(BaseModel):
    EventChangeSystem: bool

@app.post("/UpdateEventSystem/")
async def post_data(PostEventSystem: PostEventSystem):
    try:
        print('UpdateEvent:', PostEventSystem.EventChangeSystem)

        sql_update_notify = """
            UPDATE device_event_table
            SET event_saw = {};

        """.format(PostEventSystem.EventChangeSystem)

        cursor = conn.cursor()
        cursor.execute(sql_update_notify)
        conn.commit()
        cursor.close()

    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    
    

@app.get("/GetNotifyall/")
def get_table_data():
    table_name = "device_notify_table"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT * FROM device_notify_table 
        """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close

    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)

    return data



class Report(BaseModel):
    year: str
    month: str
    day: str
    page: int  # เพิ่มพารามิเตอร์สำหรับหน้า
    limit: int  # เพิ่มพารามิเตอร์สำหรับจำนวนข้อมูลต่อหน้า

@app.post("/GetTankHistoryData_DAYtest/")
async def post_data(report: Report):
    print('year:', report.year)
    print('month:', report.month)
    print('day:', report.day)

    
    offset = (report.page - 1) * report.limit  # คำนวณ offset ของข้อมูล
    
    sql_str = f"""SELECT * ,TO_CHAR(device_timestamp::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp,
        TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
        TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
        TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
        TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time
        FROM device_raw_data
        INNER JOIN device_table ON device_table.device_id = device_raw_data.device_id
        WHERE EXTRACT(YEAR FROM device_timestamp) = {report.year} AND 
        EXTRACT(MONTH FROM device_timestamp) = {report.month} AND
        EXTRACT(DAY FROM device_timestamp) = {report.day}
        LIMIT {report.limit} OFFSET {offset} ; 
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    
    if not data_start:
        NoData = {"No Data"}
        return NoData
    else:
        # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetTankHistoryData_DAY": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetTankHistoryData_DAY"].append(row_data)
        return data


class Item(BaseModel):
    name: str
    password: str

@app.post("/logintest/")
async def post_data(item: Item):
    try:
        query = "SELECT usere_mail, user_password, user_id, user_status, user_bancount FROM public.user_table_test WHERE usere_mail = %s;"
        with conn.cursor() as cursor:
            cursor.execute(query, (item.name,))
            user_data = cursor.fetchone()

        if user_data:
            user_mail, user_password, user_id, user_status, ban_count = user_data
            input_pw = item.password
            hash_pw = user_password
            # ทำการอัปเดตข้อมูล (ตัวอย่างเท่านั้น คุณอาจต้องปรับเปลี่ยนตามความต้องการ)
            update_query = "UPDATE user_table_test SET user_lastlogin = CURRENT_TIMESTAMP WHERE user_id = %s;"
            with conn.cursor() as cursor:
                cursor.execute(update_query, (user_id,))
                conn.commit()
                cursor.execute("SELECT user_lastlogin, user_bannedtime FROM user_table_test WHERE user_id = %s;", (user_id,))
                last_login_timedata = cursor.fetchone()
                login_time, ban_time_at = last_login_timedata
                cursor.close()

            #สร้างช่วงเวลาที่จะปลดแบนตามต้องการ(เปลี่ยนได้ตามต้องการ)
            bantime = timedelta(days=1)

            # เช็ค ban_time_at เป็น null หรือไม่ หากไม่ให้แปลงค่าเตรียมใช้ในการตรวจสอบ
            if ban_time_at is not None:
                bantime_check = login_time - ban_time_at
                bantime_show = bantime - bantime_check

                # ตรวจสอบว่าเวลานับถอยหลังมีค่าติดลบหรือไม่
                if bantime_show.total_seconds() < 0:
                    bantime_show = timedelta(seconds=0)

                # แปลง bantime_check เป็นวินาที
                bantime_total_seconds = int(bantime_show.total_seconds())

                # คำนวณหาชั่วโมง, นาที, และวินาที
                hours, remainder = divmod(bantime_total_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)

                # แสดงผลเป็น H:M:S
                banned_time_show = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                print(banned_time_show)

            # ตรวจเวลาว่าเลยเวลาโดน ban แล้วหรือไม่
            if ban_time_at == None:
                pass
            elif (bantime_check > bantime) :
                user_status = True
                with conn.cursor() as cursor:
                    cursor.execute("UPDATE user_table_test SET user_status = %s WHERE user_id = %s", (True, user_id,))
                    conn.commit()
            else:
                pass

            if user_status :
                if bcrypt.checkpw(input_pw.encode('utf-8'), hash_pw.encode('utf-8')):
                    #----------- event -----------------
                    event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                                VALUES (%s, %s, %s, %s) """
                    with conn.cursor() as cursor:
                        cursor.execute(event_sql, ('User Login', 'User Has Logging In', datetime.now(), user_id,))
                        conn.commit()
                    #------------------------------------
                    with conn.cursor() as cursor:
                        cursor.execute("UPDATE user_table_test SET user_bancount = %s, user_bannedtime = NULL WHERE user_id = %s", (0, user_id,))
                        conn.commit()
                    with conn.cursor() as cursor:
                        cursor.execute(f"SELECT * FROM user_table_test INNER JOIN user_role_table ON user_table_test.user_role_id=user_role_table.user_role_id where usere_mail = '{user_mail}' ;")
                        data_user = cursor.fetchall()
                        conn.commit()

                    table_name = "user_table_test"
                    data = {table_name: []}
                    for row in data_user:
                        row_data = {}
                        for i, column_name in enumerate(cursor.description):
                            row_data[column_name[0]] = row[i]
                        row_data["status"] = "ok"
                        data[table_name].append(row_data)
                    
                    return data
                else:
                    if (ban_count+1) > 2:
                        with conn.cursor() as cursor:
                            cursor.execute("UPDATE user_table_test SET user_status = %s, user_bancount = %s, user_bannedtime = CURRENT_TIMESTAMP WHERE user_id = %s", (False ,0 ,user_id,))
                            conn.commit()
                        raise HTTPException(status_code=400, detail="Invalid too much, You are Banned")
                    else:
                        with conn.cursor() as cursor:
                            cursor.execute("UPDATE user_table_test SET user_bancount = %s WHERE user_id = %s", ((ban_count+1) ,user_id,))
                            conn.commit()
                        raise HTTPException(status_code=400, detail="Invalid username or password")
            else:
                raise HTTPException(status_code=400, detail=f"You are Banned: {banned_time_show}")
        else:
            raise HTTPException(status_code=400, detail="User not found")
    finally:
        print("end")


        

        
class Report(BaseModel):
    year: str
    month: str
    day: str 
@app.post("/GetTankHistoryData_DAY_AVG/")
async def post_data(report: Report):          
    print('year:', report.year)
    print('month:', report.month)
    print('day:', report.day)
    
    sql_str = f"""SELECT * ,TO_CHAR(device_timestamp_avg::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp_avg,
        TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
        TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
        TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
        TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time
        FROM device_rawdata_avg
        INNER JOIN device_table ON device_table.device_id = device_rawdata_avg.device_id
        WHERE device_timestamp_avg BETWEEN 
        TIMESTAMP '{report.year}-{report.month}-{report.day} 00:00:00' AND 
        TIMESTAMP '{report.year}-{report.month}-{report.day} 23:59:59';
        
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
        # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetTankHistoryData_DAY": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetTankHistoryData_DAY"].append(row_data)
        return data
    
class Report(BaseModel):
    year: str
    month: str
    day: str 
@app.post("/GetTankHistoryData_DAY_AVG/")
async def post_data(report: Report):          
    print('year:', report.year)
    print('month:', report.month)
    print('day:', report.day)
    
    sql_str = f"""SELECT * ,TO_CHAR(device_timestamp_avg::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp_avg,
        TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
        TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
        TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
        TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time
        FROM device_rawdata_avg
        INNER JOIN device_table ON device_table.device_id = device_rawdata_avg.device_id
        WHERE device_timestamp_avg BETWEEN 
        TIMESTAMP '{report.year}-{report.month}-{report.day} 00:00:00' AND 
        TIMESTAMP '{report.year}-{report.month}-{report.day} 23:59:59';
        
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    if(data_start == []):
        NoData = {"No Data"}
        return NoData
    else:
        # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
        data = {"GetTankHistoryData_DAY": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetTankHistoryData_DAY"].append(row_data)
        return data
@app.get("/GetRealTimetest22/")
def get_table_data():
    table_name = "device_batch_table"

    # สร้างคำสั่ง SQL เพื่อดึงข้อมูลจากตารางที่มีชื่อตารางที่คุณกำหนด
    sql_str = f"""SELECT * ,TO_CHAR(device_batch_datetime, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_datetime,
        TO_CHAR(device_batch_start_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_start_time,
        TO_CHAR(device_batch_end_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_end_time
        FROM device_batch_table order by device_batch_id desc limit 1
        
        """
    get_to_cal_duration = f"SELECT device_batch_datetime, device_batch_start_time FROM device_batch_table ORDER BY device_batch_id DESC LIMIT 1 ;"

    with conn.cursor() as cursor:
        cursor.execute(get_to_cal_duration)
        get_duration = cursor.fetchone()
        cursor.execute(sql_str)
        data_start = cursor.fetchall()
        conn.commit()
    conn.close

    #cal to get realtime batch duration
    realtime, starttime = get_duration
    duration = realtime - starttime
    # คำนวณจำนวนชั่วโมงและนาที
    hours, remainder = divmod(int(duration.total_seconds() / 60), 60)
    minutes = remainder
    # สร้าง string ในรูปแบบที่ต้องการ
    duration_time = "{:02d} Hr {:02d} Min".format(hours, minutes)
    
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["device_batch_duration"] = duration_time
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data




# *****************  ISO PART 2 ************* #

# Test connect websocket
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()  # ยอมรับการเชื่อมต่อจาก client
    print("WebSocket connected!")  # แสดงข้อความเมื่อมีการเชื่อมต่อ
    try:
        while True:
            data = await websocket.receive_text()  # รอรับข้อความจาก client
            print(f"Received from client: {data}")
            await websocket.send_text(f"Message received: {data}")
    except Exception as e:
        print(f"WebSocket Error: {e}")
    finally:
        print("WebSocket disconnected")


cursor = conn.cursor()


last_sent_values = {
    "device_batch_status": None,
    "device_distance": None,
    "device_tank_volume": None,
    "device_timestamp": None,
    "reset_count": None
}

@app.websocket("/ws_query")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # รอรับข้อความจาก frontend
            message = await websocket.receive_text()
            print(f"Message received from frontend: {message}")  # Debug: ตรวจสอบข้อความที่ frontend ส่งมา
            if message == "get_device_data":
                # Query database
                with conn.cursor() as cursor:
                    try:
                        cursor.execute("""
                            SELECT raw_id, device_batch_status, device_distance, 
                                   device_tank_volume, device_timestamp, device_id, reset_count
                            FROM public.device_raw_data ORDER BY raw_id DESC LIMIT 1
                        """)
                        row = cursor.fetchone()
                        print(f"Database query result: {row}")  # Debug: ตรวจสอบค่าที่ query จาก database
                        
                        if row:
                            data = {
                                "device_id": row[5],
                                "device_batch_status": row[1],
                                "device_distance": row[2],
                                "device_tank_volume": row[3],
                                "device_timestamp": row[4].isoformat(),
                                "reset_count": row[6]
                            }

                            # ตรวจจับการเปลี่ยนแปลง
                            if data["device_batch_status"] != last_sent_values["device_batch_status"] or \
                               data["device_distance"] != last_sent_values["device_distance"] or \
                               data["device_tank_volume"] != last_sent_values["device_tank_volume"] or \
                               data["device_timestamp"] != last_sent_values["device_timestamp"] or \
                               data["reset_count"] != last_sent_values["reset_count"]:
                                
                                # อัปเดต cache
                                last_sent_values.update(data)
                                
                                # ส่งข้อมูล JSON กลับไปยัง frontend
                                await websocket.send_text(json.dumps(data))
                        else:
                            print("No data found in database")
                    except Exception as e:
                        print(f"Database query error: {e}")
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        print("Disconnected from client")



# class PaginationRequest(BaseModel):
#     page: int  
#     limit: int  

# @app.post("/GetTankData_Pagination/")
# async def post_data(report: PaginationRequest):
#     # คำนวณค่า offset สำหรับการแบ่งหน้า
#     offset = (report.page - 1) * report.limit

#     # การเขียน SQL Query โดยใช้ parameterized query เพื่อป้องกัน SQL Injection
#     sql_str = """
#     SELECT *, 
#            TO_CHAR(device_timestamp::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp,
#            TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
#            TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
#            TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
#            TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time
#     FROM device_raw_data
#     INNER JOIN device_table ON device_table.device_id = device_raw_data.device_id
#     LIMIT %s OFFSET %s;
#     """
    
#     # คำสั่งสำหรับคำนวณจำนวนข้อมูลทั้งหมด
#     count_sql = "SELECT COUNT(*) FROM device_raw_data INNER JOIN device_table ON device_table.device_id = device_raw_data.device_id"

#     try:
#         cursor = conn.cursor()
        
#         # คำนวณจำนวนทั้งหมด
#         cursor.execute(count_sql)
#         total_count = cursor.fetchone()[0]  # ดึงจำนวนแถวทั้งหมด

#         # ดึงข้อมูลตามหน้า
#         cursor.execute(sql_str, (report.limit, offset))  # ใช้ parameterized query
#         data_start = cursor.fetchall()
        
#         if not data_start:
#             return {"NoData": "ไม่พบข้อมูล"}  # ส่งคืนข้อความหากไม่มีข้อมูล

#         # ดึงชื่อคอลัมน์จาก cursor description
#         columns = [desc[0] for desc in cursor.description]

#         # สร้างข้อมูลผลลัพธ์
#         data = {
#             "GetTankHistoryData_DAY": [],
#             "total_count": total_count  # ส่งจำนวนข้อมูลทั้งหมดด้วย
#         }
#         for row in data_start:
#             row_data = {columns[i]: row[i] for i in range(len(row))}  # แผนที่ชื่อคอลัมน์กับข้อมูลในแถว
#             row_data["status"] = "ok"
#             data["GetTankHistoryData_DAY"].append(row_data)

#         return data
    
#     except Exception as e:
#         # จัดการข้อผิดพลาดจากฐานข้อมูลหรือ query
#         return {"error": str(e)}
#     finally:
#         cursor.close()  # ปิด cursor ทุกครั้ง


class PaginationRequest(BaseModel):
    page: int  
    limit: int  
    start_date: Optional[str] = None  # เพิ่ม start_date (Optional)
    stop_date: Optional[str] = None  # เพิ่ม stop_date (Optional)

@app.post("/GetTankData_Pagination/" , tags = ['Query_table'])
async def post_data(report: PaginationRequest):
    # คำนวณค่า offset สำหรับการแบ่งหน้า
    offset = (report.page - 1) * report.limit

    # เพิ่มเงื่อนไขกรองช่วงวันที่
    where_clause = ""
    params = []
    
    if report.start_date and report.stop_date:
        where_clause = "WHERE device_timestamp BETWEEN %s AND %s"
        params.extend([report.start_date, report.stop_date])

    # การเขียน SQL Query โดยใช้ parameterized query เพื่อป้องกัน SQL Injection
    sql_str = f"""
    SELECT *, 
           TO_CHAR(device_timestamp::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_timestamp,
           TO_CHAR(device_lastdata::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_lastdata,
           TO_CHAR(device_create_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_create_at,
           TO_CHAR(device_update_at::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_update_at,
           TO_CHAR(device_batch_time::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_time
    FROM device_raw_data
    INNER JOIN device_table ON device_table.device_id = device_raw_data.device_id
    {where_clause}
    LIMIT %s OFFSET %s;
    """

    # คำสั่งสำหรับคำนวณจำนวนข้อมูลทั้งหมด
    count_sql = f"""
    SELECT COUNT(*) 
    FROM device_raw_data 
    INNER JOIN device_table ON device_table.device_id = device_raw_data.device_id 
    {where_clause}
    """

    try:
        cursor = conn.cursor()
        
        # คำนวณจำนวนทั้งหมด
        cursor.execute(count_sql, params)
        total_count = cursor.fetchone()[0]  # ดึงจำนวนแถวทั้งหมด

        # เพิ่ม limit และ offset ใน params
        params.extend([report.limit, offset])

        # ดึงข้อมูลตามหน้า
        cursor.execute(sql_str, params)  # ใช้ parameterized query
        data_start = cursor.fetchall()
        
        if not data_start:
            return {"NoData": "ไม่พบข้อมูล"}  # ส่งคืนข้อความหากไม่มีข้อมูล

        # ดึงชื่อคอลัมน์จาก cursor description
        columns = [desc[0] for desc in cursor.description]

        # สร้างข้อมูลผลลัพธ์
        data = {
            "GetTankHistoryData_DAY": [],
            "total_count": total_count  # ส่งจำนวนข้อมูลทั้งหมดด้วย
        }
        for row in data_start:
            row_data = {columns[i]: row[i] for i in range(len(row))}  # แผนที่ชื่อคอลัมน์กับข้อมูลในแถว
            row_data["status"] = "ok"
            data["GetTankHistoryData_DAY"].append(row_data)

        return data
    
    except Exception as e:
        # จัดการข้อผิดพลาดจากฐานข้อมูลหรือ query
        return {"error": str(e)}
    finally:
        cursor.close()  # ปิด cursor ทุกครั้ง





def query_database(sql_query, params=None):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result_list = [dict(zip(columns, row)) for row in rows]
        cursor.close()
        return result_list
    except Exception as e:
        print(f"Error: {e}")
        return None

# ฟังก์ชัน query ข้อมูลจากตาราง
def execute_query_table(tablename, columtimestemp, filter, data, sampling, startdata, stopdate):
    sql = ""
    
    if data and filter == "last":
        data_duration = {
            "30 Minutes": "(NOW() AT TIME ZONE 'UTC' + INTERVAL '7 HOURS') - INTERVAL '30 MINUTES'",
            "1 Hour": "(NOW() AT TIME ZONE 'UTC' + INTERVAL '7 HOURS') - INTERVAL '1 HOURS'",
            "5 Hour": "(NOW() AT TIME ZONE 'UTC' + INTERVAL '7 HOURS') - INTERVAL '5 HOURS'",
            "10 Hour": "(NOW() AT TIME ZONE 'UTC' + INTERVAL '7 HOURS') - INTERVAL '10 HOURS'",
            "24 Hour": "(NOW() AT TIME ZONE 'UTC' + INTERVAL '7 HOURS') - INTERVAL '24 HOURS'"
        }.get(data)
        
        if data_duration:
            sql = f"""
                    SELECT *
                    FROM public.{tablename}
                    WHERE {columtimestemp} >= {data_duration}
                """
        else:
            raise HTTPException(status_code=400, detail="Invalid filter value provided.")
    
    elif data and filter == "interval":
        data_duration = {
            "To Day": "CURRENT_DATE",
            "Week": "CURRENT_DATE - INTERVAL '7 days'",
            "Month": "CURRENT_DATE - INTERVAL '1 month'"
        }.get(data)
        
        if data_duration:
            sql = f"""
                    SELECT *
                    FROM public."{tablename}"
                    WHERE "{columtimestemp}"::date >= {data_duration};
                """
        else:
            raise HTTPException(status_code=400, detail="Invalid filter value provided.")
    
    elif filter == "period":
        # ตรวจสอบว่า startdata และ stopdate เป็นวันที่ที่ถูกต้อง
        try:
            start_time = datetime.strptime(startdata, '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(stopdate, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD HH:MM:SS'.")
        
        sql = f"""
                SELECT *
                FROM public."{tablename}"
                WHERE "{columtimestemp}"::timestamp BETWEEN '{start_time}' AND '{end_time}';
            """
    
    raw_data = query_database(sql)
    
    # การทำ sample ข้อมูล
    if sampling:
        sampling_duration = {
            "30 Min": timedelta(minutes=30),
            "1 Hour": timedelta(hours=1),
            "5 Hour": timedelta(hours=5),
            "10 Hour": timedelta(hours=10),
            "1 Day": timedelta(days=1)
        }.get(sampling)
        
        if not sampling_duration:
            raise ValueError(f"Invalid sampling: {sampling}")
        
        # เรียงข้อมูลตามเวลา
        raw_data = sorted(raw_data, key=lambda x: x['device_timestamp'])
        
        grouped_data = []
        current_group = []
        start_time = raw_data[0]['device_timestamp'] if raw_data else None
        
        for record in raw_data:
            if record['device_timestamp'] < start_time + sampling_duration:
                current_group.append(record)
            else:
                grouped_data.append(current_group)
                current_group = [record]
                start_time = record['device_timestamp']
        
        if current_group:
            grouped_data.append(current_group)

        # คำนวณค่าเฉลี่ย
        averaged_data = []
        for group in grouped_data:
            avg_distance = sum(r['device_distance'] for r in group) / len(group)
            avg_tank_volume = sum(r['device_tank_volume'] for r in group) / len(group)
            averaged_data.append({
                "start_time": group[0]['device_timestamp'].isoformat(),
                "end_time": group[-1]['device_timestamp'].isoformat(),
                "avg_device_distance": avg_distance,
                "avg_device_tank_volume": avg_tank_volume
            })
        return averaged_data
    
    return raw_data

# Model สำหรับการรับค่าจาก API
class QueryRequest(BaseModel):
    tablename: str
    columtimestemp: str
    filter: Optional[str] = None
    data: Optional[str] = None
    sampling: Optional[str] = None
    startdata: Optional[str] = None
    stopdate: Optional[str] = None

# API endpoint
@app.post("/query-table", tags=["Query_table"])
async def query_table(query: QueryRequest):
    tablename = query.tablename
    columtimestemp = query.columtimestemp
    filter = query.filter
    data = query.data
    sampling = query.sampling
    startdata = query.startdata
    stopdate = query.stopdate

    # ตรวจสอบว่า startdata และ stopdate ถูกต้องหรือไม่
    if startdata and stopdate:
        try:
            start_dt = datetime.fromisoformat(startdata)
            stop_dt = datetime.fromisoformat(stopdate)
            if start_dt > stop_dt:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error": "Invalid time range",
                        "details": f"startdata ({startdata}) must not be greater than stopdate ({stopdate})"
                    }
                )
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Invalid datetime format",
                    "details": str(e)
                }
            )
    


    # เรียกใช้ฟังก์ชัน execute_query_table
    try:
        result = execute_query_table(tablename, columtimestemp, filter, data, sampling, startdata, stopdate)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to execute query",
                "details": str(e)
            }
        )


class QueryRequest(BaseModel):
    filterType: str
    data: str
    startTime: str
    stoptime: str
    samplingInterval: str

# Endpoint สำหรับ query ข้อมูล
@app.post("/query_data_consumption", tags=["Query_table"])
def get_consumption_data(request: QueryRequest):

    logger.info(f"Received request: {request.dict()}")

    try:
        # ตรวจสอบรูปแบบวันที่
        try:
            # แปลงจากรูปแบบ "yyyy-MM-ddThh:mm" เป็น "yyyy-MM-dd HH:mm:00"
            start_time = datetime.strptime(request.startTime + ":00", "%Y-%m-%dT%H:%M:%S")
            stop_time = datetime.strptime(request.stoptime + ":00", "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")

        # Query ข้อมูลจากฐานข้อมูล
        cursor = conn.cursor()
        query = """
            SELECT consumption_id, consumption_volume, consumption_timestamp, device_id
            FROM public.device_consumption_data
            WHERE consumption_timestamp BETWEEN %s AND %s
        """
        cursor.execute(query, (start_time, stop_time))
        rows = cursor.fetchall()
        cursor.close()

        # แปลงผลลัพธ์เป็น JSON
        response = [
            {
                "consumption_id": row[0],
                "consumption_volume": row[1],
                "consumption_timestamp": row[2],
                "device_id": row[3],
            }
            for row in rows
        ]

        return {"status": "success", "data": response}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")




class DateReport(BaseModel):
    StartDate: Optional[str] = None  # อนุญาตให้เป็น None ได้
    StopDate: Optional[str] = None

@app.post("/GetBatchHistoryData/", tags=["Query_table"])
async def post_data(MonthReport: DateReport):
    try:
        # ตรวจสอบว่ามีค่า StartDate และ StopDate หรือไม่
        if MonthReport.StartDate and MonthReport.StopDate:
            # แปลงวันที่จาก JSON ที่รับเข้ามาให้เป็น datetime object
            start_time = datetime.strptime(MonthReport.StartDate, "%Y-%m-%dT%H:%M")
            stop_time = datetime.strptime(MonthReport.StopDate, "%Y-%m-%dT%H:%M")
            
            # สร้างเงื่อนไขการกรองข้อมูลตามช่วงเวลา
            date_condition = f"""
                device_batch_datetime >= '{start_time.strftime('%Y-%m-%d %H:%M')}' AND 
                device_batch_datetime <= '{stop_time.strftime('%Y-%m-%d %H:%M')}'
            """
        else:
            # ถ้าไม่มีค่า StartDate และ StopDate ให้ query ทั้งหมด
            date_condition = "1=1"  # เงื่อนไขที่เป็นจริงเสมอ

        # SQL Query โดยใช้ช่วงเวลา
        sql_str = f"""
        SELECT 
            *, 
            TO_CHAR(device_batch_datetime, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_datetime,
            TO_CHAR(device_batch_start_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_start_time,
            TO_CHAR(device_batch_end_time, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_end_time,
            TO_CHAR(device_batch_history_createat::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_history_createat,
            TO_CHAR(device_batch_history_updateat::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_history_updateat,
            TO_CHAR(device_batch_history_start::timestamp, 'DD/MM/YYYY HH:MI:SS AM') as device_batch_history_start
        FROM 
            public.device_batch_table
        INNER JOIN 
            device_batch_history_table 
        ON 
            device_batch_table.device_id = device_batch_history_table.device_id
        WHERE 
            {date_condition};
        """
        print(sql_str)

        cursor = conn.cursor()
        cursor.execute(sql_str)
        data_start = cursor.fetchall()
        conn.commit()
        cursor.close()

        if not data_start:
            return {"message": "No Data"}

        # สร้าง JSON โดยรวมข้อมูลทั้งหมด
        data = {"GetBatchHistoryData": []}
        for row in data_start:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetBatchHistoryData"].append(row_data)

        return data
    except Exception as e:
        return {"error": str(e)}




class DateReport(BaseModel):
    StartDate: Optional[str] = None  # อนุญาตให้เป็น None ได้
    StopDate: Optional[str] = None

@app.post("/GetDeviceEventData/", tags=["Query_table"])
async def get_device_event_data(MonthReport: DateReport):
    try:
        # ตรวจสอบว่ามีค่า StartDate และ StopDate หรือไม่
        if MonthReport.StartDate and MonthReport.StopDate:
            # แปลงวันที่จาก JSON ที่รับเข้ามาให้เป็น datetime object
            start_time = datetime.strptime(MonthReport.StartDate, "%Y-%m-%dT%H:%M")
            stop_time = datetime.strptime(MonthReport.StopDate, "%Y-%m-%dT%H:%M")
            
            # สร้างเงื่อนไขการกรองข้อมูลตามช่วงเวลา
            date_condition = f"""
                event_timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M')}' AND 
                event_timestamp <= '{stop_time.strftime('%Y-%m-%d %H:%M')}'
            """
        else:
            # ถ้าไม่มีค่า StartDate และ StopDate ให้ query ทั้งหมด
            date_condition = "1=1"  # เงื่อนไขที่เป็นจริงเสมอ

        # SQL Query สำหรับ device_event_table
        sql_str = f"""
        SELECT 
            event_id, 
            event_type, 
            event_message, 
            TO_CHAR(event_timestamp, 'DD/MM/YYYY HH:MI:SS AM') as event_timestamp,
            event_createby, 
            event_saw
        FROM 
            public.device_event_table
        WHERE 
            {date_condition}
        ORDER BY 
            event_timestamp ASC;
        """
        print(sql_str)

        cursor = conn.cursor()
        cursor.execute(sql_str)
        event_data = cursor.fetchall()
        conn.commit()
        cursor.close()

        if not event_data:
            return {"message": "No Data"}

        # สร้าง JSON โดยรวมข้อมูลทั้งหมด
        data = {"GetDeviceEventData": []}
        for row in event_data:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            row_data["status"] = "ok"
            data["GetDeviceEventData"].append(row_data)

        return data
    except Exception as e:
        return {"error": str(e)}




class DateReport(BaseModel):
    StartDate: Optional[str] = None
    StopDate: Optional[str] = None

@app.post("/GetDeviceAlarmData/", tags=["Query_table"])
async def get_device_alarm_data(MonthReport: DateReport):
    try:
        # ตรวจสอบว่ามี StartDate และ StopDate หรือไม่
        if MonthReport.StartDate and MonthReport.StopDate:
            # แปลงวันที่จากรูปแบบ JSON เป็น datetime object
            start_time = datetime.strptime(MonthReport.StartDate, "%Y-%m-%dT%H:%M")
            stop_time = datetime.strptime(MonthReport.StopDate, "%Y-%m-%dT%H:%M")
            
            # เงื่อนไขการกรองข้อมูล
            date_condition = f"""
                device_check_start_time >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND 
                device_check_stop_time <= '{stop_time.strftime('%Y-%m-%d %H:%M:%S')}'
            """
        else:
            # ดึงข้อมูลทั้งหมด
            date_condition = "1=1"

        # SQL Query
        sql_str = f"""
        SELECT 
          *
        FROM 
            public.device_alarm_table
        WHERE 
            {date_condition};
        """
        print(sql_str)  # สำหรับ Debug

        # เรียกใช้งาน SQL
        cursor = conn.cursor()
        cursor.execute(sql_str)
        result = cursor.fetchall()
        conn.commit()
        cursor.close()

        # ตรวจสอบผลลัพธ์
        if not result:
            return {"message": "No Data Found"}

        # สร้าง JSON ตอบกลับ
        data = {"DeviceAlarmData": []}
        for row in result:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            data["DeviceAlarmData"].append(row_data)

        return data

    except Exception as e:
        return {"error": str(e)}





@app.post("/GetDeviceAlarmVolumeRecordData/", tags=["Query_table"])
async def get_device_alarm_volume_record_data(MonthReport: DateReport):
    try:
        # ตรวจสอบว่า StartDate และ StopDate มีค่าหรือไม่
        if MonthReport.StartDate and MonthReport.StopDate:
            # แปลงวันที่จากรูปแบบ JSON เป็น datetime object
            start_time = datetime.strptime(MonthReport.StartDate, "%Y-%m-%dT%H:%M")
            stop_time = datetime.strptime(MonthReport.StopDate, "%Y-%m-%dT%H:%M")
            
            # สร้างเงื่อนไขการกรองข้อมูล
            date_condition = f"""
                device_alarm_record_timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND 
                device_alarm_record_timestamp <= '{stop_time.strftime('%Y-%m-%d %H:%M:%S')}'
            """
        else:
            # ดึงข้อมูลทั้งหมด
            date_condition = "1=1"

        # SQL Query
        sql_str = f"""
        SELECT 
          *
        FROM 
            public.device_alarm_volume_record_table
        WHERE 
            {date_condition};
        """
        print(sql_str)  # สำหรับ Debug

        # เรียกใช้งาน SQL
        cursor = conn.cursor()
        cursor.execute(sql_str)
        result = cursor.fetchall()
        conn.commit()
        cursor.close()

        # ตรวจสอบผลลัพธ์
        if not result:
            return {"message": "No Data Found"}

        # สร้าง JSON ตอบกลับ
        data = {"DeviceAlarmVolumeRecordData": []}
        for row in result:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            data["DeviceAlarmVolumeRecordData"].append(row_data)

        return data

    except Exception as e:
        return {"error": str(e)}



@app.post("/GetDeviceAlarmLevelRecordData/", tags=["Query_table"])
async def get_device_alarm_level_record_data(MonthReport: DateReport):
    try:
        # ตรวจสอบว่า StartDate และ StopDate มีค่าหรือไม่
        if MonthReport.StartDate and MonthReport.StopDate:
            # แปลงวันที่จากรูปแบบ JSON เป็น datetime object
            start_time = datetime.strptime(MonthReport.StartDate, "%Y-%m-%dT%H:%M")
            stop_time = datetime.strptime(MonthReport.StopDate, "%Y-%m-%dT%H:%M")
            
            # สร้างเงื่อนไขการกรองข้อมูล
            date_condition = f"""
                device_level_record_timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND 
                device_level_record_timestamp <= '{stop_time.strftime('%Y-%m-%d %H:%M:%S')}'
            """
        else:
            # ดึงข้อมูลทั้งหมด
            date_condition = "1=1"

        # SQL Query
        sql_str = f"""
        SELECT 
           *
        FROM 
            public.device_alarm_level_record_table
        WHERE 
            {date_condition};
        """
        print(sql_str)  # สำหรับ Debugging

        # เรียกใช้งาน SQL
        cursor = conn.cursor()
        cursor.execute(sql_str)
        result = cursor.fetchall()
        conn.commit()
        cursor.close()

        # ตรวจสอบผลลัพธ์
        if not result:
            return {"message": "No Data Found"}

        # สร้าง JSON ตอบกลับ
        data = {"DeviceAlarmLevelRecordData": []}
        for row in result:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            data["DeviceAlarmLevelRecordData"].append(row_data)

        return data

    except Exception as e:
        return {"error": str(e)}





@app.post("/GetDeviceAlarmBatchToLongRecordData/", tags=["Query_table"])
async def get_device_alarm_batch_to_long_record_data(MonthReport: DateReport):
    try:
        # ตรวจสอบว่า StartDate และ StopDate มีค่าหรือไม่
        if MonthReport.StartDate and MonthReport.StopDate:
            # แปลงวันที่จากรูปแบบ JSON เป็น datetime object
            start_time = datetime.strptime(MonthReport.StartDate, "%Y-%m-%dT%H:%M")
            stop_time = datetime.strptime(MonthReport.StopDate, "%Y-%m-%dT%H:%M")
            
            # สร้างเงื่อนไขการกรองข้อมูล
            date_condition = f"""
                alarm_batch_record_timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND 
                alarm_batch_record_timestamp <= '{stop_time.strftime('%Y-%m-%d %H:%M:%S')}'
            """
        else:
            # ดึงข้อมูลทั้งหมด
            date_condition = "1=1"

        # SQL Query
        sql_str = f"""
        SELECT 
*
        FROM 
            public.device_alarm_batchtolong_record
        WHERE 
            {date_condition};
        """
        print(sql_str)  # สำหรับ Debugging

        # เรียกใช้งาน SQL
        cursor = conn.cursor()
        cursor.execute(sql_str)
        result = cursor.fetchall()
        conn.commit()
        cursor.close()

        # ตรวจสอบผลลัพธ์
        if not result:
            return {"message": "No Data Found"}

        # สร้าง JSON ตอบกลับ
        data = {"DeviceAlarmBatchToLongRecordData": []}
        for row in result:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            data["DeviceAlarmBatchToLongRecordData"].append(row_data)

        return data

    except Exception as e:
        return {"error": str(e)}




@app.post("/GetDeviceReportData/", tags=["Query_table"])
async def get_device_report_data(MonthReport: DateReport):
    try:
        # ตรวจสอบว่า StartDate และ StopDate มีค่าหรือไม่
        if MonthReport.StartDate and MonthReport.StopDate:
            # แปลงวันที่จากรูปแบบ JSON เป็น datetime object
            start_time = datetime.strptime(MonthReport.StartDate, "%Y-%m-%dT%H:%M")
            stop_time = datetime.strptime(MonthReport.StopDate, "%Y-%m-%dT%H:%M")
            
            # สร้างเงื่อนไขการกรองข้อมูล
            date_condition = f"""
                device_report_create_at >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND 
                device_report_create_at <= '{stop_time.strftime('%Y-%m-%d %H:%M:%S')}'
            """
        else:
            # ดึงข้อมูลทั้งหมด
            date_condition = "1=1"

        # SQL Query
        sql_str = f"""
        SELECT 
           *
        FROM 
            public.device_report_table
        WHERE 
            {date_condition};
        """
        print(sql_str)  # สำหรับ Debugging

        # เรียกใช้งาน SQL
        cursor = conn.cursor()
        cursor.execute(sql_str)
        result = cursor.fetchall()
        conn.commit()
        cursor.close()

        # ตรวจสอบผลลัพธ์
        if not result:
            return {"message": "No Data Found"}

        # สร้าง JSON ตอบกลับ
        data = {"DeviceReportData": []}
        for row in result:
            row_data = {}
            for i, column_name in enumerate(cursor.description):
                row_data[column_name[0]] = row[i]
            data["DeviceReportData"].append(row_data)

        return data

    except Exception as e:
        return {"error": str(e)}



class User(BaseModel):
    usere_mail: str
    user_firstname: str
    user_lastname: str
    user_password: str
    user_status: bool
    user_role_id: int
    user_create: int

@app.post("/AddUser" , tags =['Add_Data'])
async def create_user(user: User):
    try:
        # Validate input
        Input_Data = [user.usere_mail, user.user_firstname, user.user_lastname, user.user_password]
        if any(d.strip().lower() == 'none' or not d.strip() for d in Input_Data):
            response_data = {"status": "error", "message": "User input data isn't complete"}
            return JSONResponse(content=response_data, status_code=400)
        
        # Check if the email already exists
        sql_check_mail = """SELECT usere_mail FROM user_table WHERE usere_mail = %s"""
        with conn.cursor() as cursor:
            cursor.execute(sql_check_mail, (user.usere_mail,))
            existing_email = cursor.fetchone()
        
        if existing_email:
            response_data = {"status": "error", "message": "Your email already exists."}
            return JSONResponse(content=response_data, status_code=400)

        # Hash password
        password_bytes = user.user_password.encode('utf-8')
        salt = bcrypt.gensalt()
        hash_password = bcrypt.hashpw(password_bytes, salt).decode('utf-8')

        # Begin transaction
        with conn.cursor() as cursor:
            # Insert new user
            insert_sql = """INSERT INTO user_table 
                            (usere_mail, user_password, user_firstname, user_lastname, user_status, user_role_id, user_createat, user_create_by)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(insert_sql, (user.usere_mail, hash_password, user.user_firstname, user.user_lastname, 
                                         user.user_status, user.user_role_id, datetime.now(), user.user_create))
            
            # Insert event log
            event_sql = """INSERT INTO device_event_table 
                           (event_type, event_message, event_timestamp, event_createby)
                           VALUES (%s, %s, %s, %s)"""
            cursor.execute(event_sql, ('User Create', 'User added successfully', datetime.now(), user.user_create))
            
            # Commit transaction
            conn.commit()

        return {"message": "User added successfully", "status": 'true'}

    except Exception as e:
        # Rollback transaction on error
        conn.rollback()
        print(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create user")







class DeleteUser(BaseModel):
    userID: int
    UserDeleteBy: int

@app.post("/DeleteUserID/" , tags =['Delete_Data'] )
async def post_data(deleteUser: DeleteUser):
    try:
        print('User ID:', deleteUser.userID)

        # Check if UserDeleteBy exists in user_table
        sql_check_user = """SELECT user_id FROM user_table WHERE user_id = %s"""
        with conn.cursor() as cursor:
            cursor.execute(sql_check_user, (deleteUser.UserDeleteBy,))
            user_exists = cursor.fetchone()
        
        if not user_exists:
            raise HTTPException(status_code=400, detail=f"UserDeleteBy ID {deleteUser.UserDeleteBy} does not exist in user_table")

        # Delete user
        sql_del_User = """
            DELETE FROM user_table
            WHERE user_id = %s;
        """
        with conn.cursor() as cursor:
            cursor.execute(sql_del_User, (deleteUser.userID,))
            conn.commit()

        # Log the deletion event
        event_sql = """INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                       VALUES (%s, %s, %s, %s)"""
        with conn.cursor() as cursor:
            cursor.execute(
                event_sql,
                ('User Delete', f'User ID {deleteUser.userID} deleted successfully', datetime.now(), deleteUser.UserDeleteBy),
            )
            conn.commit()

        return {"message": f"User ID {deleteUser.userID} deleted successfully", "status": 'true'}

    except HTTPException as e:
        # Raise HTTP exceptions with specific details
        raise e
    except Exception as e:
        # Handle all other exceptions
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")





@app.post("/Add_device_alarm/", tags = ['Add_Data'])
async def post_data(Alarm: Alarm):  
    try:
        # Convert starttimes and endtimes to datetime format
        
        start_time = datetime.strptime(Alarm.starttimes, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(Alarm.endtimes, '%Y-%m-%d %H:%M:%S')

        alarm_type = Alarm.alarmtype
        
        # Check for required fields
        if Alarm.eventName == 'none':
            response_data = {"status": "error", "message": "Please input Alarm name!"}
            return JSONResponse(content=response_data, status_code=400)
        elif Alarm.starttimes == 'none' or Alarm.endtimes == 'none':
            response_data = {"status": "error", "message": "Please input Start time and End time!"}
            return JSONResponse(content=response_data, status_code=400)
        elif Alarm.alarmtype == 'batchToLong' and Alarm.batchDurationSets == 'none':
            response_data = {"status": "error", "message": "Please input Batch Duration!"}
            return JSONResponse(content=response_data, status_code=400)
        
        # SQL query checks
        with conn.cursor() as cursor:
            sql_check_1 = """
                SELECT * FROM device_alarm_table 
                WHERE (%s BETWEEN device_check_start_time AND device_check_stop_time OR %s BETWEEN device_check_start_time AND device_check_stop_time) AND device_alarm_type = %s
            """
            cursor.execute(sql_check_1, (start_time, end_time, alarm_type))
            check_time = cursor.fetchall()

            sql_check_2 = """
                SELECT * FROM device_alarm_table 
                WHERE (device_check_start_time BETWEEN %s AND %s OR device_check_stop_time BETWEEN %s AND %s) AND device_alarm_type = %s
            """
            cursor.execute(sql_check_2, (start_time, end_time, start_time, end_time, alarm_type))
            check_time_2 = cursor.fetchall()

            if check_time:
                response_data = {"status": "error", "message": "Your alarm times intersect with other alarm time range."}
                return JSONResponse(content=response_data, status_code=400)
            elif check_time_2:
                response_data = {"status": "error", "message": "Other alarm times intersect with your alarm time range."}
                return JSONResponse(content=response_data, status_code=400)
            
            # Insert into the database
            insert_table = "device_alarm_table"
            insert_sql = f"""
                INSERT INTO {insert_table} (
                    device_alarm_type, device_id, device_alarm_name, device_check_start_time, 
                    device_check_stop_time, notify_id, device_alarm_status, device_level_low_limit, 
                    device_level_high_limit, device_volume_low, device_volume_high, 
                    device_batch_duration_set, device_batch_id, device_alarm_create_by
                ) VALUES (%s, %s, %s, %s, %s, %s, 'True', %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                Alarm.alarmtype, Alarm.namedevice, Alarm.eventName, start_time, end_time, 
                Alarm.notifyselect, Alarm.devicelevellows, Alarm.devicelevelhighs, 
                Alarm.devicevolumelows, Alarm.devicevolumehighs, Alarm.batchDurationSets, 
                Alarm.selectBatchs, Alarm.datausercreate
            ))

            event_sql = """
                INSERT INTO device_event_table (event_type, event_message, event_timestamp, event_createby)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(event_sql, ('Alarm Create', 'Alarm added successfully', datetime.now(), Alarm.datausercreate))
        
        conn.commit()
        response_data = {"status": "success", "message": "Data successfully processed."}
        return JSONResponse(content=response_data, status_code=200)
    
    except Exception as e:
        conn.rollback()  # Rollback transaction in case of an error
        response_data = {"status": "error", "message": str(e)}
        return JSONResponse(content=response_data, status_code=500)





class Item(BaseModel):
    name: str
    password: str

@app.post("/login/", tags=['USERS MANAGEMENT'])
async def post_data(item: Item):
    try:
        if not item.name:
            raise HTTPException(status_code=400, detail="The username entered is incorrect")

        query = "SELECT usere_mail, user_password, user_id, user_status, user_bancount FROM public.user_table WHERE usere_mail = %s;"
        with conn.cursor() as cursor:
            cursor.execute(query, (item.name,))
            user_data = cursor.fetchone()
            conn.commit()

        if not user_data:
            raise HTTPException(status_code=400, detail="User not found.")

        user_mail, user_password, user_id, user_status, ban_count = user_data
        input_pw = item.password

        if bcrypt.checkpw(input_pw.encode('utf-8'), user_password.encode('utf-8')):
            # ตรวจสอบสถานะ ban
            if not user_status:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT user_bannedtime FROM user_table WHERE user_id = %s;", (user_id,))
                    ban_time_at = cursor.fetchone()[0]
                    if ban_time_at:
                        bantime = timedelta(days=1)
                        remaining_time = (ban_time_at + bantime) - datetime.now()
                        if remaining_time.total_seconds() > 0:
                            banned_time_show = str(remaining_time).split(".")[0]
                            raise HTTPException(status_code=400, detail=f"Banned time left: {banned_time_show}")
                        else:
                            # ปลดแบนผู้ใช้
                            cursor.execute("UPDATE user_table SET user_status = TRUE, user_bannedtime = NULL WHERE user_id = %s", (user_id,))
                            conn.commit()

            # อัปเดตเวลาล็อกอินล่าสุด
            with conn.cursor() as cursor:
                cursor.execute("UPDATE user_table SET user_lastlogin = CURRENT_TIMESTAMP, user_bancount = 0 WHERE user_id = %s", (user_id,))
                conn.commit()

            # สร้าง JWT Token
            payload = {
                "user_id": user_id,
                "role": "systemadmin",  # ควรดึงจากฐานข้อมูล
                "status": "ok",
                "exp": datetime.utcnow() + timedelta(seconds=10)
            }
            token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

            return {
                "success": True,
                "message": "Login successful.",
                "token": token
            }
        else:
            # หากรหัสผ่านผิด ให้เพิ่มจำนวนครั้งที่ผิด
            if ban_count + 1 > 2:
                with conn.cursor() as cursor:
                    cursor.execute("UPDATE user_table SET user_status = FALSE, user_bancount = 0, user_bannedtime = CURRENT_TIMESTAMP WHERE user_id = %s", (user_id,))
                    conn.commit()
                raise HTTPException(status_code=400, detail="You are banned for 24 hours.")
            else:
                with conn.cursor() as cursor:
                    cursor.execute("UPDATE user_table SET user_bancount = user_bancount + 1 WHERE user_id = %s", (user_id,))
                    conn.commit()
                raise HTTPException(status_code=400, detail="Invalid username or password.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        print("Login attempt finished.")




@app.post("/login_test/", tags=['USERS MANAGEMENT'])
async def post_data(item: Item):
    try:
        # ตรวจสอบว่า username และ password ถูกต้อง
        query = "SELECT usere_mail, user_password, user_id, user_status, user_bancount, user_role_table.user_role_name, user_lastlogin FROM public.user_table INNER JOIN user_role_table ON user_table.user_role_id = user_role_table.user_role_id WHERE usere_mail = %s;"
        with conn.cursor() as cursor:
            cursor.execute(query, (item.name,))
            user_data = cursor.fetchone()

        if not user_data:
            raise HTTPException(status_code=400, detail="User not found.")

        # ดึงข้อมูลจากฐานข้อมูล
        (
            user_mail, user_password, user_id, user_status, bancount, 
            user_role, last_login
        ) = user_data

        # ตรวจสอบสถานะ
        if not user_status:
            raise HTTPException(status_code=400, detail="User is banned.")

        # ตรวจสอบ password
        if not bcrypt.checkpw(item.password.encode('utf-8'), user_password.encode('utf-8')):
            if bancount >= 2:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE user_table SET user_status = %s, user_bannedtime = CURRENT_TIMESTAMP, user_bancount = 0 WHERE user_id = %s;",
                        (False, user_id)
                    )
                    conn.commit()
                raise HTTPException(status_code=400, detail="You are banned for 24 hours.")
            else:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE user_table SET user_bancount = %s WHERE user_id = %s;",
                        (bancount + 1, user_id)
                    )
                    conn.commit()
                raise HTTPException(status_code=400, detail="Invalid password.")

        # หากผ่านการตรวจสอบ
        # Reset ban count
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE user_table SET user_bancount = 0, user_lastlogin = CURRENT_TIMESTAMP WHERE user_id = %s;",
                (user_id,)
            )
            conn.commit()

        # สร้าง JWT Token
        payload = {
            "user_id": user_id,
            "role": user_role,
            "status": "ok",
            "exp": datetime.utcnow() + timedelta(hours=1)
        }
        token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")

        # Response
        return {
            "success": True,
            "message": "Login successful.",
            "token": token
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_user_data/", tags=['USERS MANAGEMENT'])
async def get_user_data(token: str):
    try:
        # ตรวจสอบและ decode token
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"], options={"verify_exp": True})  # ตรวจสอบ exp
        
        # ดึง user_id จาก payload
        user_id = payload["user_id"]
        
        # ดึงข้อมูลผู้ใช้จากฐานข้อมูล
        query = """
        SELECT user_id, usere_mail, user_firstname, user_lastname, user_role_table.user_role_name, user_lastlogin 
        FROM public.user_table 
        INNER JOIN user_role_table ON user_table.user_role_id = user_role_table.user_role_id 
        WHERE user_id = %s;
        """
        with conn.cursor() as cursor:
            cursor.execute(query, (user_id,))
            user_data = cursor.fetchone()

        if not user_data:
            raise HTTPException(status_code=404, detail="User data not found.")

        # ตรวจสอบจำนวนคอลัมน์ใน user_data
        if len(user_data) != 6:
            raise HTTPException(status_code=500, detail="Unexpected data format from database.")

        # แยกค่าจาก user_data
        user_id, user_mail, user_firstname, user_lastname, user_role, last_login = user_data
        
        return {
            "user_id": user_id,
            "usere_mail": user_mail,
            "user_firstname": user_firstname,
            "user_lastname": user_lastname,
            "user_role": user_role,
            "last_login": last_login
        }

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired.")  # ให้แสดงว่า token หมดอายุ
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token.")  # ให้แสดงว่า token ไม่ถูกต้อง
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))  # จัดการข้อผิดพลาดทั่วไป




@app.post("/login_cookie/", tags=['USERS MANAGEMENT'])
async def post_data(item: Item, response: Response):
    try:
        # ตรวจสอบว่า username และ password ถูกต้อง
        query = "SELECT usere_mail, user_password, user_id, user_status, user_bancount, user_role_table.user_role_name, user_lastlogin FROM public.user_table INNER JOIN user_role_table ON user_table.user_role_id = user_role_table.user_role_id WHERE usere_mail = %s;"
        with conn.cursor() as cursor:
            cursor.execute(query, (item.name,))
            user_data = cursor.fetchone()

        if not user_data:
            raise HTTPException(status_code=400, detail="User not found.")

        # ดึงข้อมูลจากฐานข้อมูล
        (
            user_mail, user_password, user_id, user_status, bancount, 
            user_role, last_login
        ) = user_data

        # ตรวจสอบสถานะ
        if not user_status:
            raise HTTPException(status_code=400, detail="User is banned.")

        # ตรวจสอบ password
        if not bcrypt.checkpw(item.password.encode('utf-8'), user_password.encode('utf-8')):
            if bancount >= 2:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE user_table SET user_status = %s, user_bannedtime = CURRENT_TIMESTAMP, user_bancount = 0 WHERE user_id = %s;",
                        (False, user_id)
                    )
                    conn.commit()
                raise HTTPException(status_code=400, detail="You are banned for 24 hours.")
            else:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE user_table SET user_bancount = %s WHERE user_id = %s;",
                        (bancount + 1, user_id)
                    )
                    conn.commit()
                raise HTTPException(status_code=400, detail="Invalid password.")

        # หากผ่านการตรวจสอบ
        # Reset ban count
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE user_table SET user_bancount = 0, user_lastlogin = CURRENT_TIMESTAMP WHERE user_id = %s;",
                (user_id,)
            )
            conn.commit()

        # สร้าง JWT Token
        payload = {
            "user_id": user_id,
            "role": user_role,
            "status": "ok",
            "exp": datetime.utcnow() + timedelta(seconds=3600)  # ตั้งเวลา expiration 1 ชั่วโมง
        }
        token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")

        # ส่ง token ไปเก็บใน HttpOnly Cookie
        response.set_cookie(
            key="access_token", 
            value=token, 
            httponly=True,        # ทำให้ไม่สามารถเข้าถึงจาก JavaScript
            secure=False,          # เฉพาะใน HTTPS
            samesite="Strict",    # ส่ง cookie เฉพาะในคำขอที่มาจากโดเมนเดียวกัน
            max_age=timedelta(hours=1)  # อายุของ cookie
        )

        # Response
        return {
            "success": True,
            "message": "Login successful."
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/protected" , tags=['USERS MANAGEMENT'])
async def protected_route(access_token: str = Cookie(None)):
    if not access_token:
        raise HTTPException(status_code=401, detail="Token missing or invalid")

    try:
        # ตรวจสอบ token ว่ายังไม่หมดอายุและเป็นของจริง
        payload = jwt.decode(access_token, SECRET_KEY, algorithms=["HS256"])
        # ดำเนินการกับ payload ที่ได้รับ
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token invalid")
    
    return {"protected_data": "This is protected data."}



class Userdetail(BaseModel):
    userId : int
@app.post("/get_user_id/" , tags=['USERS MANAGEMENT'])
async def post_data(Userdetail: Userdetail):

    table_name = "userData"
    sql_str = f"""SELECT * from user_table where user_id = {Userdetail.userId}
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    print(sql_str)
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data



class Alarmdetail(BaseModel):
    AlarmID : int
@app.post("/get_alarm_id/" , tags=['USERS MANAGEMENT'])
async def post_data(Alarmdetail: Alarmdetail):

    table_name = "AlarmData"
    sql_str = f"""SELECT * from device_alarm_table where device_alarm_id = {Alarmdetail.AlarmID}
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    print(sql_str)
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data


class NotifyDetail(BaseModel):
    NotifyID : int
@app.post("/get_notify_id/" , tags=['USERS MANAGEMENT'])
async def post_data(NotifyDetail: NotifyDetail):

    table_name = "AlarmData"
    sql_str = f"""SELECT * from device_notify_table where notify_id = {NotifyDetail.NotifyID}
    """

    cursor = conn.cursor()
    cursor.execute(sql_str)
    data_start = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close
    print(sql_str)
    # สร้าง JSON โดยรวมชื่อตารางและข้อมูล
    data = {table_name: []}
    for row in data_start:
        row_data = {}
        for i, column_name in enumerate(cursor.description):
            row_data[column_name[0]] = row[i]
        row_data["status"] = "ok"
        data[table_name].append(row_data)
    
    return data




@app.get("/export_alarm_table/csv", tags=['EXPORT DATA'])
def export_alarm_table_to_csv():
    try:
        # สร้าง cursor เพื่อ execute คำสั่ง SQL
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = "SELECT * FROM device_alarm_table ORDER BY device_alarm_id DESC"  # เรียงข้อมูลตาม device_alarm_id
            cursor.execute(query)
            rows = cursor.fetchall()  # ดึงข้อมูลทั้งหมด

        # สร้างไฟล์ CSV ในหน่วยความจำ
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        csv_writer.writeheader()  # เขียนหัวข้อคอลัมน์
        csv_writer.writerows(rows)  # เขียนข้อมูลทั้งหมด

        # กำหนดตำแหน่งของ pointer กลับไปที่จุดเริ่มต้นของ buffer
        csv_buffer.seek(0)

        # ส่งไฟล์ CSV กลับไปยัง client โดยไม่เก็บไฟล์ในเซิร์ฟเวอร์
        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=device_alarm_table.csv"})

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/export_device_raw_data/csv", tags=['EXPORT DATA'])
def export_device_raw_data_to_csv(startTime: str = Query(None), stopTime: str = Query(None)):
    try:
        # สร้างคำสั่ง SQL เพื่อเลือกข้อมูลจากตาราง
        query = """SELECT raw_id, device_batch_status, device_distance, 
                          device_tank_volume, device_timestamp, device_id, reset_count
                   FROM public.device_raw_data"""
        
        if startTime and stopTime:
            try:
                # แปลง startTime และ stopTime จากรูปแบบ 'YYYY-MM-DDTHH:MM' ไปเป็น 'YY-MM-DD HH:MM:SS'
                start_date_obj = datetime.strptime(startTime, '%Y-%m-%dT%H:%M')
                stop_date_obj = datetime.strptime(stopTime, '%Y-%m-%dT%H:%M')
                
                # แปลงเป็นรูปแบบที่ฐานข้อมูลสามารถรับได้ (YYYY-MM-DD HH:MM:SS)
                start_date_str = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
                stop_date_str = stop_date_obj.strftime('%Y-%m-%d %H:%M:%S')

                query += f" WHERE device_timestamp BETWEEN '{start_date_str}' AND '{stop_date_str}'"
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")
        
        # เพิ่มคำสั่ง ORDER BY
        query += " ORDER BY device_timestamp DESC"
        
        # สร้าง cursor เพื่อ execute คำสั่ง SQL
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()  # ดึงข้อมูลทั้งหมด

        # สร้างไฟล์ CSV ในหน่วยความจำ
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        csv_writer.writeheader()  # เขียนหัวข้อคอลัมน์
        csv_writer.writerows(rows)  # เขียนข้อมูลทั้งหมด

        # กำหนดตำแหน่งของ pointer กลับไปที่จุดเริ่มต้นของ buffer
        csv_buffer.seek(0)

        # ส่งไฟล์ CSV กลับไปยัง client โดยไม่เก็บไฟล์ในเซิร์ฟเวอร์
        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=device_raw_data.csv"})

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/export_device_batch_table/csv", tags=['EXPORT DATA'])
def export_device_batch_table_to_csv(startTime: str = Query(None), stopTime: str = Query(None)):
    try:
        # ตรวจสอบว่า startTime และ stopTime ถูกส่งมาแล้วหรือไม่
        query = """SELECT device_batch_id, device_batch_datetime, device_batch_value, 
                          device_batch_value_status, device_batch_start_time, device_batch_end_time, 
                          device_batch_temp, device_batch_start_volume, device_batch_end_volume, 
                          device_id, device_report_status 
                   FROM public.device_batch_table"""
        
        if startTime and stopTime:
            try:
                # แปลง startTime และ stopTime จากรูปแบบ 'YYYY-MM-DDTHH:MM' ไปเป็น 'YY-MM-DD HH:MM:SS'
                start_date_obj = datetime.strptime(startTime, '%Y-%m-%dT%H:%M')
                stop_date_obj = datetime.strptime(stopTime, '%Y-%m-%dT%H:%M')
                
                # แปลงเป็นรูปแบบที่ฐานข้อมูลสามารถรับได้ (YYYY-MM-DD HH:MM:SS)
                start_date_str = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
                stop_date_str = stop_date_obj.strftime('%Y-%m-%d %H:%M:%S')

                query += f" WHERE device_batch_start_time BETWEEN '{start_date_str}' AND '{stop_date_str}'"
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")
        
        # เพิ่มคำสั่ง ORDER BY
        query += " ORDER BY device_batch_start_time DESC"
        
        # สร้าง cursor เพื่อ execute คำสั่ง SQL
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()  # ดึงข้อมูลทั้งหมด

        # สร้างไฟล์ CSV ในหน่วยความจำ
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        csv_writer.writeheader()  # เขียนหัวข้อคอลัมน์
        csv_writer.writerows(rows)  # เขียนข้อมูลทั้งหมด

        # กำหนดตำแหน่งของ pointer กลับไปที่จุดเริ่มต้นของ buffer
        csv_buffer.seek(0)

        # ส่งไฟล์ CSV กลับไปยัง client โดยไม่เก็บไฟล์ในเซิร์ฟเวอร์
        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=device_batch_table.csv"})

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





@app.get("/export_device_event_table/csv", tags=['EXPORT DATA'])
def export_device_event_table_to_csv(startTime: str = Query(None), stopTime: str = Query(None)):
    try:
        # ตรวจสอบว่า startTime และ stopTime ถูกส่งมาแล้วหรือไม่
        query = """SELECT event_id, event_type, event_message, event_timestamp, event_createby, event_saw
                   FROM public.device_event_table"""
        
        if startTime and stopTime:
            try:
                # แปลง startTime และ stopTime จากรูปแบบ 'YYYY-MM-DDTHH:MM' ไปเป็น 'YY-MM-DD HH:MM:SS'
                start_date_obj = datetime.strptime(startTime, '%Y-%m-%dT%H:%M')
                stop_date_obj = datetime.strptime(stopTime, '%Y-%m-%dT%H:%M')
                
                # แปลงเป็นรูปแบบที่ฐานข้อมูลสามารถรับได้ (YYYY-MM-DD HH:MM:SS)
                start_date_str = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
                stop_date_str = stop_date_obj.strftime('%Y-%m-%d %H:%M:%S')

                query += f" WHERE event_timestamp BETWEEN '{start_date_str}' AND '{stop_date_str}'"
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")
        
        # เพิ่มคำสั่ง ORDER BY
        query += " ORDER BY event_timestamp DESC"
        
        # สร้าง cursor เพื่อ execute คำสั่ง SQL
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()  # ดึงข้อมูลทั้งหมด

        # สร้างไฟล์ CSV ในหน่วยความจำ
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        csv_writer.writeheader()  # เขียนหัวข้อคอลัมน์
        csv_writer.writerows(rows)  # เขียนข้อมูลทั้งหมด

        # กำหนดตำแหน่งของ pointer กลับไปที่จุดเริ่มต้นของ buffer
        csv_buffer.seek(0)

        # ส่งไฟล์ CSV กลับไปยัง client โดยไม่เก็บไฟล์ในเซิร์ฟเวอร์
        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=device_event_table.csv"})

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/export_device_alarm_volume_record_table/csv", tags=['EXPORT DATA'])
def export_device_alarm_volume_record_table_to_csv(startTime: str = Query(None), stopTime: str = Query(None)):
    try:
        # สร้างคำสั่ง SQL เพื่อเลือกข้อมูลจากตาราง
        query = """SELECT device_volume_record_id, volume_alarm_record, volume_alarm_volume_high_set, 
                          device_alarm_message, device_alarm_id, device_alarm_record_timestamp, 
                          volume_alarm_volume_low_set, alarm_volume_status, alarm_createby, alarm_volume_saw
                   FROM public.device_alarm_volume_record_table"""
        
        if startTime and stopTime:
            try:
                # แปลง startTime และ stopTime จากรูปแบบ 'YYYY-MM-DDTHH:MM' ไปเป็น 'YY-MM-DD HH:MM:SS'
                start_date_obj = datetime.strptime(startTime, '%Y-%m-%dT%H:%M')
                stop_date_obj = datetime.strptime(stopTime, '%Y-%m-%dT%H:%M')
                
                # แปลงเป็นรูปแบบที่ฐานข้อมูลสามารถรับได้ (YYYY-MM-DD HH:MM:SS)
                start_date_str = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
                stop_date_str = stop_date_obj.strftime('%Y-%m-%d %H:%M:%S')

                query += f" WHERE device_alarm_record_timestamp BETWEEN '{start_date_str}' AND '{stop_date_str}'"
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")
        
        # เพิ่มคำสั่ง ORDER BY
        query += " ORDER BY device_alarm_record_timestamp DESC"
        
        # สร้าง cursor เพื่อ execute คำสั่ง SQL
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()  # ดึงข้อมูลทั้งหมด

        # สร้างไฟล์ CSV ในหน่วยความจำ
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        csv_writer.writeheader()  # เขียนหัวข้อคอลัมน์
        csv_writer.writerows(rows)  # เขียนข้อมูลทั้งหมด

        # กำหนดตำแหน่งของ pointer กลับไปที่จุดเริ่มต้นของ buffer
        csv_buffer.seek(0)

        # ส่งไฟล์ CSV กลับไปยัง client โดยไม่เก็บไฟล์ในเซิร์ฟเวอร์
        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=device_alarm_volume_record_table.csv"})

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





@app.get("/export_device_alarm_level_record_table/csv", tags=['EXPORT DATA'])
def export_device_alarm_level_record_table_to_csv(startTime: str = Query(None), stopTime: str = Query(None)):
    try:
        # สร้างคำสั่ง SQL เพื่อเลือกข้อมูลจากตาราง
        query = """SELECT device_level_record_id, level_alarm_record, level_low_alarm_set, 
                          device_alarm_message, device_alarm_id, device_level_record_timestamp, 
                          level_high_alarm_set, level_record_status, alarm_createby, alarm_level_saw
                   FROM public.device_alarm_level_record_table"""
        
        if startTime and stopTime:
            try:
                # แปลง startTime และ stopTime จากรูปแบบ 'YYYY-MM-DDTHH:MM' ไปเป็น 'YY-MM-DD HH:MM:SS'
                start_date_obj = datetime.strptime(startTime, '%Y-%m-%dT%H:%M')
                stop_date_obj = datetime.strptime(stopTime, '%Y-%m-%dT%H:%M')
                
                # แปลงเป็นรูปแบบที่ฐานข้อมูลสามารถรับได้ (YYYY-MM-DD HH:MM:SS)
                start_date_str = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
                stop_date_str = stop_date_obj.strftime('%Y-%m-%d %H:%M:%S')

                query += f" WHERE device_level_record_timestamp BETWEEN '{start_date_str}' AND '{stop_date_str}'"
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")
        
        # เพิ่มคำสั่ง ORDER BY
        query += " ORDER BY device_level_record_timestamp DESC"
        
        # สร้าง cursor เพื่อ execute คำสั่ง SQL
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()  # ดึงข้อมูลทั้งหมด

        # สร้างไฟล์ CSV ในหน่วยความจำ
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        csv_writer.writeheader()  # เขียนหัวข้อคอลัมน์
        csv_writer.writerows(rows)  # เขียนข้อมูลทั้งหมด

        # กำหนดตำแหน่งของ pointer กลับไปที่จุดเริ่มต้นของ buffer
        csv_buffer.seek(0)

        # ส่งไฟล์ CSV กลับไปยัง client โดยไม่เก็บไฟล์ในเซิร์ฟเวอร์
        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=device_alarm_level_record_table.csv"})

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))






@app.get("/export_device_alarm_batchtolong_record/csv", tags=['EXPORT DATA'])
def export_device_alarm_batchtolong_record_to_csv(startTime: str = Query(None), stopTime: str = Query(None)):
    try:
        # สร้างคำสั่ง SQL เพื่อเลือกข้อมูลจากตาราง
        query = """SELECT alarm_batch_record_id, alarm_batch_record_timestamp, alarm_batch_record_message, 
                          device_alarm_id, device_batch_id, alarm_batch_record_value_set, 
                          alarm_batch_value_record, alarm_batch_record_status, alarm_createby, alarm_btl_saw
                   FROM public.device_alarm_batchtolong_record"""
        
        if startTime and stopTime:
            try:
                # แปลง startTime และ stopTime จากรูปแบบ 'YYYY-MM-DDTHH:MM' ไปเป็น 'YY-MM-DD HH:MM:SS'
                start_date_obj = datetime.strptime(startTime, '%Y-%m-%dT%H:%M')
                stop_date_obj = datetime.strptime(stopTime, '%Y-%m-%dT%H:%M')
                
                # แปลงเป็นรูปแบบที่ฐานข้อมูลสามารถรับได้ (YYYY-MM-DD HH:MM:SS)
                start_date_str = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
                stop_date_str = stop_date_obj.strftime('%Y-%m-%d %H:%M:%S')

                query += f" WHERE alarm_batch_record_timestamp BETWEEN '{start_date_str}' AND '{stop_date_str}'"
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")
        
        # เพิ่มคำสั่ง ORDER BY
        query += " ORDER BY alarm_batch_record_timestamp DESC"
        
        # สร้าง cursor เพื่อ execute คำสั่ง SQL
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()  # ดึงข้อมูลทั้งหมด

        # สร้างไฟล์ CSV ในหน่วยความจำ
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        csv_writer.writeheader()  # เขียนหัวข้อคอลัมน์
        csv_writer.writerows(rows)  # เขียนข้อมูลทั้งหมด

        # กำหนดตำแหน่งของ pointer กลับไปที่จุดเริ่มต้นของ buffer
        csv_buffer.seek(0)

        # ส่งไฟล์ CSV กลับไปยัง client โดยไม่เก็บไฟล์ในเซิร์ฟเวอร์
        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=device_alarm_batchtolong_record.csv"})

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/export_device_report_table/csv", tags=['EXPORT DATA'])
def export_device_report_table_to_csv(startTime: str = Query(None), stopTime: str = Query(None)):
    try:
        # สร้างคำสั่ง SQL เพื่อเลือกข้อมูลจากตาราง
        query = """SELECT device_report_id, device_report_type, device_report_create_at, 
                          device_report_update_at, device_report_create_by, device_report_status, 
                          device_id, device_report_volume, device_report_duration, device_batch_id
                   FROM public.device_report_table"""
        
        if startTime and stopTime:
            try:
                # แปลง startTime และ stopTime จากรูปแบบ 'YYYY-MM-DDTHH:MM' ไปเป็น 'YY-MM-DD HH:MM:SS'
                start_date_obj = datetime.strptime(startTime, '%Y-%m-%dT%H:%M')
                stop_date_obj = datetime.strptime(stopTime, '%Y-%m-%dT%H:%M')
                
                # แปลงเป็นรูปแบบที่ฐานข้อมูลสามารถรับได้ (YYYY-MM-DD HH:MM:SS)
                start_date_str = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
                stop_date_str = stop_date_obj.strftime('%Y-%m-%d %H:%M:%S')

                query += f" WHERE device_report_create_at BETWEEN '{start_date_str}' AND '{stop_date_str}'"
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")
        
        # เพิ่มคำสั่ง ORDER BY
        query += " ORDER BY device_report_create_at DESC"
        
        # สร้าง cursor เพื่อ execute คำสั่ง SQL
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()  # ดึงข้อมูลทั้งหมด

        # สร้างไฟล์ CSV ในหน่วยความจำ
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        csv_writer.writeheader()  # เขียนหัวข้อคอลัมน์
        csv_writer.writerows(rows)  # เขียนข้อมูลทั้งหมด

        # กำหนดตำแหน่งของ pointer กลับไปที่จุดเริ่มต้นของ buffer
        csv_buffer.seek(0)

        # ส่งไฟล์ CSV กลับไปยัง client โดยไม่เก็บไฟล์ในเซิร์ฟเวอร์
        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=device_report_table.csv"})

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/Device2/", tags=['test'])
async def get_avg_data(startTime: str, stopTime: str) -> Dict:
    # แปลง startTime และ stopTime จาก string เป็น datetime
    start_date = datetime.strptime(startTime, "%Y-%m-%dT%H:%M")
    stop_date = datetime.strptime(stopTime, "%Y-%m-%dT%H:%M")

    if (stop_date - start_date).days > 7:
        raise HTTPException(status_code=400, detail="Date range should not exceed 7 days")
    
    # หรือสามารถตรวจสอบเงื่อนไขเพิ่มเติม เช่น
    if startTime == stopTime:
        raise HTTPException(status_code=400, detail="Start time and Stop time cannot be the same")

    # สร้าง SQL query ที่ดึงข้อมูลตามช่วงเวลา
    sql_str = f"""
        SELECT raw_id, device_batch_status, device_distance, device_tank_volume, device_timestamp, device_id, reset_count
        FROM public.device_raw_data
        WHERE device_timestamp BETWEEN '{start_date}' AND '{stop_date}'
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data = cursor.fetchall()
    conn.commit()

    # ปิดการเชื่อมต่อ
    cursor.close()

    # คำนวณผลรวมของ device_tank_volume ทั้งหมดในช่วงเวลา
    total_tank_volume = sum(row[3] for row in data)  # sum of device_tank_volume

    # คำนวณค่าเฉลี่ยและเปอร์เซ็นต์ของ device_tank_volume สำหรับแต่ละวัน
    daily_data = {}
    for row in data:
        timestamp = row[4]  # device_timestamp
        day = timestamp.date()

        if day not in daily_data:
            daily_data[day] = {
                "tank_volume_sum": 0,
                "count": 0,
                "previous_tank_volume": 0,  # ค่าก่อนหน้าเริ่มต้นเป็น 0
                "volume_new_sum": 0         # ค่าเพิ่มใหม่เริ่มต้นเป็น 0
            }

        # เพิ่มค่า device_tank_volume ในวันนั้น
        daily_data[day]["tank_volume_sum"] += row[3]  # device_tank_volume
        daily_data[day]["count"] += 1

        # คำนวณส่วนต่าง (เพิ่มค่าจากค่าก่อนหน้า)
        volume_new = row[3] - daily_data[day]["previous_tank_volume"]
        if volume_new > 0:
            daily_data[day]["volume_new_sum"] += volume_new
        
        # อัปเดต previous_tank_volume สำหรับวันนั้น
        daily_data[day]["previous_tank_volume"] = row[3]

    # คำนวณค่าเฉลี่ยของ device_tank_volume สำหรับแต่ละวันและเปอร์เซ็นต์ของแต่ละวัน
    result_data = {}
    for day, values in daily_data.items():
        # คำนวณค่าเฉลี่ยของ device_tank_volume สำหรับวันนั้น
        avg_tank_volume = values["tank_volume_sum"] / values["count"] if values["count"] > 0 else 0
        avg_tank_volume2 = values["tank_volume_sum"]
        # คำนวณเปอร์เซ็นต์ของ device_tank_volume ของวันนั้นจากผลรวมทั้งหมด
        day_tank_percentage = (values["tank_volume_sum"] / total_tank_volume) * 100 if total_tank_volume > 0 else 0

        # ปัดเศษให้เป็น 2 ตำแหน่งหลังจุดทศนิยม
        result_data[str(day)] = {
            "avg_tank_volume": round(avg_tank_volume, 2),  
            "avg_tank_volume2": round(avg_tank_volume2, 2),  
            "day_tank_percentage": round(day_tank_percentage, 2),
            "volumeNew": round(values["volume_new_sum"], 2),  # ค่าเพิ่มใหม่ (volumeNew)
            "date" :str(day)
        }

    return result_data





@app.get("/Device3/", tags=['test'])
async def get_avg_data_with_increment(startTime: str, stopTime: str) -> Dict:
    # แปลง startTime และ stopTime จาก string เป็น datetime
    start_date = datetime.strptime(startTime, "%Y-%m-%dT%H:%M")
    stop_date = datetime.strptime(stopTime, "%Y-%m-%dT%H:%M")

    # สร้าง SQL query ที่ดึงข้อมูลตามช่วงเวลา
    sql_str = f"""
        SELECT raw_id, device_batch_status, device_distance, device_tank_volume, device_timestamp, device_id, reset_count
        FROM public.device_raw_data
        WHERE device_timestamp BETWEEN '{start_date}' AND '{stop_date}'
        ORDER BY device_timestamp
    """
    
    cursor = conn.cursor()
    cursor.execute(sql_str)
    data = cursor.fetchall()
    conn.commit()

    # ปิดการเชื่อมต่อ
    cursor.close()

    # คำนวณค่าที่เพิ่มขึ้นและผลรวมของการเพิ่มขึ้น
    increments = []
    total_increment = 0
    previous_volume = 0

    for row in data:
        current_volume = row[3]  # device_tank_volume
        if previous_volume > 0:  # ข้ามค่าที่เริ่มต้นด้วย 0
            increment = max(0, current_volume - previous_volume)  # หาค่าที่เพิ่มขึ้น
            increments.append(increment)
            total_increment += increment
        previous_volume = current_volume

    # สร้างผลลัพธ์สำหรับการเพิ่มขึ้น
    result_data = {
        "increments": increments,
        "total_increment": round(total_increment, 2),
    }

    return result_data


# class Alarmdetail(BaseModel):
#     startTime: str
#     stopTime: str

# # ฟังก์ชันสำหรับการดึงข้อมูลจากฐานข้อมูล
# @app.post("/get_tank_data/", tags=['test'])
# async def post_data(Alarmdetail: Alarmdetail):
#     try:
#         # แปลงเวลา startTime และ stopTime จาก "yyyy-MM-ddThh:mm" เป็น "yyyy-MM-dd HH:mm:00"
#         start_time = datetime.strptime(Alarmdetail.startTime + ":00", "%Y-%m-%dT%H:%M:%S")
#         stop_time = datetime.strptime(Alarmdetail.stopTime + ":00", "%Y-%m-%dT%H:%M:%S")
#     except ValueError:
#         raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")

#     # กำหนด SQL query
#     table_name = "AlarmData"
#     sql_str = """
#         SELECT raw_id, device_batch_status, device_distance, device_tank_volume, device_timestamp, device_id, reset_count
#         FROM public.device_raw_data
#         WHERE device_timestamp BETWEEN %s AND %s
#         ORDER BY raw_id DESC
#     """
    
#     # แปลง datetime เป็น string ในรูปแบบที่ PostgreSQL ต้องการ
#     start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
#     stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S")

#     # เชื่อมต่อกับฐานข้อมูล PostgreSQL
#     try:
#         conn = psycopg2.connect(
#            database="ISO-29110-DB",
#                         host="127.0.0.1",
#                         user="postgres",
#                         password="Automation01",
#                         port="5432"
#         )
#         cursor = conn.cursor()
#         cursor.execute(sql_str, (start_time_str, stop_time_str))
#         data_start = cursor.fetchall()
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
#     finally:
#         cursor.close()
#         conn.close()

#     # สร้าง JSON สำหรับส่งกลับ
#     data = {table_name: []}
#     for row in data_start:
#         row_data = {}
#         for i, column_name in enumerate(cursor.description):
#             row_data[column_name[0]] = row[i]
#         row_data["status"] = "ok"
#         data[table_name].append(row_data)
    
#     return data

    


# !!!!!!!!!!!!!!!!!!!!!!! ********************* !!!!!!!!!!!!!!!!!!!!!!!!!

# เขียน ดักไว้หาก query นานเกินไปให้หยุดทำงาน class Alarmdetail(BaseModel):
class TankDetail(BaseModel):
    startTime: str
    stopTime: str
    every: int

async def fetch_data(start_time_str, stop_time_str, every):
    sql_str = f"""
        SELECT DISTINCT ON (DATE_TRUNC('minute', device_timestamp))
            raw_id,
            device_batch_status,
            device_distance,
            device_tank_volume,
            device_timestamp,
            device_id,
            reset_count
        FROM 
            public.device_raw_data
        WHERE 
            EXTRACT(MINUTE FROM device_timestamp) % {every} = 0 
            AND device_timestamp BETWEEN '{start_time_str}' AND '{stop_time_str}'
        ORDER BY 
            DATE_TRUNC('minute', device_timestamp), 
            device_timestamp ASC;
    """

    try:
        conn = psycopg2.connect(
            database="ISO-29110-DB",
            host="127.0.0.1",
            user="postgres",
            password="Automation01",
            port="5432"
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql_str)
        data_start = cursor.fetchall()
        if not data_start:
            raise HTTPException(status_code=404, detail="No data found for the given conditions.")
        return data_start
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.post("/get_tank_data/", tags=['test'])
async def post_data(alarm_detail: TankDetail):
    try:
        start_time = datetime.strptime(alarm_detail.startTime + ":00", "%Y-%m-%dT%H:%M:%S")
        stop_time = datetime.strptime(alarm_detail.stopTime + ":00", "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DDTHH:MM'.")

    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S")

    try:
        data_start = await asyncio.wait_for(fetch_data(start_time_str, stop_time_str, alarm_detail.every), timeout=20.0)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="Query timeout exceeded 20 seconds.")

    data = {"AlarmData": []}
    for row in data_start:
        row_data = {column_name: row[column_name] for column_name in row.keys()}
        row_data["status"] = "ok"
        data["AlarmData"].append(row_data)

    return data

DB_CONFIG = {
    "database": "ISO-29110-DB",
    "host": "127.0.0.1",
    "user": "postgres",
    "password": "Automation01",
    "port": "5432"
}

@app.get("/get-device-alarms" , tags  = ['Notify Manage'])
async def get_device_alarms():
    query = """
        SELECT device_volume_record_id, volume_alarm_record, volume_alarm_volume_high_set, 
               device_alarm_message, device_alarm_id, device_alarm_record_timestamp, 
               volume_alarm_volume_low_set, alarm_volume_status, alarm_createby, alarm_volume_saw
        FROM public.device_alarm_volume_record_table
        WHERE alarm_volume_saw = false
    """
    try:
        # Connect to the database
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                alarms = cursor.fetchall()
                count = len(alarms)
                return {
                    "count": count,
                    "alarms": alarms
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")



@app.get("/get-device-alarms-level" , tags  = ['Notify Manage'])
async def get_device_alarms():
    query = """
        SELECT device_level_record_id, level_alarm_record, level_low_alarm_set, device_alarm_message, device_alarm_id, device_level_record_timestamp, level_high_alarm_set, level_record_status, alarm_createby, alarm_level_saw
	FROM public.device_alarm_level_record_table
        WHERE alarm_level_saw = false
    """
    try:
        # Connect to the database
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                alarms = cursor.fetchall()
                count = len(alarms)
                return {
                    "count": count,
                    "alarms": alarms
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")



@app.get("/get-device-alarms-batch" , tags  = ['Notify Manage'])
async def get_device_alarms():
    query = """
        SELECT alarm_batch_record_id, alarm_batch_record_timestamp, alarm_batch_record_message, device_alarm_id, device_batch_id, alarm_batch_record_value_set, alarm_batch_value_record, alarm_batch_record_status, alarm_createby, alarm_btl_saw
	FROM public.device_alarm_batchtolong_record
        WHERE alarm_btl_saw = false
    """
    try:
        # Connect to the database
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                alarms = cursor.fetchall()
                count = len(alarms)
                return {
                    "count": count,
                    "alarms": alarms
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")




class UpdateActknowlage(BaseModel):
    NotifyID: int

@app.post("/Update_Actknowlage_vloume/", tags=["Notify Manage"])
async def post_data(NotifyDetail: UpdateActknowlage):
    try:
        sql_update_notify = """
            UPDATE device_alarm_volume_record_table
            SET alarm_volume_saw = TRUE
            WHERE device_volume_record_id = %s;
        """

        cursor = conn.cursor()
        cursor.execute(sql_update_notify, (NotifyDetail.NotifyID,))
        conn.commit()
        cursor.close()

        return {"message": "Record updated successfully." , "status" : 'ok'}

    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")



@app.post("/Update_Actknowlage_batch/", tags=["Notify Manage"])
async def post_data(NotifyDetail: UpdateActknowlage):
    try:
        sql_update_notify = """
            UPDATE device_alarm_batchtolong_record
            SET alarm_btl_saw = TRUE
            WHERE alarm_batch_record_id = %s;
        """

        cursor = conn.cursor()
        cursor.execute(sql_update_notify, (NotifyDetail.NotifyID,))
        conn.commit()
        cursor.close()

        return {"message": "Record updated successfully." , "status" : 'ok'}

    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/Update_Actknowlage_level/", tags=["Notify Manage"])
async def post_data(NotifyDetail: UpdateActknowlage):
    try:
        sql_update_notify = """
            UPDATE device_alarm_level_record_table
            SET alarm_level_saw = TRUE
            WHERE device_level_record_id = %s;
        """

        cursor = conn.cursor()
        cursor.execute(sql_update_notify, (NotifyDetail.NotifyID,))
        conn.commit()
        cursor.close()

        return {"message": "Record updated successfully." , "status" : 'ok'}

    except Exception as e:
        # Handle exceptions and return an HTTP 500 error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")