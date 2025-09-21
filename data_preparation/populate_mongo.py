import os
import random
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
import numpy as np
import pandas as pd

from sdt_algorithm import sdt_compress

load_dotenv(dotenv_path='../.env')

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_RAW_DATA = os.getenv("COLLECTION_RAW_DATA")
COLLECTION_COMPRESSED_DATA = os.getenv("COLLECTION_COMPRESSED_DATA")

ENTITY_ID = os.getenv("ENTITY_ID")
SDT_TOLERANCE = float(os.getenv("SDT_TOLERANCE"))

def generate_simulated_power_data(start_datetime, end_datetime, frequency_seconds=15):
    timestamps = pd.date_range(start=start_datetime, end=end_datetime, freq=f'{frequency_seconds}S')
    
    base_power = 5000
    noise_amplitude = 100
    daily_pattern_amplitude = 2000
    weekly_pattern_amplitude = 1000

    data = []
    for ts in timestamps:
        hour_of_day = ts.hour + ts.minute / 60.0
        daily_pattern = daily_pattern_amplitude * (np.sin(np.pi * (hour_of_day - 6) / 12) * 0.5 + 0.5)

        day_of_week = ts.dayofweek
        weekly_factor = 1.0 if day_of_week < 5 else 0.7

        noise = random.uniform(-noise_amplitude, noise_amplitude)
        
        value = (base_power + daily_pattern) * weekly_factor + noise
        value = max(0, value)

        data.append({'timestamp': int(ts.timestamp()), 'value': round(value, 2)})
    return data

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    client.admin.command('ping')

    db[COLLECTION_RAW_DATA].drop()
    db[COLLECTION_COMPRESSED_DATA].drop()

    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=15)

    all_raw_data = generate_simulated_power_data(start_dt, end_dt, frequency_seconds=15)
    
    db[COLLECTION_RAW_DATA].insert_many(all_raw_data)

    current_day = start_dt
    while current_day <= end_dt:
        day_start_ts = int(current_day.timestamp())
        day_end_ts = int((current_day + timedelta(days=1) - timedelta(seconds=1)).timestamp())
        date_str = current_day.strftime('%Y-%m-%d')

        daily_raw_points = [
            p for p in all_raw_data 
            if day_start_ts <= p['timestamp'] <= day_end_ts
        ]

        if daily_raw_points:
            compressed_points = sdt_compress(daily_raw_points, SDT_TOLERANCE)
            
            if compressed_points:
                doc = {
                    "entity_id": ENTITY_ID,
                    "date": date_str,
                    "points": compressed_points
                }
                db[COLLECTION_COMPRESSED_DATA].insert_one(doc)
        current_day += timedelta(days=1)
        
if __name__ == '__main__':
    main()