import os
from flask import Flask, request, jsonify
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta

from sdt import linear_interpolate, calculate_weighted_average

load_dotenv(dotenv_path='../.env')

app = Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_RAW_DATA = os.getenv("COLLECTION_RAW_DATA")
COLLECTION_COMPRESSED_DATA = os.getenv("COLLECTION_COMPRESSED_DATA")
FLASK_PORT = int(os.getenv("FLASK_PORT", 5000)) 

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

@app.route('/health', methods=['GET'])
def health_check():
    try:
        client.admin.command('ping')
        return jsonify({"status": "healthy", "mongodb_connection": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

@app.route('/visualize_interpolated', methods=['GET'])
def get_interpolated_data():
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    interval_minutes = int(request.args.get('interval_minutes', 15))
    entity_id = request.args.get('entity_id', os.getenv("ENTITY_ID"))

    if not start_date_str or not end_date_str:
        return jsonify({"error": "start_date e end_date são obrigatórios."}), 400

    try:
        start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        return jsonify({"error": "Formato de data inválido. Use YYYY-MM-DD."}), 400

    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    interval_seconds = interval_minutes * 60
    
    all_compressed_points = []
    
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime('%Y-%m-%d')
        daily_doc = db[COLLECTION_COMPRESSED_DATA].find_one(
            {"entity_id": entity_id, "date": date_str}
        )
        if daily_doc and 'points' in daily_doc:
            all_compressed_points.extend(daily_doc['points'])
        current_dt += timedelta(days=1)
        
    all_compressed_points = sorted(all_compressed_points, key=lambda p: p['timestamp'])

    if not all_compressed_points:
        return jsonify({"message": "Nenhum dado compactado encontrado para o período."}), 404

    interpolated_data = linear_interpolate(
        all_compressed_points, start_ts, end_ts, interval_seconds
    )
    
    return jsonify({
        "entity_id": entity_id,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "interval_minutes": interval_minutes,
        "data": interpolated_data
    }), 200

@app.route('/calculate_aggregate', methods=['GET'])
def get_aggregate_data():
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    entity_id = request.args.get('entity_id', os.getenv("ENTITY_ID"))

    if not start_date_str or not end_date_str:
        return jsonify({"error": "start_date e end_date são obrigatórios."}), 400

    try:
        start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        return jsonify({"error": "Formato de data inválido. Use YYYY-MM-DD."}), 400
        
    all_compressed_points = []
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime('%Y-%m-%d')
        daily_doc = db[COLLECTION_COMPRESSED_DATA].find_one(
            {"entity_id": entity_id, "date": date_str}
        )
        if daily_doc and 'points' in daily_doc:
            all_compressed_points.extend(daily_doc['points'])
        current_dt += timedelta(days=1)
        
    all_compressed_points = sorted(all_compressed_points, key=lambda p: p['timestamp'])

    if not all_compressed_points:
        return jsonify({"message": "Nenhum dado compactado encontrado para o período."}), 404

    weighted_avg = calculate_weighted_average(all_compressed_points)

    return jsonify({
        "entity_id": entity_id,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "weighted_average_value": round(weighted_avg, 2)
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=FLASK_PORT)