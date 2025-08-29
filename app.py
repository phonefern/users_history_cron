import firebase_admin
from firebase_admin import credentials, firestore
import psycopg2
import os
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

from psycopg2.extras import execute_values
import time
from datetime import datetime, timedelta
import pytz
import threading
from flask import Flask, render_template_string, request, redirect, url_for, jsonify
from functools import wraps
import json
import logging
import schedule
import itertools


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
BANGKOK_TZ = pytz.timezone('Asia/Bangkok')

# --- Firebase Initialization ---
def initialize_firebase():
    """Initializes Firebase from environment variables."""
    try:
        firebase_creds_json = os.environ.get('FIREBASE_CREDS')
        if not firebase_creds_json:
            raise ValueError("FIREBASE_CREDS environment variable not set. Please set it as a JSON string.")

        firebase_creds_dict = json.loads(firebase_creds_json)
        
        if 'private_key' in firebase_creds_dict and isinstance(firebase_creds_dict['private_key'], str):
            firebase_creds_dict['private_key'] = firebase_creds_dict['private_key'].replace('\\n', '\n')
            
        cred = credentials.Certificate(firebase_creds_dict)
        firebase_admin.initialize_app(cred)
        logger.info("Firebase initialized successfully.")
        return firebase_admin.firestore.client()
    except Exception as e:
        logger.critical(f"Firebase initialization failed: {str(e)}. Exiting.")
        raise

# Initialize Firebase (this will raise an error if env var is missing/malformed)
fs_db = initialize_firebase()

# --- Supabase (PostgreSQL) Connection ---
def get_db_connection(connection_purpose=None):
    """
    Establishes a connection to the Supabase PostgreSQL database with enhanced logging.
    """
    conn = None
    connection_info = {
        'host': os.getenv("SUPABASE_HOST", "aws-0-ap-southeast-1.pooler.supabase.com"),
        'port': os.getenv("SUPABASE_PORT", "6543"),
        'dbname': os.getenv("SUPABASE_DB", "postgres"),
        'user': os.getenv("SUPABASE_USER", "postgres.mieiwzfhohifeprjtnek"),
        'connect_timeout': 10
    }
    
    try:
        logged_info = connection_info.copy()
        logged_info['password'] = '*******' if os.getenv("SUPABASE_PASSWORD") else 'default_pwd'
        
        logger.debug(f"Attempting DB connection with params: {logged_info}")
        if connection_purpose:
            logger.debug(f"Connection purpose: {connection_purpose}")
        
        start_time = time.time()
        conn = psycopg2.connect(
            password=os.getenv("SUPABASE_PASSWORD", "root"),
            **connection_info
        )
        
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid() AS pid, current_database()")
            db_info = cur.fetchone()
            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE pid != pg_backend_pid()")
            other_connections = cur.fetchone()[0]
        
        connection_time = round((time.time() - start_time) * 1000, 2)
        logger.info(
            f"DB connection established | "
            f"PID: {db_info[0]} | DB: {db_info[1]} | "
            f"Other active connections: {other_connections} | "
            f"Connection time: {connection_time}ms"
        )
        
        return conn
        
    except Exception as e:
        logger.error(
            f"DB connection failed after {round((time.time() - start_time) * 1000, 2)}ms | "
            f"Error: {str(e)}"
        )
        raise

last_execution = None 
is_running = False 
execution_history = [] 

FIELDS_TO_COUNT = [
    "balance", "dualTap", "dualTapRight", "gaitWalk",
    "pinchToSize", "pinchToSizeRight", "questionnaire",
    "tremorPostural", "tremorResting", "voiceAhh", "voiceYPL"
]

def now_thai():
    """Returns the current datetime in the Bangkok timezone."""
    return datetime.now(BANGKOK_TZ)

def format_dt(dt):
    """Formats a datetime object to a string in Bangkok timezone, or 'ยังไม่เคย' if None."""
    if not dt:
        return "ยังไม่เคย"
    return dt.astimezone(BANGKOK_TZ).strftime("%Y-%m-%d %H:%M:%S")

def parse_ts(val):
    """Parses various timestamp formats into a Bangkok-aware datetime object."""
    try:
        if isinstance(val, datetime):
            if val.tzinfo is None:
                return pytz.utc.localize(val).astimezone(BANGKOK_TZ)
            return val.astimezone(BANGKOK_TZ)
        if isinstance(val, (int, float)):
            return datetime.fromtimestamp(val / 1000, BANGKOK_TZ)
        if isinstance(val, str):
            return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(BANGKOK_TZ)
        return None
    except Exception as e:
        logger.warning(f"Failed to parse timestamp value '{val}' (type: {type(val)}): {str(e)}")
        return None

# --- Persistent State Management in Supabase ---
def get_last_execution_from_db():
    """Retrieves the last successful execution time from the Supabase `migration_status` table."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT last_execution_time_history FROM migration_status WHERE id = 'current_status'")
        result = cur.fetchone()
        if result and result[0]:
            return result[0].astimezone(BANGKOK_TZ)
        logger.info("No last execution time found in DB, assuming first run.")
        return None
    except Exception as e:
        logger.error(f"Error fetching last_execution_time_history from DB: {str(e)}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def save_last_execution_to_db(timestamp):
    """Saves the given timestamp as the last successful execution time to Supabase."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO migration_status (id, last_execution_time_history)
            VALUES ('current_status', %s)
            ON CONFLICT (id) DO UPDATE SET last_execution_time_history = EXCLUDED.last_execution_time_history, updated_at = NOW();
        """, (timestamp.astimezone(pytz.UTC),))
        conn.commit()
        logger.info(f"Saved last_execution_time_history to DB: {format_dt(timestamp)}")
    except Exception as e:
        logger.error(f"Error saving last_execution_time_history to DB: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Decorator for Background Tasks ---
def background_task(fn):
    """Decorator to manage task running status and logging."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        global is_running
        if is_running:
            logger.warning(f"Attempted to start {fn.__name__} but another task is already running.")
            return {"status": "error", "message": "Task already running"}
        
        is_running = True
        result = {"status": "error", "message": "Unknown error"}
        try:
            logger.info(f"Starting background task: {fn.__name__}")
            result = fn(*args, **kwargs)
            logger.info(f"Completed background task: {fn.__name__} (Status: {result.get('status')})")
        except Exception as e:
            logger.error(f"Critical error in background task {fn.__name__}: {str(e)}", exc_info=True)
            result = {"status": "error", "message": f"Critical task error: {str(e)}"}
        finally:
            is_running = False
        return result
    return wrapper

# --- Main Migration Logic ---
@background_task
def migrate_all(full_sync=False):
    global last_execution
    
    current_start_time = now_thai()
    since_timestamp = None if full_sync else get_last_execution_from_db()
    
    user_count = 0
    temp_count = 0
    summary_count = 0
    migration_status = "success"
    error_message = ""

    try:
        # Migrate summaries from both users and temps collections
        summary_count = migrate_summaries(since=since_timestamp, full_sync=full_sync)
        logger.info(f"Migration completed: {summary_count} summaries processed from both users and temps collections.")

    except Exception as e:
        migration_status = "failed"
        error_message = str(e)
        logger.error(f"Migration process failed: {str(e)}", exc_info=True)
    finally:
        save_last_execution_to_db(now_thai())
        last_execution = now_thai()

        execution_history.append({
            "type": "full" if full_sync else "incremental",
            "start_time": current_start_time,
            "end_time": last_execution,
            "user_count": user_count,
            "temp_count": temp_count,
            "summary_count": summary_count,
            "status": migration_status if migration_status == "success" else f"failed: {error_message}"
        })
    
    return {
        "status": migration_status,
        "user_count": user_count,
        "temp_count": temp_count,
        "summary_count": summary_count,
        "message": error_message
    }

@background_task
def test_migration(record_count=10):
    """Test migration with a limited number of records"""
    global last_execution
    
    current_start_time = now_thai()
    user_count = 0
    temp_count = 0
    summary_count = 0
    migration_status = "success"
    error_message = ""

    try:
        summary_count = migrate_summaries(limit=record_count)
        logger.info(f"Test migration completed: {summary_count} summaries processed.")
    except Exception as e:
        migration_status = "failed"
        error_message = str(e)
        logger.error(f"Test migration failed: {str(e)}", exc_info=True)
    finally:
        save_last_execution_to_db(now_thai())
        last_execution = now_thai()

        execution_history.append({
            "type": "test",
            "start_time": current_start_time,
            "end_time": last_execution,
            "user_count": user_count,
            "temp_count": temp_count,
            "summary_count": summary_count,
            "status": migration_status if migration_status == "success" else f"failed: {error_message}",
            "test_record_count": record_count
        })
    
    return {
        "status": migration_status,
        "user_count": user_count,
        "temp_count": temp_count,
        "summary_count": summary_count,
        "message": error_message
    }

def migrate_summaries(since=None, full_sync=False, limit=None, page_size=100):
    """
    Migrates user record summaries from both 'users' and 'temps' collections efficiently.
    Processes records based on timestamp and last_execution_time_history.
    """
    total_count = 0
    conn = None
    cur = None

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Process both collections
        collections = ['users', 'temps']
        
        for collection_name in collections:
            logger.info(f"Processing {collection_name} collection...")
            collection_count = process_collection(
                conn, cur, collection_name, since, full_sync, limit, page_size
            )
            total_count += collection_count
            logger.info(f"Completed {collection_name}: {collection_count} summaries")

        conn.commit()
        logger.info(f"Total summaries migrated: {total_count}")
        return total_count

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def process_collection(conn, cur, collection_name, since, full_sync, limit, page_size):
    """Process a single Firestore collection (users or temps)"""
    count = 0
    last_doc = None

    try:
        # Get all documents from the collection
        collection_ref = fs_db.collection(collection_name)
        
        while True:
            # Get a page of documents
            query = collection_ref.limit(page_size)
            if last_doc:
                query = query.start_after(last_doc)
            
            docs = query.stream()
            docs_list = list(docs)
            
            if not docs_list:
                break
            
            for doc in docs_list:
                user_id = doc.id
                last_doc = doc
                user_data = doc.to_dict()
                
                # Process all records for this user
                user_count = process_user_records(conn, cur, collection_name, user_id, user_data, since, full_sync)
                count += user_count
                
                # Check if we've reached the limit
                if limit and count >= limit:
                    break
            
            if limit and count >= limit:
                break
        
        return count
        
    except Exception as e:
        logger.error(f"Error processing collection {collection_name}: {str(e)}")
        raise

def process_user_records(conn, cur, collection_name, user_id, user_data, since, full_sync):
    """Process all records for a specific user"""
    count = 0
    
    try:
        # Get all records for this user
        records_ref = fs_db.collection(collection_name).document(user_id).collection("records")
        
        # Apply timestamp filter if this is an incremental sync
        if not full_sync and since:
            records_query = records_ref.where("timestamp", ">=", since.astimezone(pytz.UTC))
        else:
            records_query = records_ref
        
        all_records = list(records_query.stream())
        
        if not all_records:
            return 0
        
        # Group records by recorder
        grouped_records = {}
        for rec in all_records:
            data = rec.to_dict()
            recorder = data.get("recorder") or "unknown"
            timestamp = parse_ts(data.get("timestamp")) or datetime.min.replace(tzinfo=pytz.UTC)
            
            grouped_records.setdefault(recorder, []).append({
                'timestamp': timestamp,
                'id': rec.id,
                'data': data
            })
        
        # Build batch rows and insert using execute_values
        rows_to_insert = []
        for recorder, records in grouped_records.items():
            try:
                # Sort by timestamp (newest first)
                records.sort(key=lambda x: x['timestamp'], reverse=True)
                
                # Find the record with risk prediction, or fallback to newest
                record_with_risk = None
                for record in records:
                    prediction = record['data'].get('prediction', {})
                    if isinstance(prediction, dict) and prediction.get('risk') is not None:
                        record_with_risk = record
                        break
                
                if not record_with_risk:
                    record_with_risk = records[0] if records else None
                
                if not record_with_risk:
                    continue
                
                # Prepare data for insertion
                latest_data = record_with_risk['data']
                prediction = latest_data.get('prediction', {})
                risk_val = prediction.get('risk') if isinstance(prediction, dict) else None
                prediction_risk_for_db = str(risk_val) if risk_val is not None else None
                
                # Count fields across all records for this recorder
                field_counts = {field: 0 for field in FIELDS_TO_COUNT}
                for record in records:
                    for field in FIELDS_TO_COUNT:
                        if record['data'].get(field) is not None:
                            field_counts[field] += 1
                
                row = [
                    user_id,
                    recorder,
                    record_with_risk['id'],
                    latest_data.get('version'),
                    record_with_risk['timestamp'],
                    prediction_risk_for_db,
                    len(records)
                ] + [field_counts[f] for f in FIELDS_TO_COUNT]
                rows_to_insert.append(tuple(row))
                
            except Exception as e:
                logger.error(f"Error preparing recorder {recorder} for user {user_id}: {str(e)}")
                conn.rollback()
        
        if rows_to_insert:
            try:
                columns = "user_id, recorder, record_id, version, last_update, prediction_risk, record_count, " + ", ".join(FIELDS_TO_COUNT)
                placeholders = ", ".join(["%s"] * (7 + len(FIELDS_TO_COUNT)))
                sql = f"INSERT INTO users_history ({columns}) VALUES %s ON CONFLICT (user_id, recorder, record_id) DO NOTHING"
                execute_values(cur, sql, rows_to_insert, template=f"({placeholders})")
                count += len(rows_to_insert)
                conn.commit()
                logger.info(f"Committed {count} summaries so far...")
            except Exception as e:
                logger.error(f"Error executing batch insert for user {user_id}: {str(e)}")
                conn.rollback()
        
        return count
        
    except Exception as e:
        logger.error(f"Error processing user {user_id}: {str(e)}")
        raise

# --- Flask Routes ---
@app.route("/")
def index():
    """Renders the main migration console dashboard."""
    next_scheduled_run = schedule.next_run()
    
    return render_template_string("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Firebase-Supabase Migration Console (UTC+7)</title>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background-color: #f8f8f8; color: #333; line-height: 1.6;}
                .card { background: #ffffff; padding: 25px; margin: 20px 0; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
                h1, h2 { color: #2c3e50; margin-top: 0;}
                .status-info p { margin: 5px 0; }
                .status-info strong { color: #007bff; }
                .thai-time { font-weight: bold; color: #0066cc; }
                .button-group { margin-top: 20px; text-align: center; }
                .button { 
                    background: #28a745;
                    color: white; 
                    border: none; 
                    padding: 12px 20px; 
                    margin: 8px; 
                    border-radius: 6px; 
                    cursor: pointer; 
                    text-decoration: none; 
                    display: inline-block;
                    font-size: 1rem;
                    transition: background-color 0.3s ease, transform 0.2s ease;
                }
                .button:hover { 
                    background-color: #218838; 
                    transform: translateY(-2px);
                }
                .button.full { 
                    background: #dc3545;
                }
                .button.full:hover { 
                    background-color: #c82333;
                }
                .history-section { margin-top: 30px; }
                .history-item { 
                    background: #f0f0f0; 
                    padding: 15px; 
                    margin-bottom: 10px; 
                    border-radius: 5px; 
                    border-left: 5px solid;
                }
                .history-item.success { border-color: #28a745; }
                .history-item.failed { border-color: #dc3545; }
                .history-item p { margin: 3px 0; }
                .history-item strong { color: #495057; }
                .success-text { color: #28a745; font-weight: bold; }
                .failed-text { color: #dc3545; font-weight: bold; }
            </style>
        </head>
        <body>
            <div class="card">
                <h1>Firebase to Supabase Migration (ไทยเวลา UTC+7)</h1>
                <div class="status-info">
                    <p>สถานะ: <strong>{{ 'กำลังทำงาน' if is_running else 'พร้อมทำงาน' }}</strong></p>
                    <p>เวลาปัจจุบัน: <span class="thai-time">{{ now }}</span></p>
                    <p>การดำเนินการล่าสุด: <span class="thai-time">{{ last }}</span></p>
                    <p>การดำเนินการถัดไป (ในแอป): <span class="thai-time">{{ next }}</span></p>
                    <p><em>(การตั้งเวลาอัตโนมัติบน Render ต้องใช้ Cron Jobs ภายนอก)</em></p>
                </div>
                
                <form method="post" action="/run" class="button-group">
                    <button class="button" type="submit" name="type" value="incremental">ซิงค์เฉพาะข้อมูลใหม่</button>
                    <button class="button full" type="submit" name="type" value="full">ซิงค์ข้อมูลทั้งหมด</button>
                </form>
                
                <div class="test-form">
                    <form method="post" action="/test" class="button-group">
                        <label for="record_count">ทดสอบการซิงค์ (จำนวนเรคคอร์ด):</label>
                        <input type="number" id="record_count" name="record_count" min="1" max="1000" value="10">
                        <button class="button test" type="submit">ทดสอบการซิงค์</button>
                    </form>
                </div>
            </div>
            
            <div class="card history-section">
                <h2>ประวัติการทำงานล่าสุด</h2>
                {% if history %}
                    {% for entry in history %}
                    <div class="history-item {{ 'success' if 'success' in entry.status else 'failed' }}">
                        <p><strong>{{ 'ซิงค์ทั้งหมด' if entry.type == 'full' else 'ซิงค์เฉพาะใหม่' }}</strong></p>
                        <p>เริ่มต้น: <span class="thai-time">{{ entry.start_time }}</span></p>
                        <p>สิ้นสุด: <span class="thai-time">{{ entry.end_time }}</span></p>
                        <p>สถานะ: <span class="{{ 'success-text' if 'success' in entry.status else 'failed-text' }}">
                            {{ entry.status }}
                        </span></p>
                        <p>สรุปข้อมูลที่ประมวลผล: {{ entry.summary_count }}</p>
                    </div>
                    {% endfor %}
                {% else %}
                    <p>ยังไม่มีประวัติการทำงาน</p>
                {% endif %}
            </div>
        </body>
        </html>
    """, 
    now=format_dt(now_thai()),
    last=format_dt(last_execution),
    next=format_dt(next_scheduled_run) if next_scheduled_run else "ไม่ได้กำหนด/ปิดใช้งาน",
    is_running=is_running,
    history=execution_history[::-1]
    )

@app.route("/test", methods=["POST"])
def trigger_test_migration():
    """Endpoint to manually trigger a test migration."""
    try:
        record_count = int(request.form.get("record_count", 10))
        record_count = max(1, min(1000, record_count))
    except ValueError:
        record_count = 10
        
    logger.info(f"Manual test trigger for {record_count} records received.")
    
    thread = threading.Thread(target=lambda: test_migration(record_count=record_count))
    thread.daemon = True
    thread.start()
    
    return redirect(url_for("index"))

@app.route("/run", methods=["POST"])
def trigger_migration():
    """Endpoint to manually trigger a migration."""
    migration_type = request.form.get("type", "incremental")
    full_sync = (migration_type == "full")
    logger.info(f"Manual trigger for {migration_type} sync received.")
    
    thread = threading.Thread(target=lambda: migrate_all(full_sync=full_sync))
    thread.daemon = True
    thread.start()
    
    return redirect(url_for("index"))

@app.route("/api/status")
def api_status():
    """API endpoint to get the current status of the migration."""
    next_scheduled_run = schedule.next_run()
    return jsonify({
        "status": "running" if is_running else "idle",
        "timezone": "Asia/Bangkok (UTC+7)",
        "current_time": format_dt(now_thai()),
        "last_execution": format_dt(last_execution),
        "next_scheduled_in_app": format_dt(next_scheduled_run) if next_scheduled_run else None,
        "execution_history": [
            {
                "type": h["type"],
                "start_time": format_dt(h["start_time"]),
                "end_time": format_dt(h["end_time"]),
                "summary_count": h["summary_count"],
                "status": h["status"]
            } for h in execution_history[::-1]
        ]
    })

# --- Scheduler Setup ---
def schedule_jobs():
    """Configures and runs the in-app scheduler."""
    schedule.every().sunday.at("00:00", BANGKOK_TZ).do(lambda: migrate_all(full_sync=True)).tag("weekly")
    schedule.every().day.at("03:00", BANGKOK_TZ).do(lambda: migrate_all(full_sync=False)).tag("daily")
    
    logger.info("In-app scheduler started. Checking for pending jobs every 30 seconds.")
    while True:
        schedule.run_pending()
        time.sleep(30)

# --- Application Entry Point ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    last_execution = get_last_execution_from_db()

    if os.getenv('RENDER'):
        logger.info("Detected RENDER environment. Running Flask app without in-app scheduler thread.")
        app.run(host="0.0.0.0", port=port)
    else:
        logger.info("Running in local development environment. Starting Flask app with in-app scheduler.")
        threading.Thread(target=schedule_jobs, daemon=True).start()
        app.run(host="0.0.0.0", port=port)