from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, firestore
try:
    import psycopg2
    import psycopg2.extensions
    PSYCOPG_VERSION = 2
except ImportError:
    import psycopg as psycopg2
    import psycopg
    PSYCOPG_VERSION = 3
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
import pytz
import logging
from typing import Optional, Dict, List, Tuple, Union
import json
import threading

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Constants
BANGKOK_TZ = pytz.timezone('Asia/Bangkok')
FIELDS_TO_COUNT = [
    "balance", "dualTap", "dualTapRight", "gaitWalk",
    "pinchToSize", "pinchToSizeRight", "questionnaire",
    "tremorPostural", "tremorResting", "voiceAhh", "voiceYPL"
]
COLLECTIONS_TO_MIGRATE = ['users', 'temps']
DEFAULT_PAGE_SIZE = 100

# Global variables for Firebase
fs_db = None
firebase_initialized = False

def parse_ts(val: Union[datetime, int, float, str, None]) -> Optional[datetime]:
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

def initialize_firebase() -> firestore.client:
    """Initializes Firebase from environment variables."""
    global fs_db, firebase_initialized
    
    if firebase_initialized and fs_db:
        return fs_db
    
    try:
        firebase_creds_json = os.environ.get('FIREBASE_CREDS')
        if not firebase_creds_json:
            raise ValueError("FIREBASE_CREDS environment variable not set")
        
        firebase_creds_dict = json.loads(firebase_creds_json)
        
        if 'private_key' in firebase_creds_dict:
            firebase_creds_dict['private_key'] = firebase_creds_dict['private_key'].replace('\\n', '\n')
            
        cred = credentials.Certificate(firebase_creds_dict)
        
        # Check if already initialized
        try:
            firebase_admin.get_app()
            logger.info("Firebase already initialized")
        except ValueError:
            firebase_admin.initialize_app(cred)
            logger.info("Firebase initialized successfully")
        
        fs_db = firebase_admin.firestore.client()
        firebase_initialized = True
        return fs_db
        
    except Exception as e:
        logger.critical(f"Firebase initialization failed: {str(e)}")
        raise

def get_db_connection(connection_purpose: Optional[str] = None):
    """
    Establishes a connection to the Supabase PostgreSQL database.
    Compatible with both psycopg2 and psycopg3.
    """
    conn_params = {
        'host': os.getenv("SUPABASE_HOST", "aws-0-ap-southeast-1.pooler.supabase.com"),
        'port': os.getenv("SUPABASE_PORT", "6543"),
        'dbname': os.getenv("SUPABASE_DB", "postgres"),
        'user': os.getenv("SUPABASE_USER", "postgres.mieiwzfhohifeprjtnek"),
        'password': os.getenv("SUPABASE_PASSWORD", "root"),
        'connect_timeout': 10
    }
    
    try:
        start_time = time.time()
        
        if PSYCOPG_VERSION == 3:
            # psycopg3 syntax
            conn = psycopg2.connect(**conn_params)
        else:
            # psycopg2 syntax
            conn = psycopg2.connect(**conn_params)
        
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid() AS pid, current_database()")
            db_info = cur.fetchone()
        
        logger.info(
            f"DB connection established (psycopg{PSYCOPG_VERSION}) | PID: {db_info[0]} | DB: {db_info[1]} | "
            f"Time: {round((time.time() - start_time) * 1000, 2)}ms"
        )
        return conn
    except Exception as e:
        logger.error(f"DB connection failed: {str(e)}")
        raise

def get_last_execution_from_db() -> Optional[datetime]:
    """
    Retrieves the last successful execution time from the Supabase `migration_status` table.
    Returns a timezone-aware datetime in BANGKOK_TZ, or None if not found.
    """
    conn = None
    cur = None
    try:
        conn = get_db_connection("get_last_execution")
        cur = conn.cursor()
        cur.execute("SELECT last_execution_time_history FROM migration_status WHERE id = %s", ('current_status',))
        result = cur.fetchone()
        if result and result[0]:
            last_utc: datetime = result[0]
            if last_utc.tzinfo is None:
                last_utc = pytz.utc.localize(last_utc)
            return last_utc.astimezone(BANGKOK_TZ)
        logger.info("No last execution time found in DB, assuming first run.")
        return None
    except Exception as e:
        logger.error(f"Error fetching last_execution_time from DB: {str(e)}", exc_info=True)
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def set_last_execution_in_db(execution_time_utc: Optional[datetime] = None) -> bool:
    """
    Upserts the last successful execution time into migration_status.id='current_status'.
    """
    conn = None
    cur = None
    try:
        if execution_time_utc is None:
            execution_time_utc = datetime.now(pytz.UTC)
        else:
            if execution_time_utc.tzinfo is None:
                execution_time_utc = pytz.utc.localize(execution_time_utc)
            execution_time_utc = execution_time_utc.astimezone(pytz.UTC)

        conn = get_db_connection("set_last_execution")
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO migration_status (id, last_execution_time_history)
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE SET
                last_execution_time_history = EXCLUDED.last_execution_time_history;
        """, ('current_status', execution_time_utc))

        conn.commit()
        logger.info(f"Updated migration_status.last_execution_time_history = {execution_time_utc.isoformat()}")
        return True
    except Exception as e:
        logger.error(f"Failed to update migration_status in DB: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def migrate_all_to_users_history(
    collection: str,
    since: Optional[datetime] = None,
    full_sync: bool = False,
    page_size: int = DEFAULT_PAGE_SIZE,
    dry_run: bool = False
) -> Tuple[int, int]:
    """
    Migrates records from Firestore to Supabase users_history table.
    """
    if collection not in COLLECTIONS_TO_MIGRATE:
        logger.error(f"Invalid collection: {collection}")
        return 0, 0
    
    fs_db = initialize_firebase()
    total_processed = 0
    total_migrated = 0
    
    try:
        conn = get_db_connection("migration to users_history")
        cur = conn.cursor()
        
        logger.info(f"Starting migration for collection: {collection}")
        
        users_ref = fs_db.collection(collection)
        user_docs = list(users_ref.stream())
        
        for user_doc in user_docs:
            user_id = user_doc.id
            records_ref = fs_db.collection(collection).document(user_id).collection("records")
            
            records_query = records_ref
            user_records = list(records_query.stream())
            
            # Filter by since date if specified
            if since and not full_sync:
                filtered_records = []
                for rec in user_records:
                    data = rec.to_dict()
                    timestamp_ts = parse_ts(data.get("timestamp"))
                    last_update_ts = parse_ts(data.get("lastUpdate"))
                    
                    if timestamp_ts and timestamp_ts > since:
                        filtered_records.append(rec)
                    elif last_update_ts and last_update_ts > since:
                        filtered_records.append(rec)
                user_records = filtered_records
            
            total_processed += len(user_records)
            
            if not user_records:
                continue
            
            # Group records by recorder
            grouped_records: Dict[str, List[Tuple[datetime, str, Dict]]] = {}
            for rec in user_records:
                data = rec.to_dict()
                recorder = data.get("recorder") or "unknown"
                
                timestamp_ts = parse_ts(data.get("timestamp"))
                last_update_ts = parse_ts(data.get("lastUpdate"))
                
                if timestamp_ts and last_update_ts:
                    effective_timestamp = max(timestamp_ts, last_update_ts)
                elif timestamp_ts:
                    effective_timestamp = timestamp_ts
                elif last_update_ts:
                    effective_timestamp = last_update_ts
                else:
                    effective_timestamp = datetime.min.replace(tzinfo=pytz.UTC)
                
                grouped_records.setdefault(recorder, []).append((effective_timestamp, rec.id, data))
            
            # Process each recorder's records
            for recorder, rec_list in grouped_records.items():
                for last_update_ts, rec_id, data in rec_list:
                    try:
                        version = data.get("version")
                        prediction = data.get("prediction", {})
                        risk_val = prediction.get("risk") if isinstance(prediction, dict) else None
                        prediction_risk_for_db = str(risk_val) if risk_val is not None else None
                        
                        timestamp_ts = parse_ts(data.get("timestamp"))
                        last_update_ts = parse_ts(data.get("lastUpdate"))
                        
                        counts = {
                            field: 1 if data.get(field) is not None else 0 
                            for field in FIELDS_TO_COUNT
                        }
                        
                        if not dry_run:
                            fields = ', '.join(FIELDS_TO_COUNT)
                            placeholders = ', '.join(['%s'] * (6 + len(FIELDS_TO_COUNT)))
                            
                            base_values = [user_id, recorder, rec_id, version, last_update_ts, prediction_risk_for_db]
                            field_values = [counts[f] for f in FIELDS_TO_COUNT]
                            all_values = base_values + field_values
                            
                            cur.execute(f"""
                                INSERT INTO users_history (
                                    user_id, recorder, record_id, version, last_update,
                                    prediction_risk, {fields}
                                ) VALUES ({placeholders})
                                ON CONFLICT (user_id, recorder, record_id) DO NOTHING;
                            """, all_values)
                            
                            total_migrated += 1
                            if total_migrated % 100 == 0:
                                conn.commit()
                                logger.info(f"Migrated {total_migrated} records...")
                        
                    except Exception as e:
                        logger.error(
                            f"Error processing record {collection}/{user_id}/{recorder}/{rec_id}: {str(e)}",
                            exc_info=True
                        )
                        conn.rollback()
        
        logger.info(f"Finished processing {collection} collection")
        
        if not dry_run:
            conn.commit()
        
        logger.info(
            f"Migration complete. Processed {total_processed} records, "
            f"migrated {total_migrated} new records."
        )
        return total_processed, total_migrated
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def run_migration_job(collections: List[str], full_sync: bool = False, dry_run: bool = False):
    """
    Run migration job for specified collections
    """
    results = []
    start_time = time.time()
    
    try:
        # Get last execution time if not full sync
        since_date = None
        if not full_sync:
            since_date = get_last_execution_from_db()
            if since_date:
                logger.info(f"Using last execution time: {since_date.isoformat()}")
        
        total_processed = 0
        total_migrated = 0
        
        for collection in collections:
            if collection not in COLLECTIONS_TO_MIGRATE:
                logger.warning(f"Skipping invalid collection: {collection}")
                continue
                
            processed, migrated = migrate_all_to_users_history(
                collection=collection,
                since=since_date,
                full_sync=full_sync,
                page_size=DEFAULT_PAGE_SIZE,
                dry_run=dry_run
            )
            
            total_processed += processed
            total_migrated += migrated
            
            results.append({
                'collection': collection,
                'processed': processed,
                'migrated': migrated
            })
        
        # Update last execution time if successful and not dry run
        if not dry_run and total_migrated > 0:
            set_last_execution_in_db(datetime.now(pytz.UTC))
        
        duration = time.time() - start_time
        
        return {
            'success': True,
            'duration': duration,
            'total_processed': total_processed,
            'total_migrated': total_migrated,
            'collections': results,
            'since_date': since_date.isoformat() if since_date else None,
            'full_sync': full_sync,
            'dry_run': dry_run,
            'psycopg_version': PSYCOPG_VERSION
        }
        
    except Exception as e:
        logger.error(f"Migration job failed: {str(e)}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'duration': time.time() - start_time,
            'psycopg_version': PSYCOPG_VERSION
        }

# Flask Routes

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        conn = get_db_connection("health_check")
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now(pytz.UTC).isoformat(),
            'services': {
                'database': 'connected',
                'firebase': 'initialized' if firebase_initialized else 'not_initialized'
            },
            'psycopg_version': PSYCOPG_VERSION
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now(pytz.UTC).isoformat(),
            'psycopg_version': PSYCOPG_VERSION
        }), 500

@app.route('/migrate', methods=['POST'])
def trigger_migration():
    """
    Trigger migration via HTTP POST request
    """
    try:
        data = request.get_json() or {}
        
        collections = data.get('collections', COLLECTIONS_TO_MIGRATE)
        full_sync = data.get('full_sync', False)
        dry_run = data.get('dry_run', False)
        run_async = data.get('async', False)
        
        # Validate collections
        invalid_collections = [c for c in collections if c not in COLLECTIONS_TO_MIGRATE]
        if invalid_collections:
            return jsonify({
                'success': False,
                'error': f'Invalid collections: {invalid_collections}',
                'valid_collections': COLLECTIONS_TO_MIGRATE
            }), 400
        
        logger.info(f"Migration triggered via API: collections={collections}, full_sync={full_sync}, dry_run={dry_run}")
        
        if run_async:
            # Run in background thread
            def run_in_background():
                result = run_migration_job(collections, full_sync, dry_run)
                logger.info(f"Background migration completed: {result}")
            
            thread = threading.Thread(target=run_in_background)
            thread.daemon = True
            thread.start()
            
            return jsonify({
                'success': True,
                'message': 'Migration started in background',
                'collections': collections,
                'full_sync': full_sync,
                'dry_run': dry_run,
                'psycopg_version': PSYCOPG_VERSION
            }), 202
        else:
            # Run synchronously
            result = run_migration_job(collections, full_sync, dry_run)
            
            if result['success']:
                return jsonify(result), 200
            else:
                return jsonify(result), 500
                
    except Exception as e:
        logger.error(f"API error: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'psycopg_version': PSYCOPG_VERSION
        }), 500

@app.route('/migrate/status', methods=['GET'])
def migration_status():
    """Get migration status and last execution time"""
    try:
        last_execution = get_last_execution_from_db()
        
        return jsonify({
            'last_execution_time': last_execution.isoformat() if last_execution else None,
            'last_execution_time_utc': last_execution.astimezone(pytz.UTC).isoformat() if last_execution else None,
            'current_time': datetime.now(BANGKOK_TZ).isoformat(),
            'current_time_utc': datetime.now(pytz.UTC).isoformat(),
            'timezone': 'Asia/Bangkok',
            'available_collections': COLLECTIONS_TO_MIGRATE,
            'psycopg_version': PSYCOPG_VERSION
        }), 200
        
    except Exception as e:
        logger.error(f"Status check failed: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'psycopg_version': PSYCOPG_VERSION
        }), 500

@app.route('/', methods=['GET'])
def index():
    """Root endpoint with API documentation"""
    return jsonify({
        'service': 'Firestore to Supabase Migration Service',
        'version': '1.0.0',
        'psycopg_version': PSYCOPG_VERSION,
        'endpoints': {
            'GET /health': 'Health check',
            'GET /migrate/status': 'Get migration status',
            'POST /migrate': 'Trigger migration',
        },
        'migrate_payload_example': {
            'collections': ['users', 'temps'],
            'full_sync': False,
            'dry_run': False,
            'async': False
        }
    }), 200

if __name__ == '__main__':
    # Initialize Firebase on startup
    try:
        initialize_firebase()
        logger.info(f"Service initialized successfully with psycopg{PSYCOPG_VERSION}")
    except Exception as e:
        logger.error(f"Service initialization failed: {str(e)}")
    
    # Get port from environment variable (Render uses PORT)
    port = int(os.environ.get('PORT', 5000))
    
    # Run Flask app
    app.run(
        host='0.0.0.0',
        port=port,
        debug=os.environ.get('FLASK_ENV') == 'development'
    )