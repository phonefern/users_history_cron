import firebase_admin
from firebase_admin import credentials, firestore
import psycopg2
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
import pytz
import argparse
import logging
from typing import Optional, Dict, List, Tuple, Union
import json
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('migration.log')
    ]
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
    try:
        # Check if Firebase is already initialized
        if firebase_admin._apps:
            return firebase_admin.firestore.client()
            
        firebase_creds_json = os.environ.get('FIREBASE_CREDS')
        if not firebase_creds_json:
            raise ValueError("FIREBASE_CREDS environment variable not set")
        
        firebase_creds_dict = json.loads(firebase_creds_json)
        
        if 'private_key' in firebase_creds_dict:
            firebase_creds_dict['private_key'] = firebase_creds_dict['private_key'].replace('\\n', '\n')
            
        cred = credentials.Certificate(firebase_creds_dict)
        firebase_admin.initialize_app(cred)
        logger.info("Firebase initialized successfully.")
        return firebase_admin.firestore.client()
    except Exception as e:
        logger.critical(f"Firebase initialization failed: {str(e)}")
        raise

def get_db_connection(connection_purpose: Optional[str] = None) -> psycopg2.extensions.connection:
    """
    Establishes a connection to the Supabase PostgreSQL database.
    
    Args:
        connection_purpose: Optional description of connection purpose
        
    Returns:
        psycopg2 connection object
        
    Raises:
        Exception: If connection fails
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
        conn = psycopg2.connect(**conn_params)
        
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid() AS pid, current_database()")
            db_info = cur.fetchone()
        
        logger.info(
            f"DB connection established | PID: {db_info[0]} | DB: {db_info[1]} | "
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
            # DB should store UTC; convert to Bangkok timezone for local use
            last_utc: datetime = result[0]
            if last_utc.tzinfo is None:
                # assume DB returned naive UTC → localize
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
    Stores the provided datetime as UTC in the DB (or uses now UTC if None).
    Returns True on success, False otherwise.
    """
    conn = None
    cur = None
    try:
        if execution_time_utc is None:
            execution_time_utc = datetime.now(pytz.UTC)
        else:
            # ensure stored time is UTC
            if execution_time_utc.tzinfo is None:
                execution_time_utc = pytz.utc.localize(execution_time_utc)
            execution_time_utc = execution_time_utc.astimezone(pytz.UTC)

        conn = get_db_connection("set_last_execution")
        cur = conn.cursor()

        # If migration_status table already has an entry with id='current_status', update it;
        # otherwise insert a new row. Adjust columns if your table has different schema.
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
    Migrates records from Firestore to Supabase users_history table based on collection selection.

    Changes:
        - When using `since`, only filter by `timestamp`
        - `lastUpdate` is stored but no longer used for filtering or sorting

    Args:
        collection: The Firestore collection to migrate ('users' or 'temps')
        since: Only migrate records with timestamp newer than this datetime
        full_sync: Migrate all records regardless of timestamp
        page_size: Number of user documents to process at once
        dry_run: If True, only count records without inserting

    Returns:
        Tuple of (total_records_processed, records_migrated)
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

            # Always fetch all records, filtering happens in Python
            records_query = records_ref
            user_records = list(records_query.stream())

            # Filter by timestamp if since is provided and full_sync is False
            if since and not full_sync:
                filtered_records = []
                for rec in user_records:
                    data = rec.to_dict()
                    timestamp_ts = parse_ts(data.get("timestamp"))

                    # Only check timestamp, ignore lastUpdate completely
                    if timestamp_ts and timestamp_ts > since:
                        filtered_records.append(rec)
                user_records = filtered_records

            total_processed += len(user_records)
            if not user_records:
                continue

            # Group records by recorder (no need to sort by lastUpdate anymore)
            grouped_records: Dict[str, List[Tuple[datetime, str, Dict]]] = {}
            for rec in user_records:
                data = rec.to_dict()
                recorder = data.get("recorder") or "unknown"

                # Use timestamp only; fallback to datetime.min if missing
                timestamp_ts = parse_ts(data.get("timestamp"))
                effective_timestamp = timestamp_ts or datetime.min.replace(tzinfo=pytz.UTC)

                grouped_records.setdefault(recorder, []).append((effective_timestamp, rec.id, data))

            # Process each recorder's records
            for recorder, rec_list in grouped_records.items():
                for effective_timestamp, rec_id, data in rec_list:
                    try:
                        version = data.get("version")
                        prediction = data.get("prediction", {})
                        risk_val = prediction.get("risk") if isinstance(prediction, dict) else None
                        prediction_risk_for_db = str(risk_val) if risk_val is not None else None

                        # Store lastUpdate in DB, but ignore it for filtering
                        last_update_ts = parse_ts(data.get("lastUpdate"))

                        counts = {
                            field: 1 if data.get(field) is not None else 0
                            for field in FIELDS_TO_COUNT
                        }

                        if not dry_run:
                            fields = ', '.join(FIELDS_TO_COUNT)
                            placeholders = ', '.join(['%s'] * (6 + len(FIELDS_TO_COUNT)))

                            base_values = [user_id, recorder, rec_id, version, effective_timestamp, prediction_risk_for_db]
                            field_values = [counts[f] for f in FIELDS_TO_COUNT]
                            all_values = base_values + field_values

                            if len(all_values) != (6 + len(FIELDS_TO_COUNT)):
                                raise ValueError(f"Value count mismatch: {len(all_values)} vs {6 + len(FIELDS_TO_COUNT)}")

                            cur.execute(f"""
                                INSERT INTO users_history (
                                    user_id, recorder, record_id, version, last_update,
                                    prediction_risk, {fields}
                                ) VALUES ({placeholders})
                                ON CONFLICT (user_id, recorder, record_id) DO NOTHING;
                            """, all_values)

                            total_migrated += 1
                            if total_migrated % 10 == 0:
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


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Migrate data from Firestore to Supabase PostgreSQL"
    )
    parser.add_argument(
        '--collection',
        type=str,
        required=True,
        help='The Firestore collection to migrate (users or temps)'
    )
    parser.add_argument(
        '--since',
        type=str,
        help='Only migrate records newer than this date (YYYY-MM-DD or timestamp)'
    )
    parser.add_argument(
        '--full-sync',
        action='store_true',
        help='Migrate all records regardless of timestamp'
    )
    parser.add_argument(
        '--page-size',
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f'Number of user documents to process at once (default: {DEFAULT_PAGE_SIZE})'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Count records without actually migrating them'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set the logging level'
    )
    parser.add_argument(
        '--http',
        action='store_true',
        help='Run as HTTP server instead of CLI'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help='Port for HTTP server (default: 5000)'
    )

    return parser.parse_args()


@app.route('/run', methods=['GET', 'POST'])
def run_migration():
    """HTTP endpoint to trigger migration."""
    try:
        # Get parameters from query string or JSON body
        if request.method == 'GET':
            migration_type = request.args.get('type', 'incremental')
            collection = request.args.get('collection', 'users')
            dry_run = request.args.get('dry_run', 'false').lower() == 'true'
        else:
            data = request.get_json() or {}
            migration_type = data.get('type', 'incremental')
            collection = data.get('collection', 'users')
            dry_run = data.get('dry_run', False)
        
        # Validate collection
        if collection not in COLLECTIONS_TO_MIGRATE:
            return jsonify({
                'status': 'error',
                'message': f'Invalid collection: {collection}. Must be one of {COLLECTIONS_TO_MIGRATE}'
            }), 400
        
        # Determine since date based on migration type
        since_date = None
        if migration_type == 'incremental':
            logger.info("Running incremental migration")
            since_date = get_last_execution_from_db()
            if since_date:
                logger.info(f"Using last execution time from DB: {since_date.isoformat()}")
            else:
                logger.info("No prior execution time found; will process all records")
        
        # Run migration
        start_time = time.time()
        processed, migrated = migrate_all_to_users_history(
            collection=collection,
            since=since_date,
            full_sync=(migration_type == 'full'),
            dry_run=dry_run
        )
        
        # Update last execution time if not dry run
        if not dry_run and migrated > 0:
            set_last_execution_in_db(datetime.now(pytz.UTC))
        
        execution_time = time.time() - start_time
        
        return jsonify({
            'status': 'success',
            'collection': collection,
            'type': migration_type,
            'dry_run': dry_run,
            'processed': processed,
            'migrated': migrated,
            'execution_time_seconds': round(execution_time, 2)
        })
        
    except Exception as e:
        logger.error(f"HTTP migration failed: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'})


def main():
    """Main entry point for the migration script."""
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    args = parse_args()
    
    # Set logging level
    logger.setLevel(args.log_level)
    
    # Run as HTTP server if requested
    if args.http:
        logger.info(f"Starting HTTP server on port {args.port}")
        app.run(host='0.0.0.0', port=args.port)
        return
    
    # Parse since date if provided
    since_date = None
    if args.since:
        try:
            # Try to parse as timestamp (seconds or milliseconds)
            val = float(args.since)
            # assume seconds if reasonable magnitude, otherwise ms
            if val > 1e12:
                since_date = datetime.fromtimestamp(val / 1000.0, tz=BANGKOK_TZ)
            else:
                since_date = datetime.fromtimestamp(val, tz=BANGKOK_TZ)
        except ValueError:
            try:
                # Try to parse as date string YYYY-MM-DD
                since_date = datetime.strptime(args.since, '%Y-%m-%d').replace(tzinfo=BANGKOK_TZ)
            except ValueError as e:
                logger.error(f"Invalid date format for --since: {args.since}")
                raise

    # If no --since provided, try to read last execution from DB (incremental)
    if since_date is None and not args.full_sync:
        logger.info("No --since provided; attempting to read last execution time from migration_status table.")
        last_exec = get_last_execution_from_db()
        if last_exec:
            since_date = last_exec
            logger.info(f"Using last execution time from DB: {since_date.isoformat()}")
        else:
            logger.info("No prior execution time found; running full scan (or use --since to restrict).")

    
    logger.info("Starting migration process")
    logger.info(f"Arguments: {vars(args)}")
    if since_date:
        logger.info(f"Only migrating records since: {since_date}")
    
    start_time = time.time()
    
    try:
        processed, migrated = migrate_all_to_users_history(
            collection=args.collection,
            since=since_date,
            full_sync=args.full_sync,
            page_size=args.page_size,
            dry_run=args.dry_run
        )

        # Update last execution time if not dry run
        if not args.dry_run and migrated > 0:
            ok = set_last_execution_in_db(datetime.now(pytz.UTC))
            if not ok:
                logger.warning("Migration finished but failed to update migration_status.last_execution_time_history")

        logger.info(
            f"Migration completed in {time.time() - start_time:.2f} seconds. "
            f"Processed: {processed}, Migrated: {migrated}"
        )

        
    except Exception as e:
        logger.critical(f"Migration failed after {time.time() - start_time:.2f} seconds: {str(e)}")
        raise


if __name__ == "__main__":
    main()