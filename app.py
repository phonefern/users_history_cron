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
from flask import Flask, request, jsonify, Response


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
    
    This function automatically handles both timestamp and lastUpdate fields by:
    1. Using compound queries when filtering by date to check both fields
    2. Storing both timestamp values in the database
    3. Using the maximum (most recent) timestamp for sorting and processing
    
    Args:
        collection: The Firestore collection to migrate ('users' or 'temps')
        since: Only migrate records newer than this datetime (checks both timestamp and lastUpdate)
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
        
        # Get all user documents in the collection
        users_ref = fs_db.collection(collection)
        user_docs = list(users_ref.stream())
        
        for user_doc in user_docs:
            user_id = user_doc.id
            records_ref = fs_db.collection(collection).document(user_id).collection("records")
            
            if full_sync:
                records_query = records_ref
            elif since:
                # Use a compound query to get records newer than the since date
                # by checking both timestamp and lastUpdate fields
                # This ensures we don't miss records that might have different timestamp formats
                # Note: Firestore compound queries with AND require both fields to exist
                # For now, let's use a simpler approach - get all records and filter in Python
                records_query = records_ref
            else:
                records_query = records_ref
            
            user_records = list(records_query.stream())
            
            # Filter by since date if specified (in Python since Firestore compound queries are complex)
            if since and not full_sync:
                filtered_records = []
                for rec in user_records:
                    data = rec.to_dict()
                    timestamp_ts = parse_ts(data.get("timestamp"))
                    last_update_ts = parse_ts(data.get("lastUpdate"))
                    
                    # Check if either timestamp is newer than the since date
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
                
                # Get both timestamp fields and use the maximum (most recent)
                timestamp_ts = parse_ts(data.get("timestamp"))
                last_update_ts = parse_ts(data.get("lastUpdate"))
                
                # Use the maximum of both timestamps, fallback to minimum if neither exists
                # This ensures we always use the most recent timestamp available
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
                        # Prepare data for insertion
                        version = data.get("version")
                        prediction = data.get("prediction", {})
                        risk_val = prediction.get("risk") if isinstance(prediction, dict) else None
                        prediction_risk_for_db = str(risk_val) if risk_val is not None else None
                        
                        # Get both timestamp values for storage
                        timestamp_ts = parse_ts(data.get("timestamp"))
                        last_update_ts = parse_ts(data.get("lastUpdate"))
                        
                        counts = {
                            field: 1 if data.get(field) is not None else 0 
                            for field in FIELDS_TO_COUNT
                        }
                        
                        if not dry_run:
                            # Build and execute the insert query
                            fields = ', '.join(FIELDS_TO_COUNT)
                            placeholders = ', '.join(['%s'] * (6 + len(FIELDS_TO_COUNT)))  # Fixed: 6 base fields + FIELDS_TO_COUNT
                            
                            # Debug logging
                            base_values = [user_id, recorder, rec_id, version, last_update_ts, prediction_risk_for_db]
                            field_values = [counts[f] for f in FIELDS_TO_COUNT]
                            all_values = base_values + field_values
                            
                            logger.debug(f"SQL Fields: {len(FIELDS_TO_COUNT)} + 6 = {6 + len(FIELDS_TO_COUNT)}")
                            logger.debug(f"Base values: {len(base_values)}")
                            logger.debug(f"Field values: {len(field_values)}")
                            logger.debug(f"Total values: {len(all_values)}")
                            logger.debug(f"Expected: {6 + len(FIELDS_TO_COUNT)}")
                            
                            # Verify the count matches
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

    return parser.parse_args()
    

def main():
    """Main entry point for the migration script."""
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    args = parse_args()
    
    # Set logging level
    logger.setLevel(args.log_level)
    

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
            # If you prefer to treat as full sync when no status, you can set full_sync=True here.
            # For safety, we will leave full_sync as-is (default False) which will process all users but
            # filtering will be applied only if 'since' is set (we are not forcing full sync).

    
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

        # ถ้าไม่เป็น dry-run และ migration สำเร็จ ให้บันทึก last execution เป็นเวลาปัจจุบัน (UTC)
        if not args.dry_run:
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

############################
# Flask application section #
############################

# Create Flask app for Render web service
app = Flask(__name__)


def _parse_since_param(since_str: Optional[str]) -> Optional[datetime]:
    if not since_str:
        return None
    try:
        val = float(since_str)
        if val > 1e12:
            return datetime.fromtimestamp(val / 1000.0, tz=BANGKOK_TZ)
        return datetime.fromtimestamp(val, tz=BANGKOK_TZ)
    except ValueError:
        try:
            return datetime.strptime(since_str, '%Y-%m-%d').replace(tzinfo=BANGKOK_TZ)
        except ValueError:
            raise ValueError("Invalid since format. Use YYYY-MM-DD or unix(ts/ms)")


def _to_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).lower() in {"1", "true", "t", "yes", "y", "on"}


@app.before_request
def _load_env_if_needed():
    # Ensure .env is loaded in web context
    load_dotenv()


@app.get("/health")
def health() -> Response:
    return Response("ok", mimetype="text/plain")


@app.get("/")
def index() -> Response:
    html = (
        """
        <!doctype html>
        <html>
        <head>
          <meta charset='utf-8'/>
          <meta name='viewport' content='width=device-width, initial-scale=1'/>
          <title>Migration Trigger</title>
          <style>
            body { font-family: system-ui, sans-serif; margin: 2rem; }
            label { display:block; margin: .5rem 0 .25rem; }
            input, select { padding:.5rem; width: 320px; max-width: 100%; }
            button { padding:.6rem 1rem; margin-top:1rem; }
            pre { background:#f6f8fa; padding:1rem; overflow:auto; }
            .row { margin-bottom: .5rem; }
          </style>
        </head>
        <body>
          <h1>Trigger Migration</h1>
          <form id="f">
            <div class="row">
              <label>Collection</label>
              <select name="collection" required>
                <option value="users">users</option>
                <option value="temps">temps</option>
              </select>
            </div>
            <div class="row">
              <label>Since (YYYY-MM-DD or unix ts/ms)</label>
              <input name="since" placeholder="Optional" />
            </div>
            <div class="row">
              <label>Full sync</label>
              <select name="full_sync">
                <option value="false">false</option>
                <option value="true">true</option>
              </select>
            </div>
            <div class="row">
              <label>Dry run</label>
              <select name="dry_run">
                <option value="true">true</option>
                <option value="false">false</option>
              </select>
            </div>
            <div class="row">
              <label>Page size</label>
              <input type="number" name="page_size" value="%PAGE_SIZE%" min="1" />
            </div>
            <div class="row">
              <label>Log level</label>
              <select name="log_level">
                <option>INFO</option>
                <option>DEBUG</option>
                <option>WARNING</option>
                <option>ERROR</option>
                <option>CRITICAL</option>
              </select>
            </div>
            <button type="submit">Run migration</button>
          </form>
          <h2>Result</h2>
          <pre id="out">(no run yet)</pre>
          <script>
            const f = document.getElementById('f');
            const out = document.getElementById('out');
            f.addEventListener('submit', async (e) => {
              e.preventDefault();
              const data = new FormData(f);
              const body = Object.fromEntries(data.entries());
              const res = await fetch('/migrate', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
              const txt = await res.text();
              try { out.textContent = JSON.stringify(JSON.parse(txt), null, 2); } catch { out.textContent = txt; }
            });
          </script>
        </body>
        </html>
        """
        .replace("%PAGE_SIZE%", str(DEFAULT_PAGE_SIZE))
    )
    return Response(html, mimetype="text/html")


@app.route("/migrate", methods=["GET", "POST"])
def migrate_http():
    try:
        payload = request.get_json(silent=True) or {}
        # Merge in query params as fallback
        if not payload:
            payload = {**request.args}

        collection = (payload.get("collection") or "").strip()
        if collection not in COLLECTIONS_TO_MIGRATE:
            return jsonify({
                "ok": False,
                "error": f"Invalid collection. Use one of {COLLECTIONS_TO_MIGRATE}"
            }), 400

        log_level = (payload.get("log_level") or "INFO").upper()
        if log_level in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            logger.setLevel(log_level)

        full_sync = _to_bool(payload.get("full_sync"), default=False)
        dry_run = _to_bool(payload.get("dry_run"), default=True)
        page_size_raw = payload.get("page_size")
        try:
            page_size = int(page_size_raw) if page_size_raw is not None else DEFAULT_PAGE_SIZE
        except Exception:
            page_size = DEFAULT_PAGE_SIZE

        since_date = None
        since_param = payload.get("since")
        if since_param:
            since_date = _parse_since_param(str(since_param))
        elif not full_sync:
            last_exec = get_last_execution_from_db()
            if last_exec:
                since_date = last_exec

        start = time.time()
        processed, migrated = migrate_all_to_users_history(
            collection=collection,
            since=since_date,
            full_sync=full_sync,
            page_size=page_size,
            dry_run=dry_run
        )

        if not dry_run:
            set_last_execution_in_db(datetime.now(pytz.UTC))

        duration = round(time.time() - start, 3)
        return jsonify({
            "ok": True,
            "collection": collection,
            "processed": processed,
            "migrated": migrated,
            "dry_run": dry_run,
            "full_sync": full_sync,
            "since_used": since_date.isoformat() if since_date else None,
            "duration_sec": duration
        })
    except Exception as e:
        logger.error("/migrate failed", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# Keep CLI usage working
if __name__ == "__main__":
    # If launched directly, run the CLI entrypoint
    main()

