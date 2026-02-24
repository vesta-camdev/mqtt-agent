import asyncio
import json
import asyncpg
import paho.mqtt.client as mqtt
from datetime import datetime
import os
import time
import aiohttp
import base64
import hashlib
from pathlib import Path
import logging
from collections import deque
import aiofiles
# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

EDGE_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/vis_db")
MQTT_BROKER = os.getenv("MQTT_BROKER", "api.doh.camnitive.ai")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))
UPLOAD_ENDPOINT = os.getenv("UPLOAD_ENDPOINT", "https://meta-data-uploader-942931391784.me-central1.run.app/upload-alert")

# Upload retry queue and configuration
upload_retry_queue = deque()
max_retry_attempts = 5
retry_delay_base = 2  # exponential backoff base

PUB_TOPICS = {
    
    "cameras": "edge/to/core/cameras",
    "advanced_rules": "edge/to/core/advanced_rules",
    "advanced_rulesets": "edge/to/core/advanced_rulesets",
    "rule_assignments": "edge/to/core/rule_assignments",
    "detection_alerts": "edge/to/core/detection_alerts"
}
SUB_TOPICS = [
    "core/to/edge/detection_alerts",  
]

last_seen_ids = {
    "detection_alerts": 0,
    "cameras": 0,
    "advanced_rules": 0,
    "advanced_rulesets": 0,
    "rule_assignments": 0
}

# Track if initial sync has been done
initial_sync_done = {
    "detection_alerts": False,
    "cameras": False,
    "advanced_rules": False,
    "advanced_rulesets": False,
    "rule_assignments": False
}

# Initialize last_seen_ids from database
async def initialize_last_seen_ids(pool):
    global last_seen_ids
    
    if pool is None:
        print("[INIT] No database connection, using default last_seen_ids")
        return
    
    for table_name in last_seen_ids.keys():
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT COALESCE(MAX(id), 0) as max_id FROM {table_name}")
                if row:
                    # Force reset to 0 to sync all existing records
                    last_seen_ids[table_name] = 0
                    
                else:
                    last_seen_ids[table_name] = 0
                    
        except Exception as e:
            print(f"[INIT] Error initializing {table_name}: {e}")
            last_seen_ids[table_name] = 0

# ---------------- IMAGE UPLOAD FUNCTIONS ----------------
async def calculate_file_checksum(file_path: str) -> str:
    """Calculate SHA256 checksum of a file."""
    try:
        async with aiofiles.open(file_path, 'rb') as f:
            content = await f.read()
            return hashlib.sha256(content).hexdigest()
    except Exception as e:
        print(f"[UPLOAD] Error calculating checksum for {file_path}: {e}")
        return ""

async def upload_image_to_bucket(pool, alert_record):
    """Upload image to bucket and return the core_url."""
    try:
        alert_id = alert_record['id']
        detection_frame_path = alert_record.get('detection_frame_path')
        
        if not detection_frame_path:
            print(f"[UPLOAD] Alert {alert_id}: No detection_frame_path found")
            return None
        
        # Convert container path to actual mounted path
        if detection_frame_path.startswith('/app/storage/detection_frames/'):
            actual_path = detection_frame_path.replace('/app/storage/detection_frames/', '/home/camnitive-5/camnitive/camedge/storage/detection_frames/')
        else:
            actual_path = detection_frame_path
            
        # Check if file exists
        if not Path(actual_path).exists():
            print(f"[UPLOAD] Alert {alert_id}: Image file not found at {actual_path} (original: {detection_frame_path})")
            return None
            
        # Get device_uuid from edge_devices table
        device_uuid = await get_device_uuid(pool, alert_record.get('edge_id'))
        if not device_uuid:
            print(f"[UPLOAD] Alert {alert_id}: Could not retrieve device_uuid")
            return None
            
        # Read and encode image
        async with aiofiles.open(actual_path, 'rb') as f:
            image_bytes = await f.read()
            image_base64 = base64.b64encode(image_bytes).decode('utf-8')
            
        # Calculate checksum
        checksum = hashlib.sha256(image_bytes).hexdigest()
        
        # Prepare upload payload
        payload = {
            "image_data": image_base64,
            "checksum": checksum,
            "edge_id": alert_record.get('edge_id'),
            "uuid_id": device_uuid,
            "image_frame_path": detection_frame_path  # Keep original path in payload
        }
        
        # Make upload request
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(UPLOAD_ENDPOINT, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    if result.get('success'):
                        file_path = result.get('file_path')
                        print(f"[UPLOAD] Alert {alert_id}: Successfully uploaded to {file_path}")
                        return file_path
                    else:
                        print(f"[UPLOAD] Alert {alert_id}: Upload failed - {result.get('message')}")
                        return None
                else:
                    error_text = await response.text()
                    print(f"[UPLOAD] Alert {alert_id}: Upload failed with status {response.status}: {error_text}")
                    return None
                    
    except Exception as e:
        print(f"[UPLOAD] Alert {alert_id}: Upload error - {e}")
        return None

async def get_device_uuid(pool, edge_id):
    """Retrieve device_uuid from edge_devices table."""
    if pool is None or not edge_id:
        return None
        
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT uuid FROM edge_devices WHERE edge_id = $1",
                edge_id
            )
            if row:
                return row['uuid']
            else:
                print(f"[UPLOAD] No device_uuid found for edge_id: {edge_id}")
                return None
    except Exception as e:
        print(f"[UPLOAD] Error retrieving device_uuid for {edge_id}: {e}")
        return None

async def update_alert_core_url(pool, alert_id, core_url):
    """Update detection_alerts record with core_url."""
    if pool is None:
        return False
        
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE detection_alerts SET core_url = $1 WHERE id = $2",
                core_url, alert_id
            )
            print(f"[UPLOAD] Updated alert {alert_id} with core_url: {core_url}")
            return True
    except Exception as e:
        print(f"[UPLOAD] Error updating core_url for alert {alert_id}: {e}")
        return False

async def process_upload_retry_queue(pool):
    """Process failed uploads in retry queue."""
    if not upload_retry_queue:
        return
        
    # Process up to 5 items per cycle to avoid blocking
    for _ in range(min(5, len(upload_retry_queue))):
        retry_item = upload_retry_queue.popleft()
        alert_record = retry_item['alert_record']
        attempt = retry_item['attempt']
        
        if attempt >= max_retry_attempts:
            print(f"[RETRY] Alert {alert_record['id']}: Max retry attempts reached, giving up")
            continue
            
        print(f"[RETRY] Alert {alert_record['id']}: Retry attempt {attempt + 1}")
        
        # Try upload again
        core_url = await upload_image_to_bucket(pool, alert_record)
        
        if core_url:
            # Success! Update database
            if await update_alert_core_url(pool, alert_record['id'], core_url):
                print(f"[RETRY] Alert {alert_record['id']}: Retry successful")
            else:
                # Database update failed, add back to queue
                retry_item['attempt'] += 1
                upload_retry_queue.append(retry_item)
        else:
            # Upload failed again, add back to queue with exponential backoff
            retry_item['attempt'] += 1
            upload_retry_queue.append(retry_item)
            
            # Add delay before next attempt
            await asyncio.sleep(min(retry_delay_base ** attempt, 60))

# ---------------- MQTT ----------------
mqtt_client = mqtt.Client()
mqtt_client.tls_set(
    ca_certs="./certs/serverca1.crt",
    certfile="./certs/mqtt-client-c1.crt",
    keyfile="./certs/mqtt-client-c1.private.pem",
)
mqtt_client.tls_insecure_set(False)

# Add MQTT event handlers for better monitoring
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Successfully connected to MQTT broker")
    else:
        print(f"[MQTT] Failed to connect to MQTT broker with result code {rc}")

def on_disconnect(client, userdata, rc):
    print(f"[MQTT] Disconnected from MQTT broker with result code {rc}")

def on_publish(client, userdata, mid):
    print(f"[MQTT] Message {mid} published successfully")

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_publish = on_publish   
# ---------------- DB ----------------
async def get_pool():
    max_retries = 30  # Try for ~5 minutes total
    
    for attempt in range(max_retries):
        try:
            print(f"[DB] Attempting to connect to database (attempt {attempt + 1}/{max_retries})")
            pool = await asyncpg.create_pool(EDGE_DB_URL)
            print("[DB] Database connection successful!")
            return pool
        except Exception as e:
            print(f"[DB] Connection failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                # Optimized retry delays for 2-3 second DB startup:
                # First 10 attempts: 1-2 seconds (fast startup retries)
                # Next 10 attempts: 5 seconds (waiting for slower startups)
                # Remaining: 10 seconds (fallback for major issues)
                if attempt < 10:
                    retry_delay = 1 + (attempt % 2)  # 1s, 2s, 1s, 2s pattern
                elif attempt < 20:
                    retry_delay = 5
                else:
                    retry_delay = 10
                    
                print(f"[DB] Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                print("[DB] Max retries reached. Will continue trying in main loop...")
                # Return None so main loop can handle retry
                return None

# ---------------- PUBLISH FUNCTIONS ----------------
async def publish_table_data(pool, table_name):
    global last_seen_ids

    if pool is None:
        print(f"[{table_name}] No database connection available, skipping...")
        return

    try:
        async with pool.acquire() as conn:
            query_from_id = last_seen_ids[table_name]
            
            print(f"[{table_name}] Querying records with ID > {query_from_id}")

            rows = await conn.fetch(
                f"""
                SELECT *
                FROM {table_name}
                WHERE id > $1
                ORDER BY id ASC
                LIMIT 50
                """,
                query_from_id
            )

            if not rows:
                print(f"[{table_name}] No new records found")
                return
                
            print(f"[{table_name}] Found {len(rows)} new records to publish")

            for row in rows:
                payload = dict(row)

                message = json.dumps(
                    {
                        "table": table_name,
                        "op": "insert" if table_name == "detection_alerts" else "upsert",
                        "data": payload,
                    },
                    cls=DateTimeEncoder,
                )

                result = mqtt_client.publish(
                    PUB_TOPICS[table_name],
                    message,
                    qos=1,
                )

                if result.rc != 0:
                    print(f"[{table_name}] MQTT publish failed with return code {result.rc} for record ID {row['id']}")
                    print(f"[{table_name}] MQTT client connected: {mqtt_client.is_connected()}")
                    print(f"[{table_name}] Topic: {PUB_TOPICS[table_name]}")
                    print(f"[{table_name}] Message length: {len(message)} chars")
                    break

                # 🔑 advance cursor ONLY after successful publish
                last_seen_ids[table_name] = row["id"]
                print(f"[{table_name}] Successfully published record ID {row['id']}")
                
            print(f"[{table_name}] Completed batch: published {len(rows)} records, last ID: {last_seen_ids[table_name]}")

            if not initial_sync_done[table_name]:
                initial_sync_done[table_name] = True
                print(f"[{table_name}] Initial sync completed")

    except Exception as e:
        print(f"[{table_name}] ERROR in publish_table_data: {e}")
        # Don't crash, just log the error and continue

async def publish_detection_alerts(pool):
    """Publish detection_alerts with core_url validation and image upload."""
    global last_seen_ids

    if pool is None:
        print("[detection_alerts] No database connection available, skipping...")
        return

    try:
        async with pool.acquire() as conn:
            query_from_id = last_seen_ids["detection_alerts"]

            rows = await conn.fetch(
                """
                SELECT *
                FROM detection_alerts
                WHERE id > $1
                ORDER BY id ASC
                LIMIT 50
                """,
                query_from_id
            )

            if not rows:
                return

            for row in rows:
                alert_record = dict(row)
                alert_id = alert_record['id']
                core_url = alert_record.get('core_url')
                
                # Check if core_url is missing or empty
                if not core_url or core_url.strip() == '':
                    print(f"[detection_alerts] Alert {alert_id}: Missing core_url, checking image file")
                    
                    # Check if image file exists before attempting upload
                    detection_frame_path = alert_record.get('detection_frame_path')
                    if detection_frame_path:
                        # Convert container path to actual mounted path
                        if detection_frame_path.startswith('/app/storage/detection_frames/'):
                            actual_path = detection_frame_path.replace('/app/storage/detection_frames/', '/opt/camnitive/camedge/storage/detection_frames/')
                        else:
                            actual_path = detection_frame_path
                            
                        # Check if file exists
                        if not Path(actual_path).exists():
                            print(f"[detection_alerts] Alert {alert_id}: Image file not found at {actual_path}, skipping record")
                            # Skip this record and advance cursor
                            last_seen_ids["detection_alerts"] = alert_id
                            continue
                    else:
                        print(f"[detection_alerts] Alert {alert_id}: No detection_frame_path found, skipping record")
                        # Skip this record and advance cursor
                        last_seen_ids["detection_alerts"] = alert_id
                        continue
                    
                    # Try to upload image
                    uploaded_core_url = await upload_image_to_bucket(pool, alert_record)
                    
                    if uploaded_core_url:
                        # Update database with new core_url
                        if await update_alert_core_url(pool, alert_id, uploaded_core_url):
                            alert_record['core_url'] = uploaded_core_url
                            print(f"[detection_alerts] Alert {alert_id}: Upload successful, proceeding with publish")
                        else:
                            print(f"[detection_alerts] Alert {alert_id}: Database update failed, skipping publish")
                            continue
                    else:
                        # Upload failed, add to retry queue
                        print(f"[detection_alerts] Alert {alert_id}: Upload failed, adding to retry queue")
                        upload_retry_queue.append({
                            'alert_record': alert_record,
                            'attempt': 0
                        })
                        # Skip publishing this record for now
                        last_seen_ids["detection_alerts"] = alert_id
                        continue
                
                # Publish the record (either had core_url or upload was successful)
                message = json.dumps(
                    {
                        "table": "detection_alerts",
                        "op": "insert",
                        "data": alert_record,
                    },
                    cls=DateTimeEncoder,
                )

                result = mqtt_client.publish(
                    PUB_TOPICS["detection_alerts"],
                    message,
                    qos=1,
                )

                if result.rc != 0:
                    print(f"[detection_alerts] MQTT publish failed for alert {alert_id}, stopping batch")
                    break

                # Advance cursor only after successful publish
                last_seen_ids["detection_alerts"] = alert_id
                print(f"[detection_alerts] Published alert {alert_id} with core_url: {alert_record.get('core_url', 'N/A')[:50]}...")

            if not initial_sync_done["detection_alerts"]:
                initial_sync_done["detection_alerts"] = True

    except Exception as e:
        print(f"[detection_alerts] ERROR in publish_detection_alerts: {e}")
        # Don't crash, just log the error and continue

async def publish_cameras(pool):
    """Publish cameras with enhanced error handling and logging."""
    print(f"[cameras] Starting camera publish cycle, MQTT connected: {mqtt_client.is_connected()}")
    
    # Check if cameras table exists and has records
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                count_row = await conn.fetchrow("SELECT COUNT(*) as total FROM cameras")
                total_cameras = count_row['total'] if count_row else 0
                
                pending_row = await conn.fetchrow(
                    "SELECT COUNT(*) as pending FROM cameras WHERE id > $1", 
                    last_seen_ids["cameras"]
                )
                pending_cameras = pending_row['pending'] if pending_row else 0
                
                print(f"[cameras] Total cameras: {total_cameras}, Pending: {pending_cameras}, Last seen ID: {last_seen_ids['cameras']}")
                
        except Exception as e:
            print(f"[cameras] Error checking camera count: {e}")
    
    await publish_table_data(pool, "cameras")

async def publish_advanced_rules(pool):
    await publish_table_data(pool, "advanced_rules")

async def publish_advanced_rulesets(pool):
    await publish_table_data(pool, "advanced_rulesets")

async def publish_rule_assignments(pool):
    await publish_table_data(pool, "rule_assignments")

# ---------------- APPLY CORE UPDATES ----------------
async def apply_core_update(pool, table, data):
    # Only allow detection_alerts updates from core
    if table != "detection_alerts":
        return
    
    if pool is None:
        print(f"[CORE_UPDATE] No database connection, skipping update for {table}")
        return
        
    try:
        
        # Convert ISO datetime strings back to datetime objects
        processed_data = {}
        datetime_fields = ['created_at', 'updated_at', 'event_time', 'acknowledged_at']
        
        for key, value in data.items():
            if key in datetime_fields and isinstance(value, str) and value:
                try:
                    # Parse ISO format datetime string back to datetime object
                    processed_data[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    processed_data[key] = value
            else:
                processed_data[key] = value
        
        cols = ", ".join(processed_data.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(processed_data)))
        updates = ", ".join(f"{k}=EXCLUDED.{k}" for k in processed_data.keys())

        sql = f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET
        {updates}
        """

        async with pool.acquire() as conn:
            await conn.execute(sql, *processed_data.values())
            
    except Exception as e:
        print(f"[{table}] ERROR applying core update: {e}")
        # Don't crash, just log the error

# ---------------- MQTT HANDLER ----------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        table = payload.get("table")
        data = payload.get("data", {})
        
        asyncio.run_coroutine_threadsafe(
            apply_core_update(
                userdata["pool"],
                table,
                data
            ),
            userdata["loop"]
        )
    except Exception as e:
        print(f"[MQTT] Error processing core message: {e}")
        print(f"[MQTT] Message topic: {msg.topic}")
        print(f"[MQTT] Message payload: {msg.payload.decode()}")

# ---------------- MAIN ----------------
async def main():
    global last_seen_ids
    
    print("[MQTT_AGENT] Starting MQTT Agent...")
    
    # Force reset all last_seen_ids to ensure full sync
    for table_name in last_seen_ids.keys():
        last_seen_ids[table_name] = 0
    
    # Initialize database connection with retries
    pool = await get_pool()
    
    # Initialize last_seen_ids (will work even if pool is None)
    await initialize_last_seen_ids(pool)
    
    loop = asyncio.get_running_loop()

    # Setup MQTT (try to connect, but don't crash if it fails)
    try:
        mqtt_client.user_data_set({"pool": pool, "loop": loop})
        mqtt_client.on_message = on_message
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
       
        for t in SUB_TOPICS:
            mqtt_client.subscribe(t, qos=1)
            print(f"[MQTT] Subscribed to: {t}")

        mqtt_client.loop_start()
        print("[MQTT] MQTT loop started")
    except Exception as e:
        print(f"[MQTT] Failed to connect to MQTT broker: {e}")
        print("[MQTT] Will continue without MQTT...")
    
    print("[MAIN] Starting sync loop...")
    
    # Track last successful DB connection check
    last_db_check = 0
    db_check_interval = 10  # Check DB every 10 seconds if disconnected (optimized for faster recovery)

    while True:
        try:
            # If no pool, try to reconnect to database periodically
            current_time = time.time()
            if pool is None and (current_time - last_db_check) >= db_check_interval:
                print("[DB] Attempting to reconnect to database...")
                pool = await get_pool()
                if pool:
                    # Update user data with new pool
                    mqtt_client.user_data_set({"pool": pool, "loop": loop})
                    await initialize_last_seen_ids(pool)
                last_db_check = current_time
            
            # Only sync if we have a database connection
            if pool is not None:
                # Sync in dependency order: rulesets -> rules -> assignments
                await publish_advanced_rulesets(pool)  # First: no dependencies
                await publish_advanced_rules(pool)     # Second: depends on rulesets
                await publish_rule_assignments(pool)   # Third: depends on rules
                
                # Process upload retry queue first
                await process_upload_retry_queue(pool)
                
                # These can be synced in any order
                await publish_cameras(pool)
                await publish_detection_alerts(pool)
            else:
                # Just wait if no DB connection
                print("[SYNC] No database connection, waiting...")
                
        except Exception as e:
            print(f"[MAIN] ERROR in sync loop: {e}")
            # Check if it's a database connection error
            if "connection" in str(e).lower() or "pool" in str(e).lower():
                print("[DB] Database connection lost, will try to reconnect...")
                pool = None  # Force reconnection attempt
        
        await asyncio.sleep(2)

# Wrap main in try-catch to prevent crashes
def main_cli():
    """CLI entry point for the edge service."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[MQTT_AGENT] Received shutdown signal, exiting...")
    except Exception as e:
        print(f"[MQTT_AGENT] Fatal error: {e}")
        print("[MQTT_AGENT] Restarting in 10 seconds...")
        time.sleep(10)
        # Try to restart
        try:
            asyncio.run(main())
        except:
            print("[MQTT_AGENT] Could not restart, exiting...")

if __name__ == "__main__":
    main_cli()
