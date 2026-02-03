import asyncio
import json
import asyncpg
import paho.mqtt.client as mqtt
from datetime import datetime
import os
# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

EDGE_DB_URL = os.getenv("EDGE_DB_URL", "postgresql://postgres:postgres@localhost:5432/vis_db")
MQTT_BROKER = os.getenv("MQTT_BROKER", "api.doh.camnitive.ai")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))

PUB_TOPICS = {
    
    "cameras": "edge/to/core/cameras",
    "advanced_rules": "edge/to/core/advanced_rules",
    "advanced_rulesets": "edge/to/core/advanced_rulesets",
    "rule_assignments": "edge/to/core/rule_assignments",
    "detection_alerts": "edge/to/core/detection_alerts"
}
SUB_TOPICS = [
    "core/to/edge/detection_alerts",  # Only allow detection_alerts updates from core
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
            last_seen_ids[table_name] = 0

# ---------------- MQTT ----------------
mqtt_client = mqtt.Client()
mqtt_client.tls_set(
    ca_certs="certs/serverca1.crt",
    certfile="certs/mqtt-client-c1.crt",
    keyfile="certs/mqtt-client-c1.private.pem",
)
mqtt_client.tls_insecure_set(False)   
# ---------------- DB ----------------
async def get_pool():
    return await asyncpg.create_pool(EDGE_DB_URL)

# ---------------- PUBLISH FUNCTIONS ----------------
async def publish_table_data(pool, table_name):
    global last_seen_ids

    try:
        async with pool.acquire() as conn:
            query_from_id = last_seen_ids[table_name]

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
                return

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
                    print(f"[{table_name}] MQTT publish failed, stopping batch")
                    break

                # 🔑 advance cursor ONLY after successful publish
                last_seen_ids[table_name] = row["id"]

            if not initial_sync_done[table_name]:
                initial_sync_done[table_name] = True

    except Exception as e:
        print(f"[{table_name}] ERROR in publish_table_data: {e}")

async def publish_detection_alerts(pool):
    await publish_table_data(pool, "detection_alerts")

async def publish_cameras(pool):
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
    
    # Force reset all last_seen_ids to ensure full sync
    for table_name in last_seen_ids.keys():
        last_seen_ids[table_name] = 0
    
    # Initialize database connection
    pool = await get_pool()
    
    # Initialize last_seen_ids
    await initialize_last_seen_ids(pool)
    
    loop = asyncio.get_running_loop()

    # Setup MQTT
    mqtt_client.user_data_set({"pool": pool, "loop": loop})
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
   
    for t in SUB_TOPICS:
        mqtt_client.subscribe(t, qos=1)
        print(f"[MAIN] Subscribed to: {t}")

    mqtt_client.loop_start()
    print("[MAIN] MQTT loop started")
    print("[MAIN] Starting sync loop...")

    while True:
        try:
            # Sync in dependency order: rulesets -> rules -> assignments
            await publish_advanced_rulesets(pool)  # First: no dependencies
            await publish_advanced_rules(pool)     # Second: depends on rulesets
            await publish_rule_assignments(pool)   # Third: depends on rules
            
            # These can be synced in any order
            await publish_cameras(pool)
            await publish_detection_alerts(pool)
        except Exception as e:
            print(f"[MAIN] ERROR in sync loop: {e}")
        
        await asyncio.sleep(2)

asyncio.run(main())
