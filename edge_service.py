import asyncio
import json
import asyncpg
import paho.mqtt.client as mqtt
from datetime import datetime

# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

EDGE_DB_URL = "postgresql://postgres:postgres@localhost:5432/vis_db"
MQTT_BROKER = "34.18.159.205"
MQTT_PORT = 1883

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

# ---------------- MQTT ----------------
mqtt_client = mqtt.Client()

# ---------------- DB ----------------
async def get_pool():
    return await asyncpg.create_pool(EDGE_DB_URL)

# ---------------- PUBLISH FUNCTIONS ----------------
async def publish_table_data(pool, table_name):
    global last_seen_ids

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT * FROM {table_name}
            WHERE id > $1
            ORDER BY id ASC
            """,
            last_seen_ids[table_name]
        )

        print(f"[{table_name}] Found {len(rows)} new records to sync (last_seen_id: {last_seen_ids[table_name]})")
        
        for row in rows:
            payload = dict(row)
            last_seen_ids[table_name] = row["id"]

            message = json.dumps({
                "table": table_name,
                "op": "upsert",
                "data": payload
            }, cls=DateTimeEncoder)
            
            result = mqtt_client.publish(
                PUB_TOPICS[table_name],
                message,
                qos=1
            )
            
            print(f"[{table_name}] Published record ID {row['id']} to {PUB_TOPICS[table_name]} - Result: {result.rc}")

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
        print(f"[{table}] Ignoring core update - only detection_alerts updates are allowed")
        return
        
    try:
        print(f"[{table}] Received core update for record ID {data.get('id', 'unknown')}")
        
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
            print(f"[{table}] Successfully applied core update for record ID {processed_data.get('id', 'unknown')}")
            
    except Exception as e:
        print(f"[{table}] ERROR applying core update: {e}")
        print(f"[{table}] Data that failed: {data}")

# ---------------- MQTT HANDLER ----------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        table = payload.get("table")
        data = payload.get("data", {})
        
        print(f"[MQTT] Received {table} data from core: ID {data.get('id', 'unknown')}")
        
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
    pool = await get_pool()
    loop = asyncio.get_running_loop()

    mqtt_client.user_data_set({"pool": pool, "loop": loop})
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    for t in SUB_TOPICS:
        mqtt_client.subscribe(t, qos=1)

    mqtt_client.loop_start()

    while True:
        await publish_detection_alerts(pool)
        await publish_cameras(pool)
        await publish_advanced_rules(pool)
        await publish_advanced_rulesets(pool)
        await publish_rule_assignments(pool)
        await asyncio.sleep(2)

asyncio.run(main())
