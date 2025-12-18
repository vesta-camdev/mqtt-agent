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
MQTT_BROKER = "192.168.18.234"
MQTT_PORT = 1883

PUB_TOPICS = {
    "detection_alerts": "edge/to/core/detection_alerts",
    "cameras": "edge/to/core/cameras",
    "advanced_rules": "edge/to/core/advanced_rules",
    "advanced_rulesets": "edge/to/core/advanced_rulesets",
    "rule_assignments": "edge/to/core/rule_assignments"
}
SUB_TOPICS = [
    "core/to/edge/cameras",
    "core/to/edge/advanced_rules",
    "core/to/edge/advanced_rulesets",
    "core/to/edge/rule_assignments",
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
    cols = ", ".join(data.keys())
    placeholders = ", ".join(f"${i+1}" for i in range(len(data)))
    updates = ", ".join(f"{k}=EXCLUDED.{k}" for k in data.keys())

    sql = f"""
    INSERT INTO {table} ({cols})
    VALUES ({placeholders})
    ON CONFLICT (id) DO UPDATE SET
    {updates}
    """

    async with pool.acquire() as conn:
        await conn.execute(sql, *data.values())

# ---------------- MQTT HANDLER ----------------
def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    asyncio.run_coroutine_threadsafe(
        apply_core_update(
            userdata["pool"],
            payload["table"],
            payload["data"]
        ),
        userdata["loop"]
    )

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
