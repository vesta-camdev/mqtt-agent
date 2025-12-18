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

CORE_DB_URL = "postgresql://postgres:postgres@localhost:5434/vis_db"
MQTT_BROKER = "192.168.18.234"  # Same broker as edge
MQTT_PORT = 1883

CONFIG_TABLES = [
    "cameras",
    "advanced_rules",
    "advanced_rulesets",
    "rule_assignments",
]

# Tables that have organization_id field
TABLES_WITH_ORGANIZATION_ID = [
    "cameras",
    "advanced_rules", 
    "advanced_rulesets",
    "rule_assignments"
]

# Subscribe to all edge-to-core topics
SUB_TOPICS = [
    
    "edge/to/core/cameras",
    "edge/to/core/advanced_rulesets",
    "edge/to/core/advanced_rules",
    "edge/to/core/rule_assignments",
    "edge/to/core/detection_alerts",
]

mqtt_client = mqtt.Client()

last_sync = {}

async def get_pool():
    return await asyncpg.create_pool(CORE_DB_URL)

# ---------------- PUBLISH CONFIG ----------------
async def publish_table(pool, table):
    last_ts = last_sync.get(table, "1970-01-01")

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT * FROM {table}
            """
        )

        print(f"[{table}] Publishing {len(rows)} records to edge")
        
        for row in rows:
            data = dict(row)
            if "updated_at" in data:
                last_sync[table] = data["updated_at"]

            mqtt_client.publish(
                f"core/to/edge/{table}",
                json.dumps({
                    "table": table,
                    "op": "upsert",
                    "data": data
                }, cls=DateTimeEncoder),
                qos=1
            )

# ---------------- APPLY EDGE DATA ----------------
async def apply_edge_data(pool, table, data):
    try:
        # print(f"[{table}] Attempting to upsert data: {data}")
        
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
        
        async with pool.acquire() as conn:
            # Fetch organization_id from edge_devices table if edge_id is present
            # and only for tables that have organization_id field
            if ('edge_id' in processed_data and processed_data['edge_id'] and 
                table in TABLES_WITH_ORGANIZATION_ID):
                try:
                    edge_device = await conn.fetchrow(
                        "SELECT organization_id FROM edge_devices WHERE edge_id = $1", 
                        processed_data['edge_id']
                    )
                    if edge_device and edge_device['organization_id']:
                        processed_data['organization_id'] = edge_device['organization_id']
                        # print(f"[{table}] Fetched organization_id: {edge_device['organization_id']} for edge_id: {processed_data['edge_id']}")
                    else:
                        print(f"[{table}] WARNING: No organization_id found for edge_id: {processed_data['edge_id']}")
                except Exception as e:
                    print(f"[{table}] ERROR fetching organization_id: {e}")
            elif table not in TABLES_WITH_ORGANIZATION_ID:
                print(f"[{table}] Table does not have organization_id field, skipping organization lookup")
            
            # Check if record exists by trying to find it with id and edge_id (if both present)
            record_exists = False
            if 'id' in processed_data and 'edge_id' in processed_data:
                try:
                    existing = await conn.fetchrow(
                        f"SELECT 1 FROM {table} WHERE id = $1 AND edge_id = $2", 
                        processed_data['id'], processed_data['edge_id']
                    )
                    record_exists = existing is not None
                except:
                    record_exists = False
            elif 'id' in processed_data:
                try:
                    existing = await conn.fetchrow(f"SELECT 1 FROM {table} WHERE id = $1", processed_data['id'])
                    record_exists = existing is not None
                except:
                    record_exists = False
            
            if record_exists:
                # Update existing record
                print(f"[{table}] Record exists, updating...")
                
                # Build UPDATE statement (exclude id and edge_id from SET clause)
                update_data = {k: v for k, v in processed_data.items() if k not in ['id', 'edge_id']}
                if update_data:  # Only update if there are fields other than id/edge_id
                    set_clauses = ", ".join(f"{k} = ${i+3}" for i, k in enumerate(update_data.keys()))
                    
                    if 'edge_id' in processed_data:
                        update_sql = f"UPDATE {table} SET {set_clauses} WHERE id = $1 AND edge_id = $2"
                        await conn.execute(update_sql, processed_data['id'], processed_data['edge_id'], *update_data.values())
                    else:
                        update_sql = f"UPDATE {table} SET {set_clauses} WHERE id = $1"
                        await conn.execute(update_sql, processed_data['id'], *update_data.values())
                    
                    print(f"[{table}] Successfully updated record ID {processed_data.get('id', 'unknown')}")
                else:
                    print(f"[{table}] No fields to update besides id/edge_id")
                    
            else:
                # Insert new record
                print(f"[{table}] Record doesn't exist, inserting...")
                
                cols = ", ".join(processed_data.keys())
                placeholders = ", ".join(f"${i+1}" for i in range(len(processed_data)))
                
                insert_sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
                await conn.execute(insert_sql, *processed_data.values())
                print(f"[{table}] Successfully inserted new record ID {processed_data.get('id', 'unknown')}")
            
            # Verify the record exists (using id and edge_id if both present)
            if 'id' in processed_data:
                if 'edge_id' in processed_data:
                    verify_result = await conn.fetchrow(
                        f"SELECT id FROM {table} WHERE id = $1 AND edge_id = $2", 
                        processed_data.get('id'), processed_data.get('edge_id')
                    )
                else:
                    verify_result = await conn.fetchrow(f"SELECT id FROM {table} WHERE id = $1", processed_data.get('id'))
                
                if verify_result:
                    print(f"[{table}] Verified: Record ID {processed_data.get('id')} exists in database")
                else:
                    print(f"[{table}] WARNING: Record ID {processed_data.get('id')} not found after operation!")
            else:
                print(f"[{table}] No id field to verify, operation completed")
                
    except Exception as e:
        pass

# ---------------- MQTT HANDLER ----------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        table = payload.get("table")
        data = payload.get("data", {})
        
        # print(f"[MQTT] Received {table} data: ID {data.get('id', 'unknown')}")
        
        asyncio.run_coroutine_threadsafe(
            apply_edge_data(userdata["pool"], table, data),
            userdata["loop"]
        )
    except Exception as e:
        print(f"[MQTT] Error processing message: {e}")

# ---------------- MAIN ----------------
async def main():
    pool = await get_pool()
    loop = asyncio.get_running_loop()

    mqtt_client.user_data_set({"pool": pool, "loop": loop})
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    
    # Subscribe to all edge-to-core topics
    for topic in SUB_TOPICS:
        mqtt_client.subscribe(topic, qos=1)
        print(f"[MQTT] Subscribed to {topic}")
    
    mqtt_client.loop_start()

    while True:
        for table in CONFIG_TABLES:
            await publish_table(pool, table)
        await asyncio.sleep(3)

asyncio.run(main())