import os
import json
import asyncio
import asyncpg
import paho.mqtt.client as mqtt
from datetime import datetime
from typing import Dict, Any

# =========================================================
# ENV CONFIG
# =========================================================
CORE_DB_URL="postgresql://postgres:7heC%40mCorE@10.15.160.6:5432/postgres"
MQTT_BROKER="34.18.211.31"
MQTT_PORT=8883
# =========================================================
# CONSTANTS
# =========================================================
TABLES_WITH_ORGANIZATION_ID = {
    "cameras",
    "advanced_rules", 
    "advanced_rulesets",
    "rule_assignments"
}

SUB_TOPICS = [
    "edge/to/core/cameras",
    "edge/to/core/advanced_rulesets",
    "edge/to/core/advanced_rules",
    "edge/to/core/rule_assignments",
    "edge/to/core/detection_alerts",
]

# Only allow detection_alerts updates from core to edge
CORE_TO_EDGE_TOPICS = [
    "core/to/edge/detection_alerts"
]

DATETIME_FIELDS = {
    "created_at",
    "updated_at",
    "event_time",
    "acknowledged_at",
}

# =========================================================
# JSON ENCODER
# =========================================================
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# =========================================================
# DB
# =========================================================
async def get_pool():
    return await asyncpg.create_pool(CORE_DB_URL, min_size=1, max_size=5)

def parse_datetimes(data: Dict[str, Any]) -> Dict[str, Any]:
    parsed = {}
    for k, v in data.items():
        if k in DATETIME_FIELDS and isinstance(v, str) and v:
            try:
                parsed[k] = datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                parsed[k] = v
        else:
            parsed[k] = v
    return parsed

# =========================================================
# MQTT
# =========================================================
mqtt_client = mqtt.Client()
mqtt_client.tls_set(
    ca_certs="/etc/mosquitto/certs/ca.crt",
    certfile="/etc/mosquitto/certs/broker.crt",
    keyfile="/etc/mosquitto/certs/broker.key",
)
mqtt_client.tls_insecure_set(False)
# =========================================================
# CORE → EDGE (DETECTION ALERTS ONLY)
# =========================================================
async def apply_core_update(pool, table: str, data: Dict[str, Any]):
    # Only allow detection_alerts updates from core
    if table != "detection_alerts":
        print(f"[{table}] Ignoring core update - only detection_alerts updates are allowed")
        return
        
    try:
        print(f"[{table}] Received core update for record ID {data.get('id', 'unknown')}")
        
        processed = parse_datetimes(data)
        
        cols = ", ".join(processed.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(processed)))
        updates = ", ".join(f"{k}=EXCLUDED.{k}" for k in processed.keys())

        sql = f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET
        {updates}
        """

        async with pool.acquire() as conn:
            await conn.execute(sql, *processed.values())
            print(f"[{table}] Successfully applied core update for record ID {processed.get('id', 'unknown')}")
            
    except Exception as e:
        print(f"[{table}] ERROR applying core update: {e}")
        print(f"[{table}] Data that failed: {data}")

# =========================================================
# EDGE → CORE (DATA INGEST) 
# =========================================================
async def apply_edge_data(pool, table: str, data: Dict[str, Any]):
    try:
        processed = parse_datetimes(data)
        record_id = processed.get("id", "unknown")
        print(f"[{table}] Processing edge data for record ID {record_id}")

        # --------------------------------------------------
        # ---- Schema fixes ----
        # --------------------------------------------------
        schema_fixes = {
            "advanced_rulesets": ["camera_id"],
            "advanced_rules": [],
            "rule_assignments": [],
            "cameras": [],
            "detection_alerts": [],
        }

        if table in schema_fixes:
            for field in schema_fixes[table]:
                processed.pop(field, None)

        async with pool.acquire() as conn:

            # --------------------------------------------------
            # ---- Resolve organization_id ----
            # --------------------------------------------------
            if table in TABLES_WITH_ORGANIZATION_ID and processed.get("edge_id"):
                row = await conn.fetchrow(
                    "SELECT organization_id FROM edge_devices WHERE edge_id=$1",
                    processed["edge_id"],
                )
                if row:
                    processed["organization_id"] = row["organization_id"]

            # --------------------------------------------------
            # ---- Detect existence ----
            # --------------------------------------------------
            exists = False
            existing_core = None

            if processed.get("id") and processed.get("edge_id"):
                if table == "detection_alerts":
                    existing_core = await conn.fetchrow(
                        """
                        SELECT acknowledged
                        FROM detection_alerts
                        WHERE id=$1 AND edge_id=$2
                        """,
                        processed["id"],
                        processed["edge_id"],
                    )
                else:
                    existing_core = await conn.fetchrow(
                        f"""
                        SELECT 1
                        FROM {table}
                        WHERE id=$1 AND edge_id=$2
                        """,
                        processed["id"],
                        processed["edge_id"],
                    )
                exists = bool(existing_core)

            elif processed.get("id"):
                if table == "detection_alerts":
                    existing_core = await conn.fetchrow(
                        """
                        SELECT acknowledged
                        FROM detection_alerts
                        WHERE id=$1
                        """,
                        processed["id"],
                    )
                else:
                    existing_core = await conn.fetchrow(
                        f"""
                        SELECT 1
                        FROM {table}
                        WHERE id=$1
                        """,
                        processed["id"],
                    )
                exists = bool(existing_core)

            # --------------------------------------------------
            # 🔒 CORE AUTHORITY GUARD (detection_alerts ONLY)
            # --------------------------------------------------
            if table == "detection_alerts" and exists:
                edge_ack = processed.get("acknowledged")
                core_ack = existing_core["acknowledged"]

                if edge_ack != core_ack:
                    print(
                        f"[detection_alerts] CORE owns acknowledgment. "
                        f"Ignoring EDGE UPDATE for ID {processed['id']} "
                        f"(EDGE={edge_ack}, CORE={core_ack})"
                    )
                    return  # ❌ block update only

            # --------------------------------------------------
            # ---- UPDATE ----
            # --------------------------------------------------
            if exists:
                print(f"[{table}] Record exists, updating ID {record_id}")

                if table == "detection_alerts":
                    update_data = {
                        k: v for k, v in processed.items()
                        if k not in {
                            "id",
                            "edge_id",
                            "acknowledged",
                            "acknowledged_at",
                        }
                    }
                else:
                    update_data = {
                        k: v for k, v in processed.items()
                        if k not in {"id", "edge_id"}
                    }

                if update_data:
                    set_clause = ", ".join(
                        f"{k}=${i+3}" for i, k in enumerate(update_data)
                    )

                    if processed.get("edge_id"):
                        sql = f"""
                            UPDATE {table}
                            SET {set_clause}
                            WHERE id=$1 AND edge_id=$2
                        """
                        await conn.execute(
                            sql,
                            processed["id"],
                            processed["edge_id"],
                            *update_data.values(),
                        )
                    else:
                        sql = f"""
                            UPDATE {table}
                            SET {set_clause}
                            WHERE id=$1
                        """
                        await conn.execute(
                            sql,
                            processed["id"],
                            *update_data.values(),
                        )

            # --------------------------------------------------
            # ---- INSERT ----
            # --------------------------------------------------
            else:
                print(f"[{table}] Inserting new record ID {record_id}")

                cols = ", ".join(processed.keys())
                placeholders = ", ".join(f"${i+1}" for i in range(len(processed)))
                sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

                try:
                    await conn.execute(sql, *processed.values())
                except Exception as insert_error:
                    if "foreign key constraint" in str(insert_error).lower():
                        print(
                            f"[{table}] FK violation for ID {record_id} — "
                            "dependencies not yet synced, will retry"
                        )
                        return
                    raise

    except Exception as e:
        print(f"[{table}] ERROR in apply_edge_data: {e}")
        print(f"[{table}] Data that failed: {data}")
        import traceback
        traceback.print_exc()

# =========================================================
# MQTT CALLBACK
# =========================================================
def on_message(client, userdata, msg):
    try:
        print(f"[MQTT] Received message on topic: {msg.topic}")
        payload = json.loads(msg.payload.decode())
        table = payload["table"]
        data = payload["data"]
        
        print(f"[MQTT] Processing {table} data - ID: {data.get('id', 'unknown')}, Size: {len(str(payload))} chars")
        
        # Determine if this is from core or edge based on topic
        if msg.topic.startswith("core/to/edge/"):
            print(f"[MQTT] Received {table} data from core: ID {data.get('id', 'unknown')}")
            asyncio.run_coroutine_threadsafe(
                apply_core_update(userdata["pool"], table, data),
                userdata["loop"],
            )
        elif msg.topic.startswith("edge/to/core/"):
            print(f"[MQTT] Received {table} data from edge: ID {data.get('id', 'unknown')}")
            # This would be for when running as core service
            asyncio.run_coroutine_threadsafe(
                apply_edge_data(userdata["pool"], table, data),
                userdata["loop"],
            )
        else:
            print(f"[MQTT] Unknown topic pattern: {msg.topic}")
            
    except json.JSONDecodeError as e:
        print(f"[MQTT ERROR] JSON decode error: {e}")
        print(f"[MQTT ERROR] Raw payload: {msg.payload.decode()}")
    except KeyError as e:
        print(f"[MQTT ERROR] Missing key in payload: {e}")
        print(f"[MQTT ERROR] Payload: {msg.payload.decode()}")
    except Exception as e:
        print(f"[MQTT ERROR] Unexpected error: {e}")
        print(f"[MQTT ERROR] Topic: {msg.topic}")
        print(f"[MQTT ERROR] Payload: {msg.payload.decode()}")
        import traceback
        traceback.print_exc()

# =========================================================
# MAIN LOOP
# =========================================================
async def main():
    print("[SERVICE] Starting MQTT Sync Service...")
    
    pool = await get_pool()
    print("[SERVICE] Database connection established")
    
    loop = asyncio.get_running_loop()

    mqtt_client.user_data_set({"pool": pool, "loop": loop})
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    print(f"[SERVICE] Connected to MQTT broker: {MQTT_BROKER}:{MQTT_PORT}")

    # Subscribe to edge-to-core topics (if running as core)
    for topic in SUB_TOPICS:
        mqtt_client.subscribe(topic, qos=1)
        print(f"[MQTT] Subscribed: {topic}")
    
    # Subscribe to core-to-edge detection_alerts (if running as edge)
    for topic in CORE_TO_EDGE_TOPICS:
        mqtt_client.subscribe(topic, qos=1)
        print(f"[MQTT] Subscribed: {topic}")

    mqtt_client.loop_start()
    
    print("[SERVICE] Started - Core service ready to receive data from edge")
    print("[SERVICE] Waiting for edge data...")

    # Keep the service running to receive edge data
    try:
        while True:
            await asyncio.sleep(30)  # Just keep alive, all work is done by MQTT callbacks
    except KeyboardInterrupt:
        print("[SERVICE] Shutting down...")
    finally:
        mqtt_client.loop_stop()
        await pool.close()

# =========================================================
# ENTRY
# =========================================================
if __name__ == "__main__":
    asyncio.run(main())
