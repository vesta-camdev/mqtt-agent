import os
import json
import asyncio
import asyncpg
import paho.mqtt.client as mqtt
from datetime import datetime
from typing import Dict, Any
#new upload
# =========================================================
# ENV CONFIG
# =========================================================
CORE_DB_URL="postgresql://postgres:7heC%40mCorE@10.15.160.6:5432/postgres"
MQTT_BROKER="api.doh.camnitive.ai"
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

# Topics for publishing from core to edge
PUB_TOPICS = {
    "detection_alerts": "core/to/edge/detection_alerts"
}

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
    ca_certs="/home/saif/core_syncagent/certfiles/serverca1.crt",
    certfile="/home/saif/core_syncagent/certfiles/core-client.crt",
    keyfile="/home/saif/core_syncagent/certfiles/core-client.private.pem",
)
mqtt_client.tls_insecure_set(False)
# =========================================================
# CORE → EDGE PUBLISHING
# =========================================================
async def publish_to_edge(table: str, data: Dict[str, Any]):
    """Publish detection_alerts updates from core back to edge"""
    if table != "detection_alerts":
        return
        
    try:
        message = json.dumps(
            {
                "table": table,
                "op": "update",
                "data": data,
            },
            cls=DateTimeEncoder,
        )
        
        result = mqtt_client.publish(
            PUB_TOPICS[table],
            message,
            qos=1,
        )
        
        if result.rc == 0:
            print(f"[{table}] Published update to edge for ID {data.get('id', 'unknown')}")
        else:
            print(f"[{table}] Failed to publish to edge for ID {data.get('id', 'unknown')}")
            
    except Exception as e:
        print(f"[{table}] ERROR publishing to edge: {e}")

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
        # print(f"[{table}] Data that failed: {data}")

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
                        print(f"[{table}] FK Error details: {insert_error}")
                        print(f"[{table}] Data being inserted: {processed}")
                        
                        # Check if referenced records exist
                        if table == "cameras" and processed.get("edge_id"):
                            edge_exists = await conn.fetchrow(
                                "SELECT edge_id, organization_id FROM edge_devices WHERE edge_id=$1",
                                processed["edge_id"]
                            )
                            print(f"[{table}] Edge device exists: {edge_exists}")
                            
                            if processed.get("organization_id"):
                                org_exists = await conn.fetchrow(
                                    "SELECT id FROM organizations WHERE id=$1",
                                    processed["organization_id"]
                                )
                                print(f"[{table}] Organization exists: {org_exists}")
                        
                        return
                    raise

    except Exception as e:
        print(f"[{table}] ERROR in apply_edge_data: {e}")
        print(f"[{table}] Data that failed: {data}")
        import traceback
        traceback.print_exc()

# =========================================================
# PERIODIC CORE → EDGE SYNC
# =========================================================
async def sync_core_updates_to_edge(pool):
    """Periodically sync detection_alerts updates from core back to edge"""
    try:
        async with pool.acquire() as conn:
            # Find detection_alerts that have been updated in the last 5 minutes
            # and have acknowledgment changes
            rows = await conn.fetch(
                """
                SELECT *
                FROM detection_alerts
                WHERE updated_at >= NOW() - INTERVAL '5 minutes'
                AND (acknowledged IS NOT NULL OR acknowledged_at IS NOT NULL)
                ORDER BY updated_at DESC
                LIMIT 50
                """
            )
            
            for row in rows:
                data = dict(row)
                await publish_to_edge("detection_alerts", data)
                
    except Exception as e:
        print(f"[CORE_SYNC] Error syncing core updates to edge: {e}")

# =========================================================
# MANUAL ACKNOWLEDGMENT TRIGGER
# =========================================================
async def trigger_acknowledgment_update(pool, alert_id: int, acknowledged: bool, edge_id: str = None):
    """Manually trigger acknowledgment update and publish to edge"""
    try:
        async with pool.acquire() as conn:
            # Update acknowledgment in core database
            acknowledged_at = datetime.utcnow() if acknowledged else None
            
            if edge_id:
                await conn.execute(
                    """
                    UPDATE detection_alerts 
                    SET acknowledged=$1, acknowledged_at=$2, updated_at=NOW()
                    WHERE id=$3 AND edge_id=$4
                    """,
                    acknowledged, acknowledged_at, alert_id, edge_id
                )
            else:
                await conn.execute(
                    """
                    UPDATE detection_alerts 
                    SET acknowledged=$1, acknowledged_at=$2, updated_at=NOW()
                    WHERE id=$3
                    """,
                    acknowledged, acknowledged_at, alert_id
                )
            
            # Fetch updated record and publish to edge
            if edge_id:
                updated_row = await conn.fetchrow(
                    "SELECT * FROM detection_alerts WHERE id=$1 AND edge_id=$2",
                    alert_id, edge_id
                )
            else:
                updated_row = await conn.fetchrow(
                    "SELECT * FROM detection_alerts WHERE id=$1",
                    alert_id
                )
            
            if updated_row:
                data = dict(updated_row)
                await publish_to_edge("detection_alerts", data)
                print(f"[MANUAL_ACK] Triggered acknowledgment update for alert ID {alert_id}")
                
    except Exception as e:
        print(f"[MANUAL_ACK] Error triggering acknowledgment update: {e}")

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

    # Keep the service running and periodically sync core updates back to edge
    try:
        while True:
            await asyncio.sleep(30)  # Wait 30 seconds
            await sync_core_updates_to_edge(pool)  # Sync any core updates back to edge
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
