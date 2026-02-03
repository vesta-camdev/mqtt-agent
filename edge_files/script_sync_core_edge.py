import json
import time
import threading
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, Table, MetaData, select, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

# -------------------- CONFIG --------------------
EDGE_DB_URL = "postgresql://postgres:postgres@localhost:5432/vis_db"
CORE_DB_URL = "postgresql://postgres:postgres@localhost:5434/vis_db"
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "cam-edge/detection-alerts"
POLL_INTERVAL = 2  # seconds
# ------------------------------------------------

# Connect to edge DB
edge_engine = create_engine(EDGE_DB_URL)
edge_meta = MetaData()
edge_meta.reflect(bind=edge_engine)

# Table objects for all relevant tables
edge_table = edge_meta.tables["detection_alerts"]
edge_cameras_table = edge_meta.tables["cameras"]
edge_advance_rule_table = edge_meta.tables["advanced_rules"]
edge_advance_ruleset_table = edge_meta.tables["advanced_rulesets"]
edge_rule_assignment_table = edge_meta.tables["rule_assignments"]

# Connect to core DB
core_engine = create_engine(CORE_DB_URL)
core_meta = MetaData()
core_meta.reflect(bind=core_engine)

# Table objects for all relevant tables
core_table = core_meta.tables["detection_alerts"]
core_cameras_table = core_meta.tables["cameras"]
core_advance_rule_table = core_meta.tables["advanced_rules"]
core_advance_ruleset_table = core_meta.tables["advanced_rulesets"]
core_rule_assignment_table = core_meta.tables["rule_assignments"]
core_edge_table = core_meta.tables["edge_devices"]

# MQTT client
mqtt_client = mqtt.Client()


# ---------------- MQTT PUBLISHER ----------------
def publish_alert(alert_row):
    row_dict = dict(alert_row._mapping)  # FIXED

    # Convert non-JSON types (datetime etc.)
    for k, v in row_dict.items():
        if hasattr(v, "isoformat"):  # datetime
            row_dict[k] = v.isoformat()

    mqtt_client.publish(MQTT_TOPIC, json.dumps(row_dict))


# ---------------- EDGE POLLER ----------------

def sync_table(edge_table, core_table, key_column="id"):
    """Sync rows from edge_table to core_table by key_column."""
    with edge_engine.connect() as edge_conn, core_engine.connect() as core_conn:
        edge_rows = list(edge_conn.execute(select(edge_table)))
        core_ids = set(row[0] for row in core_conn.execute(select(core_table.c[key_column])))
        
        # Check if core table has organization_id column
        has_org_id = 'organization_id' in core_table.c
        
        for row in edge_rows:
            row_dict = dict(row._mapping)
            row_id = row_dict[key_column]
            
            # Get org_id from core database using edge_id (only if table has organization_id column)
            if has_org_id and 'edge_id' in row_dict:
                edge_id = row_dict['edge_id']
                org_result = core_conn.execute(
                    select(core_edge_table.c.id).where(core_edge_table.c.edge_id == edge_id)
                ).fetchone()
                
                if org_result:
                    row_dict['organization_id'] = org_result[0]
                else:
                    print(f"Warning: No org found for edge_id {edge_id}")
                    continue  # Skip this row if no org found
            
            if row_id not in core_ids:
                try:
                    stmt = insert(core_table).values(row_dict)
                    core_conn.execute(stmt)
                except Exception as e:
                    print(f"Error inserting row {row_id}: {e}")
            else:
                # Update existing row - use simple insert with on_conflict_do_nothing for safety
                try:
                    stmt = pg_insert(core_table).values(row_dict)
                    stmt = stmt.on_conflict_do_nothing()
                    core_conn.execute(stmt)
                except Exception as e:
                    print(f"Error upserting row {row_id}: {e}")
        core_conn.commit()

def poll_edge_db():
    last_seen_ids = set()

    while True:
        # Sync all tables
        sync_table(edge_cameras_table, core_cameras_table)
        sync_table(edge_advance_ruleset_table, core_advance_ruleset_table)
        sync_table(edge_advance_rule_table, core_advance_rule_table)
        sync_table(edge_rule_assignment_table, core_rule_assignment_table)

        # Existing detection_alerts logic
        with edge_engine.connect() as conn:
            result = conn.execute(select(edge_table))
            for row in result:
                row_dict = row._mapping
                alert_id = row_dict["id"]
                if alert_id not in last_seen_ids:
                    publish_alert(row)
                    last_seen_ids.add(alert_id)
        time.sleep(POLL_INTERVAL)


# ---------------- CORE SUBSCRIBER ----------------
def on_connect(client, userdata, flags, rc):
    print("[MQTT] Connected with result code", rc)
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())

    with core_engine.connect() as conn:
        # Check if detection_alerts table has organization_id column
        has_org_id = 'organization_id' in core_table.c
        
        # Get org_id from core database using edge_id (only if table has organization_id column)
        if has_org_id and 'edge_id' in payload:
            edge_id = payload['edge_id']
            org_result = conn.execute(
                select(core_edge_table.c.id).where(core_edge_table.c.edge_id == edge_id)
            ).fetchone()
            
            if org_result:
                payload['organization_id'] = org_result[0]
            else:
                print(f"Warning: No org found for edge_id {edge_id}")
                return  # Skip this message if no org found

        try:
            stmt = pg_insert(core_table).values(payload)
            stmt = stmt.on_conflict_do_nothing()
            conn.execute(stmt)
            conn.commit()
        except Exception as e:
            print(f"Error inserting detection alert: {e}")
            conn.rollback()


def start_core_subscriber():
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_forever()


# ---------------- MAIN ----------------
if __name__ == "__main__":
    # Start core subscriber in separate thread
    threading.Thread(target=start_core_subscriber, daemon=True).start()

    # Start polling edge DB
    poll_edge_db()