import os
import json
import time
import random
import requests
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

# Config
BOOTSTRAP_SERVER   = os.getenv("CONFLUENT_BOOTSTRAP_SERVER")
API_KEY            = os.getenv("CONFLUENT_API_KEY")
API_SECRET         = os.getenv("CONFLUENT_API_SECRET")
SR_URL             = os.getenv("SCHEMA_REGISTRY_URL")
SR_API_KEY         = os.getenv("SCHEMA_REGISTRY_API_KEY")
SR_API_SECRET      = os.getenv("SCHEMA_REGISTRY_API_SECRET")
TOPIC              = "order-events"
SCHEMA_PATH        = "schemas/order_event.avsc"

# Static reference data (simulates a facility lookup table)
FACILITIES = [
    {"facility_id": "FAC-001", "facility_type": "WAREHOUSE","region": "us-central"},
    {"facility_id": "FAC-002", "facility_type": "FULFILLMENT_CENTER","region": "us-west"},
    {"facility_id": "FAC-003", "facility_type": "DARK_KITCHEN","region": "us-east"},
    {"facility_id": "FAC-004", "facility_type": "WAREHOUSE","region": "us-south"},
    {"facility_id": "FAC-005", "facility_type": "FULFILLMENT_CENTER","region": "us-central"},
    {"facility_id": "FAC-006", "facility_type": "DARK_KITCHEN","region": "us-west"},
]

ORDER_STATES = [
    "ORDER_CREATED",
    "FACILITY_ASSIGNED",
    "PROCESSING",
    "IN_TRANSIT",
    "DELIVERED",
]

# Active orders: order_id  {facility, state_index, customer_id, item_count, value}
active_orders = {}

def load_schema(path: str) -> str:
    with open(path) as f:
        return f.read()

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[OK] order_id={msg.key()} → partition={msg.partition()} offset={msg.offset()}")

def build_producer(schema_str: str) -> SerializingProducer:
    sr_client = SchemaRegistryClient({
        "url": SR_URL,
        "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
    })
    avro_serializer = AvroSerializer(sr_client, schema_str)
    return SerializingProducer({
        "bootstrap.servers":       BOOTSTRAP_SERVER,
        "security.protocol":       "SASL_SSL",
        "sasl.mechanism":          "PLAIN",
        "sasl.username":           API_KEY,
        "sasl.password":           API_SECRET,
        "key.serializer":          lambda k, ctx: k.encode("utf-8"),
        "value.serializer":        avro_serializer,
    })

def next_event() -> dict:
    """
    Either progress an existing order or create a new one.
    Occasionally inject a CANCELLED event for realism.
    """
    # 30% chance: create a brand new order
    if not active_orders or random.random() < 0.30:
        order_id   = f"ORD-{fake.uuid4()[:8].upper()}"
        facility   = random.choice(FACILITIES)
        active_orders[order_id] = {
            "facility":    facility,
            "state_index": 0,
            "customer_id": f"CUST-{fake.uuid4()[:6].upper()}",
            "item_count":  random.randint(1, 20),
            "order_value": round(random.uniform(15.0, 500.0), 2),
        }
        order = active_orders[order_id]
        return {
            "order_id":        order_id,
            "facility_id":     order["facility"]["facility_id"],
            "facility_type":   order["facility"]["facility_type"],
            "region":          order["facility"]["region"],
            "order_state":     ORDER_STATES[0],
            "customer_id":     order["customer_id"],
            "item_count":      order["item_count"],
            "order_value":     order["order_value"],
            "event_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "previous_state":  None,
        }

    # Pick a random active order and advance its state
    order_id = random.choice(list(active_orders.keys()))
    order    = active_orders[order_id]
    prev     = ORDER_STATES[order["state_index"]]

    # 5% chance: cancel instead of advancing
    if random.random() < 0.05:
        del active_orders[order_id]
        return {
            "order_id":        order_id,
            "facility_id":     order["facility"]["facility_id"],
            "facility_type":   order["facility"]["facility_type"],
            "region":          order["facility"]["region"],
            "order_state":     "CANCELLED",
            "customer_id":     order["customer_id"],
            "item_count":      order["item_count"],
            "order_value":     order["order_value"],
            "event_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "previous_state":  prev,
        }

    # Advance to next state
    order["state_index"] += 1
    new_state = ORDER_STATES[order["state_index"]]

    if order["state_index"] == len(ORDER_STATES) - 1:
        del active_orders[order_id]  # order completed

    return {
        "order_id":        order_id,
        "facility_id":     order["facility"]["facility_id"],
        "facility_type":   order["facility"]["facility_type"],
        "region":          order["facility"]["region"],
        "order_state":     new_state,
        "customer_id":     order["customer_id"],
        "item_count":      order["item_count"],
        "order_value":     order["order_value"],
        "event_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "previous_state":  prev,
    }

def main():
    schema_str = load_schema(SCHEMA_PATH)
    producer   = build_producer(schema_str)

    print(f"Producer started → topic: {TOPIC}")
    print("Ctrl+C to stop\n")

    try:
        while True:
            event = next_event()
            producer.produce(
                topic=TOPIC,
                key=event["facility_id"],   # partition by facility
                value=event,
                on_delivery=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(0.3, 0.8))  # ~80-150 events/min
    except KeyboardInterrupt:
        print("\nFlushing producer...")
    finally:
        producer.flush()
        print("Done.")

if __name__ == "__main__":
    main()