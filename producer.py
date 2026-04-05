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
stuck_order_ids: set = set() #tracks orders currently in SLA breach

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

def make_event(order_id: str, order: dict, order_state: str, previous_state: str) -> dict:
    """
    Build an Avro-compatible order event dict and
    centralizes event construction.
    """
    return {
        "order_id":        order_id,
        "facility_id":     order["facility"]["facility_id"],
        "facility_type":   order["facility"]["facility_type"],
        "region":          order["facility"]["region"],
        "order_state":     order_state,
        "customer_id":     order["customer_id"],
        "item_count":      order["item_count"],
        "order_value":     order["order_value"],
        "event_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "previous_state":  previous_state,
        "priority_tier":   random.choices(
            ["STANDARD", "EXPRESS", "PRIORITY"],
            weights=[70, 20, 10],
            k=1
        )[0],
    }

def next_event() -> dict:
    """
    Generate the next order lifecycle event for streaming to Kafka.

    Simulates realistic order behavior by assigning each new order
    a predetermined fate at creation time (complete, cancel, or
    sla_breach) to reflect real-world outcomes which are driven by
    upstream factors rather than random state-by-state transitions.

    Fate distribution (weighted):
        - complete (89.9%):    order progresses through all states to DELIVERED
        - cancel (10%):       order cancels at ORDER_CREATED or FACILITY_ASSIGNED
                             (cancellations after PROCESSING are not realistic)
        - sla_breach (0.1%):   order stalls at PROCESSING, emitting repeated
                             PROCESSING events to simulate a stuck fulfillment

    Returns:
        dict: Avro-compatible order event with all required schema fields.
    """
    # 30% chance: create a new order
    if not active_orders or random.random() < 0.30:
        order_id = f"ORD-{fake.uuid4()[:8].upper()}"
        facility = random.choice(FACILITIES)
        active_orders[order_id] = {
            "facility":    facility,
            "state_index": 0,
            "customer_id": f"CUST-{fake.uuid4()[:6].upper()}",
            "item_count":  random.randint(1, 20),
            "order_value": round(random.uniform(15.0, 500.0), 2),
            "fate": random.choices(
                ["complete", "cancel", "sla_breach"],
                weights=[89.9, 10, 0.1],
                k=1
            )[0]
        }
        order = active_orders[order_id]
        return make_event(order_id, order, ORDER_STATES[0], None)

    order_id = random.choice(list(active_orders.keys()))
    order    = active_orders[order_id]
    prev     = ORDER_STATES[order["state_index"]]
    fate     = order["fate"]


    if stuck_order_ids and random.random() < 0.70:
        order_id = random.choice(list(stuck_order_ids))
    else:
        order_id = random.choice(list(active_orders.keys()))
 
    order = active_orders[order_id]
    prev  = ORDER_STATES[order["state_index"]]
    fate  = order["fate"]

    # Cancellation fate
    # cancel at ORDER_CREATED or FACILITY_ASSIGNED
    if fate == "cancel" and order["state_index"] < 2:
        if random.random() < 0.4:  # 40% chance to cancel at this step
            del active_orders[order_id]
            return make_event(order_id, order, "CANCELLED", prev)
    
    # Normal advancement 
    order["state_index"] += 1
    new_state = ORDER_STATES[order["state_index"]]

    # SLA breach: order just hit PROCESSING and is fated to stall
    if fate == "sla_breach" and new_state == "PROCESSING":
        # revert advancement — keep at PROCESSING
        order["state_index"] -= 1
 
        # record when it got stuck (only on first encounter)
        if "stuck_since" not in order:
            order["stuck_since"] = datetime.now(timezone.utc)
            stuck_order_ids.add(order_id)
 
        order.setdefault("resolution_threshold", random.uniform(8, 12))
 
        stuck_minutes = (
            datetime.now(timezone.utc) - order["stuck_since"]
        ).total_seconds() / 60
 
        if stuck_minutes >= order["resolution_threshold"]:
            # resolved: advance past PROCESSING
            order["state_index"] += 1
            new_state = ORDER_STATES[order["state_index"]]
            order.pop("stuck_since", None)
            order.pop("resolution_threshold", None)
            stuck_order_ids.discard(order_id)
            if order["state_index"] == len(ORDER_STATES) - 1:
                del active_orders[order_id]
                stuck_order_ids.discard(order_id)
            return make_event(order_id, order, new_state, "PROCESSING")
 
        # still stuck: emit PROCESSING with previous_state = PROCESSING
        return make_event(order_id, order, "PROCESSING", "PROCESSING")

    # Complete order advancement
    if order["state_index"] == len(ORDER_STATES) - 1:
        del active_orders[order_id]
        stuck_order_ids.discard(order_id)
 
    return make_event(order_id, order, new_state, prev)

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