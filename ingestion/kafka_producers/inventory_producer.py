import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker("en_IN")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

PRODUCTS = ["P001", "P002", "P003", "P004", "P005", "P006", "P007", "P008"]
WAREHOUSES = ["WH-HYD", "WH-BLR", "WH-MUM", "WH-DEL", "WH-CHN"]
ACTIONS = ["restock", "sale", "return", "adjustment"]

def generate_inventory_event():
    return {
        "event_id": fake.uuid4(),
        "product_id": random.choice(PRODUCTS),
        "warehouse_id": random.choice(WAREHOUSES),
        "quantity": random.randint(1, 500),
        "action": random.choice(ACTIONS),
        "timestamp": datetime.utcnow().isoformat()
    }

print("Inventory producer started. Sending messages to Kafka...")
print("Press Ctrl+C to stop.\n")

try:
    while True:
        event = generate_inventory_event()
        producer.send("inventory_updates", value=event)
        print(f"Sent inventory: {event['product_id']} | "
              f"Warehouse: {event['warehouse_id']} | "
              f"Qty: {event['quantity']} | "
              f"Action: {event['action']}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping inventory producer...")
    producer.close()
    print("Producer closed cleanly.")