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

STATUSES = ["placed", "confirmed", "shipped", "delivered", "cancelled"]
PRODUCTS = ["P001", "P002", "P003", "P004", "P005", "P006", "P007", "P008"]

def generate_order():
    return {
        "order_id": fake.uuid4(),
        "user_id": f"U{random.randint(1000, 9999)}",
        "product_id": random.choice(PRODUCTS),
        "amount": round(random.uniform(199.0, 9999.0), 2),
        "status": random.choice(STATUSES),
        "timestamp": datetime.utcnow().isoformat()
    }

print("Orders producer started. Sending messages to Kafka...")
print("Press Ctrl+C to stop.\n")

try:
    while True:
        order = generate_order()
        producer.send("orders", value=order)
        print(f"Sent order: {order['order_id']} | "
              f"Product: {order['product_id']} | "
              f"Amount: ₹{order['amount']} | "
              f"Status: {order['status']}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping orders producer...")
    producer.close()
    print("Producer closed cleanly.")