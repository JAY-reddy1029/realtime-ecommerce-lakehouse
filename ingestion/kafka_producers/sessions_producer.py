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

PAGES = [
    "home", "product_listing", "product_detail",
    "cart", "checkout", "order_confirmation", "account"
]
DEVICES = ["mobile", "desktop", "tablet"]

def generate_session():
    return {
        "session_id": fake.uuid4(),
        "user_id": f"U{random.randint(1000, 9999)}",
        "page_viewed": random.choice(PAGES),
        "duration_seconds": random.randint(5, 600),
        "device": random.choice(DEVICES),
        "timestamp": datetime.utcnow().isoformat()
    }

print("Sessions producer started. Sending messages to Kafka...")
print("Press Ctrl+C to stop.\n")

try:
    while True:
        session = generate_session()
        producer.send("user_sessions", value=session)
        print(f"Sent session: {session['session_id'][:8]}... | "
              f"Page: {session['page_viewed']} | "
              f"Device: {session['device']} | "
              f"Duration: {session['duration_seconds']}s")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping sessions producer...")
    producer.close()
    print("Producer closed cleanly.")