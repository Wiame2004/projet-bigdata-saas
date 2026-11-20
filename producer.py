import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

CITIES = {
    "Casablanca": "CAS",
    "Rabat": "RBA",
    "Kenitra": "KEN",
    "Marrakech": "RAK",
    "Tanger": "TNG",
}
STATUSES = ["CREATED", "PICKED_UP", "DELIVERED", "CANCELLED"]
STATUS_WEIGHTS = [0.15, 0.15, 0.55, 0.15]

def generate_event(order_num: int, now: datetime) -> dict:
    city = random.choice(list(CITIES.keys()))
    city_code = CITIES[city]
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

    # ~5% d'événements en retard (jusqu'à 15 minutes)
    if random.random() < 0.05:
        delay_seconds = random.randint(660, 900)  # 11 à 15 min de retard
    else:
        delay_seconds = random.randint(0, 30)

    event_time = (now - timedelta(seconds=delay_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "order_id": f"ORD-{order_num:07d}",
        "restaurant_id": f"R-{random.randint(1, 200):03d}",
        "customer_id": f"C-{random.randint(1, 15000):05d}",
        "city": city,
        "status": status,
        "amount_mad": round(random.uniform(35.0, 450.0), 2),
        "items_count": random.randint(1, 8),
        "distance_km": round(random.uniform(0.5, 12.0), 2),
        "courier_id": f"L-{city_code}-{random.randint(1, 25):02d}",
        "event_time": event_time,
    }

def run_file_mode(n: int, output_path: str):
    random.seed(42)
    base_time = datetime(2026, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
    recent_events = []

    with open(output_path, "w", encoding="utf-8") as f:
        for i in range(1, n + 1):
            current_time = base_time + timedelta(seconds=i // 2)
            # ~2% de doublons (même order_id et même status)
            if recent_events and random.random() < 0.02:
                ev = random.choice(recent_events)
            else:
                ev = generate_event(i, current_time)
                recent_events.append(ev)
                if len(recent_events) > 500:
                    recent_events.pop(0)
            f.write(json.dumps(ev) + "\n")
    print(f"{n} événements générés dans {output_path}")

def run_kafka_mode(rate: int, bootstrap_servers: str, topic: str):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    print(f"Envoi continu vers Kafka ({topic}) à {rate} msg/sec... Ctrl+C pour arrêter.")
    order_num = 100000
    recent_events = []
    interval = 1.0 / rate

    try:
        while True:
            start = time.time()
            now = datetime.now(timezone.utc)
            if recent_events and random.random() < 0.02:
                ev = random.choice(recent_events)
            else:
                order_num += 1
                ev = generate_event(order_num, now)
                recent_events.append(ev)
                if len(recent_events) > 500:
                    recent_events.pop(0)

            producer.send(topic, key=ev["city"], value=ev)
            elapsed = time.time() - start
            if elapsed < interval:
                time.sleep(interval - elapsed)
    except KeyboardInterrupt:
        print("\nArrêt du producteur Kafka.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["file", "kafka"], required=True)
    parser.add_argument("--n", type=int, default=50000)
    parser.add_argument("--rate", type=int, default=20)
    parser.add_argument("--output", type=str, default="data/orders_history.jsonl")
    parser.add_argument("--bootstrap", type=str, default="kafka:9092")
    parser.add_argument("--topic", type=str, default="orders")
    args = parser.parse_args()

    if args.mode == "file":
        run_file_mode(args.n, args.output)
    else:
        run_kafka_mode(args.rate, args.bootstrap, args.topic)