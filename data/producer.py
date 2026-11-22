import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

CITIES = ["Casablanca", "Rabat", "Kenitra", "Marrakech", "Tanger"]
STATUSES = ["CREATED", "PICKED_UP", "DELIVERED", "CANCELLED"]
STATUS_WEIGHTS = [0.15, 0.15, 0.60, 0.10]


def load_restaurants(path: str):
  p = Path(path)
  if not p.exists():
    return []
  with p.open("r", encoding="utf-8") as f:
    data = json.load(f)
  if isinstance(data, dict):
    for val in data.values():
      if isinstance(val, list):
        return val
  return data if isinstance(data, list) else []


def make_event(idx: int, restaurants: list, base_time: datetime):
  resto = random.choice(restaurants) if restaurants else {}
  city = resto.get("city") or random.choice(CITIES)
  resto_id = (
      resto.get("restaurant_id")
      or resto.get("id")
      or f"R-{random.randint(1, 200):03d}"
  )
  status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

  # ~5% d'événements en retard jusqu'à 15 minutes
  event_dt = base_time + timedelta(seconds=idx * 2)
  if random.random() < 0.05:
    event_dt = event_dt - timedelta(minutes=random.uniform(1, 15))

  return {
      "order_id": f"ORD-{idx:07d}",
      "restaurant_id": resto_id,
      "customer_id": f"C-{random.randint(1, 15000):05d}",
      "city": city,
      "status": status,
      "amount_mad": round(random.uniform(35.0, 350.0), 2),
      "items_count": random.randint(1, 6),
      "distance_km": round(random.uniform(0.5, 12.0), 1),
      "courier_id": f"L-{city[:3].upper()}-{random.randint(1, 30):02d}",
      "event_time": event_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
  }


def run_file_mode(n: int, out_path: str, restaurants: list):
  target = Path(out_path)
  target.parent.mkdir(parents=True, exist_ok=True)
  base_time = datetime(2026, 11, 20, 10, 0, 0, tzinfo=timezone.utc)
  generated = []

  with target.open("w", encoding="utf-8") as f:
    for i in range(1, n + 1):
      ev = make_event(i, restaurants, base_time)
      f.write(json.dumps(ev) + "\n")
      generated.append(ev)
      # ~2% de doublons (même order_id et même status)
      if random.random() < 0.02:
        f.write(json.dumps(ev) + "\n")

  print(f"Fichier généré : {target} ({n} commandes de base)")


def run_kafka_mode(rate: float, bootstrap: str, topic: str, restaurants: list):
  try:
    from kafka import KafkaProducer
  except ImportError:
    raise SystemExit(
        "Installez kafka-python dans l'environnement ou exécutez depuis un"
        " conteneur ayant kafka-python."
    )

  producer = KafkaProducer(
      bootstrap_servers=bootstrap,
      value_serializer=lambda v: json.dumps(v).encode("utf-8"),
  )
  interval = 1.0 / max(rate, 0.1)
  idx = 1
  recent_events = []

  print(
      f"Envoi vers Kafka ({bootstrap}, topic={topic}) à ~{rate} msg/s..."
      " (Ctrl+C pour arrêter)"
  )
  try:
    while True:
      now_utc = datetime.now(timezone.utc)
      ev = make_event(idx, restaurants, now_utc)
      producer.send(topic, ev)
      recent_events.append(ev)
      if len(recent_events) > 200:
        recent_events.pop(0)

      # ~2% de doublons
      if recent_events and random.random() < 0.02:
        producer.send(topic, random.choice(recent_events))

      producer.flush()
      idx += 1
      time.sleep(interval)
  except KeyboardInterrupt:
    print("\nArrêt du producteur Kafka.")
  finally:
    producer.close()


def main():
  parser = argparse.ArgumentParser(description="Générateur d'événements orders")
  parser.add_argument(
      "--mode", choices=["file", "kafka"], required=True, help="Mode de sortie"
  )
  parser.add_argument(
      "--n", type=int, default=50000, help="Nombre de commandes en mode file"
  )
  parser.add_argument(
      "--out",
      default="/workspace/data/orders_history.jsonl",
      help="Chemin de sortie en mode file",
  )
  parser.add_argument(
      "--rate", type=float, default=20.0, help="Événements/sec en mode kafka"
  )
  parser.add_argument(
      "--bootstrap", default="kafka:9092", help="Bootstrap servers Kafka"
  )
  parser.add_argument("--topic", default="orders", help="Topic Kafka cible")
  parser.add_argument(
      "--restaurants",
      default="/workspace/data/restaurants.json",
      help="Fichier restaurants.json",
  )
  args = parser.parse_args()

  restos = load_restaurants(args.restaurants)
  if args.mode == "file":
    run_file_mode(args.n, args.out, restos)
  else:
    run_kafka_mode(args.rate, args.bootstrap, args.topic, restos)


if __name__ == "__main__":
  main()