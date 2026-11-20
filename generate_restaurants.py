import json
import os
import random

CITIES = {
    "Casablanca": (33.5731, -7.5898),
    "Rabat": (34.0209, -6.8416),
    "Kenitra": (34.2610, -6.5802),
    "Marrakech": (31.6295, -7.9811),
    "Tanger": (35.7595, -5.8340),
}

CATEGORIES = ["pizza", "tacos", "sushi", "marocain", "burger", "italien", "asiatique"]


def main():
    os.makedirs("data", exist_ok=True)
    random.seed(42)
    with open("data/restaurants.json", "w", encoding="utf-8") as f:
        for i in range(1, 201):
            city = random.choice(list(CITIES.keys()))
            base_lat, base_lon = CITIES[city]
            category = random.choice(CATEGORIES)
            record = {
                "restaurant_id": f"R-{i:03d}",
                "name": f"Resto {category.capitalize()} {i}",
                "city": city,
                "category": category,
                "latitude": round(base_lat + random.uniform(-0.05, 0.05), 6),
                "longitude": round(base_lon + random.uniform(-0.05, 0.05), 6),
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print("Fichier data/restaurants.json généré (200 lignes).")


if __name__ == "__main__":
    main()