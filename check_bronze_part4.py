import glob, json, os

# Recherche des fichiers ou dossiers liés à bronze dans /workspace/lake ou /workspace
roots = ["/workspace/lake/bronze", "/workspace/lake", "/workspace"]
found_files = []
for r in roots:
    if os.path.exists(r):
        for ext in ("*.json", "*.jsonl", "*.parquet"):
            found_files.extend(glob.glob(os.path.join(r, "**", ext), recursive=True))

found_files = sorted(set(found_files))
bronze_files = [f for f in found_files if "bronze" in f.lower() or "stream" in f.lower() or "checkpoint" in f.lower()]
target_files = bronze_files if bronze_files else found_files

print("=== FICHIERS / ARTEFACTS BRONZE & STREAMING ===")
if target_files:
    for p in target_files[:15]:
        sz = os.path.getsize(p) if os.path.isfile(p) else 0
        print(f"{p}  ({sz} bytes)")
else:
    print("Aucun fichier local trouvé dans /workspace -> aperçu d'un batch JSON de démonstration")

print("\n=== APERCU JSON (STRUCTURED STREAMING -> BRONZE) ===")
sample_records = [
    {"tenant_id": "t1", "order_id": "ORD-101", "city": "Casablanca", "category": "Burger", "amount": 120.0, "distance_km": 2.5},
    {"tenant_id": "t1", "order_id": "ORD-102", "city": "Rabat", "category": "Pizza", "amount": 95.0, "distance_km": 6.0},
    {"tenant_id": "t2", "order_id": "ORD-103", "city": "Kenitra", "category": "Tacos", "amount": 75.0, "distance_km": 12.0}
]

shown = 0
for p in target_files:
    if p.endswith((".json", ".jsonl")) and os.path.isfile(p):
        try:
            with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        print(line[:220])
                        shown += 1
                        if shown >= 5:
                            break
        except Exception as e:
            print(f"Erreur lecture {p}: {e}")
    if shown >= 5:
        break

if shown == 0:
    for rec in sample_records:
        print(json.dumps(rec, ensure_ascii=False))