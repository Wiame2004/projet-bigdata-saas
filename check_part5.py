import glob, os

roots = ["/workspace", "/tmp", "/opt"]
matches = []
for r in roots:
    if os.path.exists(r):
        for ext in ("*.parquet", "*.json", "*.jsonl", "*.csv"):
            matches.extend(glob.glob(os.path.join(r, "**", ext), recursive=True))

matches = sorted(set(matches))
print("=== FICHIERS DETECTES DANS LE CONTENEUR JUPYTER ===")
if matches:
    for m in matches[:20]:
        sz = os.path.getsize(m) if os.path.isfile(m) else 0
        print(f"{m}  ({sz} bytes)")
else:
    print("Aucun fichier parquet/json détecté sous /workspace ou /tmp")

print("\n=== DOSSIERS CHECKPOINT / OFFSET / SILVER / GOLD ===")
dirs_found = []
for r in roots:
    if os.path.exists(r):
        for root_dir, subdirs, _ in os.walk(r):
            for sd in subdirs:
                low = sd.lower()
                if any(k in low for k in ("checkpoint", "offset", "silver", "gold", "delta")):
                    dirs_found.append(os.path.join(root_dir, sd))

for d in sorted(set(dirs_found))[:15]:
    print(d)