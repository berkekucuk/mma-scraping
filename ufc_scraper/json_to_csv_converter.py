import json
import csv
from pathlib import Path

# 📁 Klasör yolları
input_dir = Path("data/json_output")
output_dir = Path("data/csv_output")
output_dir.mkdir(parents=True, exist_ok=True)

# 🔹 Her JSON dosyasına ait kolon tanımları
schemas = {
    "events": [
        "event_id",
        "event_type",
        "event_name",
        "date_time",
        "venue",
        "location"
    ],
    "fighters": [
        "fighter_id",
        "name",
        "profile_url",
        "image_url"
    ],
    "fights": [
        "fight_id",
        "event_id",
        "weight_class",
        "method",
        "round_info",
        "fight_result",
        "winner_id"
    ],
    "participations": [
        "fight_id",
        "fighter_id",
        "corner",
        "odds",
        "age_at_fight"
    ]
}

# 🔁 JSON → CSV dönüştürme işlemi
for name, fields in schemas.items():
    input_file = input_dir / f"{name}.json"
    output_file = output_dir / f"{name}.csv"

    if not input_file.exists():
        print(f"⚠️ {input_file.name} bulunamadı, atlanıyor...")
        continue

    with open(input_file, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"❌ {input_file.name} okunamadı: {e}")
            continue

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for item in data:
            # Eksik alanları boş olarak tamamla
            row = {key: item.get(key, "") for key in fields}
            writer.writerow(row)

    print(f"✅ {output_file.name} oluşturuldu ({len(data)} kayıt).")

print("\n🎯 Tüm JSON dosyaları CSV'ye dönüştürüldü.")
