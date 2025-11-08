import json
import csv
import os

def json_to_csv(json_file_path, csv_file_path):
    """
    JSON dosyasını CSV dosyasına çevirir
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)

        if not data:
            print(f"Uyarı: {json_file_path} boş veya geçersiz!")
            return

        all_keys = set()
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())

        fieldnames = sorted(list(all_keys))

        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for item in data:
                if isinstance(item, dict):
                    row = {}
                    for key in fieldnames:
                        value = item.get(key, "")
                        # Eğer değer dict veya list ise JSON stringine çevir
                        if isinstance(value, (dict, list)):
                            value = json.dumps(value, ensure_ascii=False)
                        row[key] = value
                    writer.writerow(row)

        print(f"✓ {json_file_path} -> {csv_file_path} başarıyla dönüştürüldü!")
        print(f"  Toplam {len(data)} kayıt işlendi.")

    except FileNotFoundError:
        print(f"Hata: {json_file_path} dosyası bulunamadı!")
    except json.JSONDecodeError:
        print(f"Hata: {json_file_path} geçerli bir JSON dosyası değil!")
    except Exception as e:
        print(f"Hata: {json_file_path} işlenirken bir hata oluştu: {str(e)}")

def main():
    base_path = "/Users/berkekucuk/Documents/Scraping/ufc-scraping/data/"
    output_dir = os.path.join(base_path, "data_csv")

    # Klasör yoksa oluştur
    os.makedirs(output_dir, exist_ok=True)

    files_to_convert = [
        ("fights.json", "fights.csv"),
        ("participations.json", "participations.csv"),
        ("fighters.json", "fighters.csv"),
        ("events.json", "events.csv")
    ]

    print("JSON'dan CSV'ye Dönüştürme İşlemi Başladı...\n")

    for json_file, csv_file in files_to_convert:
        json_path = os.path.join(base_path, json_file)
        csv_path = os.path.join(output_dir, csv_file)

        json_to_csv(json_path, csv_path)
        print()

    print("Tüm dönüştürme işlemleri tamamlandı!")
    print(f"CSV dosyaları {output_dir} klasörüne kaydedildi.\n")

if __name__ == "__main__":
    main()
