
import json
import logging
from pathlib import Path
from scrapy import Item

class JsonExportPipeline:

    def __init__(self):
        self.output_dir = Path("data/json_output")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.data = {
            "EventItem": {},
            "FightItem": {},
            "FighterItem": {},
            "FightParticipationItem": {}
        }

        # JSON dosya yolları
        self.file_paths = {
            "EventItem": self.output_dir / "events.json",
            "FightItem": self.output_dir / "fights.json",
            "FighterItem": self.output_dir / "fighters.json",
            "FightParticipationItem": self.output_dir / "participations.json",
        }

    def open_spider(self, spider):
        logging.info(f"[{spider.name}] JSON Pipeline başlatıldı.")

        # Var olan verileri yükle (duplicate kontrolü için)
        for key, path in self.file_paths.items():
            if path.exists():
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        existing_data = json.load(f)
                        # id tabanlı dict oluştur
                        if key == "EventItem":
                            self.data[key] = {d["event_id"]: d for d in existing_data}
                        elif key == "FightItem":
                            self.data[key] = {d["fight_id"]: d for d in existing_data}
                        elif key == "FighterItem":
                            self.data[key] = {d["fighter_id"]: d for d in existing_data}
                        elif key == "FightParticipationItem":
                            self.data[key] = {f'{d["fight_id"]}-{d["fighter_id"]}': d for d in existing_data}
                except json.JSONDecodeError:
                    self.data[key] = {}

    def process_item(self, item: Item, spider):
        class_name = type(item).__name__
        if class_name not in self.data:
            return item  # ilgisiz item

        if class_name == "EventItem":
            key = item.get("event_id")
        elif class_name == "FightItem":
            key = item.get("fight_id")
        elif class_name == "FighterItem":
            key = item.get("fighter_id")
        elif class_name == "FightParticipationItem":
            key = f"{item.get('fight_id')}-{item.get('fighter_id')}"
        else:
            return item

        if not key:
            spider.logger.warning(f"{class_name} item anahtar eksik: {dict(item)}")
            return item

        # Duplicate kontrolü
        if key not in self.data[class_name]:
            self.data[class_name][key] = dict(item)

        return item

    def close_spider(self, spider):
        # Her tabloyu diske kaydet
        for class_name, items_dict in self.data.items():
            path = self.file_paths[class_name]
            items_list = list(items_dict.values())

            with open(path, "w", encoding="utf-8") as f:
                json.dump(items_list, f, indent=2, ensure_ascii=False)

            logging.info(f"[{spider.name}] {path.name} başarıyla kaydedildi ({len(items_list)} kayıt).")

        logging.info(f"[{spider.name}] ✅ JsonExportPipeline tamamlandı — tüm veriler güncellendi.")
