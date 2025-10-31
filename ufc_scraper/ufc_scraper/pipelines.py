# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
from pathlib import Path
from datetime import datetime


class UfcScraperPipeline:
    def process_item(self, item, spider):
        return item


class JsonExportPipeline:
    """
    Her item türünü (Event, Fight, Fighter, FightParticipation)
    ayrı JSON dosyasına yazar.
    FighterItem için duplicate (aynı fighter_id) kayıtları atlar.
    """

    def __init__(self):
        # JSON dosyalarının kaydedileceği klasör
        self.output_dir = Path("data/json_output")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Timestamp ekleyerek dosya adlarını benzersiz yap
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.files = {
            "EventItem": open(self.output_dir / f"events_{timestamp}.json", "w", encoding="utf-8"),
            "FightItem": open(self.output_dir / f"fights_{timestamp}.json", "w", encoding="utf-8"),
            "FighterItem": open(self.output_dir / f"fighters_{timestamp}.json", "w", encoding="utf-8"),
            "FightParticipationItem": open(self.output_dir / f"participations_{timestamp}.json", "w", encoding="utf-8"),
        }

        # Her dosya için başlangıç köşeli parantezi yaz
        for f in self.files.values():
            f.write("[\n")

        # Item sayaçları (virgül ekleme kontrolü için)
        self.item_counts = {key: 0 for key in self.files}

        # Duplicate kontrolü için set
        self.seen_fighter_ids = set()

    def process_item(self, item, spider):
        """Her item geldiğinde doğru JSON dosyasına yaz."""
        item_type = type(item).__name__

        # Desteklenmeyen item tipi varsa uyarı ver
        if item_type not in self.files:
            spider.logger.warning(f"Unknown item type: {item_type}")
            return item

        # ---- Duplicate kontrolü (FighterItem için) ----
        if item_type == "FighterItem":
            fighter_id = item.get("fighter_id")
            if not fighter_id:
                raise DropItem("Missing fighter_id field in FighterItem")

            if fighter_id in self.seen_fighter_ids:
                raise DropItem(f"Duplicate fighter ignored: {fighter_id}")

            self.seen_fighter_ids.add(fighter_id)

        # ---- JSON dosyasına yazım işlemi ----
        file = self.files[item_type]

        # Eğer bu dosyada daha önce bir item yazıldıysa araya virgül ekle
        if self.item_counts[item_type] > 0:
            file.write(",\n")

        # JSON olarak yaz
        json.dump(dict(item), file, ensure_ascii=False, indent=2)
        self.item_counts[item_type] += 1

        return item

    def close_spider(self, spider):
        """Scrapy kapanırken dosyaları düzgün kapat."""
        for name, file in self.files.items():
            file.write("\n]\n")
            file.close()
            spider.logger.info(f"{name} JSON dosyası başarıyla kapatıldı.")

        spider.logger.info("✅ JsonExportPipeline tamamlandı — tüm veriler kaydedildi.")
