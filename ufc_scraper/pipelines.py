import scrapy
from itemadapter import ItemAdapter
from .parsers.fighter_parser import FighterParser

class EventPipeline:

    def open_spider(self, spider):
        self.supabase = spider.supabase
        self.logger = spider.logger

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Sadece EventItem'ları işle
        if adapter.get("item_type") != "event":
            return item

        event_id = adapter.get("event_id")
        if not event_id:
            self.logger.error("EventItem has no event_id. Skipping.")
            return item

        # DB'ye gönderilecek veriyi oluştur
        event_data = adapter.asdict()

        # ❗ item_type DB'ye gönderilmemeli → kaldırıyoruz
        event_data.pop("item_type", None)

        # Var olan kayıt
        existing = self.supabase.get_event_by_id(event_id)

        # INSERT
        if not existing:
            self.logger.info(f"[EVENT] Inserting event: {event_id}")
            res = self.supabase.insert_event(event_data)
            if res is None:
                self.logger.error(f"[EVENT] Insert failed: {event_id}")
            return item

        # UPDATE (sadece değişiklik varsa)
        has_changes = any(
            event_data.get(field) != existing.get(field)
            for field in event_data.keys()
        )

        if has_changes:
            self.logger.info(f"[EVENT] Updating event: {event_id}")
            res = self.supabase.update_event(event_id, event_data)
            if res is None:
                self.logger.error(f"[EVENT] Update failed: {event_id}")
        else:
            self.logger.info(f"[EVENT] No changes for event: {event_id}")

        return item



class FighterPipeline:

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # sadece fighter item'larını işle
        if adapter.get("item_type") != "fighter":
            return item

        fighter_id = adapter.get("fighter_id")
        profile_url = adapter.get("profile_url")
        name = adapter.get("name")
        image_url = adapter.get("image_url")

        # DB'deki mevcut fighter'ı kontrol et
        existing = spider.supabase.get_fighter_by_id(fighter_id)

        # Eğer DB'de veri yoksa veya incomplete ise -> scraping yap
        if not existing or not spider.is_fighter_complete(existing):
            spider.logger.info(f"[PIPELINE] Scraping fighter: {fighter_id}")

            return scrapy.Request(
                url=profile_url,
                callback=FighterParser.parse_fighter_profile,
                cb_kwargs={"fighter_id": fighter_id, "name": name, "profile_url": profile_url, "image_url": image_url},
            )

        # DB'de varsa → DB'yi güncelle
        spider.supabase.insert_or_update_fighter(adapter.asdict())

        return item
