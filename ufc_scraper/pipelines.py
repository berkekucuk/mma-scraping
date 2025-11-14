import logging
from itemadapter import ItemAdapter
from .services.supabase_manager import SupabaseManager

class EventPipeline:

    def __init__(self):
        """Pipeline başladığında kendi logger'ını ve DB yöneticisini hazırlar."""
        self.supabase = SupabaseManager
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("EventPipeline initialized.")

    async def process_item(self, item, spider):
        """
        Gelen item'ı asenkron olarak işler.
        Spider zaten ön filtreleme yaptığı için, bu item'ın
        ya YENİ ya da GÜNCELLENECEK olduğunu varsayarız.
        """
        adapter = ItemAdapter(item)

        # Sadece EventItem'ları işle
        if adapter.get("item_type") != "event":
            return item

        event_id = adapter.get("event_id")
        if not event_id:
            self.logger.error("EventItem has no event_id. Skipping.")
            return item

        # DB'ye gönderilecek veriyi hazırla
        event_data = adapter.asdict()
        event_data.pop("item_type", None)

        existing = await self.supabase.get_event_by_id(event_id)

        # 1. KARAR: INSERT (Eğer yoksa)
        if not existing:
            self.logger.info(f"[EVENT] Inserting new event: {event_id}")
            res = await self.supabase.insert_event(event_data)
            if res is None:
                self.logger.error(f"[EVENT] Insert failed: {event_id}")
            return item

        # 2. KARAR: UPDATE (Eğer varsa)
        # Spider buraya sadece "incomplete" olanları yolladı,
        # bu yüzden 'has_changes' kontrolüne gerek yok, direkt güncelleyebiliriz.
        # Yine de emin olmak için bir değişiklik kontrolü yapmakta zarar yok.

        has_changes = any(
            str(event_data.get(field)) != str(existing.get(field))
            for field in event_data.keys()
        )

        if has_changes:
            self.logger.info(f"[EVENT] Updating incomplete event: {event_id}")
            res = await self.supabase.update_event(event_id, event_data)
            if res is None:
                self.logger.error(f"[EVENT] Update failed: {event_id}")
        else:
            # Bu durumun olmaması gerekir (çünkü spider eksik diye yolladı)
            # ama loglamak iyidir.
            self.logger.info(f"[EVENT] No changes detected for supposedly incomplete event: {event_id}")

        return item


# class FighterPipeline:

#     def process_item(self, item, spider):
#         adapter = ItemAdapter(item)

#         # sadece fighter item'larını işle
#         if adapter.get("item_type") != "fighter":
#             return item

#         fighter_id = adapter.get("fighter_id")
#         profile_url = adapter.get("profile_url")
#         name = adapter.get("name")
#         image_url = adapter.get("image_url")

#         # DB'deki mevcut fighter'ı kontrol et
#         existing = spider.supabase.get_fighter_by_id(fighter_id)

#         # Eğer DB'de veri yoksa veya incomplete ise -> scraping yap
#         if not existing or not spider.is_fighter_complete(existing):
#             spider.logger.info(f"[PIPELINE] Scraping fighter: {fighter_id}")

#             return scrapy.Request(
#                 url=profile_url,
#                 callback=FighterParser.parse_fighter_profile,
#                 cb_kwargs={"fighter_id": fighter_id, "name": name, "profile_url": profile_url, "image_url": image_url},
#             )

#         # DB'de varsa → DB'yi güncelle
#         spider.supabase.insert_or_update_fighter(adapter.asdict())

#         return item
