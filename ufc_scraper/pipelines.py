import logging
import scrapy
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem  # <-- YENİ IMPORT (Item'ı durdurmak için)
from .services.supabase_manager import SupabaseManager
from .parsers.fighter_parser import FighterParser


class DatabasePipeline:

    def __init__(self):
        self.supabase = SupabaseManager
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Unified DatabasePipeline initialized.")

    async def process_item(self, item, spider):
        """
        Gelen item'ı asenkron olarak işler.
        Item'ı türüne (item_type) göre ilgili 'private' metoda yönlendirir.
        """
        adapter = ItemAdapter(item)
        item_type = adapter.get("item_type")

        if not item_type:
            return item

        try:
            # === YÖNLENDİRİCİ (ROUTER) ===
            if item_type == "event":
                await self._process_event(adapter)

            elif item_type == "fight":
                await self._process_fight(adapter)

            elif item_type == "fighter":
                # ❗ SPIDER OBJESİNİ GÖNDERİYORUZ
                # Pipeline'ın spider'a "geri seslenebilmesi" için
                # 'spider' parametresine ihtiyacımız var.
                await self._process_fighter(adapter, spider)

            elif item_type == "participation":
                await self._process_participation(adapter)

        except DropItem:
            # DropItem'ı yakalayıp yeniden yükselterek Scrapy'nin item'ı durdurmasını sağlıyoruz.
            raise
        except Exception as e:
            self.logger.error(f"Error processing item_type '{item_type}': {e}", exc_info=True)

        return item

    # -----------------------------------------------------------------
    # === Private Helper Metotlar ===
    # -----------------------------------------------------------------

    async def _process_event(self, adapter: ItemAdapter):
        # ... (Bu metot bir önceki cevaptaki gibi, değişiklik yok) ...
        event_id = adapter.get("event_id")
        if not event_id:
            self.logger.warning("[EVENT] EventItem has no event_id. Skipping.")
            return
        event_data = adapter.asdict(); event_data.pop("item_type", None)
        existing = await self.supabase.get_event_by_id(event_id)
        if existing == "invalid": return
        if not existing:
            self.logger.info(f"[EVENT] Inserting new event: {event_id}")
            await self.supabase.insert_event(event_data)
        elif self._has_changes(event_data, existing):
            self.logger.info(f"[EVENT] Updating incomplete event: {event_id}")
            await self.supabase.update_event(event_id, event_data)
        else:
            self.logger.debug(f"[EVENT] No changes detected for event: {event_id}")


    async def _process_fight(self, adapter: ItemAdapter):
        # ... (Bu metot bir önceki cevaptaki gibi, değişiklik yok) ...
        fight_id = adapter.get("fight_id")
        if not fight_id:
            self.logger.warning("[FIGHT] FightItem has no fight_id. Skipping.")
            return
        fight_data = adapter.asdict(); fight_data.pop("item_type", None)
        existing = await self.supabase.get_fight_by_id(fight_id)
        if existing == "invalid": return
        if not existing:
            self.logger.info(f"[FIGHT] Inserting new fight: {fight_id}")
            await self.supabase.insert_fight(fight_data)
        elif self._has_changes(fight_data, existing):
            self.logger.info(f"[FIGHT] Updating fight: {fight_id}")
            await self.supabase.update_fight(fight_id, fight_data)
        else:
            self.logger.debug(f"[FIGHT] No changes for fight: {fight_id}")


    # ❗ YENİ GÜNCELLENMİŞ METOT
    async def _process_fighter(self, adapter: ItemAdapter, spider):
        """
        FighterItem'ları işler.
        Eğer dövüşçü DB'de yoksa, profilini scrape etmek için
        Spider'a 'geri seslenir' (schedules new request).
        """

        fighter_id = adapter.get("fighter_id")
        if not fighter_id:
            self.logger.warning("[FIGHTER] FighterItem has no fighter_id. Skipping.")
            return

        fighter_data = adapter.asdict()
        profile_url = adapter.get("profile_url")

        existing = await self.supabase.get_fighter_by_id(fighter_id)

        if existing == "invalid":
            self.logger.error(f"[FIGHTER] Supabase SELECT failed, skipping: {fighter_id}")
            return

        # === 1. YENİ DÖVÜŞÇÜ (NEW FIGHTER) ===
        if not existing:
            # Dövüşçü DB'de yok. Profilin tamamı scrape EDİLMELİ.

            if not profile_url:
                # Profil URL'si yoksa scrape edemeyiz.
                # Eksik veriyi basıp bırakalım.
                self.logger.warning(f"[FIGHTER] New fighter {fighter_id} has no profile_url. Inserting incomplete data.")
                fighter_data.pop("item_type", None)
                await self.supabase.insert_fighter(fighter_data)
                return

            # A. ÖNCE EKSİK KAYDI AT (Sonsuz döngüyü önlemek için)
            # Bu, "bu dövüşçüyü scrape etmeye başladım" diye bir
            # 'placeholder' (yer tutucu) oluşturur.
            self.logger.info(f"[FIGHTER] New fighter {fighter_id}. Inserting placeholder & scheduling scrape.")
            fighter_data.pop("item_type", None)
            await self.supabase.insert_fighter(fighter_data)

            # B. SONRA SCRAPE İSTEĞİNİ ZAMANLA (SCHEDULE)
            request = scrapy.Request(
                url=profile_url,
                callback=FighterParser.parse_fighter_profile,
                cb_kwargs={
                    "fighter_id": fighter_id,
                    "name": adapter.get("name"),
                    "profile_url": profile_url,
                    "image_url": adapter.get("image_url")
                }
            )

            # İsteği doğrudan Scrapy motoruna enjekte et
            await spider.crawler.engine.crawl(request)

            # Bu 'eksik' item ile işimiz bitti, pipeline'da devam etmesin.
            raise DropItem(f"Scraping new fighter profile: {fighter_id}")


        # === 2. MEVCUT DÖVÜŞÇÜ (EXISTING FIGHTER) ===
        # Dövüşçü DB'de zaten var.
        # Bu item, 'parse_fighter_profile'dan gelen TAM veri olabilir
        # VEYA 'FightParser'dan gelen 'record' gibi güncel bir özet olabilir.

        fighter_data.pop("item_type", None)
        if self._has_changes(fighter_data, existing):
            self.logger.info(f"[FIGHTER] Updating existing fighter: {fighter_id}")
            await self.supabase.update_fighter(fighter_id, fighter_data)
        else:
            self.logger.debug(f"[FIGHTER] No changes for fighter: {fighter_id}")


    async def _process_participation(self, adapter: ItemAdapter):
        # ... (Bu metot bir önceki cevaptaki gibi, değişiklik yok) ...
        fight_id = adapter.get("fight_id"); fighter_id = adapter.get("fighter_id")
        if not fight_id or not fighter_id:
            self.logger.warning("[PARTICIPATION] ParticipationItem is missing key IDs. Skipping.")
            return
        participation_data = adapter.asdict(); participation_data.pop("item_type", None)
        existing = await self.supabase.get_participation(fight_id, fighter_id)
        if existing == "invalid": return
        if not existing:
            self.logger.info(f"[PARTICIPATION] Inserting participation: {fight_id}/{fighter_id}")
            await self.supabase.insert_participation(participation_data)
        elif self._has_changes(participation_data, existing):
            self.logger.info(f"[PARTICIPATION] Updating participation: {fight_id}/{fighter_id}")
            await self.supabase.update_participation(fight_id, fighter_id, participation_data)
        else:
            self.logger.debug(f"[PARTICIPATION] No changes for participation: {fight_id}/{fighter_id}")

    # -----------------------------------------------------------------
    # === Ortak Yardımcı Metot ===
    # -----------------------------------------------------------------

    def _has_changes(self, new_data: dict, old_data: dict) -> bool:
        # ... (Bu metot bir önceki cevaptaki gibi, değişiklik yok) ...
        for key in new_data.keys():
            new_value = str(new_data.get(key))
            old_value = str(old_data.get(key))
            if new_value != old_value:
                return True
        return False
