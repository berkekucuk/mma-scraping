import logging
import scrapy
import httpx
import os
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from .services.supabase_manager import SupabaseManager
from .parsers.fighter_parser import FighterParser


class DatabasePipeline:

    def __init__(self):
        self.supabase = SupabaseManager
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Unified DatabasePipeline initialized.")
        self._httpx_client = httpx.AsyncClient()

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


    async def _process_fighter(self, adapter: ItemAdapter, spider):
        """
        FighterItem'ları işler.
        Eğer dövüşçü DB'de yoksa VEYA profil sayfasından gelen TAM veriyse
        resim indirme/yükleme işlemini de tetikler.
        """

        fighter_id = adapter.get("fighter_id")
        if not fighter_id:
            self.logger.warning("[FIGHTER] FighterItem has no fighter_id. Skipping.")
            return

        fighter_data = adapter.asdict()
        profile_url = adapter.get("profile_url")
        original_image_url = adapter.get("image_url") # Kazınan orjinal resim URL'si

        existing = await self.supabase.get_fighter_by_id(fighter_id)

        if existing == "invalid":
            self.logger.error(f"[FIGHTER] Supabase SELECT failed, skipping: {fighter_id}")
            return

        # === DURUM 1: YENİ DÖVÜŞÇÜ (DB'de yok) ===
        if not existing:
            self.logger.info(f"[FIGHTER] New fighter {fighter_id}. Processing...")

            # --- Resim İşlemini Tetikle ---
            if original_image_url:
                public_url = await self.handle_image_upload(original_image_url, fighter_id)
                if public_url:
                    # 'fighter_data' içindeki URL'i bizim bucket URL'imiz ile değiştir
                    fighter_data["image_url"] = public_url

            # --- Yer Tutucu (Placeholder) Ekle ---
            self.logger.info(f"[FIGHTER] Inserting placeholder for {fighter_id}")
            fighter_data.pop("item_type", None)
            await self.supabase.insert_fighter(fighter_data)

            # --- Profil Scrape Görevini Planla ---
            if profile_url:
                self.logger.info(f"[FIGHTER] Scheduling profile scrape for {fighter_id}")
                request = scrapy.Request(
                    url=profile_url,
                    callback=FighterParser.parse_fighter_profile,
                    cb_kwargs={
                        "fighter_id": fighter_id,
                        "name": adapter.get("name"),
                        "profile_url": profile_url,
                        "image_url": original_image_url # Orijinal URL'i yolluyoruz
                    }
                )
                spider.crawler.engine.crawl(request)
                raise DropItem(f"Scraping new fighter profile: {fighter_id}")

            return # Profil URL'si yoksa işlem bitti

        # === DURUM 2: MEVCUT DÖVÜŞÇÜ (DB'de var) ===
        # Bu, muhtemelen 'parse_fighter_profile'dan gelen TAM veridir.

        # --- Resim İşlemini Tekrar Kontrol Et ---
        # DB'deki URL bizim bucket URL'imiz mi, yoksa hala eski Tapology URL'si mi?
        existing_image_url = existing.get("image_url")
        needs_image_upload = False

        if original_image_url and "supabase.co" not in str(existing_image_url):
            # DB'deki resim URL'si bizim bucket linkimiz değil.
            # YENİDEN İNDİR/YÜKLE.
            needs_image_upload = True

        if needs_image_upload:
            public_url = await self.handle_image_upload(original_image_url, fighter_id)
            if public_url:
                fighter_data["image_url"] = public_url

        # --- Değişiklik Varsa Güncelle ---
        fighter_data.pop("item_type", None)
        if self._has_changes(fighter_data, existing):
            self.logger.info(f"[FIGHTER] Updating existing fighter: {fighter_id}")
            await self.supabase.update_fighter(fighter_id, fighter_data)
        else:
            self.logger.debug(f"[FIGHTER] No changes for fighter: {fighter_id}")


    async def handle_image_upload(self, original_image_url: str, fighter_id: str) -> str | None:
        """
        Resim indirme ve Supabase'e yükleme işlemini birleştiren yardımcı metot.
        """
        self.logger.debug(f"[IMAGE_HANDLER] Downloading image for {fighter_id} from {original_image_url}")

        # 1. Resmi Asenkron İndir
        file_body = await self.download_image_async(original_image_url)
        if not file_body:
            return None

        # 2. Dosya Adını Belirle (örn: 12345.jpg)
        # Orijinal URL'den uzantıyı al, alamazsan .jpg kullan
        file_ext = os.path.splitext(original_image_url)[1].lower()
        if file_ext not in ['.jpg', '.jpeg', '.png', '.webp']:
            file_ext = '.jpg'

        file_name = f"{fighter_id}{file_ext}"

        # 3. Supabase'e Yükle
        public_url = await self.supabase.upload_fighter_image(file_name, file_body)
        return public_url


    async def download_image_async(self, original_image_url: str) -> bytes | None:
        """Paylaşımlı httpx istemcisi ile bir resmi indirir."""
        try:
            response = await self._httpx_client.get(original_image_url)
            response.raise_for_status() # 4xx/5xx hatalarını yakala
            return response.content
        except Exception as e:
            self.logger.error(f"Failed to download image {original_image_url}: {e}")
            return None


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

