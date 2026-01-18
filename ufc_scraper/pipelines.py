import logging
from itemadapter import ItemAdapter
from scrapy.utils.defer import deferred_from_coro
from .services.supabase_manager import SupabaseManager

class DatabasePipeline:

    def __init__(self):
        self.supabase = SupabaseManager()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Unified DatabasePipeline initialized.")

        self.event_buffer = []
        self.fight_buffer = []
        self.fighter_buffer = []
        self.participation_buffer = []


    async def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        item_type = adapter.get("item_type")

        if not item_type:
            return item

        item_data = adapter.asdict()
        item_data.pop("item_type", None)

        if item_type == "event":
            self.event_buffer.append(item_data)
        elif item_type == "fight":
            self.fight_buffer.append(item_data)
        elif item_type == "fighter":
            self.fighter_buffer.append(item_data)
        elif item_type == "participation":
            self.participation_buffer.append(item_data)

        return item


    def close_spider(self, spider):
        self.logger.info(f"[BATCH] Processing buffered items: {len(self.event_buffer)} events, "
                        f"{len(self.fight_buffer)} fights, {len(self.fighter_buffer)} fighters, "
                        f"{len(self.participation_buffer)} participations")

        return deferred_from_coro(self._flush_all())


    async def _flush_all(self):
        if self.event_buffer:
            await self.supabase.bulk_upsert("events", self.event_buffer)
            self.event_buffer.clear()

        if self.fighter_buffer:
            await self.supabase.bulk_upsert("fighters", self.fighter_buffer)
            self.fighter_buffer.clear()

        if self.fight_buffer:
            await self.supabase.bulk_upsert("fights", self.fight_buffer)
            self.fight_buffer.clear()

        if self.participation_buffer:
            await self.supabase.bulk_upsert("participants", self.participation_buffer)
            self.participation_buffer.clear()

        self.logger.info("[BATCH END] All items processed.")
