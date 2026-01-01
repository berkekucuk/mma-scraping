import logging
from itemadapter import ItemAdapter
from scrapy.utils.defer import deferred_from_coro
from .services.supabase_manager import SupabaseManager

class DatabasePipeline:

    def __init__(self):
        self.supabase = SupabaseManager
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Unified DatabasePipeline initialized with Batch Buffer Mechanism.")

        self.event_buffer = {}
        self.fight_buffer = {}
        self.fighter_buffer = {}
        self.participation_buffer = {}

    async def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        item_type = adapter.get("item_type")

        if not item_type:
            return item

        item_data = adapter.asdict()
        item_data.pop("item_type", None)

        if item_type == "event":
            event_id = adapter.get("event_id")
            if event_id: self.event_buffer[event_id] = item_data
        elif item_type == "fight":
            fight_id = adapter.get("fight_id")
            if fight_id: self.fight_buffer[fight_id] = item_data
        elif item_type == "fighter":
            fighter_id = adapter.get("fighter_id")
            if fighter_id: self.fighter_buffer[fighter_id] = item_data
        elif item_type == "participation":
            fight_id = adapter.get("fight_id")
            fighter_id = adapter.get("fighter_id")
            if fight_id and fighter_id:
                self.participation_buffer[(fight_id, fighter_id)] = item_data

        return item


    def close_spider(self, spider):
        self.logger.info(f"[BATCH] Processing remaining items: {len(self.event_buffer)} events, "
                        f"{len(self.fight_buffer)} fights, {len(self.fighter_buffer)} fighters, "
                        f"{len(self.participation_buffer)} participations")

        return deferred_from_coro(self._flush_all())


    async def _flush_all(self):
        if self.event_buffer: await self._process_events_batch()
        if self.fight_buffer: await self._process_fights_batch()
        if self.fighter_buffer: await self._process_fighters_batch()
        if self.participation_buffer: await self._process_participations_batch()

        self.logger.info("[BATCH END] All buffered items processed.")


    async def _process_events_batch(self):
        if not self.event_buffer: return

        current_batch = self.event_buffer.copy()
        self.event_buffer.clear()

        event_ids = list(current_batch.keys())
        remote_events = await self.supabase.get_events_by_ids(event_ids)

        for event_id, event_data in current_batch.items():
            remote_data = remote_events.get(event_id)
            if not remote_data:
                self.logger.info(f"[EVENT] Inserting new event: {event_id}")
                await self.supabase.insert_event(event_data)
            elif self._has_changes(event_data, remote_data):
                self.logger.info(f"[EVENT] Updating event: {event_id}")
                await self.supabase.update_event(event_id, event_data)


    async def _process_fights_batch(self):
        if not self.fight_buffer: return

        current_batch = self.fight_buffer.copy()
        self.fight_buffer.clear()

        fight_ids = list(current_batch.keys())
        remote_fights = await self.supabase.get_fights_by_ids(fight_ids)

        for fight_id, fight_data in current_batch.items():
            remote_data = remote_fights.get(fight_id)
            if not remote_data:
                self.logger.info(f"[FIGHT] Inserting new fight: {fight_id}")
                await self.supabase.insert_fight(fight_data)
            elif self._has_changes(fight_data, remote_data):
                self.logger.info(f"[FIGHT] Updating fight: {fight_id}")
                await self.supabase.update_fight(fight_id, fight_data)


    async def _process_fighters_batch(self):
        if not self.fighter_buffer: return

        current_batch = self.fighter_buffer.copy()
        self.fighter_buffer.clear()

        fighter_ids = list(current_batch.keys())
        remote_fighters = await self.supabase.get_fighters_by_ids(fighter_ids)

        for fighter_id, fighter_data in current_batch.items():
            if not remote_fighters.get(fighter_id):
                self.logger.info(f"[FIGHTER] Inserting new fighter: {fighter_id}")
                await self.supabase.insert_fighter(fighter_data)


    async def _process_participations_batch(self):
        if not self.participation_buffer: return

        current_batch = self.participation_buffer.copy()
        self.participation_buffer.clear()

        participation_keys = list(current_batch.keys())
        remote_participations = await self.supabase.get_participations_by_keys(participation_keys)

        for key, participation_data in current_batch.items():
            fight_id, fighter_id = key
            remote_data = remote_participations.get(key)
            if not remote_data:
                self.logger.info(f"[PARTICIPATION] Inserting: {fight_id}/{fighter_id}")
                await self.supabase.insert_participation(participation_data)
            else:
                if remote_data.get("is_red_corner") is not None:
                    participation_data.pop("is_red_corner", None)
                if self._has_changes(participation_data, remote_data):
                    self.logger.info(f"[PARTICIPATION] Updating: {fight_id}/{fighter_id}")
                    await self.supabase.update_participation(fight_id, fighter_id, participation_data)


    def _has_changes(self, new_data: dict, remote_data: dict) -> bool:
        JSONB_FIELDS = {'record', 'height', 'reach', 'record_after_fight'}
        NUMERIC_FIELDS = {'weight_class_lbs', 'odds_value', 'fight_order'}
        IGNORE_FIELDS = {'created_at', 'updated_at'}

        for key, new_value in new_data.items():
            if key in IGNORE_FIELDS: continue
            if key not in remote_data: return True
            remote_value = remote_data.get(key)
            if (new_value is None) != (remote_value is None): return True
            if new_value is None: continue
            if key in JSONB_FIELDS:
                if new_value != remote_value: return True
                continue
            if key in NUMERIC_FIELDS:
                try:
                    if float(new_value) != float(remote_value): return True
                except (ValueError, TypeError):
                    if str(new_value) != str(remote_value): return True
                continue
            if str(new_value) != str(remote_value): return True
        return False
