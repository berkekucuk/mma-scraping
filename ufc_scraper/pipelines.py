import logging
from itemadapter import ItemAdapter
from .services.supabase_manager import SupabaseManager


class DatabasePipeline:

    def __init__(self):
        self.supabase = SupabaseManager
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Unified DatabasePipeline initialized.")
        self.processed_fighter_ids = set()

    async def process_item(self, item, spider):

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
                await self._process_fighter(adapter)

            elif item_type == "participation":
                await self._process_participation(adapter)

        except Exception as e:
            self.logger.error(f"Error processing item_type '{item_type}': {e}", exc_info=True)

        return item


    async def _process_event(self, adapter: ItemAdapter):

        event_id = adapter.get("event_id")
        if not event_id:
            self.logger.warning("[EVENT] EventItem has no event_id. Skipping.")
            return

        event_data = adapter.asdict()
        event_data.pop("item_type", None)
        existing = await self.supabase.get_event_by_id(event_id)

        if existing == "invalid":
            self.logger.error(f"[EVENT] Supabase SELECT failed, skipping: {event_id}")
            return

        if not existing:
            self.logger.info(f"[EVENT] Inserting new event: {event_id}")
            await self.supabase.insert_event(event_data)
        elif self._has_changes(event_data, existing):
            self.logger.info(f"[EVENT] Updating incomplete event: {event_id}")
            await self.supabase.update_event(event_id, event_data)
        else:
            self.logger.debug(f"[EVENT] No changes detected for event: {event_id}")


    async def _process_fight(self, adapter: ItemAdapter):
        fight_id = adapter.get("fight_id")

        if not fight_id:
            self.logger.warning("[FIGHT] FightItem has no fight_id. Skipping.")
            return

        fight_data = adapter.asdict()
        fight_data.pop("item_type", None)
        existing = await self.supabase.get_fight_by_id(fight_id)

        if existing == "invalid":
            self.logger.error(f"[FIGHT] Supabase SELECT failed, skipping: {fight_id}")
            return

        if not existing:
            self.logger.info(f"[FIGHT] Inserting new fight: {fight_id}")
            await self.supabase.insert_fight(fight_data)
        elif self._has_changes(fight_data, existing):
            self.logger.info(f"[FIGHT] Updating fight: {fight_id}")
            await self.supabase.update_fight(fight_id, fight_data)
        else:
            self.logger.debug(f"[FIGHT] No changes for fight: {fight_id}")


    async def _process_fighter(self, adapter: ItemAdapter):
        fighter_id = adapter.get("fighter_id")

        if not fighter_id:
            self.logger.warning("[FIGHTER] FighterItem has no fighter_id. Skipping.")
            return

        # === 1. BU ÇALIŞTIRMADA İŞLENDİ Mİ? ===
        if fighter_id in self.processed_fighter_ids:
            self.logger.debug(f"[FIGHTER] Already processed in this run: {fighter_id}. Skipping.")
            return

        fighter_data = adapter.asdict()
        fighter_data.pop("item_type", None)
        existing = await self.supabase.get_fighter_by_id(fighter_id)

        if existing == "invalid":
            self.logger.error(f"[FIGHTER] Supabase SELECT failed, skipping: {fighter_id}")
            return

        if not existing:
            self.logger.info(f"[FIGHTER] New fighter {fighter_id}. Inserting (with data from event page).")
            await self.supabase.insert_fighter(fighter_data)
        else:
            self.logger.debug(f"[FIGHTER] Fighter {fighter_id} already exists.")
        self.processed_fighter_ids.add(fighter_id)


    async def _process_participation(self, adapter: ItemAdapter):
        fight_id = adapter.get("fight_id"); fighter_id = adapter.get("fighter_id")

        if not fight_id or not fighter_id:
            self.logger.warning("[PARTICIPATION] ParticipationItem is missing key IDs. Skipping.")
            return

        participation_data = adapter.asdict()
        participation_data.pop("item_type", None)
        existing = await self.supabase.get_participation(fight_id, fighter_id)

        if existing == "invalid":
            self.logger.error(f"[PARTICIPATION] Supabase SELECT failed, skipping: {fight_id}/{fighter_id}")
            return

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
        for key in new_data.keys():
            new_value = str(new_data.get(key))
            old_value = str(old_data.get(key))
            if new_value != old_value:
                return True
        return False
