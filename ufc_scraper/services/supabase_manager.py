import os
import logging
from supabase import acreate_client, AsyncClient
from dotenv import load_dotenv

load_dotenv()


class SupabaseManager:

    _instance: AsyncClient = None
    _logger = logging.getLogger(__name__)

    @classmethod
    async def get_client(cls) -> AsyncClient:
        """
        initializes async supabase client if not already done,
        or return the existing one (Singleton pattern).
        """
        if cls._instance is None:
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_KEY")

            if not supabase_url or not supabase_key:
                cls._logger.error("SUPABASE_URL and SUPABASE_KEY are required")
                raise ValueError("SUPABASE_URL and SUPABASE_KEY are required")

            try:
                cls._instance = await acreate_client(supabase_url, supabase_key)
                cls._logger.info("Supabase client initialized (Async)")

            except Exception as e:
                cls._logger.error(f"Failed to initialize Supabase client: {e}")
                raise

        return cls._instance

    # ──────────────────────────────────────────────────────────
    # EVENT OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_events_by_ids(cls, event_ids: list):
        if not event_ids:
            return {}

        try:
            client = await cls.get_client()
            response = (
                await client.table("events")
                .select("*")
                .in_("event_id", event_ids)
                .execute()
            )

            events_dict = {event["event_id"]: event for event in response.data}
            cls._logger.debug(f"Fetched {len(events_dict)} events from {len(event_ids)} requested IDs")
            return events_dict

        except Exception as e:
            cls._logger.error(f"Failed to get events by IDs: {e}")
            raise e

    @classmethod
    async def get_live_event(cls):
        try:
            client = await cls.get_client()

            response = (
                await client.table("events")
                .select("event_id, event_url")
                .eq("status", "live")
                .limit(1)
                .execute()
            )

            if response.data:
                return response.data[0]
            else:
                cls._logger.info("No live event found.")
                return None

        except Exception as e:
            cls._logger.error(f"Failed to get LIVE event: {e}")
            return None

    @classmethod
    async def insert_event(cls, event_data: dict):
        try:
            client = await cls.get_client()
            await client.table("events").insert(event_data).execute()

        except Exception as e:
            cls._logger.error(f"Failed to insert event {event_data.get('event_id')}: {e}")
            raise e

    @classmethod
    async def update_event(cls, event_id: str, event_data: dict):
        try:
            client = await cls.get_client()
            await client.table("events").update(event_data).eq("event_id", event_id).execute()

        except Exception as e:
            cls._logger.error(f"Failed to update event {event_id}: {e}")
            raise e

    # ──────────────────────────────────────────────────────────
    # FIGHT OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_fights_by_ids(cls, fight_ids: list):
        if not fight_ids:
            return {}

        try:
            client = await cls.get_client()
            response = (
                await client.table("fights")
                .select("*")
                .in_("fight_id", fight_ids)
                .execute()
            )

            fights_dict = {fight["fight_id"]: fight for fight in response.data}
            cls._logger.debug(f"Fetched {len(fights_dict)} fights from {len(fight_ids)} requested IDs")
            return fights_dict

        except Exception as e:
            cls._logger.error(f"Failed to get fights by IDs: {e}")
            raise e

    @classmethod
    async def insert_fight(cls, fight_data: dict):
        try:
            client = await cls.get_client()
            await client.table("fights").insert(fight_data).execute()

        except Exception as e:
            cls._logger.error(f"Failed to insert fight {fight_data.get('fight_id')}: {e}")
            raise e

    @classmethod
    async def update_fight(cls, fight_id: str, fight_data: dict):
        try:
            client = await cls.get_client()
            await client.table("fights").update(fight_data).eq("fight_id", fight_id).execute()

        except Exception as e:
            cls._logger.error(f"Failed to update fight {fight_id}: {e}")
            raise e

    # ──────────────────────────────────────────────────────────
    # FIGHTER OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_fighters_by_ids(cls, fighter_ids: list):
        if not fighter_ids:
            return {}

        try:
            client = await cls.get_client()
            response = (
                await client.table("fighters")
                .select("fighter_id, name")
                .in_("fighter_id", fighter_ids)
                .execute()
            )

            fighters_dict = {fighter["fighter_id"]: fighter for fighter in response.data}
            cls._logger.debug(f"Fetched {len(fighters_dict)} fighters from {len(fighter_ids)} requested IDs")
            return fighters_dict

        except Exception as e:
            cls._logger.error(f"Failed to get fighters by IDs: {e}")
            raise e

    @classmethod
    async def insert_fighter(cls, fighter_data: dict):
        try:
            client = await cls.get_client()
            await client.table("fighters").insert(fighter_data).execute()

        except Exception as e:
            cls._logger.error(f"Failed to insert fighter {fighter_data.get('fighter_id')}: {e}")
            raise e

    # ──────────────────────────────────────────────────────────
    # PARTICIPATION OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_participations_by_keys(cls, participation_keys: list):
        if not participation_keys:
            return {}

        try:
            client = await cls.get_client()

            fight_ids = list(set(fight_id for fight_id, _ in participation_keys))
            fighter_ids = list(set(fighter_id for _, fighter_id in participation_keys))

            response = (
                await client.table("participants")
                .select("*")
                .in_("fight_id", fight_ids)
                .in_("fighter_id", fighter_ids)
                .execute()
            )

            participations_dict = {
                (part["fight_id"], part["fighter_id"]): part
                for part in response.data
                if (part["fight_id"], part["fighter_id"]) in participation_keys
            }

            cls._logger.debug(f"Fetched {len(participations_dict)} participations from {len(participation_keys)} requested keys")
            return participations_dict

        except Exception as e:
            cls._logger.error(f"Failed to get participations by keys: {e}")
            raise e

    @classmethod
    async def insert_participation(cls, participation_data: dict):
        try:
            client = await cls.get_client()
            await client.table("participants").insert(participation_data).execute()

        except Exception as e:
            cls._logger.error(f"Failed to insert participation {participation_data.get('fight_id')}: {e}")
            raise e

    @classmethod
    async def update_participation(cls, fight_id: str, fighter_id: str, participation_data: dict):
        try:
            client = await cls.get_client()
            await client.table("participants").update(participation_data).eq("fight_id", fight_id).eq("fighter_id", fighter_id).execute()

        except Exception as e:
            cls._logger.error(f"Failed to update participation {fight_id}/{fighter_id}: {e}")
            raise e
