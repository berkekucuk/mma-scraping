import os
import logging
import asyncio
from supabase import acreate_client, AsyncClient
from dotenv import load_dotenv

load_dotenv()

class SupabaseManager:

    _primary_client: AsyncClient = None
    _secondary_client: AsyncClient = None
    _logger = logging.getLogger(__name__)

    @classmethod
    async def init_clients(cls):
        if cls._primary_client is None or cls._secondary_client is None:
            url_1 = os.getenv("SUPABASE_PROD_URL")
            key_1 = os.getenv("SUPABASE_PROD_KEY")

            url_2 = os.getenv("SUPABASE_DEV_URL")
            key_2 = os.getenv("SUPABASE_DEV_KEY")

            if not all([url_1, key_1, url_2, key_2]):
                cls._logger.error("Both credentials are required in .env")
                raise ValueError("Missing Supabase credentials")

            try:
                cls._primary_client, cls._secondary_client = await asyncio.gather(
                    acreate_client(url_1, key_1),
                    acreate_client(url_2, key_2)
                )
                cls._logger.info("Both Supabase clients initialized (Async)")

            except Exception as e:
                cls._logger.error(f"Failed to initialize Supabase clients: {e}")
                raise

    @classmethod
    async def get_primary_client(cls) -> AsyncClient:
        if cls._primary_client is None:
            await cls.init_clients()
        return cls._primary_client

    @classmethod
    async def get_both_clients(cls):
        if cls._primary_client is None or cls._secondary_client is None:
            await cls.init_clients()
        return cls._primary_client, cls._secondary_client

    # ──────────────────────────────────────────────────────────
    # EVENT OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_events_by_ids(cls, event_ids: list):
        if not event_ids: return {}

        try:
            client = await cls.get_primary_client()
            response = await client.table("events").select("*").in_("event_id", event_ids).execute()
            events_dict = {event["event_id"]: event for event in response.data}
            cls._logger.info(f"Fetched {len(events_dict)} events from {len(event_ids)} requested IDs")
            return events_dict

        except Exception as e:
            cls._logger.error(f"Failed to get events: {e}")
            raise e

    @classmethod
    async def get_live_event(cls):
        try:
            client = await cls.get_primary_client()
            response = await client.table("events").select("event_id, event_url").eq("status", "live").limit(1).execute()
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
            client1, client2 = await cls.get_both_clients()
            await asyncio.gather(
                client1.table("events").insert(event_data).execute(),
                client2.table("events").insert(event_data).execute()
            )
            cls._logger.info(f"Successfully inserted event {event_data.get('event_id')}")

        except Exception as e:
            cls._logger.error(f"Failed to insert event {event_data.get('event_id')}: {e}")
            raise e

    @classmethod
    async def update_event(cls, event_id: str, event_data: dict):
        try:
            client1, client2 = await cls.get_both_clients()
            await asyncio.gather(
                client1.table("events").update(event_data).eq("event_id", event_id).execute(),
                client2.table("events").update(event_data).eq("event_id", event_id).execute()
            )
            cls._logger.info(f"Successfully updated event {event_id}")

        except Exception as e:
            cls._logger.error(f"Failed to update event {event_id}: {e}")
            raise e

    # ──────────────────────────────────────────────────────────
    # FIGHT OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_fights_by_ids(cls, fight_ids: list):
        if not fight_ids: return {}
        try:
            client = await cls.get_primary_client()
            response = await client.table("fights").select("*").in_("fight_id", fight_ids).execute()
            fights_dict = {fight["fight_id"]: fight for fight in response.data}
            cls._logger.info(f"Fetched {len(fights_dict)} fights from {len(fight_ids)} requested IDs")
            return fights_dict

        except Exception as e:
            cls._logger.error(f"Failed to get fights: {e}")
            raise e

    @classmethod
    async def insert_fight(cls, fight_data: dict):
        try:
            client1, client2 = await cls.get_both_clients()
            await asyncio.gather(
                client1.table("fights").insert(fight_data).execute(),
                client2.table("fights").insert(fight_data).execute()
            )
            cls._logger.info(f"Successfully inserted fight {fight_data.get('fight_id')}")

        except Exception as e:
            cls._logger.error(f"Failed to insert fight {fight_data.get('fight_id')}: {e}")
            raise e

    @classmethod
    async def update_fight(cls, fight_id: str, fight_data: dict):
        try:
            client1, client2 = await cls.get_both_clients()
            await asyncio.gather(
                client1.table("fights").update(fight_data).eq("fight_id", fight_id).execute(),
                client2.table("fights").update(fight_data).eq("fight_id", fight_id).execute()
            )
            cls._logger.info(f"Successfully updated fight {fight_id}")

        except Exception as e:
            cls._logger.error(f"Failed to update fight {fight_id}: {e}")
            raise e

    # ──────────────────────────────────────────────────────────
    # FIGHTER OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_fighters_by_ids(cls, fighter_ids: list):
        if not fighter_ids: return {}
        try:
            client = await cls.get_primary_client()
            response = await client.table("fighters").select("fighter_id, name").in_("fighter_id", fighter_ids).execute()
            fighters_dict = {fighter["fighter_id"]: fighter for fighter in response.data}
            cls._logger.info(f"Fetched {len(fighters_dict)} fighters from {len(fighter_ids)} requested IDs")
            return fighters_dict

        except Exception as e:
            cls._logger.error(f"Failed to get fighters: {e}")
            raise e

    @classmethod
    async def insert_fighter(cls, fighter_data: dict):
        try:
            client1, client2 = await cls.get_both_clients()
            await asyncio.gather(
                client1.table("fighters").insert(fighter_data).execute(),
                client2.table("fighters").insert(fighter_data).execute()
            )
            cls._logger.info(f"Successfully inserted fighter {fighter_data.get('fighter_id')}")

        except Exception as e:
            cls._logger.error(f"Failed to insert fighter {fighter_data.get('fighter_id')}: {e}")
            raise e

    @classmethod
    async def update_fighter(cls, fighter_id: str, fighter_data: dict):
        try:
            client1, client2 = await cls.get_both_clients()
            await asyncio.gather(
                client1.table("fighters").update(fighter_data).eq("fighter_id", fighter_id).execute(),
                client2.table("fighters").update(fighter_data).eq("fighter_id", fighter_id).execute()
            )
            cls._logger.info(f"Successfully updated fighter {fighter_id}")

        except Exception as e:
            cls._logger.error(f"Failed to update fighter {fighter_id}: {e}")
            raise e

    # ──────────────────────────────────────────────────────────
    # PARTICIPATION OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_participations_by_keys(cls, participation_keys: list):
        if not participation_keys: return {}
        try:
            client = await cls.get_primary_client()
            fight_ids = list(set(k[0] for k in participation_keys))
            fighter_ids = list(set(k[1] for k in participation_keys))
            response = await client.table("participants").select("*").in_("fight_id", fight_ids).in_("fighter_id", fighter_ids).execute()
            participations_dict = {
                (part["fight_id"], part["fighter_id"]): part
                for part in response.data
                if (part["fight_id"], part["fighter_id"]) in participation_keys
            }
            cls._logger.info(f"Fetched {len(participations_dict)} participations from {len(participation_keys)} requested keys")
            return participations_dict

        except Exception as e:
            cls._logger.error(f"Failed to get participations: {e}")
            raise e

    @classmethod
    async def insert_participation(cls, participation_data: dict):
        try:
            client1, client2 = await cls.get_both_clients()
            await asyncio.gather(
                client1.table("participants").insert(participation_data).execute(),
                client2.table("participants").insert(participation_data).execute()
            )
            cls._logger.info(f"Successfully inserted participation {participation_data.get('fight_id')}/{participation_data.get('fighter_id')}")

        except Exception as e:
            cls._logger.error(f"Failed to insert participation: {e}")
            raise e

    @classmethod
    async def update_participation(cls, fight_id: str, fighter_id: str, participation_data: dict):
        try:
            client1, client2 = await cls.get_both_clients()
            await asyncio.gather(
                client1.table("participants").update(participation_data).eq("fight_id", fight_id).eq("fighter_id", fighter_id).execute(),
                client2.table("participants").update(participation_data).eq("fight_id", fight_id).eq("fighter_id", fighter_id).execute()
            )
            cls._logger.info(f"Successfully updated participation {fight_id}/{fighter_id}")

        except Exception as e:
            cls._logger.error(f"Failed to update participation: {e}")
            raise e

    # ──────────────────────────────────────────────────────────
    # RANKING OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def load_fighter_cache(cls):
        try:
            client = await cls.get_primary_client()
            fighter_cache = {}
            batch_size = 1000
            offset = 0

            while True:
                response = await client.table('fighters').select('fighter_id, name').range(offset, offset + batch_size - 1).execute()
                if not response.data: break

                for f in response.data:
                    fighter_cache[f['name'].strip()] = f['fighter_id']

                if len(response.data) < batch_size: break
                offset += batch_size

            cls._logger.info(f"Successfully loaded {len(fighter_cache)} fighters into cache")
            return fighter_cache

        except Exception as e:
            cls._logger.error(f"Failed to load fighter cache: {e}")
            raise e

    @classmethod
    async def batch_upsert_rankings(cls, rankings_data: list):
        if not rankings_data: return
        try:
            client1, client2 = await cls.get_both_clients()
            await asyncio.gather(
                client1.table("rankings").upsert(rankings_data, on_conflict="weight_class_id, rank_number").execute(),
                client2.table("rankings").upsert(rankings_data, on_conflict="weight_class_id, rank_number").execute()
            )
            cls._logger.info(f"Batch upserted {len(rankings_data)} rankings to BOTH DBs")

        except Exception as e:
            cls._logger.error(f"Failed to batch upsert rankings: {e}")
            raise e
