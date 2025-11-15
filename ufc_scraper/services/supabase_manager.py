from datetime import timedelta, timezone, datetime
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

    @classmethod
    async def _get_by_id(cls, table_name: str, id_column: str, id_value: str):
        """
        General helper method to perform a 'get' operation by unique ID.
        """
        try:
            client = await cls.get_client()
            response = await client.table(table_name).select("*").eq(id_column, id_value).single().execute()
            return response.data

        except Exception as e:
            if "PGRST116" in str(e) or "0 rows" in str(e):
                cls._logger.debug(f"[{table_name}] Item not found in DB: {id_value}")
                return None  # Hata değil, 'yok' anlamına gelir

            cls._logger.error(f"Unexpected error getting {table_name} {id_value}: {e}")
            return "invalid"  # Gerçek bir veritabanı hatası

    # ──────────────────────────────────────────────────────────
    # EVENT OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_event_by_id(cls, event_id: str):
        return await cls._get_by_id("events", "event_id", event_id)

    @classmethod
    async def get_upcoming_events(cls):
        try:
            client = await cls.get_client()
            response = await client.table("events").select("event_id, event_url").eq("status", "Upcoming").execute()
            return response.data

        except Exception as e:
            cls._logger.error(f"Failed to get upcoming events: {e}")
            return None

    @classmethod
    async def get_live_event(cls):
        """
        Fetch a single live event that is currently ongoing or recently started.
        An event is considered 'live' if its status is not 'Completed' and its datetime_utc
        is within the last 12 hours up to now.
        """
        try:
            client = await cls.get_client()

            now = datetime.now(timezone.utc)
            twelve_hours_ago = now - timedelta(hours=12)

            now_str = now.replace(microsecond=0).isoformat()
            twelve_hours_ago_str = twelve_hours_ago.replace(microsecond=0).isoformat()

            response = (
                await client.table("events")
                .select("event_id, event_url")
                .neq("status", "Completed")
                .lte("datetime_utc", now_str)
                .gte("datetime_utc", twelve_hours_ago_str)
                .limit(1)
                .execute()
            )

            # .limit(1) bir liste döndürür. Listenin dolu olup olmadığını kontrol etmeliyiz.
            if response.data:
                # Liste doluysa, ilk (ve tek) elemanı döndür
                return response.data[0]
            else:
                # Liste boşsa (canlı etkinlik yoksa) None döndür
                cls._logger.info("No live event found.")
                return None

        except Exception as e:
            # .single() kullanmadığımız için "PGRST116" hatası almayacağız.
            # "0 rows" durumu artık bir hata değil, boş bir liste.
            cls._logger.error(f"Failed to get LIVE event: {e}")
            return None

    @classmethod
    async def insert_event(cls, event_data: dict):
        try:
            client = await cls.get_client()
            await client.table("events").insert(event_data).execute()

        except Exception as e:
            cls._logger.error(f"Failed to insert event {event_data.get('event_id')}: {e}")

    @classmethod
    async def update_event(cls, event_id: str, event_data: dict):
        try:
            client = await cls.get_client()
            await client.table("events").update(event_data).eq("event_id", event_id).execute()

        except Exception as e:
            cls._logger.error(f"Failed to update event {event_id}: {e}")

    # ──────────────────────────────────────────────────────────
    # FIGHT OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_fight_by_id(cls, fight_id: str):
        return await cls._get_by_id("fights", "fight_id", fight_id)

    @classmethod
    async def insert_fight(cls, fight_data: dict):
        try:
            client = await cls.get_client()
            await client.table("fights").insert(fight_data).execute()

        except Exception as e:
            cls._logger.error(f"Failed to insert fight {fight_data.get('fight_id')}: {e}")

    @classmethod
    async def update_fight(cls, fight_id: str, fight_data: dict):
        try:
            client = await cls.get_client()
            await client.table("fights").update(fight_data).eq("fight_id", fight_id).execute()

        except Exception as e:
            cls._logger.error(f"Failed to update fight {fight_id}: {e}")

    # ──────────────────────────────────────────────────────────
    # FIGHTER OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_fighter_by_id(cls, fighter_id: str):
        return await cls._get_by_id("fighters", "fighter_id", fighter_id)

    @classmethod
    async def insert_fighter(cls, fighter_data: dict):
        try:
            client = await cls.get_client()
            await client.table("fighters").insert(fighter_data).execute()

        except Exception as e:
            cls._logger.error(f"Failed to insert fighter {fighter_data.get('fighter_id')}: {e}")

    @classmethod
    async def update_fighter(cls, fighter_id: str, fighter_data: dict):
        try:
            client = await cls.get_client()
            await client.table("fighters").update(fighter_data).eq("fighter_id", fighter_id).execute()

        except Exception as e:
            cls._logger.error(f"Failed to update fighter {fighter_id}: {e}")

    # ──────────────────────────────────────────────────────────
    # PARTICIPATION OPERATIONS (Composite Key - Bileşik Anahtar)
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_participation(cls, fight_id: str, fighter_id: str):
        try:
            client = await cls.get_client()
            response = (
                await client.table("participants").select("*").eq("fight_id", fight_id).eq("fighter_id", fighter_id).single().execute()
            )
            return response.data

        except Exception as e:
            if "PGRST116" in str(e) or "0 rows" in str(e):
                cls._logger.debug(f"[PARTICIPATION] Item not found: {fight_id}/{fighter_id}")
                return None

            cls._logger.error(f"Unexpected error getting participation {fight_id}/{fighter_id}: {e}")
            return "invalid"

    @classmethod
    async def insert_participation(cls, participation_data: dict):
        try:
            client = await cls.get_client()
            await client.table("participants").insert(participation_data).execute()

        except Exception as e:
            cls._logger.error(f"Failed to insert participation {participation_data.get('fight_id')}: {e}")

    @classmethod
    async def update_participation(cls, fight_id: str, fighter_id: str, participation_data: dict):
        try:
            client = await cls.get_client()
            await client.table("participants").update(participation_data).eq("fight_id", fight_id).eq("fighter_id", fighter_id).execute()

        except Exception as e:
            cls._logger.error(f"Failed to update participation {fight_id}/{fighter_id}: {e}")
