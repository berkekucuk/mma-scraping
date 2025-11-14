import os
import logging
from supabase import acreate_client, AsyncClient
from dotenv import load_dotenv

load_dotenv()

class SupabaseManager:
    """Async Singleton Supabase client."""

    _instance: AsyncClient = None
    _logger = logging.getLogger(__name__)

    @classmethod
    async def get_client(cls) -> AsyncClient:
        """
        Asenkron Supabase istemcisini (client) başlatır veya
        mevcut olanı döndürür.
        """
        if cls._instance is None:
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_KEY")

            if not supabase_url or not supabase_key:
                cls._logger.error("SUPABASE_URL and SUPABASE_KEY are required")
                raise ValueError("SUPABASE_URL and SUPABASE_KEY are required")

            try:
                # 'acreate_client' (async create) kullanılıyor
                cls._instance = await acreate_client(supabase_url, supabase_key)
                cls._logger.info("Supabase client initialized (Async)")
            except Exception as e:
                cls._logger.error(f"Failed to initialize Supabase client: {e}")
                raise

        return cls._instance

    # ──────────────────────────────────────────────────────────
    # EVENT OPERATIONS (Tümü async/await kullanır)
    # ──────────────────────────────────────────────────────────

    @classmethod
    async def get_event_by_id(cls, event_id: str):
        """Event ID'ye göre bir etkinliği getirir."""
        try:
            client = await cls.get_client()
            response = await client.table("events").select("*").eq("event_id", event_id).single().execute()
            return response.data
        except Exception as e:
            # ❗ BU KISMI EKLEYİN ❗
            # .single() 0 satır bulduğunda bu hatayı verir.
            # Bu bir 'HATA' değil, 'bulunamadı' durumudur.
            if "PGRST116" in str(e) or "0 rows" in str(e):
                cls._logger.debug(f"Event not found in DB (PGRST116): {event_id}")
                return None  # Hata değil, 'yok' anlamına gelir
            
            cls._logger.error(f"Error getting event {event_id}: {e}")
            return None

    @classmethod
    async def insert_event(cls, event_data: dict):
        """Yeni bir etkinlik ekler."""
        try:
            client = await cls.get_client()
            response = await client.table("events").insert(event_data).execute()
            return response.data
        except Exception as e:
            cls._logger.error(f"Failed to insert event {event_data.get('event_id')}: {e}")
            return None

    @classmethod
    async def update_event(cls, event_id: str, event_data: dict):
        """Mevcut bir etkinliği günceller."""
        try:
            client = await cls.get_client()
            response = await client.table("events").update(event_data).eq("event_id", event_id).execute()
            return response.data
        except Exception as e:
            cls._logger.error(f"Failed to update event {event_id}: {e}")
            return None
