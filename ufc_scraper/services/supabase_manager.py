import os
from supabase import create_client, Client
from dotenv import load_dotenv
import logging

load_dotenv()

class SupabaseManager:
    """Singleton Supabase client for the entire application."""

    _instance: Client = None
    _logger = logging.getLogger(__name__)

    @classmethod
    def get_client(cls) -> Client:
        if cls._instance is None:
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_KEY")

            if not supabase_url or not supabase_key:
                raise ValueError("SUPABASE_URL and SUPABASE_KEY are required")

            try:
                cls._instance = create_client(supabase_url, supabase_key)
                cls._logger.info("Supabase client initialized")
            except Exception as e:
                cls._logger.error(f"Failed to initialize Supabase client: {e}")
                raise

        return cls._instance

    # ──────────────────────────────────────────────────────────
    # EVENT OPERATIONS
    # ──────────────────────────────────────────────────────────

    @classmethod
    def get_event_by_id(cls, event_id: str):
        """Fetch event by primary key."""
        try:
            response = cls.get_client().table("events")\
                .select("*")\
                .eq("event_id", event_id)\
                .single()\
                .execute()
            return response.data
        except Exception:
            return None

    @classmethod
    def insert_event(cls, event_data: dict):
        """Insert a new event."""
        try:
            response = cls.get_client().table("events")\
                .insert(event_data)\
                .execute()
            return response.data
        except Exception as e:
            cls._logger.error(f"Failed to insert event {event_data.get('event_id')}: {e}")
            return None

    @classmethod
    def update_event(cls, event_id: str, event_data: dict):
        """Update an existing event."""
        try:
            response = cls.get_client().table("events")\
                .update(event_data)\
                .eq("event_id", event_id)\
                .execute()
            return response.data
        except Exception as e:
            cls._logger.error(f"Failed to update event {event_id}: {e}")
            return None
