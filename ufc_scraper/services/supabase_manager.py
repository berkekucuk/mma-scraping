import os
import logging
from supabase import AsyncClient
from dotenv import load_dotenv

load_dotenv()

class SupabaseManager:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        url = os.getenv("SUPABASE_PROD_URL")
        key = os.getenv("SUPABASE_PROD_KEY")

        if not all([url, key]):
            self.logger.error("Supabase credentials are missing in .env")
            raise ValueError("Missing Supabase credentials")

        try:
            self.client = AsyncClient(url, key)
            self.logger.info("Supabase client initialized (Async)")
        except Exception as e:
            self.logger.error(f"Failed to initialize Supabase client: {e}")
            raise e


    async def bulk_upsert(self, table_name: str, data: list, ignore_duplicates=False, on_conflict=None):
        if not data:
            return None

        try:
            response = await self.client.table(table_name).upsert(
                data,
                ignore_duplicates=ignore_duplicates,
                on_conflict=on_conflict
            ).execute()

            self.logger.debug(f"[{table_name.upper()}] Successfully upserted {len(data)} rows.")
            return response

        except Exception as e:
            self.logger.error(f"Error in bulk upsert for table '{table_name}': {e}")
            raise e


    async def get_events_by_ids(self, event_ids: list):
        if not event_ids:
            return {}

        try:
            response = await self.client.table("events")\
                .select("*")\
                .in_("event_id", event_ids)\
                .execute()

            events_dict = {event["event_id"]: event for event in response.data}

            self.logger.info(f"Fetched {len(events_dict)} events from {len(event_ids)} requested IDs")
            return events_dict

        except Exception as e:
            self.logger.error(f"Failed to get events: {e}")
            raise e


    async def get_live_event(self):
        try:
            response = await self.client.table("events")\
                .select("event_id, event_url")\
                .eq("status", "live")\
                .limit(1)\
                .execute()

            if response.data:
                return response.data[0]
            else:
                self.logger.info("No live event found.")
                return None

        except Exception as e:
            self.logger.error(f"Failed to get LIVE event: {e}")
            return None


    async def load_fighter_cache(self):
        fighter_cache = {}
        batch_size = 1000
        offset = 0

        try:
            self.logger.info("Loading fighter cache...")

            while True:
                response = await self.client.table('fighters')\
                    .select('fighter_id, name')\
                    .range(offset, offset + batch_size - 1)\
                    .execute()

                if not response.data:
                    break

                for f in response.data:
                    if f.get('name'):
                        fighter_cache[f['name'].strip()] = f['fighter_id']

                if len(response.data) < batch_size:
                    break

                offset += batch_size

            self.logger.info(f"Successfully loaded {len(fighter_cache)} fighters into cache.")
            return fighter_cache

        except Exception as e:
            self.logger.error(f"Failed to load fighter cache: {e}")
            return {}
