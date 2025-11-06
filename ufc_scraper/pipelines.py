import json
import logging
from pathlib import Path
import os
from supabase import create_client, Client


class JsonPipeline:
    """Unified pipeline for handling all item types with JSON output."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_dir = Path("data")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Separate data stores for each item type
        self.events = {}
        self.fights = {}
        self.fighters = {}
        self.participations = {}

        # File paths
        self.events_file = self.output_dir / "events.json"
        self.fights_file = self.output_dir / "fights.json"
        self.fighters_file = self.output_dir / "fighters.json"
        self.participations_file = self.output_dir / "participations.json"

    def open_spider(self, spider):
        """Load existing data from all JSON files."""
        self.logger.info(f"[{spider.name}] JsonPipeline initialized")

        # Load events
        if self.events_file.exists():
            try:
                with open(self.events_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.events = {item['event_id']: item for item in data if item.get('event_id')}
                self.logger.info(f"[{spider.name}] Loaded {len(self.events)} events")
            except json.JSONDecodeError as e:
                self.logger.error(f"[{spider.name}] Failed to load events: {e}")

        # Load fights
        if self.fights_file.exists():
            try:
                with open(self.fights_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.fights = {item['fight_id']: item for item in data if item.get('fight_id')}
                self.logger.info(f"[{spider.name}] Loaded {len(self.fights)} fights")
            except json.JSONDecodeError as e:
                self.logger.error(f"[{spider.name}] Failed to load fights: {e}")

        # Load fighters
        if self.fighters_file.exists():
            try:
                with open(self.fighters_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.fighters = {item['fighter_id']: item for item in data if item.get('fighter_id')}
                self.logger.info(f"[{spider.name}] Loaded {len(self.fighters)} fighters")
            except json.JSONDecodeError as e:
                self.logger.error(f"[{spider.name}] Failed to load fighters: {e}")

        # Load participations
        if self.participations_file.exists():
            try:
                with open(self.participations_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.participations = {
                        f"{item['fight_id']}-{item['fighter_id']}": item
                        for item in data
                        if item.get('fight_id') and item.get('fighter_id')
                    }
                self.logger.info(f"[{spider.name}] Loaded {len(self.participations)} participations")
            except json.JSONDecodeError as e:
                self.logger.error(f"[{spider.name}] Failed to load participations: {e}")

    def process_item(self, item, spider):
        """Process items based on their type."""
        item_type = type(item).__name__
        item_dict = dict(item)

        if item_type == "EventItem":
            event_id = item_dict.get('event_id')
            if event_id:
                if event_id in self.events:
                    # Update existing event with non-None values
                    for field, value in item_dict.items():
                        if value is not None:
                            self.events[event_id][field] = value
                    self.logger.debug(f"[{spider.name}] Event updated: {event_id}")
                else:
                    self.events[event_id] = item_dict
                    self.logger.info(f"[{spider.name}] Event added: {event_id}")

        elif item_type == "FightItem":
            fight_id = item_dict.get('fight_id')
            if fight_id:
                if fight_id in self.fights:
                    for field, value in item_dict.items():
                        if value is not None:
                            self.fights[fight_id][field] = value
                    self.logger.debug(f"[{spider.name}] Fight updated: {fight_id}")
                else:
                    self.fights[fight_id] = item_dict
                    self.logger.info(f"[{spider.name}] Fight added: {fight_id}")

        elif item_type == "FighterItem":
            fighter_id = item_dict.get('fighter_id')
            if fighter_id:
                if fighter_id in self.fighters:
                    for field, value in item_dict.items():
                        if value is not None:
                            self.fighters[fighter_id][field] = value
                    self.logger.debug(f"[{spider.name}] Fighter updated: {fighter_id}")
                else:
                    self.fighters[fighter_id] = item_dict
                    self.logger.info(f"[{spider.name}] Fighter added: {fighter_id}")

        elif item_type == "FightParticipationItem":
            fight_id = item_dict.get('fight_id')
            fighter_id = item_dict.get('fighter_id')
            if fight_id and fighter_id:
                key = f"{fight_id}-{fighter_id}"
                if key in self.participations:
                    for field, value in item_dict.items():
                        if value is not None:
                            self.participations[key][field] = value
                    self.logger.debug(f"[{spider.name}] Participation updated: {key}")
                else:
                    self.participations[key] = item_dict
                    self.logger.info(f"[{spider.name}] Participation added: {key}")

        return item

    def close_spider(self, spider):
        """Save all data to JSON files."""
        # Save events
        with open(self.events_file, "w", encoding="utf-8") as f:
            json.dump(list(self.events.values()), f, indent=2, ensure_ascii=False)
        self.logger.info(f"[{spider.name}] ✅ events.json saved ({len(self.events)} records)")

        # Save fights
        with open(self.fights_file, "w", encoding="utf-8") as f:
            json.dump(list(self.fights.values()), f, indent=2, ensure_ascii=False)
        self.logger.info(f"[{spider.name}] ✅ fights.json saved ({len(self.fights)} records)")

        # Save fighters
        with open(self.fighters_file, "w", encoding="utf-8") as f:
            json.dump(list(self.fighters.values()), f, indent=2, ensure_ascii=False)
        self.logger.info(f"[{spider.name}] ✅ fighters.json saved ({len(self.fighters)} records)")

        # Save participations
        with open(self.participations_file, "w", encoding="utf-8") as f:
            json.dump(list(self.participations.values()), f, indent=2, ensure_ascii=False)
        self.logger.info(f"[{spider.name}] ✅ participations.json saved ({len(self.participations)} records)")



class SupabasePipeline:
    """Pipeline for uploading all scraped data to Supabase."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        # Initialize Supabase client
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            self.logger.warning("Supabase credentials not found. Pipeline will be disabled.")
            self.supabase = None
        else:
            try:
                self.supabase: Client = create_client(supabase_url, supabase_key)
                self.logger.info("Supabase client initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize Supabase client: {e}")
                self.supabase = None

        # Separate collections for each item type
        self.events_to_upload = []
        self.fights_to_upload = []
        self.fighters_to_upload = []
        self.participations_to_upload = []

    def process_item(self, item, spider):
        """Collect items for batch upload based on type."""
        item_type = type(item).__name__
        item_dict = dict(item)

        if item_type == "EventItem":
            self.events_to_upload.append(item_dict)
        elif item_type == "FightItem":
            self.fights_to_upload.append(item_dict)
        elif item_type == "FighterItem":
            self.fighters_to_upload.append(item_dict)
        elif item_type == "FightParticipationItem":
            self.participations_to_upload.append(item_dict)

        return item

    def close_spider(self, spider):
        """Upload all collected items to Supabase."""
        if not self.supabase:
            self.logger.warning("Supabase client not available. Skipping upload.")
            return

        # Upload events
        self._upload_events()

        # Upload fighters
        self._upload_fighters()

        # Upload fights
        self._upload_fights()

        # Upload participations (must be last due to foreign key constraints)
        self._upload_participations()

    def _upload_events(self):
        """Upload events to Supabase."""
        if not self.events_to_upload:
            self.logger.info("No events to upload.")
            return

        successful = 0
        failed = 0
        self.logger.info(f"Uploading {len(self.events_to_upload)} events...")

        for item in self.events_to_upload:
            try:
                record = {
                    'event_id': item['event_id'],
                    'event_url': item['event_url'],
                    'status': item.get('status'),
                    'name': item['name'],
                    'datetime_utc': item['datetime_utc'],
                    'venue': item.get('venue'),
                    'location': item.get('location')
                }

                self.supabase.table('events').upsert(
                    record,
                    on_conflict='event_id'
                ).execute()

                successful += 1
                self.logger.debug(f"✓ Event uploaded: {item['event_id']}")

            except Exception as e:
                failed += 1
                self.logger.error(f"✗ Failed to upload event {item['event_id']}: {str(e)}")

        self.logger.info(f"Events upload completed: {successful} successful, {failed} failed")

    def _upload_fighters(self):
        """Upload fighters to Supabase."""
        if not self.fighters_to_upload:
            self.logger.info("No fighters to upload.")
            return

        successful = 0
        failed = 0
        self.logger.info(f"Uploading {len(self.fighters_to_upload)} fighters...")

        for item in self.fighters_to_upload:
            try:
                record = {
                    'fighter_id': item['fighter_id'],
                    'name': item['name'],
                    'profile_url': item.get('profile_url'),
                    'image_url': item.get('image_url')
                }

                self.supabase.table('fighters').upsert(
                    record,
                    on_conflict='fighter_id'
                ).execute()

                successful += 1
                self.logger.debug(f"✓ Fighter uploaded: {item['fighter_id']}")

            except Exception as e:
                failed += 1
                self.logger.error(f"✗ Failed to upload fighter {item['fighter_id']}: {str(e)}")

        self.logger.info(f"Fighters upload completed: {successful} successful, {failed} failed")

    def _upload_fights(self):
        """Upload fights to Supabase."""
        if not self.fights_to_upload:
            self.logger.info("No fights to upload.")
            return

        successful = 0
        failed = 0
        self.logger.info(f"Uploading {len(self.fights_to_upload)} fights...")

        for item in self.fights_to_upload:
            try:
                record = {
                    'fight_id': item['fight_id'],
                    'event_id': item['event_id'],
                    'method_type': item.get('method_type'),
                    'method_detail': item.get('method_detail'),
                    'round_summary': item.get('round_summary'),
                    'bout_type': item.get('bout_type'),
                    'weight_class_lbs': item.get('weight_class_lbs'),
                    'rounds_format': item.get('rounds_format'),
                    'fight_order': item.get('fight_order')
                }

                self.supabase.table('fights').upsert(
                    record,
                    on_conflict='fight_id'
                ).execute()

                successful += 1
                self.logger.debug(f"✓ Fight uploaded: {item['fight_id']}")

            except Exception as e:
                failed += 1
                self.logger.error(f"✗ Failed to upload fight {item['fight_id']}: {str(e)}")

        self.logger.info(f"Fights upload completed: {successful} successful, {failed} failed")

    def _upload_participations(self):
        """Upload participations to Supabase."""
        if not self.participations_to_upload:
            self.logger.info("No participations to upload.")
            return

        successful = 0
        failed = 0
        self.logger.info(f"Uploading {len(self.participations_to_upload)} participations...")

        for item in self.participations_to_upload:
            try:
                record = {
                    'fight_id': item['fight_id'],
                    'fighter_id': item['fighter_id'],
                    'odds_value': item.get('odds_value'),
                    'odds_label': item.get('odds_label'),
                    'age_at_fight': item.get('age_at_fight'),
                    'result': item.get('result')
                }

                # Add record_after_fight fields if available
                if item.get('record_after_fight'):
                    record['record_wins'] = item['record_after_fight'].get('wins')
                    record['record_losses'] = item['record_after_fight'].get('losses')
                    record['record_draws'] = item['record_after_fight'].get('draws')
                else:
                    record['record_wins'] = None
                    record['record_losses'] = None
                    record['record_draws'] = None

                self.supabase.table('participants').upsert(
                    record,
                    on_conflict='fight_id,fighter_id'
                ).execute()

                successful += 1
                self.logger.debug(f"✓ Participation uploaded: {item['fight_id']} - {item['fighter_id']}")

            except Exception as e:
                failed += 1
                self.logger.error(f"✗ Failed to upload participation {item['fight_id']} - {item['fighter_id']}: {str(e)}")

        self.logger.info(f"Participations upload completed: {successful} successful, {failed} failed")
