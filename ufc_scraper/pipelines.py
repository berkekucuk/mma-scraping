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

