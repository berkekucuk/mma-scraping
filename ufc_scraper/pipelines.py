import json
import logging
from pathlib import Path
from typing import Dict, Optional, Any


class BasePipeline:
    """Base pipeline for handling item updates with ID-based deduplication."""

    item_class_name: str = None
    id_field: str = None
    file_name: str = None

    def __init__(self):
        if not self.item_class_name or not self.id_field or not self.file_name:
            raise NotImplementedError("Subclasses must define item_class_name, id_field, and file_name")

        self.output_dir = Path("data")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.file_path = self.output_dir / self.file_name
        self.data: Dict[str, Dict[str, Any]] = {}

    def get_item_key(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract the unique key from an item. Override for composite keys."""
        return item.get(self.id_field)

    def open_spider(self, spider):
        """Load existing data from file."""
        logging.info(f"[{spider.name}] {self.__class__.__name__} initialized for {self.item_class_name}")

        if self.file_path.exists():
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                    # Build ID-based dictionary
                    self.data = {self.get_item_key(item): item for item in existing_data if self.get_item_key(item)}
                logging.info(f"[{spider.name}] Loaded {len(self.data)} existing {self.item_class_name} records")
            except json.JSONDecodeError as e:
                logging.error(f"[{spider.name}] Failed to load {self.file_path}: {e}")
                self.data = {}
        else:
            logging.info(f"[{spider.name}] No existing data file found for {self.item_class_name}")

    def process_item(self, item, spider):
        """Process item only if it matches this pipeline's item type."""
        item_type = type(item).__name__

        if item_type != self.item_class_name:
            return item

        key = self.get_item_key(dict(item))

        if key is None:
            logging.warning(f"[{spider.name}] {item_type} has no valid key, skipping: {dict(item)}")
            return item

        # Update or add item
        if key in self.data:
            existing = self.data[key]
            # Update only non-None fields
            for field, value in dict(item).items():
                if value is not None:
                    existing[field] = value
            self.data[key] = existing
            logging.debug(f"[{spider.name}] {item_type} updated: {key}")
        else:
            self.data[key] = dict(item)
            logging.info(f"[{spider.name}] {item_type} added: {key}")

        return item

    def close_spider(self, spider):
        """Save data to file."""
        items_list = list(self.data.values())

        with open(self.file_path, "w", encoding="utf-8") as f:
            json.dump(items_list, f, indent=2, ensure_ascii=False)

        logging.info(f"[{spider.name}] ✅ {self.file_path.name} saved successfully ({len(items_list)} records)")


class EventPipeline(BasePipeline):
    """Pipeline for EventItem."""
    item_class_name = "EventItem"
    id_field = "event_id"
    file_name = "events.json"


class FightPipeline(BasePipeline):
    """Pipeline for FightItem."""
    item_class_name = "FightItem"
    id_field = "fight_id"
    file_name = "fights.json"


class FighterPipeline(BasePipeline):
    """Pipeline for FighterItem."""
    item_class_name = "FighterItem"
    id_field = "fighter_id"
    file_name = "fighters.json"


class FightParticipationPipeline(BasePipeline):
    """Pipeline for FightParticipationItem with composite key."""
    item_class_name = "FightParticipationItem"
    id_field = "composite"  # Composite key indicator
    file_name = "participations.json"

    def get_item_key(self, item: Dict[str, Any]) -> Optional[str]:
        """Generate composite key from fight_id and fighter_id."""
        fight_id = item.get("fight_id")
        fighter_id = item.get("fighter_id")

        if fight_id and fighter_id:
            return f"{fight_id}-{fighter_id}"
        return None
