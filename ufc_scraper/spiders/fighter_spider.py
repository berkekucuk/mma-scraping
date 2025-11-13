import scrapy
import json
import os
from scrapy.http import HtmlResponse
from ..items import FighterItem
from ..parsers.fighter_parser import FighterParser


class FighterSpider(scrapy.Spider):
    name = "fighter"
    allowed_domains = ["tapology.com"]

    def start_requests(self):
        fighters_path = "data/fighters.json"

        if not os.path.exists(fighters_path):
            self.logger.error(f"Fighters file not found: {fighters_path}")
            return

        with open(fighters_path, "r", encoding="utf-8") as f:
            fighters = json.load(f)

        self.logger.info(f"Loaded {len(fighters)} fighters from JSON")

        processed = 0
        skipped = 0

        for fighter in fighters:
            fighter_id = fighter.get("fighter_id")

            # Skip fighters that don't have fighter_id (incomplete records)
            if not fighter_id:
                skipped += 1
                continue

            html_path = f"data/fighter_profiles/{fighter_id}.html"

            if not os.path.exists(html_path):
                self.logger.warning(f"HTML file not found for fighter {fighter_id}: {html_path}")
                skipped += 1
                continue

            try:
                with open(html_path, "r", encoding="utf-8") as f:
                    html_content = f.read()

                response = HtmlResponse(
                    url=fighter.get("profile_url"),
                    body=html_content,
                    encoding='utf-8'
                )

                # Pass existing fighter data to merge with new parsed data
                yield from self.parse_fighter(response, fighter)
                processed += 1

            except Exception as e:
                self.logger.error(f"Error processing fighter {fighter_id}: {e}")
                skipped += 1

        self.logger.info(f"Processing complete: {processed} processed, {skipped} skipped")

    def parse_fighter(self, response, existing_fighter):
        try:
            # Parse new data from HTML
            parsed_data = FighterParser.parse_fighter(response)

            # Merge: Keep existing fighter_id, name, profile_url, image_url
            # Update with newly parsed data
            fighter_data = {
                "fighter_id": existing_fighter.get("fighter_id"),
                "name": existing_fighter.get("name"),
                "profile_url": existing_fighter.get("profile_url"),
                "image_url": existing_fighter.get("image_url"),
                **parsed_data  # Add all parsed fields (nickname, record, height, etc.)
            }

            yield FighterItem(**fighter_data)

        except Exception as e:
            self.logger.error(f"Error parsing fighter {existing_fighter.get('fighter_id')} ({existing_fighter.get('name')}): {e}")
