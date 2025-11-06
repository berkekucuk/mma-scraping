class UrlParser:

    @staticmethod
    def extract_event_id(url):
        try:
            event_part = url.split("/events/")[1]
            first_part = event_part.split("-")[0]
            event_id = first_part if first_part.isdigit() else event_part
            return event_id.strip().lower()

        except (IndexError, AttributeError):
            return None

    @staticmethod
    def extract_fighter_id(url):
        try:
            fighter_part = url.split("/fighters/")[1]
            first_part = fighter_part.split("-")[0]
            fighter_id = first_part if first_part.isdigit() else fighter_part
            return fighter_id.strip().lower()

        except (IndexError, AttributeError):
            return None

    @staticmethod
    def extract_fight_id(url):
        try:
            fight_part = url.split("/bouts/")[1]
            first_part = fight_part.split("-")[0]
            fight_id = first_part if first_part.isdigit() else fight_part
            return fight_id.strip().lower()

        except (IndexError, AttributeError):
            return None
