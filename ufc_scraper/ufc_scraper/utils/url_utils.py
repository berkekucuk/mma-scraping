
class URLUtils:

    @staticmethod
    def extract_event_id(url):
        try:
            event_part = url.split("/events/")[1]
            first_part = event_part.split("-")[0]

            if first_part.isdigit():
                return first_part
            else:
                return event_part
                
        except (IndexError, AttributeError):
            return None

    @staticmethod
    def extract_fighter_id(url):
        try:
            fighter_part = url.split("/fighters/")[1]
            first_part = fighter_part.split("-")[0]
            
            if first_part.isdigit():
                return first_part
            else:
                return fighter_part
                
        except (IndexError, AttributeError):
            return None

    @staticmethod
    def extract_fight_id(url):
        try:
            return url.split("/bouts/")[1].split("-")[0]
        except (IndexError, AttributeError):
            return None
