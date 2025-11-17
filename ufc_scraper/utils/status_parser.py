class StatusParser:

    @staticmethod
    def parse(status_string: str) -> str | None:

        if not status_string:
            return None

        if not status_string:
            return None

        if status_string.startswith("In Progress"):
            return "Live"

        return status_string.split()[0]
