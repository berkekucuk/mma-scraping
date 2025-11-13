class StatusParser:

    @staticmethod
    def parse(status_string: str) -> str:

        if not status_string:
            return None

        # Trim
        clean = status_string.strip()

        if not clean:
            return None

        # İlk kelimeyi böl
        return clean.split()[0]
