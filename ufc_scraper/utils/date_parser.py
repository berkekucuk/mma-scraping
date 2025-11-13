from datetime import datetime


class DateParser:

    @staticmethod
    def parse_date_to_iso(date_string):
        """
        Parse date string to ISO format (YYYY-MM-DD)

        Supported formats:
        - "1987 Jul 19" -> "1987-07-19"
        - "1990 January 15" -> "1990-01-15"
        - "N/A" -> None
        - "" -> None

        Returns:
            str: ISO formatted date (YYYY-MM-DD) or None
        """
        if not date_string or date_string.strip() == "" or date_string.upper() == "N/A":
            return None

        # Clean the string
        date_string = date_string.strip()

        # Try different date formats
        date_formats = [
            '%Y %b %d',  # 1987 Jul 19
            '%Y %B %d',  # 1987 July 19
            '%Y-%m-%d',  # 1987-07-19 (already ISO)
            '%d %b %Y',  # 19 Jul 1987
            '%d %B %Y',  # 19 July 1987
        ]

        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_string, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue

        # If no format matched, return None
        return None
