from dateutil import parser
from zoneinfo import ZoneInfo

class DateTimeUtils:

    @staticmethod
    def parse_tapology_datetime(date_time_str):
        try:
            eastern = ZoneInfo("America/New_York")
            dt = parser.parse(date_time_str, fuzzy=True)

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=eastern)
            else:
                dt = dt.astimezone(eastern)

            dt_utc = dt.astimezone(ZoneInfo("UTC"))
            return dt_utc.strftime("%Y-%m-%d %H:%M:%S%z")
            
        except Exception as e:
            return f"Error parsing datetime: {e}"
