import re

class RecordParser:

    @staticmethod
    def parse_record(record_string):
        """
        Parse record string like:
        - "28-1-0, 1 NC (Win-Loss-Draw)"
        - "3-4-0 (Win-Loss-Draw)"

        Returns dict with wins, losses, draws
        """
        if not record_string or record_string.strip() == "" or record_string == "N/A":
            return {
                "wins": 0,
                "losses": 0,
                "draws": 0
            }

        # Extract W-L-D from the first part (before comma or parenthesis)
        record_part = record_string.split(',')[0].strip()
        wld_match = re.match(r'(\d+)-(\d+)-(\d+)', record_part)

        if not wld_match:
            return {
                "wins": 0,
                "losses": 0,
                "draws": 0
            }

        return {
            "wins": int(wld_match.group(1)),
            "losses": int(wld_match.group(2)),
            "draws": int(wld_match.group(3))
        }
