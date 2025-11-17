class RecordParser:

    @staticmethod
    def parse_record(record_after_fight_str):

        parts = record_after_fight_str.split("-")

        wins = int(parts[0]) if len(parts) > 0 else 0
        losses = int(parts[1]) if len(parts) > 1 else 0
        draws = int(parts[2]) if len(parts) > 2 else 0

        return {"wins": wins, "losses": losses, "draws": draws}
