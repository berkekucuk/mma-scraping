import re

class OddsUtil:
    @staticmethod
    def split_odds(odds_str: str) -> dict:
        """
        '-350 (Moderate Favorite)' → {'odds_value': -350, 'odds_label': 'Moderate Favorite'}
        '+390 (Moderate Underdog)' → {'odds_value': 390, 'odds_label': 'Moderate Underdog'}
        """
        if not odds_str or not isinstance(odds_str, str):
            return {"odds_value": None, "odds_label": None}

        odds_str = odds_str.strip()

        # Regex ile sayısal değer ve parantez içini yakala
        match = re.match(r'([+-]?\d+)\s*\(([^)]+)\)', odds_str)
        if match:
            value = int(match.group(1))
            label = match.group(2).strip()
            return {"odds_value": value, "odds_label": label}

        # Eğer parantez yoksa sadece sayıyı döndür
        try:
            value = int(odds_str)
            return {"odds_value": value, "odds_label": None}
        except ValueError:
            return {"odds_value": None, "odds_label": odds_str}

