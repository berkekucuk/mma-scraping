import re


class FighterAgeUtil:

    @staticmethod
    def parse_fighter_age(age_at_fight_str):

        if not age_at_fight_str or not isinstance(age_at_fight_str, str):
            return None

        # Clean the input string
        age_str = age_at_fight_str.strip()

        # Handle empty or invalid strings
        if not age_str or age_str.lower() in ["", "n/a", "unknown", "tbd"]:
            return None

        try:
            # Extract years using regex
            years_match = re.search(r"(\d+)\s*years?", age_str)

            if years_match:
                years = int(years_match.group(1))

                # Validate age range (reasonable fighter age)
                if 0 <= years <= 100:
                    return years
                else:
                    return None
            else:
                # If no years found, return None
                return None

        except (ValueError, AttributeError):
            return None
