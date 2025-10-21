
class FightResult:

    @staticmethod
    def determine_fight_result(fight):
        color_map = {
            "text-blue-100": "draw",
            "text-neutral-100": "no_contest",
            "text-green-100": "win"
        }

        for color_class, result in color_map.items():
            text = fight.css(f"span.{color_class}.font-bold::text").get()
            if text:
                return result

        return "pending"

