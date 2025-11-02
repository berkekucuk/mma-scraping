
class MethodUtil:
    @staticmethod
    def split_method(method_str: str) -> dict:
        if not method_str or not isinstance(method_str, str):
            return {"method_type": None, "method_detail": None}

        # İlk virgüle göre böl
        parts = [p.strip() for p in method_str.split(",", 1)]

        if len(parts) == 1:
            # Virgül yoksa tamamı method_type kabul edilir
            return {"method_type": parts[0], "method_detail": None}
        else:
            return {"method_type": parts[0], "method_detail": parts[1]}

