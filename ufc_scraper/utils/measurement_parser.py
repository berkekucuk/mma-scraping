import re

class MeasurementParser:

    @staticmethod
    def parse_measurement(measurement_string):
        """
        Parse measurement string and extract both imperial and metric values.

        Examples:
        - "5'0\" (152cm)" -> {"imperial": "5'0\"", "metric": 152}
        - "61.0\" (155cm)" -> {"imperial": "61.0\"", "metric": 155}
        - "N/A" -> {"imperial": None, "metric": None}

        Returns dict with imperial (str) and metric (int) values
        """
        if not measurement_string or measurement_string.strip() == "" or measurement_string == "N/A":
            return {
                "imperial": None,
                "metric": None
            }

        # Extract imperial (everything before opening parenthesis)
        imperial_match = re.search(r'^(.+?)\s*\(', measurement_string)
        imperial = imperial_match.group(1).strip() if imperial_match else measurement_string.strip()

        # Extract metric value (number inside parenthesis before 'cm')
        metric_match = re.search(r'\((\d+\.?\d*)cm\)', measurement_string)
        metric = int(float(metric_match.group(1))) if metric_match else None

        return {
            "imperial": imperial,
            "metric": metric
        }
