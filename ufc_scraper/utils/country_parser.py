import os
import json
import threading


class CountryParser:
    _lock = threading.Lock()

    @staticmethod
    def extract_country_src(country_flag_url):
        if not country_flag_url:
            return None

        # Split by '/' and get the last part (filename)
        filename = country_flag_url.split('/')[-1]

        # Split by '-' and get the first part (country code)
        country_code = filename.split('-')[0]

        return country_code

    @staticmethod
    def save_country_to_json(country_code, country_flag_url, filepath="data/countries.json"):
        try:
            with CountryParser._lock:  # FighterParser yerine CountryParser
                # Ensure directory exists
                os.makedirs(os.path.dirname(filepath), exist_ok=True)

                # Load existing data if file exists
                if os.path.exists(filepath):
                    with open(filepath, 'r', encoding='utf-8') as f:
                        countries = json.load(f)
                else:
                    countries = {}

                # Only save if country doesn't exist or URL is different
                if country_code not in countries or countries[country_code] != country_flag_url:
                    countries[country_code] = country_flag_url

                    # Save to file
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(countries, f, indent=2, ensure_ascii=False)

        except (json.JSONDecodeError, IOError) as e:
            print(f"Error saving country data for {country_code}: {e}")
