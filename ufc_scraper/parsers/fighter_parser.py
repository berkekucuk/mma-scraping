import os
import json

class FighterParser:

    @staticmethod
    def parse_fighter(response):
        header = response.css("div#fighterPageHeader")
        container = response.css("div#standardDetails")

        country_relative_url = header.css("img::attr(src)").get(default="").strip()
        country_url = response.urljoin(country_relative_url)
        country_code = FighterParser.extract_country_src(country_url)

        if country_code and country_url:
            FighterParser.save_country_to_json(country_code, country_url)

        fighter_data = {
            "nickname": FighterParser.extract_detail(container, "Nickname:"),
            "date_of_birth": FighterParser.extract_detail(container, "Date of Birth:"),
            "born": FighterParser.extract_detail(container, "Born:"),
            "fighting_out_of": FighterParser.extract_detail(container, "Fighting out of:"),
            "height": FighterParser.extract_detail(container, "Height:"),
            "weight_class_name": FighterParser.extract_detail(container, "Weight Class:"),
            "reach": FighterParser.extract_reach(container),
        }

        return fighter_data

    @staticmethod
    def extract_detail(container, label):
        return container.xpath(
            f'//strong[contains(text(), "{label}")]/following-sibling::span[1]/text()'
        ).get(default="").strip()

    @staticmethod
    def extract_reach(container):
        return (
            container.xpath('//strong[contains(text(), "Reach")]/ancestor::div/following-sibling::div[1]/span/text()')
            .get(default="")
            .strip()
        )

    @staticmethod
    def extract_country_src(country_src):

        if not country_src:
            return ""

        # Split by '/' and get the last part (filename)
        filename = country_src.split('/')[-1]

        # Split by '-' and get the first part (country code)
        country_code = filename.split('-')[0]

        return country_code

    @staticmethod
    def save_country_to_json(country_code, country_url, filepath="countries.json"):

        # Load existing data if file exists
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                countries = json.load(f)
        else:
            countries = {}

        # Add or update country data
        countries[country_code] = country_url

        # Save to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(countries, f, indent=2, ensure_ascii=False)
