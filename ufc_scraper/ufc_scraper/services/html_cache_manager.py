import os
from scrapy.http import HtmlResponse


class HtmlCacheManager:

    @staticmethod
    def load_from_cache(url):
        local_path = HtmlCacheManager.get_local_path(url)
        if os.path.exists(local_path):
            with open(local_path, "r", encoding="utf-8") as f:
                html = f.read()
            return HtmlResponse(url=url, body=html, encoding="utf-8")
        return None

    @staticmethod
    def get_local_path(url):

        if "/fighters" in url:
            base_dir = os.path.join("html_cache", "fighters_cache")
        else:
            base_dir = os.path.join("html_cache", "events_cache")

        filename = url.split("://")[-1].replace("/", "_").replace("?", "_").replace("=", "_")

        return os.path.join(base_dir, f"{filename}.html")

    @staticmethod
    def save_to_cache(url, response):
        local_path = HtmlCacheManager.get_local_path(url)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(response.text)
