import json
import os

class FighterCacheManager:
    
    CACHE_FILE = "fighter_cache.json"
    
    @staticmethod
    def load_fighter_cache():
        if os.path.exists(FighterCacheManager.CACHE_FILE):
            try:
                with open(FighterCacheManager.CACHE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return {}
        return {}
    
    @staticmethod
    def save_fighter_url(fighter_id, fighter_url, image_url, fighter_name):
        fighter_cache_data = FighterCacheManager.load_fighter_cache()
        
        if fighter_id not in fighter_cache_data:
            fighter_cache_data[fighter_id] = {
                'url': fighter_url,
                'image_url': image_url,
                'name': fighter_name
            }
            
            with open(FighterCacheManager.CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(fighter_cache_data, f, indent=2, ensure_ascii=False)
            
            return True  
        return False  