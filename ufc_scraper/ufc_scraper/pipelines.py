# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
from supabase import create_client, Client
from dotenv import load_dotenv

class UfcScraperPipeline:
    def process_item(self, item, spider):
        return item

class PrettyJsonPipeline:
    def __init__(self):
        self.file = None
        self.first_item = True
    
    def open_spider(self, spider):
        try:
            file_name = f'{spider.name}.json'
            self.file = open(file_name, 'w', encoding='utf-8')
            self.first_item = True
            self.file.write('[\n')  
            spider.logger.info(f"Pipeline dosyası açıldı: {file_name}")
        except Exception as e:
            spider.logger.error(f"Dosya açma hatası: {str(e)}")
            raise
    
    def close_spider(self, spider):
        try:
            if self.file:
                self.file.write('\n]\n')  
                self.file.close()
                spider.logger.info("Pipeline dosyası kapatıldı")
        except Exception as e:
            spider.logger.error(f"Dosya kapatma hatası: {str(e)}")
    
    def process_item(self, item, spider):
        try:
            if not self.file:
                spider.logger.error("Dosya açık değil")
                return item
            
            if not self.first_item:
                self.file.write(',\n')
            else:
                self.first_item = False
            
            # Item validation
            item_dict = dict(item)
            if not item_dict:
                spider.logger.warning(f"Boş item: {item}")
                return item
            
            json.dump(item_dict, self.file, indent=4, ensure_ascii=False)
            return item
            
        except Exception as e:
            spider.logger.error(f"Item işleme hatası: {str(e)}")
            return item


class SupabasePipeline:
    def __init__(self):
        load_dotenv()
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL ve SUPABASE_KEY environment variables gerekli")

        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        self.processed_events = set()
        self.processed_fighters = set()

    def open_spider(self, spider):
        spider.logger.info("✅ Supabase Pipeline başlatıldı")

    def close_spider(self, spider):
        spider.logger.info(f"✅ Supabase Pipeline tamamlandı. Toplam {len(self.processed_events)} event işlendi.")

    def process_item(self, item, spider):
        try:
            item_dict = dict(item)
            
            # Event item'ı kontrol et
            if 'event_id' in item_dict and 'fights' in item_dict:
                self._save_event_and_fights(item_dict, spider)
            else:
                spider.logger.warning(f"⚠️ Bilinmeyen item tipi: {item_dict.keys()}")
            
            return item
        except Exception as e:
            spider.logger.error(f"❌ Supabase kaydetme hatası: {str(e)}")
            return item

    def _save_event_and_fights(self, event_data, spider):
        try:
            event_id = event_data['event_id']
            
            # Event'i kaydet
            event_record = {
                'event_id': event_id,
                'event_type': event_data.get('event_type', ''),
                'event_name': event_data.get('event_name', ''),
                'date_time': event_data.get('date_time', ''),
                'venue': event_data.get('venue', ''),
                'location': event_data.get('location', '')
            }
            
            # Event'i upsert et
            result = self.supabase.table('events').upsert(event_record, on_conflict='event_id').execute()
            spider.logger.info(f"✅ Event kaydedildi: {event_data['event_name']}")
            
            # Fight'ları kaydet
            fights = event_data.get('fights', [])
            for fight in fights:
                self._save_fight(fight, event_id, spider)
                
            self.processed_events.add(event_id)
            
        except Exception as e:
            spider.logger.error(f"❌ Event kaydetme hatası: {str(e)}")

    def _save_fight(self, fight_data, event_id, spider):
        try:
            # Fighter'ları kaydet (sadece temel bilgiler)
            fighter1_id = fight_data.get('fighter1', {}).get('id')
            fighter1_name = fight_data.get('fighter1', {}).get('name', '')
            fighter2_id = fight_data.get('fighter2', {}).get('id')
            fighter2_name = fight_data.get('fighter2', {}).get('name', '')
            
            # Fighter1'i kaydet
            if fighter1_id and fighter1_name:
                self._save_fighter_basic(fighter1_id, fighter1_name, spider)
            
            # Fighter2'yi kaydet
            if fighter2_id and fighter2_name:
                self._save_fighter_basic(fighter2_id, fighter2_name, spider)
            
            # Fight'ı kaydet
            fight_record = {
                'fight_id': fight_data.get('fight_id'),
                'event_id': event_id,
                'fighter1_id': fighter1_id,
                'fighter2_id': fighter2_id,
                'winner_id': fight_data.get('winner_id'),
                'fight_result': fight_data.get('fight_result', ''),
                'method': fight_data.get('method', ''),
                'round_info': fight_data.get('round_info', ''),
                'weight_class': fight_data.get('weight_class', ''),
                'fighter1_age_at_fight': fight_data.get('fighter1', {}).get('age_at_fight', ''),
                'fighter2_age_at_fight': fight_data.get('fighter2', {}).get('age_at_fight', ''),
            }
            
            # Fight'ı upsert et
            result = self.supabase.table('fights').upsert(fight_record, on_conflict='fight_id').execute()
            spider.logger.info(f"✅ Fight kaydedildi: {fighter1_name} vs {fighter2_name}")
            
        except Exception as e:
            spider.logger.error(f"❌ Fight kaydetme hatası: {str(e)}")

    def _save_fighter_basic(self, fighter_id, fighter_name, spider):
        """Sadece temel fighter bilgilerini kaydeder"""
        try:
            fighter_record = {
                'fighter_id': fighter_id,
                'name': fighter_name
            }
            
            # Fighter'ı upsert et
            result = self.supabase.table('fighters').upsert(fighter_record, on_conflict='fighter_id').execute()
            spider.logger.debug(f"✅ Fighter kaydedildi: {fighter_name}")
            
        except Exception as e:
            spider.logger.error(f"❌ Fighter kaydetme hatası: {str(e)}")


