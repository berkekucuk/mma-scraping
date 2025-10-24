# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import logging 

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
        
        # Sadece bu pipeline çalıştığı sürece event'leri tekrar işlememek için set
        self.processed_events = set()
        logging.info("✅ Supabase Pipeline başlatıldı")

    def open_spider(self, spider):
        spider.logger.info("🕷️ Spider açıldı.")

    def close_spider(self, spider):
        spider.logger.info(f"✅ Supabase Pipeline tamamlandı. Bu çalışmada {len(self.processed_events)} event işlendi.")

    def process_item(self, item, spider):
        try:
            item_dict = dict(item)
            
            # Sadece 'event_id' ve 'fights' içeren ana item'ı işle
            if 'event_id' in item_dict and 'fights' in item_dict:
                tapology_event_id = item_dict['event_id']
                
                # Eğer bu çalıştırmada zaten işlendiyse atla
                if tapology_event_id in self.processed_events:
                    spider.logger.debug(f"⏭️ Event bu çalışmada zaten işlendi: {tapology_event_id}")
                    return item
                
                # İşleniyor olarak işaretle
                self.processed_events.add(tapology_event_id)
                self._process_normalized_event(item_dict, spider)
            else:
                spider.logger.warning(f"⚠️ Bilinmeyen item tipi: {item_dict.keys()}")
            
            return item
        except Exception as e:
            spider.logger.error(f"❌ 'process_item' içinde ana hata: {str(e)}", exc_info=True)
            return item

    def _safe_int(self, value):
        """Gelen değeri güvenli bir şekilde integer'a çevirir, değilse None döner."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _get_or_create_id_for_lookup(self, spider, table_name: str, pk_column: str, name_value: str):
        """
        Arama tabloları (methods, results, vs.) için kullanılır.
        Verilen 'name' değerini 'upsert' eder ve tablonun 'SERIAL' ID'sini döner.
        """
        if not name_value:
            return None
            
        try:
            record = {'name': name_value}
            # 'name' sütununa göre 'upsert' et, 'name' UNIQUE olmalı.
            data = self.supabase.table(table_name).upsert(record, on_conflict='name').execute().data
            
            if data:
                return data[0][pk_column]
            return None
        except Exception as e:
            spider.logger.error(f"❌ Arama tablosu hatası [{table_name}]: {str(e)}", exc_info=True)
            return None

    def _get_or_create_fighter(self, spider, fighter_data: dict):
        """
        Gelen 'fighter_data'yı 'tapology_fighter_id'ye göre 'upsert' eder.
        'fighters' tablosundaki 'SERIAL' (iç) 'fighter_id'yi döner.
        """
        if not fighter_data or 'id' not in fighter_data:
            return None
            
        tapology_id = fighter_data.get('id')
        if not tapology_id:
            return None

        try:
            fighter_record = {
                'tapology_fighter_id': tapology_id,
                'name': fighter_data.get('name', 'Bilinmeyen Dövüşçü'),
                'url': fighter_data.get('url'),
                'image_url': fighter_data.get('image_url')
            }
            # 'tapology_fighter_id'ye göre 'upsert' et.
            data = self.supabase.table('fighters').upsert(fighter_record, on_conflict='tapology_fighter_id').execute().data
            
            if data:
                return data[0]['fighter_id'] # Bu, SERIAL olan iç ID'dir
            return None
        except Exception as e:
            spider.logger.error(f"❌ Dövüşçü kaydetme hatası [{tapology_id}]: {str(e)}", exc_info=True)
            return None

    def _get_or_create_event(self, spider, event_item: dict, event_type_internal_id: int):
        """
        Gelen 'event_item'ı 'tapology_event_id'ye göre 'upsert' eder.
        'events' tablosundaki 'SERIAL' (iç) 'event_id'yi döner.
        """
        tapology_id = event_item.get('event_id')
        if not tapology_id:
            raise ValueError("Event ID (tapology_event_id) bulunamadı.")
            
        try:
            event_record = {
                'tapology_event_id': tapology_id,
                'event_name': event_item.get('event_name'),
                'date_time': event_item.get('date_time'), # Artık TIMESTAMPTZ uyumlu
                'venue': event_item.get('venue'),
                'location': event_item.get('location'),
                'event_type_id': event_type_internal_id # Arama tablosundan gelen İÇ ID
            }
            # 'tapology_event_id'ye göre 'upsert' et.
            data = self.supabase.table('events').upsert(event_record, on_conflict='tapology_event_id').execute().data
            
            if data:
                return data[0]['event_id'] # Bu, SERIAL olan iç ID'dir
            return None
        except Exception as e:
            spider.logger.error(f"❌ Event kaydetme hatası [{tapology_id}]: {str(e)}", exc_info=True)
            return None

    def _process_normalized_event(self, event_item, spider):
        """
        Ana ETL (Dönüşüm) fonksiyonu.
        Tüm metin ID'leri ve değerleri, İÇ 'SERIAL' ID'lere dönüştürür.
        """
        try:
            spider.logger.info(f"🔄 Event işleniyor: {event_item.get('event_name')}")
            
            # 1. Event'in İÇ ID'sini al/oluştur
            event_type_str = event_item.get('event_type')
            event_type_internal_id = self._get_or_create_id_for_lookup(
                spider, "event_types", "event_type_id", event_type_str
            )
            
            event_internal_id = self._get_or_create_event(spider, event_item, event_type_internal_id)
            if not event_internal_id:
                spider.logger.error(f"❌ Event için İÇ ID alınamadı: {event_item.get('event_id')}")
                return

            # 2. Event'e ait tüm dövüşleri işle
            fights = event_item.get('fights', [])
            for fight in fights:
                try:
                    # 3. Dövüşçülerin İÇ ID'lerini al/oluştur
                    fighter1_internal_id = self._get_or_create_fighter(spider, fight.get('fighter1', {}))
                    fighter2_internal_id = self._get_or_create_fighter(spider, fight.get('fighter2', {}))
                    
                    if not fighter1_internal_id or not fighter2_internal_id:
                        spider.logger.warning(f"⚠️ Dövüşçü ID'leri eksik, dövüş atlanıyor: {fight.get('fight_id')}")
                        continue
                        
                    # 4. Arama tablolarının İÇ ID'lerini al/oluştur
                    method_internal_id = self._get_or_create_id_for_lookup(
                        spider, "fight_methods", "method_id", fight.get('method')
                    )
                    result_internal_id = self._get_or_create_id_for_lookup(
                        spider, "fight_results", "result_id", fight.get('fight_result')
                    )
                    weight_class_internal_id = self._get_or_create_id_for_lookup(
                        spider, "weight_classes", "weight_class_id", fight.get('weight_class')
                    )
                    
                    # 5. Kazananın (Winner) İÇ ID'sini belirle
                    tapology_winner_id = fight.get('winner_id') # "daniel-cormier" veya None
                    winner_internal_id = None # Başlangıçta NULL
                    
                    if tapology_winner_id:
                        if tapology_winner_id == fight.get('fighter1', {}).get('id'):
                            winner_internal_id = fighter1_internal_id
                        elif tapology_winner_id == fight.get('fighter2', {}).get('id'):
                            winner_internal_id = fighter2_internal_id

                    # 6. Veri tiplerini temizle
                    fighter1_age = self._safe_int(fight.get('fighter1', {}).get('age_at_fight'))
                    fighter2_age = self._safe_int(fight.get('fighter2', {}).get('age_at_fight'))
                    
                    # 7. 'fights' tablosu için TAMAMEN DÖNÜŞTÜRÜLMÜŞ kaydı hazırla
                    fight_record = {
                        'tapology_fight_id': fight.get('fight_id'),
                        'event_id': event_internal_id,
                        'fighter1_id': fighter1_internal_id,
                        'fighter2_id': fighter2_internal_id,
                        'winner_id': winner_internal_id,
                        'weight_class_id': weight_class_internal_id,
                        'method_id': method_internal_id,
                        'result_id': result_internal_id,
                        'fighter1_age_at_fight': fighter1_age,
                        'fighter2_age_at_fight': fighter2_age,
                        'round_info_raw': fight.get('round_info')
                    }
                    
                    # 8. 'fights' tablosuna 'upsert' et
                    self.supabase.table('fights').upsert(fight_record, on_conflict='tapology_fight_id').execute()
                    
                except Exception as e:
                    spider.logger.error(f"❌ Dövüş kaydetme hatası [Tapology Fight ID: {fight.get('fight_id')}]: {str(e)}", exc_info=True)

        except Exception as e:
            spider.logger.error(f"❌ '_process_normalized_event' hatası: {str(e)}", exc_info=True)