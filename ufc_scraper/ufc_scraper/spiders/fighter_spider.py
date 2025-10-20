import scrapy
from ..services.html_cache_manager import HtmlCacheManager
from ..services.fighter_cache_manager import FighterCacheManager
from ..parsers.fighter_parser import FighterParser
from ..items import FighterItem

class FighterSpider(scrapy.Spider):
    name = "fighter"
    allowed_domains = ["tapology.com"]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Cache'i bir kez yükle, her request'te tekrar yüklemeyi önle
        self.fighter_cache = FighterCacheManager.load_fighter_cache()
        self.logger.info(f"Loaded {len(self.fighter_cache)} fighters from cache")
    
    def start_requests(self):
        """Cache dosyasından fighter URL'lerini okuyup request'leri oluşturur"""
        if not self.fighter_cache:
            self.logger.error("No fighters found in cache. Run ufc_events spider first!")
            return
        
        for fighter_id, cached_fighter_info in self.fighter_cache.items():  
            fighter_url = cached_fighter_info.get('url')  
            if not fighter_url:
                self.logger.warning(f"No URL found for fighter {fighter_id}")
                continue
                
            yield scrapy.Request(
                url=fighter_url,
                callback=self.parse_fighter,
                meta={'fighter_id': fighter_id},
                errback=self.handle_error,
                dont_filter=True
            )
    
    def parse_fighter(self, response):
        """Fighter sayfasını parse eder"""
        try:
            parsed_fighter_info = FighterParser.parse_fighter(response)  
            
            if not parsed_fighter_info.get('name'):  
                self.logger.warning(f"Fighter bilgisi bulunamadı: {response.url}")
                return
            
            fighter_item = FighterItem()
            fighter_id = response.meta.get('fighter_id')
            
            # Cache'den image_url bilgisini al
            cached_fighter_info = self.fighter_cache.get(fighter_id, {})  
            image_url = cached_fighter_info.get('image_url', '')  
            
            # Item'ı doldur
            fighter_item['fighter_id'] = fighter_id
            fighter_item['image_url'] = image_url
            fighter_item.update(parsed_fighter_info)  
            
            self.logger.info(f"Successfully parsed fighter: {parsed_fighter_info.get('name')}")  
            yield fighter_item
            
        except Exception as e:
            self.logger.error(f"Error parsing fighter {response.url}: {str(e)}")
    
    def handle_error(self, failure):
        """Request hatalarını handle eder"""
        self.logger.error(f"Request failed for {failure.request.url}: {failure.value}")