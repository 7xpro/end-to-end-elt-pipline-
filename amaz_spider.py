import scrapy
import datetime
from scraper.items import AmazItem
import os
import boto3

class AmazSpider(scrapy.Spider):
    name = "amaz"
    allowed_domains = ["www.amazon.in"]
    start_urls = [
        "https://www.amazon.in/mobile-phones/b/?ie=UTF8&node=1389401031&ref_=nav_cs_mobiles"
    ]
    
    

    def start_requests(self):
      
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, meta={'dont_merge_cookies': True})      
    
    def parse(self, response):
     
        category_links = response.css('ul[aria-labelledby="p_123-title"] li a::attr(href)').getall()
        category_name=response.css('ul[aria-labelledby="p_123-title"] li a span.a-size-base::text').getall()
        # ]
        target_categories = ['Apple','OnePlus','realme','Redmi','Nokia','VIVO','Xiaomi']
        for name ,link in zip(category_name,category_links):
            if name in  target_categories:
                full_link = response.urljoin(link)
                yield scrapy.Request(full_link, callback=self.parse_category, meta={'category_name':name,'dont_merge_cookies': True})
            
    def parse_category(self, response):
       
        products=response.css('div.a-section div.s-product-image-container span.rush-component a.a-link-normal::attr(href)').getall()
        for product in products:
            product_link='https://www.amazon.in'+ product
            yield scrapy.Request(product_link,callback=self.product_data,meta={'dont_merge_cookies':True})
        next_page= response.css('a.s-pagination-item ::attr(href)').get()
        if next_page is not None:
            next_page_link=response.urljoin(next_page)
            yield response.follow(next_page_link,callback=self.parse_category,meta={'dont_merge_cookies':True})
     
    if os.path.exists('mobiles.json'):
        os.remove('mobiles.json')
    def product_data(self, response):
        
        item = AmazItem()
        if response.status == 200:
            name = response.css('div.celwidget div.a-section h1.a-size-large  span.a-size-large::text').get()
            if name:
                item['name']=name.strip()
            else:
                item['name']=None    
                
                
            item['brand'] = response.css('table.a-normal tr.po-brand td.s-span9 span::text').get()
            item['current_price'] = response.css('span.a-price-whole::text').get()
            item['original_price'] = response.css('span.a-text-price span::text').get()
            item['discount_percent'] = response.css('span.savingPriceOverride::text').get()
            item['availability'] = response.css('div#availability span::text').get()
            item['rating'] = response.css('span#acrCustomerReviewText::text').get()
            item['site_name'] = 'Amazon.in'
            item['date'] = datetime.datetime.now().strftime('%Y-%m-%d') 
            item['category'] = response.css('a[aria-current="page"].a-link-normal::text').get()
            
       
        yield item

     
        
