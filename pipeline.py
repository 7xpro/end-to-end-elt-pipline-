# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class ScraperPipeline:
    def process_item(self, item, spider):
        return item
# myproject/pipelines.py
import json

class SaveToJsonPipeline:
    def open_spider(self, spider):
        self.file = open('mobiles.json', 'a')  # append mode

    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        return item

    def close_spider(self, spider):
        self.file.close()
        
        
import json
import boto3
from datetime import datetime

class S3JsonExportPipeline:
    def __init__(self):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(dict(item))  # Collect each scraped item
        return item

    def close_spider(self, spider):
        # Convert items to newline-delimited JSON (JSON Lines format)
        json_data = '\n'.join(json.dumps(item) for item in self.items)

        # Prepare file path with date-based partitioning (e.g. input/2025/07/02/raw_data.json)
        now = datetime.now()
        s3_key = f"input/{now.strftime('year=%Y/month=%m/day=%d')}/raw_data.json"

        # Upload to S3
        s3 = boto3.client('s3')
        if json_data:
            s3.put_object(
                Bucket='mobiles-clean-data',
                Key=s3_key,
                Body=json_data,
                ContentType='application/json'
            )
