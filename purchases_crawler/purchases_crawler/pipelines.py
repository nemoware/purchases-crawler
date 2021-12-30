# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json

from itemadapter import ItemAdapter
from pymongo import MongoClient
from ruamel import yaml
from scrapy.exporters import JsonItemExporter


class PurchasesCrawlerPipeline:

    def open_spider(self, spider):
        self.client = spider.client
        self.collection = self.client.get_database()['purchases']
        self.collection.create_index([("id", 1)], unique=True)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.collection.insert_one(item)
        return item
