# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from ruamel import yaml


class PurchasesCrawlerPipeline:

    def open_spider(self, spider):
        self.file = open(spider.output_file, 'w', encoding='utf-8')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        yaml.dump([item], self.file, allow_unicode=True, default_flow_style=False)
        return item
