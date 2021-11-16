# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json

from itemadapter import ItemAdapter
from ruamel import yaml
from scrapy.exporters import JsonItemExporter


class PurchasesCrawlerPipeline:

    def open_spider(self, spider):
        self.yml_file = open(spider.output_file + '.yml', 'w', encoding='utf-8')
        self.json_file = open(spider.output_file + '.json', 'wb')
        self.exporter = JsonItemExporter(self.json_file, encoding='utf-8', indent=4)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.yml_file.close()
        self.exporter.finish_exporting()
        self.json_file.close()

    def process_item(self, item, spider):
        yaml.dump([item], self.yml_file, allow_unicode=True, default_flow_style=False)
        self.exporter.export_item(item)
        # json.dump(item, self.json_file, indent=4, ensure_ascii=False)
        return item
