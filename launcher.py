import argparse
import os

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from purchases_crawler.purchases_crawler.spiders.goszakupki.purchase_object_spider import PurchaseObjectSpider


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", help="Start url with search parameters.")
    parser.add_argument('-o', '--out-file', help='Output file path (objects.yml by default)', action='store', default='objects.yml')
    args = parser.parse_args()
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'purchases_crawler.purchases_crawler.settings'
    settings = get_project_settings()
    crawler_process = CrawlerProcess(settings)
    crawler_process.crawl(PurchaseObjectSpider, start_urls=[args.url], output_file=args.out_file)
    crawler_process.start()


if __name__ == '__main__':
    run()