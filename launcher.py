import argparse
import os

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from purchases_crawler.purchases_crawler.spiders.goszakupki.purchase_object_spider import PurchaseObjectSpider


def run():
    parser = argparse.ArgumentParser()
    required_named = parser.add_argument_group('required named arguments')
    required_named.add_argument("-u", "--url", help="Start url with search parameters.", required=True)
    parser.add_argument('-cs', '--connection-string', help='Connection string to mongo db', action='store', default='objects')
    args = parser.parse_args()
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'purchases_crawler.purchases_crawler.settings'
    settings = get_project_settings()
    crawler_process = CrawlerProcess(settings)
    crawler_process.crawl(PurchaseObjectSpider, start_urls=[args.url], connection_string=args.connection_string)
    crawler_process.start()


if __name__ == '__main__':
    run()
