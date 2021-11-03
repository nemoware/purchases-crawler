import logging
import re
import urllib.parse

import scrapy
normalization_pattern = re.compile(r'\s+|\r|\n|\r\n')


class PurchaseObjectSpider(scrapy.Spider):
    def __init__(self, start_urls, output_file, *args, **kwargs):
        self.output_file = output_file
        self.name = "objects"
        url_parts = list(urllib.parse.urlparse(start_urls[0]))
        query = dict(urllib.parse.parse_qsl(url_parts[4]))
        self.page_number = int(query['pageNumber'])

        self.start_urls = start_urls
        super(PurchaseObjectSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        purchases = response.css('div.registry-entry__form')
        if len(purchases) == 0:
            return
        for purchase in response.css('div.registry-entry__form'):
            url = purchase.css('div.registry-entry__header-mid__number a::attr(href)').get()
            yield scrapy.Request(response.urljoin(url), callback=self.parse_card)

        self.page_number += 1
        url_parts = list(urllib.parse.urlparse(self.start_urls[0]))
        query = dict(urllib.parse.parse_qsl(url_parts[4]))
        query['pageNumber'] = str(self.page_number)
        url_parts[4] = urllib.parse.urlencode(query)

        yield scrapy.Request(urllib.parse.urlunparse(url_parts), callback=self.parse)

    def parse_card(self, response):
        id = response.css('span.cardMainInfo__purchaseLink a::text').get(default='')
        main_info = response.css('div.sectionMainInfo__body')
        purchase_object = ''
        customer = None
        if main_info:
            main_info_sections = main_info.css('div.cardMainInfo__section')
            for main_info_section in main_info_sections:
                main_info_section_title = main_info_section.css('span.cardMainInfo__title::text').get()
                if main_info_section_title and 'Объект закупки' in main_info_section_title:
                    purchase_object = main_info_section.css('span.cardMainInfo__content::text').get(default='')
                # if main_info_section_title and customer_pattern.search(main_info_section_title):
                #     customer = main_info_section.css('span.cardMainInfo__content a::text').get().strip()

        application_deadline = None
        placement_date = None
        date_div = response.css('div.date')
        if date_div:
            sections = date_div.css('div.cardMainInfo__section')
            for section in sections:
                section_title = section.css('span.cardMainInfo__title::text').get()
                if section_title and 'Размещено' in section_title:
                    placement_date = section.css('span.cardMainInfo__content::text').get(default='').strip()
                if section_title and 'Окончание подачи заявок' in section_title:
                    application_deadline = section.css('span.cardMainInfo__content::text').get(default='').strip()

        region = None
        start_price = None
        currency = None
        purchase_positions = []
        blocks = response.css('div.blockInfo')
        for block in blocks:
            block_title = block.css('h2.blockInfo__title').get()
            if block_title and 'Контактная информация' in block_title:
                sections = block.css('section')
                for section in sections:
                    section_title = section.css('span.section__title::text').get()
                    if section_title and 'Организация' in section_title:
                        customer = section.css('span.section__info::text').get(default='').strip()
                    if section_title and 'Регион' in section_title:
                        region = section.css('span.section__info::text').get(default='').strip()
            if block_title and 'цена контракта' in block_title:
                sections = block.css('section')
                for section in sections:
                    section_title = section.css('span.section__title::text').get()
                    if section_title and 'цена контракта' in section_title:
                        start_price = section.css('span.section__info::text').get(default='').strip()
                    if section_title and 'Валюта' in section_title:
                        currency = section.css('span.section__info::text').get(default='').strip()
            if block_title and 'Информация об объекте закупки' in block_title:
                column_mapping = {
                    'Код позиции': 'code',
                    'Наименование Товара, Работы, Услуги по КТРУ': 'name',
                    'Лек. форма, дозировка и ед. измерения': 'name',
                    'Ед. измерения': 'unit',
                    'Количество': 'quantity',
                    'Цена за ед., ₽': 'price_per_unit',
                    'Начальная цена за единицу товара': 'price_per_unit',
                    'Стоимость, ₽': 'total_price',
                }
                purchase_positions = self.parse_table(block.css('table.tableBlock'), column_mapping, response.request.url)

        result = {
            'id': id.replace('№', '').strip(),
            'url': response.request.url,
            'object': self.normalize_string(purchase_object),
            'customer': customer,
            'placement_date': placement_date,
            'application_deadline': application_deadline,
            'region': region,
            'start_price': start_price,
            'currency': currency,
            'purchase_positions': purchase_positions
        }
        nav_tabs = response.css('a.tabsNav__item')
        for nav_link in nav_tabs:
            if 'Результаты определения поставщика' in nav_link.css('::text').get(default=''):
                nav_url = nav_link.css('::attr(href)').get(default=None)
                if nav_url:
                    yield scrapy.Request(response.urljoin(nav_url), callback=self.parse_suppliers, cb_kwargs=dict(result=result))

    def parse_suppliers(self, response, result):
        supplier_div = response.css('div[id^=supplier-def-result-participant-table]')
        if supplier_div:
            column_mapping = {
                'Участник(и), с которыми планируется заключить контракт': 'name',
                'Наименование участника': 'name',
                'Порядковые номера, полученные по результатам рассмотрения заявок': 'number',
                'Порядковый номер, полученный по результатам рассмотрения заявки': 'number',
                'Предложения участников, ₽': 'offer',
                'Предложение участника, ₽': 'offer',
            }
            suppliers = self.parse_table(supplier_div.css('table'), column_mapping, response.request.url)
            result['suppliers'] = suppliers
        yield result

    def parse_table(self, table, column_mapping, url) -> []:
        result = []
        index_mapping = {}
        excluded_idx = []
        if table:
            thead = table.css('thead')
            if len(thead) > 1:
                thead = thead[0]
            column_headers = thead.css('th, td')
            for idx, column_header in enumerate(column_headers):
                column_header_text = column_header.css('::text').get().strip()
                column_name = column_mapping.get(column_header_text)
                if column_name:
                    index_mapping[idx] = column_name
                else:
                    excluded_idx.append(idx)
                    if column_header_text.strip() != '':
                        logging.debug(f'Column mapping not found: {column_header_text} URL: {url}')
            tbody = table.css('tbody.tableBlock__body')
            rows = tbody.css('tr.tableBlock__row, table')
            for row in rows:
                if row.root.tag == 'table':
                    break
                cells = row.css('td.tableBlock__col')
                record = {}
                for idx, cell in enumerate(cells):
                    name = index_mapping.get(idx)
                    if name:
                        values = cell.css('::text, *::text').getall()
                        record[name] = self.normalize_string(' '.join(values))
                    else:
                        if idx not in excluded_idx:
                            logging.debug(f'Column name for index {idx} not found. URL: {url}')
                result.append(record)
        return result

    def normalize_string(self, input) -> str:
        return normalization_pattern.sub(' ', input).strip()