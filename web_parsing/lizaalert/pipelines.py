# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json


class LizaalertRegionsPipeline:
    def process_item(self, item, spider):
        spider.counter += 1
        print(f'processing link {spider.counter:07d}. Total links: {len(spider.all_visited_links_set):07d}', end='\r')
        if spider.counter % 500 == 0:
            print()
        return item

    def close_spider(self, spider):
        with open("result11.json", "w", encoding='utf8') as o_f:
            json.dump(spider.results_by_region, o_f, ensure_ascii=False, indent=4)

class LizaalertArchivePipeline:
    def process_item(self, item, spider):
        spider.counter += 1
        print(f'processing link {spider.counter:07d}. Total links: {len(spider.all_visited_links_set):07d}', end='\r')
        if spider.counter % 500 == 0:
            print()
        return item

    def close_spider(self, spider):
        with open("result22.json", "w", encoding='utf8') as o_f:
            json.dump(spider.results_by_region, o_f, ensure_ascii=False, indent=4)
