# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json


class LizaalertPipeline:
    def process_item(self, item, spider):
        return item

    def close_spider(self, spider):
        with open("result.json", "w", encoding='utf8') as o_f:
            json.dump(spider.results_by_region, o_f, ensure_ascii=False)
        print('Done')
