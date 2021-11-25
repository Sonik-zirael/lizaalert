# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json

from lizaalert.kafka_utils import connect_kafka_producer
from lizaalert.kafka_utils import publish_message


class LizaalertRegionsPipeline:
    def open_spider(self, spider):
        spider.publisher = connect_kafka_producer()

    def process_item(self, item, spider):
        spider.counter += 1
        print(f'processing link {spider.counter:07d}. Total links: {len(spider.all_visited_links_set):07d}', end='\r')
        if spider.counter % 500 == 0:
            print()
        return item

    def close_spider(self, spider):
        if spider.publisher is not None:
            publish_message(spider.publisher, 'regions_topic', 'topic', 'end')
            spider.publisher.close()
        with open("tmp_r.json", "w", encoding='utf8') as o_f:
            json.dump(spider.results_by_region, o_f, ensure_ascii=False, indent=4)

class LizaalertArchivePipeline:
    def open_spider(self, spider):
        spider.publisher = connect_kafka_producer()

    def process_item(self, item, spider):
        spider.counter += 1
        print(f'processing link {spider.counter:07d}. Total links: {len(spider.all_visited_links_set):07d}', end='\r')
        if spider.counter % 500 == 0:
            print()
        return item

    def close_spider(self, spider):
        if spider.publisher is not None:
            publish_message(spider.publisher, 'archive_topic', 'topic', 'end')
            spider.publisher.close()
        with open("tmp_a.json", "w", encoding='utf8') as o_f:
            json.dump(spider.results_by_region, o_f, ensure_ascii=False, indent=4)
