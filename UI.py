import os
import sys
from scrapy.crawler import CrawlerProcess

from web_parsing.lizaalert.spiders.archive_spider import ArchiveSearchSpider
from web_parsing.lizaalert.spiders.regions_spider import RegionsSpider

choice = None

while choice != 'q':

    choice = input('Select how to run the pipeline '
                '[1. Open Kibana dashboard with ready data]: \n'
                '1. Open Kibana dashboard with ready data\n'
                '2. Re-collect data and open Kiabana dashboard '
                '(May take plenty of time):\n'
                'q. Press q to quit')

    if choice not in ['1', '2', 'q', 'r']:
        print('Only chocies "1" or "2" or "quit" are available')

    if choice == '1':
        print('Option #1 is choosen.')
        print('Use this link to go to the Kibana lizaalert dashboard: http://localhost:5601/app/dashboards#/view/1bb49e00-4f81-11ec-b4de-4946803edccc')
        choice = input('Press r to return to menu\n'
                    'Press q to quit')

    if choice == '2':
        print('Option #2 is choosen.')
        process = CrawlerProcess({
            'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
        })
        process.crawl(RegionsSpider)
        process.start()

        print('Results are available at http://localhost:5601/app/dashboards#/view/1bb49e00-4f81-11ec-b4de-4946803edccc')
        choice = input('Press r to return to menu\n'
                    'Press q to quit')
