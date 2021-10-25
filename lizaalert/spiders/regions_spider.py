from pprint import pprint

import scrapy
from scrapy.http import Response
from scrapy.selector import SelectorList

class RegionsSpider(scrapy.Spider):
    name = "regions"
    allowed_domains = ["lizaalert.org"]
    start_urls = [
        'https://lizaalert.org/forum/viewforum.php?f=119',
    ]
    results_by_region={}

    def parse(self, response: Response):
        # print(f"parsing URL {response.url}")
        subforums_section = response.xpath('//div[@class="forabg"]')
        topics_section = response.xpath('//div[@class="forumbg"]')
        # receive data from page https://lizaalert.org/forum/viewforum.php?f=119

        if len(subforums_section) != 0:
            subforums_links = self._extract_links(subforums_section, "forums")
            # print("FOUND SUBFORUMS")
            for new_link in subforums_links:
                new_page = response.urljoin(new_link)
                yield scrapy.Request(new_page, dont_filter=True, callback=self.parse)

        if len(topics_section) != 0:
            topics_links = self._extract_links(topics_section, "topics")
            # print(f"FOUND TOPICS: {topics_links}")
            for new_link in topics_links:
                # print(f"\tPROCESSING {new_link}")
                new_page = response.urljoin(new_link)
                yield scrapy.Request(new_page, dont_filter=True, callback=self.parse_topic)

        # links_in_topics_div = response.xpath(f'//*[@id="page-body"]/div[1]/div/ul[2]/li/dl/dt/div')
        # links_to_topics = [links_in_this_div.css('a::attr(href)').get() for links_in_this_div in links_in_topics_div]
        # print("\n\n\n=======================================\n=======================================\n=======================================")
        # print("STILL PARSING")
        # print("\n\n\n=======================================\n=======================================\n=======================================")

        # for new_link in links_to_topics:
        #     next_page = response.urljoin(new_link)
        #     yield scrapy.Request(next_page, dont_filter=True, callback=self.parse_regions)

    def parse_topic(self, response: Response) -> None:
        # print(F'STARTED PARSING TOPIC {response.url}')
        PRIMARY_REGION_POSITION = 4
        SECONDARY_REGION_POSITION = 5

        path_to_region = '//li[@class="breadcrumbs"][1]/span[{}]/a/span/text()'

        primary_region = response.xpath(path_to_region.format(PRIMARY_REGION_POSITION)).get()
        secondary_region = response.xpath(path_to_region.format(SECONDARY_REGION_POSITION)).get()
        if not secondary_region:
            secondary_region = "undefined"

        if primary_region not in self.results_by_region.keys():
            self.results_by_region[primary_region] = {}

        if secondary_region not in self.results_by_region[primary_region].keys():
            self.results_by_region[primary_region][secondary_region] = []

        title = response.xpath('//h2[@class="topic-title"]/a/text()').get()
        description = response.xpath('//div[@class="post has-profile bg2"]/' \
                                     'div[1]/div[@class="postbody"]/div[1]/' \
                                     'div[@class="content"]//text()').getall()
        description = ''.join(description)
        volounteeres = {}
        message_with_volounteeres = response.xpath('//div[@class="post has-profile bg1"]/' \
                                     'div[1]/div[@class="postbody"]/div[1]/' \
                                     'div[@class="content"]')
        possible_extraction = message_with_volounteeres.xpath('dl[@class="codebox"]')
        # if possible_extraction.get() is None:
        #     date = response.xpath('//div[@class="post has-profile bg1"]/' \
        #                              'div[1]/div[@class="postbody"]/div[1]/' \
        #                              'p[@class="author"]/time/text()').extract_first()
        #     list_of_volounteeres = response.xpath('//div[@class="post has-profile bg1"]/' \
        #                              'div[1]/div[@class="postbody"]/div[1]/' \
        #                              'div[@class="content"]/text()').extract_first()
        #     if '\nДОМА' in list_of_volounteeres:
        #         volounteeres[date] = list_of_volounteeres
        #     print(list_of_volounteeres)

        for i in possible_extraction:
            data = i.css('dt a::text').get()
            list_of_volounteeres = i.css('dd::text').getall()
            volounteeres[data] = list_of_volounteeres
        
        self.results_by_region[primary_region][secondary_region].append({
            "title": title,
            "description": description,
            "volounteeres": volounteeres
        })
        # print(self.results_by_region)

    def _extract_links(self, section: SelectorList, type: str) -> 'list[str]':
        topic_list = section.xpath(f'//ul[@class="topiclist {type}"]/li/dl/dt/div')
        return [topic_div.css('a::attr(href)').get() for topic_div in topic_list]

    def parse_regions(self, response):
        region_amount = {}
        list_item = 1
        while response.xpath('//*[@id="page-body"]/div[2]/div/ul[2]/li['+ str(list_item) +']/dl/dt/div/a[1]').get() is not None:
            region = response.xpath('//*[@id="page-body"]/div[2]/div/ul[2]/li['+ str(list_item) +']/dl/dt/div/a[1]/text()').get()
            topics_num = response.xpath('//*[@id="page-body"]/div[2]/div/ul[2]/li['+ str(list_item) +']/dl/dd[1]/text()').get()
            region_amount[region] = topics_num
            list_item = list_item + 1
        print(region_amount)