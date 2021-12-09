import scrapy
from scrapy.http import Response
from scrapy.selector import SelectorList
from lizaalert.kafka_utils import connect_kafka_producer, publish_message


class ArchiveSearchSpider(scrapy.Spider):
    name = "archive"
    allowed_domains = ["lizaalert.org"]
    start_urls = [
        'https://lizaalert.org/forum/viewforum.php?f=133',
        'https://lizaalert.org/forum/viewforum.php?f=119',
    ]
    custom_settings = {
        'ITEM_PIPELINES': {
            'lizaalert.pipelines.LizaalertArchivePipeline': 400
        }
    }
    results_by_region={}
    counter = 1
    all_visited_links_set = {'https://lizaalert.org/forum/viewforum.php?f=133'}
    next_pages, subforums, topics = set(), set(), set()
    publisher = None

    def parse(self, response: Response):

        # receive data from page https://lizaalert.org/forum/viewforum.php?f=133
        subforums_section = response.xpath('//div[@class="forabg"]')
        topics_section = response.xpath('//div[@class="forumbg"]')
        next_page_button = response.xpath(
            '//div[@class="action-bar bar-top"]/div[@class="pagination"]//li[@class="arrow next"]')

        if len(next_page_button) != 0:
            return scrapy.Request(response.url, dont_filter=True, callback=self._parse_pagination)

        elif len(subforums_section) != 0:
            return scrapy.Request(response.url, dont_filter=True, callback=self._parse_subforum)

        elif len(topics_section) != 0:
            return scrapy.Request(response.url, dont_filter=True, callback=self._parse_topics)

    def _parse_pagination(self, response: Response):
        self.all_visited_links_set.add(response.url)
        if response.status in [404, 500]:
            print('='*30+'\n'+'RECEIVED 500 OR 404 ERROR-CODE\n'+'='*30)
        else:
            subforums_section = response.xpath('//div[@class="forabg"]')
            topics_section = response.xpath('//div[@class="forumbg"]')

            self.subforums.update(self._extract_links(subforums_section, 'forums'))
            self.topics.update(self._extract_links(topics_section, 'topics'))

        next_page_button = response.xpath(
            '//div[@class="action-bar bar-top"]/div[@class="pagination"]//li[@class="arrow next"]')

        if len(next_page_button) != 0:
            new_page = next_page_button.xpath('a/@href').get()
            new_url = response.urljoin(new_page)
            return scrapy.Request(new_url, dont_filter=True, callback=self._parse_pagination)
        elif len(self.subforums) != 0:
            new_page = self.subforums.pop()
            new_url = response.urljoin(new_page)
            return scrapy.Request(new_url, dont_filter=True, callback=self._parse_subforum)
        elif len(self.topics) != 0:
            new_page = self.topics.pop()
            new_url = response.urljoin(new_page)
            return scrapy.Request(new_url, dont_filter=True, callback=self._parse_topic)

    def _parse_subforum(self, response: Response):
        self.all_visited_links_set.add(response.url)
        if response.status in [404, 500]:
            print('='*30+'\n'+'RECEIVED 500 OR 404 ERROR-CODE\n'+'='*30)
        next_page_button = response.xpath(
            '//div[@class="action-bar bar-top"]/div[@class="pagination"]//li[@class="arrow next"]')
        if len(next_page_button) != 0:
            return scrapy.Request(response.url, dont_filter=True, callback=self._parse_pagination)
        else:
            subforums_section = response.xpath('//div[@class="forabg"]')
            topics_section = response.xpath('//div[@class="forumbg"]')

            self.subforums.update(self._extract_links(subforums_section, 'forums'))
            self.topics.update(self._extract_links(topics_section, 'topics'))

            if len(self.subforums) != 0:
                new_page = self.subforums.pop()
                new_url = response.urljoin(new_page)
                return scrapy.Request(new_url, dont_filter=True, callback=self._parse_subforum)
            elif len(self.topics) != 0:
                new_page = self.topics.pop()
                new_url = response.urljoin(new_page)
                return scrapy.Request(new_url, dont_filter=True, callback=self._parse_topic)




    def _parse_topic(self, response: Response) -> None:
        self.all_visited_links_set.add(response.url)
        if response.status in [404, 500]:
            print('='*30+'\n'+'RECEIVED 500 OR 404 ERROR-CODE\n'+'='*30)
        else:
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
                self.results_by_region[primary_region][secondary_region] = {}

            topic_url_subdomain, topic_url_params = response.url.split('?')
            topic_params_unique = list(
                filter(
                    (lambda param:
                        param.startswith('f=') or param.startswith('t=')),
                    topic_url_params.split('&')
                )
            )
            new_topic_params = '&'.join(topic_params_unique)
            unique_topic_url = '?'.join((topic_url_subdomain, new_topic_params))
            if unique_topic_url not in self.results_by_region[primary_region][secondary_region].keys():
                title = response.xpath('//h2[@class="topic-title"]/a/text()').get()
                self.results_by_region[primary_region][secondary_region][unique_topic_url] = {
                    'title': title,
                    'posts': [],
                }

            posts_in_topic_selector = response.xpath('//div[starts-with(@class, "post ")]')
            for post_selector in posts_in_topic_selector:
                post_id = post_selector.xpath('//div[starts-with(@class, "post ")]/@id').get()

                author_details = post_selector.xpath('div[@class="inner"]/dl[@class="postprofile"]')
                if len(author_details.xpath('dt/a')):
                    author_username = author_details.xpath('dt/a/text()').get()
                    author_url_subdomain, author_url_params = author_details.xpath('dt/a/@href').get().split('?')
                    author_ids_in_url = list(
                        filter(
                            (lambda param:
                                param.startswith('u=')),
                            author_url_params.split('&')
                        )
                    )
                    author_id = author_ids_in_url[0] if author_ids_in_url else None
                    author_url = '?'.join((author_url_subdomain, author_id))
                else:
                    author_username = author_details.xpath('dt//*[starts-with(@class, "username")]/text()').get()
                    author_url = None

                author = {
                    'username': author_username,
                    'profile_url': author_url
                }

                post_contents = post_selector.xpath('div[1]/div[@class="postbody"]/div[1]/' \
                                                    'div[@class="content"]//text()').getall()
                post_images = post_selector.xpath('div[1]/div[@class="postbody"]/div[1]/' \
                                                'div[@class="content"]//img/@src').getall()
                post_timestamp = post_selector.xpath('div[1]/div[@class="postbody"]//p[@class="author"]' \
                                                    '/time/@datetime').get()
                post = {
                    'author': author,
                    'contents': post_contents,
                    'media': post_images,
                    'timestamp': post_timestamp
                }
                self.results_by_region[primary_region][secondary_region][unique_topic_url]['posts'].append(post)


        next_page_button = response.xpath(
            '//div[@class="action-bar bar-top"]/div[@class="pagination"]//li[@class="arrow next"]')

        if len(next_page_button) != 0:
            new_page = next_page_button.xpath('a/@href').get()
            new_url = response.urljoin(new_page)
            return scrapy.Request(new_url, dont_filter=True, callback=self._parse_topic)

        tmp_dict = {
            primary_region: {
                secondary_region: {
                    unique_topic_url: self.results_by_region[primary_region][secondary_region][unique_topic_url]
                }
            }
        }
        publish_message(self.publisher, 'archive_topic', 'topic', tmp_dict)

        if len(self.subforums) != 0:
            new_page = self.subforums.pop()
            new_url = response.urljoin(new_page)
            return scrapy.Request(new_url, dont_filter=True, callback=self._parse_subforum)
        elif len(self.topics) != 0:
            new_page = self.topics.pop()
            new_url = response.urljoin(new_page)
            return scrapy.Request(new_url, dont_filter=True, callback=self._parse_topic)

    def _extract_links(self, section: SelectorList, type: str) -> 'list[str]':
        topic_list = section.xpath(f'//ul[@class="topiclist {type}"]/li/dl/dt/div')
        return [topic_div.css('a::attr(href)').get() for topic_div in topic_list]
