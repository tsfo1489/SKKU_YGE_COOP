import scrapy
import json
import re
from datetime import datetime
from urllib import parse
from bs4 import BeautifulSoup as bs
from ..items import *
from ..apikey import NAVER_ID, NAVER_SECRET

NAVER_API_LINK = 'https://openapi.naver.com/v1/search/'
NAVER_NEWS_LINK = 'https://entertain.naver.com/read'
NAVER_REACTION_LINK = 'https://news.like.naver.com/v1/search/contents'

class NewsAPISpider(scrapy.Spider):
    name = 'NewsAPI'

    def __init__(self, keywords='', **kwargs):
        super().__init__(**kwargs)
        self.keywords = []
        for keyword in keywords.split(','):
            if keyword != '' :
                self.keywords.append(keyword)
        self.default_headers = {
            'X-Naver-Client-ID': NAVER_ID,
            'X-Naver-Client-Secret': NAVER_SECRET
        }

    def start_requests(self):
        query = {
            'display': '100',
            'start': 1
        }
        for keyword in self.keywords:
            query['query'] = keyword
            query_str = parse.urlencode(query)
            yield scrapy.Request(
                f'{NAVER_API_LINK}news.json?{query_str}',
                self.parse_news_list,
                headers=self.default_headers,
                meta={'keyword': keyword, 'start':1}
            )

    def url_checker(self, url):
        parsed_url = parse.urlparse(url)
        parsed_query = dict(parse.parse_qsl(parsed_url.query))
        if 'oid' in parsed_query and 'aid' in parsed_query:
            return True, (parsed_query['oid'], parsed_query['aid'])
        else:
            return False, 'Not'

    def parse_news_list(self, response):
        data = json.loads(response.body)
        for item in data['items']:
            flag, link = self.url_checker(item['link'])
            if flag:
                yield scrapy.Request(
                    f'{NAVER_NEWS_LINK}?oid={link[0]}&aid={link[1]}',
                    self.parse_news_article,
                    meta={
                        'keyword': response.meta['keyword'],
                        'pubDate': item['pubDate']
                    }
                )
        if response.meta['start'] + 100 < 1000: 
            query = {
                'display': '100',
                'start': response.meta['start'] + 100
            }
            query['query'] = response.meta['keyword']
            query_str = parse.urlencode(query)
            yield scrapy.Request(
                f'{NAVER_API_LINK}news.json?{query_str}',
                self.parse_news_list,
                headers=self.default_headers,
                meta={
                    'keyword': response.meta['keyword'], 
                    'start':response.meta['start'] + 100
                }
            )
    
    def parse_news_article(self, response):
        soup = bs(response.body, 'html.parser')
        if 'entertain' in response.url:
            title = soup.select_one('h2.end_tit').text
            body = soup.select_one('#articeBody')
        else:
            title = soup.select_one('#articleTitle').text
            body = soup.select_one('#articleBodyContents')
        body = body.text if body is not None else ''
        body = ' '.join(body.split('\n'))
        body = re.sub('(\ |\t)+', ' ', body.strip())
        title = ' '.join(title.split('\n'))
        title = re.sub('(\ |\t)+', ' ', title.strip())
        press = soup.select_one('.press_logo > img')['alt']
        reporter = soup.select_one('.journalistcard_summary_name')
        if reporter is not None:
            reporter = reporter.text.replace('기자', '').strip()
        else: 
            reporter = re.search('[^가-힣][가-힣]{2,4} 기자[^가-힣]', body)
            if reporter is not None:
                reporter = reporter[0][1:-4]
        pub_date = soup.select_one('div.article_info > span > em').text.strip()
        pub_date = self.korean_date_to_iso8601(pub_date)
        item = NewsItem(
            press=press,
            reporter=reporter,
            title=title,
            body=body,
            url=response.url,
            keyword=response.meta['keyword'],
            datetime=pub_date
        )
        data_cid = soup.select_one('._reactionModule')['data-cid']
        query = {
            'callback': 'A',
            'q': f'ENTERTAIN[{data_cid}]'
        }
        query_str = parse.urlencode(query)
        yield scrapy.Request(
            f'{NAVER_REACTION_LINK}?{query_str}',
            self.get_article_reaction,
            meta={'item': item}
        )
    
    def get_article_reaction(self, response):
        data = json.loads(response.body[6:-2])
        reactions = {}
        for reaction in data['contents'][0]['reactions']:
            reactions[reaction['reactionType']] = reaction['count']
        item = response.meta['item']
        item['reaction'] = reactions
        yield item