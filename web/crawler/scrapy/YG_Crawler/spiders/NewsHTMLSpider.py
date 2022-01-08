import re
import sys
import json
import scrapy
from datetime import datetime
from urllib import parse
from bs4 import BeautifulSoup as bs
from ..items import *

NAVER_SEARCH_LINK = 'https://search.naver.com/search.naver'
NAVER_NEWS_LINK = 'https://entertain.naver.com/read'
NAVER_REACTION_LINK = 'https://news.like.naver.com/v1/search/contents'

class NewsHTMLSpider(scrapy.Spider):
    name = 'NewsHTML'

    def __init__(self, keywords='', from_date='', to_date='', **kwargs):
        super().__init__(**kwargs)
        self.keywords = []
        for keyword in keywords.split(','):
            if keyword != '' :
                self.keywords.append(keyword)
        
        self.date_filter = False
        if (from_date == '') ^ (to_date == ''):
            print('Error, from_date and to_date must insert together')
            sys.exit(1)
        elif from_date != '':
            self.date_filter = True
            self.from_date = from_date
            self.to_date = to_date

    def start_requests(self):
        query = {
            'start': 1,
            'sort': 1,
            'where': 'news'
        }
        if self.date_filter:
            query['nso'] = f'p:from{self.from_date}to{self.to_date}'
        for keyword in self.keywords:
            query['query'] = f'\"{keyword}\"'
            query_str = parse.urlencode(query)
            yield scrapy.Request(
                f'{NAVER_SEARCH_LINK}?{query_str}',
                self.parse_news_list,
                meta={'keyword': keyword, 'start':1}
            )

    def url_checker(self, url):
        parsed_url = parse.urlparse(url)
        parsed_query = dict(parse.parse_qsl(parsed_url.query))
        if 'oid' in parsed_query and 'aid' in parsed_query:
            return True, (parsed_query['oid'], parsed_query['aid'])
        else:
            return False, 'Not NaverNews'

    def parse_news_list(self, response):
        soup = bs(response.body, 'html.parser')
        links = soup.select('div.info_group > a')
        for link in links:
            link = link['href']
            flag, link = self.url_checker(link)
            if flag:
                yield scrapy.Request(
                    f'{NAVER_NEWS_LINK}?oid={link[0]}&aid={link[1]}',
                    self.parse_news_article,
                    meta={
                        'keyword': response.meta['keyword'],
                    }
                )
        next_btn = soup.select_one('.btn_next')
        if next_btn['aria-disabled'] == 'false':
            query = {
                'start': response.meta['start'] + 10,
                'sort': 1,
                'where': 'news'
            }
            if self.date_filter:
                query['nso'] = f'p:from{self.from_date}to{self.to_date}'
            query['query'] = f'\"{response.meta["keyword"]}\"'
            query_str = parse.urlencode(query)
            yield scrapy.Request(
                f'{NAVER_SEARCH_LINK}?{query_str}',
                self.parse_news_list,
                meta={'keyword': response.meta["keyword"], 'start':response.meta['start'] + 10}
            )
    
    def korean_date_to_iso8601(self, korean_date):
        # ex 2021.12.21 오후 1:36
        date_arr = korean_date.split()
        date = datetime.strptime(date_arr[0], '%Y.%m.%d.')
        time = datetime.strptime(date_arr[2], '%I:%M')
        if date_arr[1] == '오후':
            date = date.replace(hour=time.hour+12, minute = time.minute)
        else:
            date = date.replace(hour=time.hour, minute = time.minute)
        return date
    
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
        _, (oid, aid) = self.url_checker(response.url)
        item = NewsItem(
            data_id=f'{oid}_{aid}',
            press=press,
            reporter=reporter,
            title=title,
            body=body,
            url=response.url,
            keyword=response.meta['keyword'],
            create_dt=pub_date
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