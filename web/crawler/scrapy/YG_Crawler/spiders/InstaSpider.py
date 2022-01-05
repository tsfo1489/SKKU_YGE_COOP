import re
import json
import scrapy
import logging

from bs4 import BeautifulSoup as bs
from datetime import datetime, timezone
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.remote_connection import LOGGER
from ..apikey import CHROMEDRIVER_PATH, CROWDTANGLE_EMAIL, CROWDTANGLE_PASSWORD, CROWDTANGLE_IG_LINK
from ..items import IGItem

class InstaSpider(scrapy.Spider):
    name = 'Insta'
    start_urls = ['https://apps.crowdtangle.com/']
    custom_settings = {
        'SPIDER_MIDDLEWARES': {
            'YG_Crawler.middlewares.IGSpiderMiddleware': 100
        }
    }
    post_per_day=100
    channels={
        "(여자)아이들":"official_g_i_dle",
        "강다니엘":"daniel.k.here",
        "뉴이스트":"nuest_official",
        "레드벨벳":"redvelvet.smtown",
        "레드벨벳 아이린":"renebaebae",
        "레드벨벳 슬기":"hi_sseulgi",
        "레드벨벳 조이":"_imyour_joy",
        "레드벨벳 웬디":"todayis_wendy",
        "레드벨벳 예리":"yerimiese",
        "마마무":"mamamoo_official",
        "마마무 화사":"_mariahwasa",
        "몬스타엑스":"official_monsta_x",
        "방탄소년단":"BTS.bighitofficial",
        "BLACKPINK":"BLACKPINKOFFICIAL",
        "BLACKPINK 로제":"roses_are_rosie",
        "BLACKPINK 리사":"lalalalisa_m",
        "BLACKPINK 제니":"jennierubyjane",
        "BLACKPINK 지수":"sooyaaa__",
        "BIGBANG 지드래곤":"xxxibgdrgn",
        "샤이니":"shinee",
        "세븐틴":"saythename_17",
        "슈퍼주니어":"superjunior",
        "스트레이키즈":"realstraykids",
        "아스트로":"officialastro",
        "아스트로 차은우":"eunwo.o_c",
        "에스파":"aespa_official",
        "WINNER":"winnercity",
        "크래비티":"cravity_official",
        "트와이스":"twicetagram",
        "AB6IX":"ab6ix_official",
        "에이티즈":"ateez_official_",
        "엔하이픈":"enhypen",
        "EXO":"weareone.exo",
        "엑소 백현":"baekhyunee_exo",
        "iKON":"withikonic",
        "ITZY":"itzy.all.in.us",
        "NCT":"nct",
        "NCT 127":"nct127",
        "NCT DREAM":"nct_dream",
        "더보이즈":"official_theboyz",
        "TREASURE":"yg_treasure_official",
        "TXT":"txt_bighit",
        "WayV":"wayvofficial",
        "청하":"chungha_official",
        "선미":"miyayeah",
        "전소미":"somsomi0309",
        "AKMU 이수현":"akmu_suhyun",
        "아이유":"dlwlrma",
        "헤이즈":"heizeheize",
        "소녀시대":"girlsgeneration",
        "소녀시대 태연":"taeyeon_ss",
        "이하이":"leehi_hi",
        "정은지":"artist_eunji",
        "백예린":"yerin_the_genuine",
        "볼빨간사춘기":"hey_miss_true",
        "현아":"hyunah_aa",
        "비투비":"cube_official_btob",
        "SF9":"sf9official",
        "스테이씨":"stayc_highup",
        "오마이걸":"wm_ohmygirl",
        "위클리":"_weeekly"
    }
    keywords=[
        '블랙핑크',
        '위너'
    ]

    def __init__(self, **kwargs):
        options = webdriver.ChromeOptions()
        options.add_argument('window-size=1280,720')
        options.add_argument('loglevel=3')
        options.add_argument('headless')
        options.add_argument('no-sandbox')
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        self.driver = webdriver.Chrome(CHROMEDRIVER_PATH, options=options)
        self.driver.implicitly_wait(3)
        LOGGER.setLevel(logging.CRITICAL)
        self.crowdtangle_login()
        for cookie in self.driver.get_cookies():
            if cookie['name'] == 'cisession':
                self.cookie = {
                    'cisession': cookie['value']
                }
        self.check_artist_channel()
        self.check_keyword()
    
    def crowdtangle_login(self):
        self.driver.get('https://apps.crowdtangle.com')
        main_window_handle = self.driver.current_window_handle
        self.driver.find_element_by_css_selector('button').click()
        sign_window_handle = None
        while not sign_window_handle:
            for handle in self.driver.window_handles:
                if handle != main_window_handle:
                    sign_window_handle = handle
        self.driver.switch_to.window(sign_window_handle)
        email_input = self.driver.find_element_by_css_selector('#email.inputtext')
        email_input.send_keys(CROWDTANGLE_EMAIL)
        pass_input = self.driver.find_element_by_css_selector('#pass.inputtext')
        pass_input.send_keys(CROWDTANGLE_PASSWORD)
        self.driver.find_element(By.XPATH, '//*[@id="loginbutton"]').click()
        self.driver.switch_to.window(self.driver.window_handles[0])
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div/div/div[1]/h1'))
        )

    def check_artist_channel(self):
        self.driver.get(CROWDTANGLE_IG_LINK)
        soup = bs(self.driver.page_source, 'html.parser')
        builded_channel = [x.text for x in soup.select('.rc-collapse-content-active .list-item-group a')]
        for artist in self.channels:
            if artist not in builded_channel:
                now_url = self.driver.current_url
                create_list_btn = self.driver.find_element(By.CSS_SELECTOR, 'div.create-list')
                create_list_btn.click()
                WebDriverWait(self.driver, 10).until(EC.url_changes(now_url))
                channel_name = self.driver.find_element_by_css_selector('div.dashboard-name-container input')
                channel_name.send_keys(artist)
                submit_btn = self.driver.find_element_by_css_selector('div.dashboard-name-container button')
                submit_btn.click()
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((
                        By.CSS_SELECTOR, 
                        'span.edit-report-title-icon > i'
                    ))
                )
                channel_link = self.driver.find_element_by_css_selector('.ui-searchbar-main input')
                channel_link.send_keys(self.channels[artist])
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((
                        By.CSS_SELECTOR, 
                        'div.add-producer-result'
                    ))
                )
                self.driver.find_element_by_css_selector('.add-producer-result button').click()
        soup = bs(self.driver.page_source, 'html.parser')
        self.channel_ids = {}
        for tag in soup.select('.list-item-group a'):
            self.channel_ids[tag.text] = tag['href'][tag['href'].rfind('/') + 1:]
        
    def check_keyword(self):
        self.driver.get(CROWDTANGLE_IG_LINK+'/search/new')
        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((
                By.XPATH, 
                '//*[@id="body-container-inner"]/div[2]/div[5]/div[2]/div[2]/div[1]/div/div[1]'
            ))
        )
        soup = bs(self.driver.page_source, 'html.parser')
        builded_keyword = [x.text for x in soup.select('.rc-collapse-content-active .list-item-group a')]
        for keyword in self.keywords:
            if keyword not in builded_keyword:
                keyword_input = self.driver.find_element_by_css_selector('input.advanced-search-input')
                keyword_input.send_keys(keyword)
                WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR, 
                        '.react-save-search-button'
                    ))
                )
                submit_btn = self.driver.find_element_by_css_selector('button.react-save-search-button')
                submit_btn.click()
                self.driver.get(CROWDTANGLE_IG_LINK+'/search/new')
                WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((
                        By.XPATH, 
                        '//*[@id="body-container-inner"]/div[2]/div[5]/div[2]/div[2]/div[1]/div/div[1]'
                    ))
                )
        soup = bs(self.driver.page_source, 'html.parser')
        self.keyword_ids = {}
        for tag in soup.select('.rc-collapse-content-active .list-item-group a'):
            self.keyword_ids[tag.text] = tag['href'][tag['href'].rfind('/') + 1:]
    
    def start_requests(self):
        for keyword in self.keyword_ids:
            yield scrapy.Request(
                f'{CROWDTANGLE_IG_LINK}/{self.keyword_ids[keyword]}/stream/popular/0/1/0/0/0/0/1day/raw/0/8',
                self.parse_post,
                meta={
                    'by': 'keyword', 'keyword': keyword, 
                    'id': self.keyword_ids[keyword], 
                    'crawled_item_cnt': 0
                },
                cookies=self.cookie
            )
        for channel in self.channel_ids:
            yield scrapy.Request(
                f'{CROWDTANGLE_IG_LINK}/{self.channel_ids[channel]}/stream/popular/0/1/0/0/0/0/1day/raw/0/8',
                self.parse_post,
                meta={
                    'by': 'channel', 'channel': f'{channel}', 
                    'id': self.channel_ids[channel], 
                    'crawled_item_cnt': 0
                },
                cookies=self.cookie
            )

    def parse_post(self, response):
        soup = bs(response.body, 'html.parser')
        crawled_item_cnt = response.meta['crawled_item_cnt']
        limit = min(self.post_per_day- crawled_item_cnt, len(soup.select('li.feed_item')))
        for post in soup.select('li.feed_item')[:limit]:
            channel_name = post.select_one('h3.post_group_name a').text.strip()
            post_url = post.select_one('div.go-to-post > a')['href']
            post_type = post['class'][2]
            post_body = post.select_one('.description_instagram').text.strip()
            post_stat = {}
            for stat_item in post.select('.stat_bar_item'):
                stat_name = stat_item.img['src']
                stat_name = re.search('comment|heart|view', stat_name)[0]
                stat_val = int(stat_item.text.strip().replace(',', ''))
                post_stat[stat_name] = stat_val
            post_date = post.select_one('.timestamp-tooltip')['title'][:-4]
            if re.match('\d\d\/\d\d\/\d\d\d\d', post_date):
                post_date = datetime.strptime(post_date, '%m/%d/%Y %H:%M%p')
            else:
                post_date = datetime.strptime(post_date, '%H:%M%p')
                post_date = post_date.replace(
                    year=datetime.now().year,
                    month=datetime.now().month,
                    day=datetime.now().day,
                )
            yield IGItem(
                channel=channel_name,
                post_url=post_url,
                post_type=post_type,
                body=post_body,
                datetime=post_date,
                stat=post_stat,
                by=response.meta['by']+':'+response.meta[response.meta['by']]
            )
        crawled_item_cnt += limit
        response.meta['crawled_item_cnt'] = crawled_item_cnt
        if crawled_item_cnt < self.post_per_day and limit == 12:
            yield scrapy.Request(
                f'{CROWDTANGLE_IG_LINK}/{response.meta["id"]}/stream/popular/0/1/{response.meta["crawled_item_cnt"]}/0/0/0/1day/raw/0/8',
                self.parse_post,
                meta=response.meta,
                cookies=self.cookie
            )