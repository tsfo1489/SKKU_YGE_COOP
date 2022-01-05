import re
import sys
import json
import scrapy
from urllib import parse
from ..apikey import YT_APIKEY
from ..items import *

YT_API_LINK = 'https://www.googleapis.com/youtube/v3/'

class YoutubeSpider(scrapy.Spider):
    name = 'Youtube'
    allowed_domains = ['youtube.com', 'googleapis.com']

    def __init__(self, channel_ids='', keywords='', from_date='', to_date='', **kwargs):
        super().__init__(**kwargs)
        self.ids = []
        for id in channel_ids.split(','):
            if id != '' :
                self.ids.append({'type':'channel_id', 'id': id})
        for keyword in keywords.split(','):
            if keyword != '' :
                self.ids.append({'type':'keyword', 'id': keyword})
        
        self.date_filter = False
        if (from_date == '') ^ (to_date == ''):
            print('Error, from_date and to_date must insert together')
            sys.exit(1)
        elif from_date != '':
            self.date_filter = True
            self.from_date = from_date
            self.to_date = to_date

    def start_requests(self):
        for id in self.ids :
            query = {
                'key': YT_APIKEY,
                'part': 'snippet'
            }
            #Channel ID
            if id['type'] == 'channel_id' :
                query['part'] = 'snippet, statistics'
                query['id'] = id['id']
                query_str = parse.urlencode(query)
                yield scrapy.Request(
                        f'{YT_API_LINK}channels?{query_str}', 
                        self.get_meta_channel,
                        meta={'type': 'Channel','id': id['id']}
                    )
            #Keyword
            if id['type'] == 'keyword' :
                query['part'] = 'snippet, id'
                query['type'] = 'video'
                query['q'] = id['id']
                query_str = parse.urlencode(query)
                yield scrapy.Request(
                        f'{YT_API_LINK}search?{query_str}', 
                        self.get_search_result,
                        meta={'id': id['id']}
                    )

    def get_meta_channel(self, response) :
        data = json.loads(response.body)
        data = data['items'][0]
        doc = YoutubeChannelItem()
        doc['id'] = data['id']
        doc['title'] = data['snippet']['title']
        doc['desc'] = data['snippet']['description']
        doc['view'] = data['statistics']['viewCount']
        doc['subs'] = data['statistics']['subscriberCount']
        doc['video'] = data['statistics']['videoCount']
        yield doc
        query = {
            'key': YT_APIKEY,
            'part': 'snippet',
        }
        query['type'] = 'video'
        query['channelId'] = response.meta['id']
        query_str = parse.urlencode(query)
        yield scrapy.Request(
                f'{YT_API_LINK}search?{query_str}',
                self.get_search_result,
                meta={'Channel':doc}
            )
    
    def get_meta_video(self, response) :
        data = json.loads(response.body)
        meta = response.meta
        data = data['items'][0]
        doc = YoutubeVideoItem()
        doc['id'] = data['id']
        doc['channelId'] = data['snippet']['channelId']
        doc['title'] = data['snippet']['title']
        doc['desc'] = data['snippet']['description']
        doc['date'] = data['snippet']['publishedAt'][:10]
        doc['view'] = data['statistics']['viewCount']
        doc['like'] = data['statistics']['likeCount']
        meta['Video'] = doc
    
        if 'type' in response.meta and response.meta['type'] == 'Video' :
            query = {
                'key': YT_APIKEY,
                'part': 'snippet, statistics',
                'id': data['snippet']['channelId']
            }
            query_str = parse.urlencode(query)
            yield scrapy.Request(
                    f'{YT_API_LINK}channels?{query_str}', 
                    self.get_meta_channel,
                    meta={'type': 'Video','id': response.meta['id'], 'video_item':doc},
                    dont_filter=True
                    )
        else :
            yield doc
            query = {
                'key': YT_APIKEY,
                'part': 'snippet, id, replies',
                'maxResults': 100,
                'videoId': data['id']
            }
            query_str = parse.urlencode(query)
            yield scrapy.Request(f'{YT_API_LINK}commentThreads?{query_str}', self.parse_video, meta=meta)

    def get_search_result(self, response):
        data = json.loads(response.body)
        for video in data['items'] :
            query = {
                'key': YT_APIKEY,
                'part': 'snippet,statistics',
                'id': video['id']['videoId']
            }
            query_str = parse.urlencode(query)
            yield scrapy.Request(
                    f'{YT_API_LINK}videos?{query_str}', 
                    self.get_meta_video, 
                    meta=response.meta
                )
        
        if 'nextPageToken' in data :
            parsed_query = dict(parse.parse_qsl(response.url[response.url.find('?') + 1:]))
            parsed_query['pageToken'] = data['nextPageToken']
            query_str = parse.urlencode(parsed_query)
            yield scrapy.Request(
                    f'{YT_API_LINK}search?{query_str}', 
                    self.get_search_result,
                    meta=response.meta
                )

    def parse_video(self, response) :
        data = json.loads(response.body)
        for comment in data['items'] :
            top_comment = comment['snippet']['topLevelComment']
            doc = YoutubeCommentItem()
            doc['commentId'] = top_comment['id']
            doc['videoId']   = top_comment['snippet']['videoId']
            doc['body']      = top_comment['snippet']['textOriginal']
            doc['datetime']  = top_comment['snippet']['publishedAt']
            doc['like']      = top_comment['snippet']['likeCount']
            yield doc
            # if comment['snippet']['totalReplyCount'] > 0 :
            #     for repl in comment['replies']['comments'] :
            #         doc = YoutubeCommentItem()
            #         doc['commentId'] = repl['id']
            #         doc['videoId']   = repl['snippet']['videoId']
            #         doc['body']      = repl['snippet']['textDisplay']
            #         doc['datetime']  = repl['snippet']['publishedAt']
            #         doc['like']      = repl['snippet']['likeCount']
            #         yield doc
        
        if 'nextPageToken' in data :
            parsed_query = dict(parse.parse_qsl(response.url[response.url.find('?') + 1:]))
            parsed_query['pageToken'] = data['nextPageToken']
            query_str = parse.urlencode(parsed_query)
            yield scrapy.Request(f'{YT_API_LINK}commentThreads?{query_str}', self.parse_video, meta=response.meta)