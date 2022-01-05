# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class YoutubeChannelItem(scrapy.Item) :
    id = scrapy.Field()
    title = scrapy.Field()
    desc = scrapy.Field()
    view = scrapy.Field()
    subs = scrapy.Field()
    video = scrapy.Field()
    def __repr__(self):
        return ''

class YoutubeVideoItem(scrapy.Item) :
    id = scrapy.Field()
    channelId = scrapy.Field()
    title = scrapy.Field()
    desc = scrapy.Field()
    date = scrapy.Field()
    view = scrapy.Field()
    like = scrapy.Field()
    def __repr__(self):
        return ''

class YoutubeCommentItem(scrapy.Item):
    commentId = scrapy.Field()
    channelId = scrapy.Field()
    videoId   = scrapy.Field()
    body      = scrapy.Field()
    datetime  = scrapy.Field()
    lang      = scrapy.Field()
    like      = scrapy.Field()
    def __repr__(self):
        return ''
    
class NewsItem(scrapy.Item):
    press       = scrapy.Field()
    reporter    = scrapy.Field()
    title       = scrapy.Field()
    body        = scrapy.Field()
    datetime    = scrapy.Field()
    url         = scrapy.Field()
    keyword     = scrapy.Field()
    reaction    = scrapy.Field()
    def __repr__(self):
        return ''
    
class IGItem(scrapy.Item):
    channel = scrapy.Field()
    post_url = scrapy.Field()
    post_type = scrapy.Field()
    datetime = scrapy.Field()
    body = scrapy.Field()
    stat = scrapy.Field()
    by = scrapy.Field()
    def __repr__(self):
        return ''