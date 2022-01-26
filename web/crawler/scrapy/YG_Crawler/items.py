# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class YoutubeChannelItem(scrapy.Item) :
    target = 'YoutubeChannel'
    data_id = scrapy.Field()
    title = scrapy.Field()
    desc = scrapy.Field()
    view = scrapy.Field()
    subs = scrapy.Field()
    video = scrapy.Field()
    def __repr__(self):
        return ''

class YoutubeVideoItem(scrapy.Item) :
    target = 'YoutubeVideo'
    data_id    = scrapy.Field()
    channelId  = scrapy.Field()
    title      = scrapy.Field()
    desc       = scrapy.Field()
    create_dt  = scrapy.Field()
    view       = scrapy.Field()
    like       = scrapy.Field()
    def __repr__(self):
        return ''

class YoutubeCommentItem(scrapy.Item):
    target = 'YoutubeComment'
    data_id = scrapy.Field()
    channelId = scrapy.Field()
    videoId   = scrapy.Field()
    body      = scrapy.Field()
    create_dt = scrapy.Field()
    like      = scrapy.Field()
    def __repr__(self):
        return ''
    
class NewsItem(scrapy.Item):
    target      = 'News'
    _id         = scrapy.Field()
    data_id     = scrapy.Field()
    press       = scrapy.Field()
    reporter    = scrapy.Field()
    title       = scrapy.Field()
    body        = scrapy.Field()
    create_dt   = scrapy.Field()
    url         = scrapy.Field()
    keyword     = scrapy.Field()
    reaction    = scrapy.Field()
    
class IGItem(scrapy.Item):
    target      = 'Instagram'
    data_id     = scrapy.Field()
    channel = scrapy.Field()
    post_url = scrapy.Field()
    post_type = scrapy.Field()
    create_dt = scrapy.Field()
    body = scrapy.Field()
    stat = scrapy.Field()
    by = scrapy.Field()
    def __repr__(self):
        return ''
    
class FBItem(scrapy.Item):
    target      = 'Facebook'
    data_id = scrapy.Field()
    channel = scrapy.Field()
    post_url = scrapy.Field()
    create_dt = scrapy.Field()
    body = scrapy.Field()
    stat = scrapy.Field()
    by = scrapy.Field()
    def __repr__(self):
        return ''
    
    
class TwitterItem(scrapy.Item):
    target =    "Twitter"
    data_id =   scrapy.Field()
    create_dt = scrapy.Field()
    body =      scrapy.Field()
    user_id =   scrapy.Field()
    username =  scrapy.Field()
    name =      scrapy.Field()
    lang =      scrapy.Field()
    keyword =   scrapy.Field()
    geo =       scrapy.Field()
    hashtags =  scrapy.Field()
    object_id = scrapy.Field()
    channel =   scrapy.Field()
    def __repr__(self):
        return ''
    