import pymongo
import scrapy.pipelines as pipelines

from .apikey import *
from scrapy.exceptions import DropItem

class MongoDBPipelines:
    def __init__(self) -> None:
        super().__init__()
        connection = pymongo.MongoClient(
            MONGO_ADDR,
            MONGO_PORT
        )
        self.db = connection['crawling_tuto']
        
    def process_item(self, item, spider):
        self.db[item.target].insert_one(dict(item))
        return item