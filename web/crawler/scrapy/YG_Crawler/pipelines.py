import pymongo
import scrapy.pipelines as pipelines

from .apikey import *
from scrapy.exceptions import DropItem

class MongoDBPipelines:
    BUF_SIZE = 300
    def __init__(self) -> None:
        super().__init__()
        connection = pymongo.MongoClient(
            MONGO_ADDR,
            MONGO_PORT
        )
        self.db = connection[MONGO_DB]
        self.buffer = {}
    def process_item(self, item, spider):
        if item.target not in self.buffer:
            self.buffer[item.target] = []
        self.buffer[item.target].append(dict(item))
        
        if len(self.buffer[item.target]) == self.BUF_SIZE:
            self.db[item.target].insert_many(self.buffer[item.target])
            del self.buffer[item.target]
        return item
    
    def close_spider(self, spider):
        for key in self.buffer:
            self.db[key].insert_many(self.buffer[key])