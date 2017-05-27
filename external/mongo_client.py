from gridfs import GridFS
from pymongo import MongoClient
from bson.objectid import ObjectId


class MongoC(object):
    def __init__(self, host, db_name, collection):
        self.connect_info = 'mongodb://{}:27017/'.format(host)
        self.db_name = db_name
        self.collection = collection

    def get_cover(self, cover_id):
        client = MongoClient(self.connect_info)
        fs = GridFS(client[self.db_name], self.collection)
        cover = fs.find_one({'_id': ObjectId(cover_id) })
        cover_file = cover.read() if cover is not None else None
        client.close()
        return cover_file
