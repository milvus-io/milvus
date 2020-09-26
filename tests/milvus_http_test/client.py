import logging
import pdb
import json
import requests
import utils
from milvus import Milvus
from utils import *

url_collections = "collections"

class MilvusClient(object):
    def __init__(self, url):
        self._url = url

    def create_collection(self, collection_name, fields):
        url = self._url+url_collections
        fields.update({"collection_name": collection_name})
        try:
            res_create = requests.post(url, data=json.dumps(fields))
            logging.getLogger().info("Create collection: <%s> successfully" % collection_name)
        except Exception as e:
            logging.getLogger().error(str(e))
            raise

    def list_collections(self):
        pass

    def has_collection(self, collection_name):
        pass

    def drop_collection(self, collection_name):
        pass