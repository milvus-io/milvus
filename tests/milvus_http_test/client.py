import logging
import pdb
import json
import requests
import utils
from milvus import Milvus
from utils import *

url_collections = "collections"
url_system = "system/"

class Request(object):
    def __init__(self, url):
        self._url = url

    def _check_status(self, result):
        # logging.getLogger().info(result.text)
        if result.status_code not in [200, 201, 204]:
            return False
        if not result.text or "code" not in json.loads(result.text):
            return True
        elif json.loads(result.text)["code"] == 0:
            return True
        else:
            logging.getLogger().error(result.status_code)
            logging.getLogger().error(result.reason)
            return False

    def get(self, data=None):
        res_get = requests.get(self._url, params=data)
        if self._check_status(res_get):
            # TODO:
            return json.loads(res_get.text)

    def post(self, data):
        res_post = requests.post(self._url, data=json.dumps(data))
        return self._check_status(res_post)

    def delete(self, data=None):
        if data:
            res_delete = requests.delete(self._url, data=json.dumps(data))
        else:
            res_delete = requests.delete(self._url)
        return self._check_status(res_delete)

    def push(self, data):
        

class MilvusClient(object):
    def __init__(self, url):
        self._url = url

    def create_collection(self, collection_name, fields):
        url = self._url+url_collections
        r = Request(url)
        fields.update({"collection_name": collection_name})
        try:
            return r.post(fields)
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def list_collections(self):
        url = self._url+url_collections
        r = Request(url)
        try:
            collections = r.get()
            return collections["collections"]
        except Exception as e:
            logging.getLogger().error(str(e))
            return False 

    def has_collection(self, collection_name):
        url = self._url+url_collections+'/'+collection_name
        r = Request(url)
        try:
            return r.get()
        except Exception as e:
            logging.getLogger().error(str(e))
            return False 

    def drop_collection(self, collection_name):
        url = self._url+url_collections+'/'+collection_name
        r = Request(url)
        try:
            res_drop = r.delete()
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def flush(self, collection_names):
        url = self._url+url_system+'/task'
        r = Request(url)
        flush_params = {
            "flush": {"collection_names": collection_names}}
        try:
            return r.put()
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def insert(self, collection_name, entities, tag=None):
        url = self._url+url_collections+'/'+collection_name+'/entities'
        r = Request(url)
        insert_params = {"entities": entities}
        if tag:
            insert_params.update({"partition_tag": tag})
        try:
            return r.post(insert_params)
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def get_entity(self, ids):
        url = None
        r = Request(url)
        try:
            collections = r.get()["entity"]
        except Exception as e:
            logging.getLogger().error(str(e))
            return False 

    def system_cmd(self, cmd):
        url = self._url+url_system+cmd
        r = Request(url)
        try:
            return r.get()["reply"]
        except Exception as e:
            logging.getLogger().error(str(e))
            return False