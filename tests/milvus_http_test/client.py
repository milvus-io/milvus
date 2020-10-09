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
        logging.getLogger().error(url)
        self._url = url

    def _check_status(self, result):
        logging.getLogger().info(result.text)
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

    def put(self, data=None):
        if data:
            res_put = requests.put(self._url, data=json.dumps(data))
        else:
            res_put = requests.put(self._url)
        return self._check_status(res_put)


class MilvusClient(object):
    def __init__(self, url):
        logging.getLogger().debug(url)
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

    def list_collections(self, offset=0, page_size=10):
        url = self._url+url_collections+'?'+'offset='+str(offset)+'&page_size='+str(page_size)
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
        url = self._url+url_collections+'/'+str(collection_name)
        r = Request(url)
        try:
            res_drop = r.delete()
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def info_collection(self, collection_name):
        url = self._url+url_collections+'/'+collection_name
        r = Request(url)
        try:
            return r.get()
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def stat_collection(self, collection_name):
        url = self._url+url_collections+'/'+collection_name
        r = Request(url)
        try:
            return r.get(data={"info": "stat"})
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def count_collection(self, collection_name):
        return self.stat_collection(collection_name)["row_count"]

    def create_partition(self, collection_name, tag):
        url = self._url+url_collections+'/'+collection_name+'/partitions'
        r = Request(url)
        create_params = {"partition_tag": tag}
        try:
            return r.post(create_params)
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def list_partitions(self, collection_name):
        url = self._url+url_collections+'/'+collection_name+'/partitions'
        r = Request(url)
        try:
            ret = r.get()
            return ret["partitions"]
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def drop_partition(self, collection_name, tag):
        url = self._url+url_collections+'/'+collection_name+'/partitions/'+tag;
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
            return r.put(data=flush_params)
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

    def delete(self, collection_name, ids):
        url = self._url+url_collections+'/'+collection_name+'/entities'
        r = Request(url)
        delete_params = {"ids": ids}
        try:
            res_delete = r.delete(data=delete_params)
            return res_delete
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    '''
    method: get entities by ids
    '''
    def get_entities(self, collection_name, ids):
        ids = ','.join(str(i) for i in ids)
        url = self._url+url_collections+'/'+collection_name+'/entities?ids='+ids
        # url = self._url+url_collections+'/'+collection_name+'/entities'
        r = Request(url)
        try:
            collections = r.get()
            return collections["entities"]
        except Exception as e:
            logging.getLogger().error(str(e))
            return False 

    '''
    method: create index
    '''
    def create_index(self, collection_name, field_name, index_params):
        url = self._url+url_collections+'/'+collection_name+'/fields/'+field_name+'/indexes'
        r = Request(url)
        try:
            return r.post(index_params)
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def drop_index(self, collection_name, field_name):
        url = self._url+url_collections+'/'+collection_name+'/fields/'+field_name+'/indexes'
        r = Request(url)
        try:
            return r.delete()
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    def describe_index(self, collection_name, field_name):
        info = self.info_collection(collection_name)
        for field in info["fields"]:
            if field["field_name"] == field_name:
                return field["index_params"]

    '''
    method: drop all collections in db
    '''
    def clear_db(self):
        collections = self.list_collections(page_size=10000)
        for item in collections:
            self.drop_collection(item["collection_name"])

    def system_cmd(self, cmd):
        url = self._url+url_system+cmd
        r = Request(url)
        try:
            return r.get()["reply"]
        except Exception as e:
            logging.getLogger().error(str(e))
            return False