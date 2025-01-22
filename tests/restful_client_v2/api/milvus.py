import json
import requests
import time
import uuid
from utils.util_log import test_log as logger
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
from tenacity import retry, retry_if_exception_type, stop_after_attempt
from requests.exceptions import ConnectionError
import urllib.parse

REQUEST_TIMEOUT = 120


ENABLE_LOG_SAVE = False


def simplify_list(lst):
    if len(lst) > 20:
        return [lst[0], '...', lst[-1]]
    return lst


def simplify_dict(d):
    if d is None:
        d = {}
    if len(d) > 20:
        keys = list(d.keys())
        d = {keys[0]: d[keys[0]], '...': '...', keys[-1]: d[keys[-1]]}
    simplified = {}
    for k, v in d.items():
        if isinstance(v, list):
            simplified[k] = simplify_list([simplify_dict(item) if isinstance(item, dict) else simplify_list(
                item) if isinstance(item, list) else item for item in v])
        elif isinstance(v, dict):
            simplified[k] = simplify_dict(v)
        else:
            simplified[k] = v
    return simplified


def build_curl_command(method, url, headers, data=None, params=None):
    if isinstance(params, dict):
        query_string = urllib.parse.urlencode(params)
        url = f"{url}?{query_string}"
    curl_cmd = [f"curl -X {method} '{url}'"]

    for key, value in headers.items():
        curl_cmd.append(f" -H '{key}: {value}'")

    if data:
        # process_and_simplify(data)
        data = json.dumps(data, indent=4)
        curl_cmd.append(f" -d '{data}'")

    return " \\\n".join(curl_cmd)


def logger_request_response(response, url, tt, headers, data, str_data, str_response, method, params=None):
    # save data to jsonl file

    data_dict = json.loads(data) if data else {}
    data_dict_simple = simplify_dict(data_dict)
    if ENABLE_LOG_SAVE:
        with open('request_response.jsonl', 'a') as f:
            f.write(json.dumps({
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "data": data_dict_simple,
                "response": response.json()
            }) + "\n")
    data = json.dumps(data_dict_simple, indent=4)
    try:
        if response.status_code == 200:
            if ('code' in response.json() and response.json()["code"] == 0) or (
                    'Code' in response.json() and response.json()["Code"] == 0):
                logger.debug(
                    f"\nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {str_response}")

            else:
                logger.debug(
                    f"\nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {response.text}")
        else:
            logger.debug(
                f"method: \nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {response.text}")
    except Exception as e:
        logger.debug(
            f"method: \nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {response.text}, \nerror: {e}")


class Requests():
    uuid = str(uuid.uuid1())
    api_key = None

    def __init__(self, url=None, api_key=None):
        self.url = url
        self.api_key = api_key
        if self.uuid is None:
            self.uuid = str(uuid.uuid1())
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}',
            'RequestId': self.uuid,
            "Request-Timeout": REQUEST_TIMEOUT
        }

    @classmethod
    def update_uuid(cls, _uuid):
        cls.uuid = _uuid

    @classmethod
    def update_headers(cls):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}',
            'RequestId': cls.uuid
        }
        return headers

    # retry when request failed caused by network or server error

    @retry(retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(3))
    def post(self, url, headers=None, data=None, params=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + '...' + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        response = requests.post(url, headers=headers, data=data, params=params)
        tt = time.time() - t0
        str_response = response.text[:200] + '...' + response.text[-200:] if len(response.text) > 400 else response.text
        logger_request_response(response, url, tt, headers, data, str_data, str_response, "post", params=params)
        return response

    @retry(retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(3))
    def get(self, url, headers=None, params=None, data=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + '...' + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        if data is None or data == "null":
            response = requests.get(url, headers=headers, params=params)
        else:
            response = requests.get(url, headers=headers, params=params, data=data)
        tt = time.time() - t0
        str_response = response.text[:200] + '...' + response.text[-200:] if len(response.text) > 400 else response.text
        logger_request_response(response, url, tt, headers, data, str_data, str_response, "get", params=params)
        return response

    @retry(retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(3))
    def put(self, url, headers=None, data=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + '...' + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        response = requests.put(url, headers=headers, data=data)
        tt = time.time() - t0
        str_response = response.text[:200] + '...' + response.text[-200:] if len(response.text) > 400 else response.text
        logger_request_response(response, url, tt, headers, data, str_data, str_response, "put")
        return response

    @retry(retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(3))
    def delete(self, url, headers=None, data=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + '...' + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        response = requests.delete(url, headers=headers, data=data)
        tt = time.time() - t0
        str_response = response.text[:200] + '...' + response.text[-200:] if len(response.text) > 400 else response.text
        logger_request_response(response, url, tt, headers, data, str_data, str_response, "delete")
        return response


class VectorClient(Requests):
    def __init__(self, endpoint, token):
        super().__init__(url=endpoint, api_key=token)
        self.endpoint = endpoint
        self.token = token
        self.api_key = token
        self.db_name = None
        self.headers = self.update_headers()

    @classmethod
    def update_headers(cls):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}',
            'Accept-Type-Allow-Int64': "true",
            'RequestId': cls.uuid
        }
        return headers

    def vector_search(self, payload, db_name="default", timeout=10):
        time.sleep(1)
        url = f'{self.endpoint}/v2/vectordb/entities/search'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        rsp = response.json()
        if "data" in rsp and len(rsp["data"]) == 0:
            t0 = time.time()
            while time.time() - t0 < timeout:
                response = self.post(url, headers=self.update_headers(), data=payload)
                rsp = response.json()
                if len(rsp["data"]) > 0:
                    break
                time.sleep(1)
            else:
                response = self.post(url, headers=self.update_headers(), data=payload)
                rsp = response.json()
                if "data" in rsp and len(rsp["data"]) == 0:
                    logger.info(f"after {timeout}s, still no data")

        return response.json()

    def vector_advanced_search(self, payload, db_name="default", timeout=10):
        time.sleep(1)
        url = f'{self.endpoint}/v2/vectordb/entities/advanced_search'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        rsp = response.json()
        if "data" in rsp and len(rsp["data"]) == 0:
            t0 = time.time()
            while time.time() - t0 < timeout:
                response = self.post(url, headers=self.update_headers(), data=payload)
                rsp = response.json()
                if len(rsp["data"]) > 0:
                    break
                time.sleep(1)
            else:
                response = self.post(url, headers=self.update_headers(), data=payload)
                rsp = response.json()
                if "data" in rsp and len(rsp["data"]) == 0:
                    logger.info(f"after {timeout}s, still no data")

        return response.json()

    def vector_hybrid_search(self, payload, db_name="default", timeout=10):
        time.sleep(1)
        url = f'{self.endpoint}/v2/vectordb/entities/hybrid_search'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        rsp = response.json()
        if "data" in rsp and len(rsp["data"]) == 0:
            t0 = time.time()
            while time.time() - t0 < timeout:
                response = self.post(url, headers=self.update_headers(), data=payload)
                rsp = response.json()
                if len(rsp["data"]) > 0:
                    break
                time.sleep(1)
            else:
                response = self.post(url, headers=self.update_headers(), data=payload)
                rsp = response.json()
                if "data" in rsp and len(rsp["data"]) == 0:
                    logger.info(f"after {timeout}s, still no data")

        return response.json()

    def vector_query(self, payload, db_name="default", timeout=5):
        time.sleep(1)
        url = f'{self.endpoint}/v2/vectordb/entities/query'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        rsp = response.json()
        if "data" in rsp and len(rsp["data"]) == 0:
            t0 = time.time()
            while time.time() - t0 < timeout:
                response = self.post(url, headers=self.update_headers(), data=payload)
                rsp = response.json()
                if len(rsp["data"]) > 0:
                    break
                time.sleep(1)
            else:
                response = self.post(url, headers=self.update_headers(), data=payload)
                rsp = response.json()
                if "data" in rsp and len(rsp["data"]) == 0:
                    logger.info(f"after {timeout}s, still no data")

        return response.json()

    def vector_get(self, payload, db_name="default"):
        time.sleep(1)
        url = f'{self.endpoint}/v2/vectordb/entities/get'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def vector_delete(self, payload, db_name="default"):
        url = f'{self.endpoint}/v2/vectordb/entities/delete'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def vector_insert(self, payload, db_name="default"):
        url = f'{self.endpoint}/v2/vectordb/entities/insert'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def vector_upsert(self, payload, db_name="default"):
        url = f'{self.endpoint}/v2/vectordb/entities/upsert'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()


class CollectionClient(Requests):

    def __init__(self, endpoint, token):
        super().__init__(url=endpoint, api_key=token)
        self.endpoint = endpoint
        self.api_key = token
        self.db_name = None
        self.name_list = []
        self.headers = self.update_headers()

    def wait_load_completed(self, collection_name, db_name="default", timeout=5):
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            rsp = self.collection_describe(collection_name, db_name=db_name)
            if "data" in rsp and "load" in rsp["data"] and rsp["data"]["load"] == "LoadStateLoaded":
                logger.info(f"collection {collection_name} load completed in {time.time() - t0} seconds")
                break
            else:
                time.sleep(1)


    @classmethod
    def update_headers(cls, headers=None):
        if headers is not None:
            return headers
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}',
            'RequestId': cls.uuid
        }
        return headers

    def collection_has(self, db_name="default", collection_name=None):
        url = f'{self.endpoint}/v2/vectordb/collections/has'
        if self.db_name is not None:
            db_name = self.db_name
        data = {
            "dbName": db_name,
            "collectionName": collection_name
        }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def collection_rename(self, payload, db_name="default"):
        url = f'{self.endpoint}/v2/vectordb/collections/rename'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def collection_stats(self, db_name="default", collection_name=None):
        url = f'{self.endpoint}/v2/vectordb/collections/get_stats'
        if self.db_name is not None:
            db_name = self.db_name
        data = {
            "dbName": db_name,
            "collectionName": collection_name
        }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def collection_load(self, db_name="default", collection_name=None):
        url = f'{self.endpoint}/v2/vectordb/collections/load'
        if self.db_name is not None:
            db_name = self.db_name
        payload = {
            "dbName": db_name,
            "collectionName": collection_name
        }
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def collection_release(self, db_name="default", collection_name=None):
        url = f'{self.endpoint}/v2/vectordb/collections/release'
        if self.db_name is not None:
            db_name = self.db_name
        payload = {
            "dbName": db_name,
            "collectionName": collection_name
        }
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def collection_load_state(self, db_name="default", collection_name=None, partition_names=None):
        url = f'{self.endpoint}/v2/vectordb/collections/get_load_state'
        if self.db_name is not None:
            db_name = self.db_name
        data = {
            "dbName": db_name,
            "collectionName": collection_name,
        }
        if partition_names is not None:
            data["partitionNames"] = partition_names
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def collection_list(self, db_name="default"):
        url = f'{self.endpoint}/v2/vectordb/collections/list'
        params = {}
        if self.db_name is not None:
            params = {
                "dbName": self.db_name
            }
        if db_name != "default":
            params = {
                "dbName": db_name
            }
        response = self.post(url, headers=self.update_headers(), params=params)
        res = response.json()
        return res

    def collection_create(self, payload, db_name="default"):
        time.sleep(1)  # wait for collection created and in case of rate limit
        c_name = payload.get("collectionName", None)
        db_name = payload.get("dbName", db_name)
        self.name_list.append((db_name, c_name))

        url = f'{self.endpoint}/v2/vectordb/collections/create'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        if not ("params" in payload and "consistencyLevel" in payload["params"]):
            if "params" not in payload:
                payload["params"] = {}
            payload["params"]["consistencyLevel"] = "Strong"
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def collection_describe(self, collection_name, db_name="default"):
        url = f'{self.endpoint}/v2/vectordb/collections/describe'
        data = {"collectionName": collection_name}
        if self.db_name is not None:
            data = {
                "collectionName": collection_name,
                "dbName": self.db_name
            }
        if db_name != "default":
            data = {
                "collectionName": collection_name,
                "dbName": db_name
            }
        response = self.post(url, headers=self.update_headers(), data=data)
        return response.json()

    def collection_drop(self, payload, db_name="default"):
        time.sleep(1)  # wait for collection drop and in case of rate limit
        url = f'{self.endpoint}/v2/vectordb/collections/drop'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def refresh_load(self, collection_name, db_name="default"):
        """Refresh load collection"""
        url = f"{self.endpoint}/v2/vectordb/collections/refresh_load"
        payload = {
            "collectionName": collection_name
        }
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def alter_collection_properties(self, collection_name, properties, db_name="default"):
        """Alter collection properties"""
        url = f"{self.endpoint}/v2/vectordb/collections/alter_properties"
        payload = {
            "collectionName": collection_name,
            "properties": properties
        }
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def drop_collection_properties(self, collection_name, delete_keys, db_name="default"):
        """Drop collection properties"""
        url = f"{self.endpoint}/v2/vectordb/collections/drop_properties"
        payload = {
            "collectionName": collection_name,
            "deleteKeys": delete_keys
        }
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def alter_field_properties(self, collection_name, field_name, field_params, db_name="default"):
        """Alter field properties"""
        url = f"{self.endpoint}/v2/vectordb/collections/fields/alter_properties"
        payload = {
            "collectionName": collection_name,
            "fieldName": field_name,
            "fieldParams": field_params
        }
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def alter_index_properties(self, collection_name, index_name, properties, db_name="default"):
        """Alter index properties"""
        url = f"{self.endpoint}/v2/vectordb/indexes/alter_properties"
        payload = {
            "collectionName": collection_name,
            "indexName": index_name,
            "properties": properties
        }
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def drop_index_properties(self, collection_name, index_name, delete_keys, db_name="default"):
        """Drop index properties"""
        url = f"{self.endpoint}/v2/vectordb/indexes/drop_properties"
        payload = {
            "collectionName": collection_name,
            "indexName": index_name,
            "deleteKeys": delete_keys
        }
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()


class PartitionClient(Requests):

    def __init__(self, endpoint, token):
        super().__init__(url=endpoint, api_key=token)
        self.endpoint = endpoint
        self.api_key = token
        self.db_name = None
        self.headers = self.update_headers()

    @classmethod
    def update_headers(cls):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}',
            'RequestId': cls.uuid
        }
        return headers

    def partition_list(self, db_name="default", collection_name=None):
        url = f'{self.endpoint}/v2/vectordb/partitions/list'
        data = {
            "collectionName": collection_name
        }
        if self.db_name is not None:
            data = {
                "dbName": self.db_name,
                "collectionName": collection_name
            }
        if db_name != "default":
            data = {
                "dbName": db_name,
                "collectionName": collection_name
            }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def partition_create(self, db_name="default", collection_name=None, partition_name=None):
        url = f'{self.endpoint}/v2/vectordb/partitions/create'
        if self.db_name is not None:
            db_name = self.db_name
        payload = {
            "dbName": db_name,
            "collectionName": collection_name,
            "partitionName": partition_name
        }
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def partition_drop(self, db_name="default", collection_name=None, partition_name=None):
        url = f'{self.endpoint}/v2/vectordb/partitions/drop'
        if self.db_name is not None:
            db_name = self.db_name
        payload = {
            "dbName": db_name,
            "collectionName": collection_name,
            "partitionName": partition_name
        }
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def partition_load(self, db_name="default", collection_name=None, partition_names=None):
        url = f'{self.endpoint}/v2/vectordb/partitions/load'
        if self.db_name is not None:
            db_name = self.db_name
        payload = {
            "dbName": db_name,
            "collectionName": collection_name,
            "partitionNames": partition_names
        }
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def partition_release(self, db_name="default", collection_name=None, partition_names=None):
        url = f'{self.endpoint}/v2/vectordb/partitions/release'
        if self.db_name is not None:
            db_name = self.db_name
        payload = {
            "dbName": db_name,
            "collectionName": collection_name,
            "partitionNames": partition_names
        }
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def partition_has(self, db_name="default", collection_name=None, partition_name=None):
        url = f'{self.endpoint}/v2/vectordb/partitions/has'
        if self.db_name is not None:
            db_name = self.db_name
        data = {
            "dbName": db_name,
            "collectionName": collection_name,
            "partitionName": partition_name
        }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def partition_stats(self, db_name="default", collection_name=None, partition_name=None):
        url = f'{self.endpoint}/v2/vectordb/partitions/get_stats'
        if self.db_name is not None:
            db_name = self.db_name
        data = {
            "dbName": db_name,
            "collectionName": collection_name,
            "partitionName": partition_name
        }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res


class UserClient(Requests):

    def __init__(self, endpoint, token):
        super().__init__(url=endpoint, api_key=token)
        self.endpoint = endpoint
        self.api_key = token
        self.db_name = None
        self.headers = self.update_headers()

    @classmethod
    def update_headers(cls):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}',
            'RequestId': cls.uuid
        }
        return headers

    def user_list(self):
        url = f'{self.endpoint}/v2/vectordb/users/list'
        response = self.post(url, headers=self.update_headers())
        res = response.json()
        return res

    def user_create(self, payload):
        url = f'{self.endpoint}/v2/vectordb/users/create'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def user_password_update(self, payload):
        url = f'{self.endpoint}/v2/vectordb/users/update_password'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def user_describe(self, user_name):
        url = f'{self.endpoint}/v2/vectordb/users/describe'
        data = {
            "userName": user_name
        }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def user_drop(self, payload):
        url = f'{self.endpoint}/v2/vectordb/users/drop'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def user_grant(self, payload):
        url = f'{self.endpoint}/v2/vectordb/users/grant_role'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def user_revoke(self, payload):
        url = f'{self.endpoint}/v2/vectordb/users/revoke_role'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res


class RoleClient(Requests):

    def __init__(self, endpoint, token):
        super().__init__(url=endpoint, api_key=token)
        self.endpoint = endpoint
        self.api_key = token
        self.db_name = None
        self.headers = self.update_headers()
        self.role_names = []

    @classmethod
    def update_headers(cls):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}',
            'RequestId': cls.uuid
        }
        return headers

    def role_list(self):
        url = f'{self.endpoint}/v2/vectordb/roles/list'
        response = self.post(url, headers=self.update_headers())
        res = response.json()
        return res

    def role_create(self, payload):
        url = f'{self.endpoint}/v2/vectordb/roles/create'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        if res["code"] == 0:
            self.role_names.append(payload["roleName"])
        return res

    def role_describe(self, role_name):
        url = f'{self.endpoint}/v2/vectordb/roles/describe'
        data = {
            "roleName": role_name
        }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def role_drop(self, payload):
        url = f'{self.endpoint}/v2/vectordb/roles/drop'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def role_grant(self, payload):
        url = f'{self.endpoint}/v2/vectordb/roles/grant_privilege'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def role_revoke(self, payload):
        url = f'{self.endpoint}/v2/vectordb/roles/revoke_privilege'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res


class IndexClient(Requests):

    def __init__(self, endpoint, token):
        super().__init__(url=endpoint, api_key=token)
        self.endpoint = endpoint
        self.api_key = token
        self.db_name = None
        self.headers = self.update_headers()

    @classmethod
    def update_headers(cls):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}',
            'RequestId': cls.uuid
        }
        return headers

    def index_create(self, payload, db_name="default"):
        url = f'{self.endpoint}/v2/vectordb/indexes/create'
        if self.db_name is not None:
            db_name = self.db_name
        payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def index_describe(self, collection_name=None, index_name=None, db_name="default",):
        url = f'{self.endpoint}/v2/vectordb/indexes/describe'
        if self.db_name is not None:
            db_name = self.db_name
        data = {
            "dbName": db_name,
            "collectionName": collection_name,
            "indexName": index_name
        }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def index_list(self, collection_name=None, db_name="default"):
        url = f'{self.endpoint}/v2/vectordb/indexes/list'
        if self.db_name is not None:
            db_name = self.db_name
        data = {
            "dbName": db_name,
            "collectionName": collection_name
        }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def index_drop(self, payload, db_name="default"):
        url = f'{self.endpoint}/v2/vectordb/indexes/drop'
        if self.db_name is not None:
            db_name = self.db_name
        payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res


class AliasClient(Requests):

    def __init__(self, endpoint, token):
        super().__init__(url=endpoint, api_key=token)
        self.endpoint = endpoint
        self.api_key = token
        self.db_name = None
        self.headers = self.update_headers()

    @classmethod
    def update_headers(cls):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}',
            'RequestId': cls.uuid
        }
        return headers

    def list_alias(self):
        url = f'{self.endpoint}/v2/vectordb/aliases/list'
        response = self.post(url, headers=self.update_headers())
        res = response.json()
        return res

    def describe_alias(self, alias_name):
        url = f'{self.endpoint}/v2/vectordb/aliases/describe'
        data = {
            "aliasName": alias_name
        }
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def alter_alias(self, payload):
        url = f'{self.endpoint}/v2/vectordb/aliases/alter'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def drop_alias(self, payload):
        url = f'{self.endpoint}/v2/vectordb/aliases/drop'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def create_alias(self, payload):
        url = f'{self.endpoint}/v2/vectordb/aliases/create'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res


class ImportJobClient(Requests):

    def __init__(self, endpoint, token):
        super().__init__(url=endpoint, api_key=token)
        self.endpoint = endpoint
        self.api_key = token
        self.db_name = None
        self.headers = self.update_headers()

    @classmethod
    def update_headers(cls):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}',
            'RequestId': cls.uuid
        }
        return headers

    def list_import_jobs(self, payload, db_name="default"):
        if self.db_name is not None:
            db_name = self.db_name
        payload["dbName"] = db_name
        if db_name is None:
            payload.pop("dbName")
        url = f'{self.endpoint}/v2/vectordb/jobs/import/list'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def create_import_jobs(self, payload, db_name="default"):
        if self.db_name is not None:
            db_name = self.db_name
        url = f'{self.endpoint}/v2/vectordb/jobs/import/create'
        payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def get_import_job_progress(self, job_id, db_name="default"):
        if self.db_name is not None:
            db_name = self.db_name
        payload = {
            "dbName": db_name,
            "jobID": job_id
        }
        if db_name is None:
            payload.pop("dbName")
        if job_id is None:
            payload.pop("jobID")
        url = f'{self.endpoint}/v2/vectordb/jobs/import/get_progress'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def wait_import_job_completed(self, job_id):
        finished = False
        t0 = time.time()
        rsp = self.get_import_job_progress(job_id)
        while not finished:
            rsp = self.get_import_job_progress(job_id)
            if rsp['data']['state'] == "Completed":
                finished = True
            time.sleep(5)
            if time.time() - t0 > 120:
                break
        return rsp, finished


class DatabaseClient(Requests):
    def __init__(self, endpoint, token):
        super().__init__(url=endpoint, api_key=token)
        self.endpoint = endpoint
        self.api_key = token
        self.headers = self.update_headers()
        self.db_name = None
        self.db_names = []  # Track created databases

    @classmethod
    def update_headers(cls):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.api_key}'
        }
        return headers

    def database_create(self, payload):
        """Create a database"""
        url = f"{self.endpoint}/v2/vectordb/databases/create"
        rsp = self.post(url, data=payload).json()
        if rsp['code'] == 0:
            self.db_name = payload['dbName']
            self.db_names.append(payload['dbName'])
        return rsp

    def database_list(self, payload):
        """List all databases"""
        url = f"{self.endpoint}/v2/vectordb/databases/list"
        return self.post(url, data=payload).json()

    def database_describe(self, payload):
        """Describe a database"""
        url = f"{self.endpoint}/v2/vectordb/databases/describe"
        return self.post(url, data=payload).json()

    def database_alter(self, payload):
        """Alter database properties"""
        url = f"{self.endpoint}/v2/vectordb/databases/alter"
        return self.post(url, data=payload).json()

    def database_drop(self, payload):
        """Drop a database"""
        url = f"{self.endpoint}/v2/vectordb/databases/drop"
        rsp = self.post(url, data=payload).json()
        if rsp['code'] == 0 and payload['dbName'] in self.db_names:
            self.db_names.remove(payload['dbName'])
        return rsp


class StorageClient():

    def __init__(self, endpoint, access_key, secret_key, bucket_name, root_path="file"):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.root_path = root_path
        self.client = Minio(
            self.endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )

    def upload_file(self, file_path, object_name):
        try:
            self.client.fput_object(self.bucket_name, object_name, file_path)
        except S3Error as exc:
            logger.error("fail to copy files to minio", exc)

    def copy_file(self, src_bucket, src_object, dst_bucket, dst_object):
        try:
            # if dst bucket not exist, create it
            if not self.client.bucket_exists(dst_bucket):
                self.client.make_bucket(dst_bucket)
            self.client.copy_object(dst_bucket, dst_object, CopySource(src_bucket, src_object))
        except S3Error as exc:
            logger.error("fail to copy files to minio", exc)

    def get_collection_binlog(self, collection_id):
        dir_list = [
            "delta_log",
            "insert_log"
        ]
        binlog_list = []
        # list objects dir/collection_id in bucket
        for dir in dir_list:
            prefix = f"{self.root_path}/{dir}/{collection_id}/"
            objects = self.client.list_objects(self.bucket_name, prefix=prefix)
            for obj in objects:
                binlog_list.append(f"{self.bucket_name}/{obj.object_name}")
        print(binlog_list)
        return binlog_list


if __name__ == "__main__":
    sc = StorageClient(
        endpoint="10.104.19.57:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket_name="milvus-bucket"
    )
    sc.get_collection_binlog("448305293023730313")
