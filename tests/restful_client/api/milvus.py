import json
import requests
import time
import uuid
from utils.util_log import test_log as logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt
from requests.exceptions import ConnectionError

def logger_request_response(response, url, tt, headers, data, str_data, str_response, method):
    if len(data) > 2000:
        data = data[:1000] + "..." + data[-1000:]
    try:
        if response.status_code == 200:
            if ('code' in response.json() and response.json()["code"] == 200) or ('Code' in response.json() and response.json()["Code"] == 0):
                logger.debug(
                    f"method: {method}, url: {url}, cost time: {tt}, header: {headers}, payload: {str_data}, response: {str_response}")
            else:
                logger.debug(
                    f"method: {method}, url: {url}, cost time: {tt}, header: {headers}, payload: {data}, response: {response.text}")
        else:
            logger.debug(
                f"method: {method}, url: {url}, cost time: {tt}, header: {headers}, payload: {data}, response: {response.text}")
    except Exception as e:
        logger.debug(
            f"method: {method}, url: {url}, cost time: {tt}, header: {headers}, payload: {data}, response: {response.text}, error: {e}")


class Requests:
    def __init__(self, url=None, api_key=None):
        self.url = url
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}',
            'RequestId': str(uuid.uuid1())
        }

    def update_headers(self):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}',
            'RequestId': str(uuid.uuid1())
        }
        return headers

    @retry(retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(3))
    def post(self, url, headers=None, data=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + '...' + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        response = requests.post(url, headers=headers, data=data)
        tt = time.time() - t0
        str_response = response.text[:200] + '...' + response.text[-200:] if len(response.text) > 400 else response.text
        logger_request_response(response, url, tt, headers, data, str_data, str_response, "post")
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
        logger_request_response(response, url, tt, headers, data, str_data, str_response, "get")
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

    def update_headers(self):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}',
            'RequestId': str(uuid.uuid1())
        }
        return headers

    def vector_search(self, payload, db_name="default", timeout=10):
        time.sleep(1)
        url = f'{self.endpoint}/vector/search'
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

    def vector_query(self, payload, db_name="default", timeout=10):
        time.sleep(1)
        url = f'{self.endpoint}/vector/query'
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
        url = f'{self.endpoint}/vector/get'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def vector_delete(self, payload, db_name="default"):
        url = f'{self.endpoint}/vector/delete'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def vector_insert(self, payload, db_name="default"):
        url = f'{self.endpoint}/vector/insert'
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
        self.headers = self.update_headers()

    def update_headers(self):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}',
            'RequestId': str(uuid.uuid1())
        }
        return headers

    def collection_list(self, db_name="default"):
        url = f'{self.endpoint}/vector/collections'
        params = {}
        if self.db_name is not None:
            params = {
                "dbName": self.db_name
            }
        if db_name != "default":
            params = {
                "dbName": db_name
            }
        response = self.get(url, headers=self.update_headers(), params=params)
        res = response.json()
        return res

    def collection_create(self, payload, db_name="default"):
        time.sleep(1)  # wait for collection created and in case of rate limit
        url = f'{self.endpoint}/vector/collections/create'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def collection_describe(self, collection_name, db_name="default"):
        url = f'{self.endpoint}/vector/collections/describe'
        params = {"collectionName": collection_name}
        if self.db_name is not None:
            params = {
                "collectionName": collection_name,
                "dbName": self.db_name
            }
        if db_name != "default":
            params = {
                "collectionName": collection_name,
                "dbName": db_name
            }
        response = self.get(url, headers=self.update_headers(), params=params)
        return response.json()

    def collection_drop(self, payload, db_name="default"):
        time.sleep(1)  # wait for collection drop and in case of rate limit
        url = f'{self.endpoint}/vector/collections/drop'
        if self.db_name is not None:
            payload["dbName"] = self.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()
