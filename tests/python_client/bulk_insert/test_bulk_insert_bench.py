import logging
import time
import pytest
from pymilvus import DataType
import numpy as np
from pathlib import Path
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.milvus_sys import MilvusSys
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
from common.bulk_insert_data import (
    prepare_bulk_insert_json_files,
    prepare_bulk_insert_new_json_files,
    prepare_bulk_insert_numpy_files,
    prepare_bulk_insert_parquet_files,
    prepare_bulk_insert_csv_files,
    DataField as df,
)
import json
import requests
import time
import uuid
from utils.util_log import test_log as logger
from minio import Minio
from minio.error import S3Error


def logger_request_response(response, url, tt, headers, data, str_data, str_response, method):
    if len(data) > 2000:
        data = data[:1000] + "..." + data[-1000:]
    try:
        if response.status_code == 200:
            if ('code' in response.json() and response.json()["code"] == 200) or (
                    'Code' in response.json() and response.json()["Code"] == 0):
                logger.debug(
                    f"\nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {str_data}, \nresponse: {str_response}")
            else:
                logger.debug(
                    f"\nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {response.text}")
        else:
            logger.debug(
                f"method: \nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {response.text}")
    except Exception as e:
        logger.debug(
            f"method: \nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {response.text}, \nerror: {e}")


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

    def post(self, url, headers=None, data=None, params=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + '...' + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        response = requests.post(url, headers=headers, data=data, params=params)
        tt = time.time() - t0
        str_response = response.text[:200] + '...' + response.text[-200:] if len(response.text) > 400 else response.text
        logger_request_response(response, url, tt, headers, data, str_data, str_response, "post")
        return response

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


class ImportJobClient(Requests):

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

    def list_import_jobs(self, payload, db_name="default"):
        payload["dbName"] = db_name
        data = payload
        url = f'{self.endpoint}/v2/vectordb/jobs/import/list'
        response = self.post(url, headers=self.update_headers(), data=data)
        res = response.json()
        return res

    def create_import_jobs(self, payload):
        url = f'{self.endpoint}/v2/vectordb/jobs/import/create'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def get_import_job_progress(self, task_id):
        payload = {
            "jobId": task_id
        }
        url = f'{self.endpoint}/v2/vectordb/jobs/import/get_progress'
        response = self.post(url, headers=self.update_headers(), data=payload)
        res = response.json()
        return res

    def wait_import_job_completed(self, task_id_list, timeout=1800):
        success = False
        success_states = {}
        t0 = time.time()
        while time.time() - t0 < timeout:
            for task_id in task_id_list:
                res = self.get_import_job_progress(task_id)
                if res['data']['state'] == "Completed":
                    success_states[task_id] = True
                else:
                    success_states[task_id] = False
                time.sleep(5)
            # all task success then break
            if all(success_states.values()):
                success = True
                break
        states = []
        for task_id in task_id_list:
            res = self.get_import_job_progress(task_id)
            states.append({
                "task_id": task_id,
                "state": res['data']
            })
        return success, states


default_vec_only_fields = [df.vec_field]
default_multi_fields = [
    df.vec_field,
    df.int_field,
    df.string_field,
    df.bool_field,
    df.float_field,
    df.array_int_field
]
default_vec_n_int_fields = [df.vec_field, df.int_field, df.array_int_field]


# milvus_ns = "chaos-testing"
base_dir = "/tmp/bulk_insert_data"


def entity_suffix(entities):
    if entities // 1000000 > 0:
        suffix = f"{entities // 1000000}m"
    elif entities // 1000 > 0:
        suffix = f"{entities // 1000}k"
    else:
        suffix = f"{entities}"
    return suffix


class TestcaseBaseBulkInsert(TestcaseBase):
    import_job_client = None
    @pytest.fixture(scope="function", autouse=True)
    def init_minio_client(self, minio_host):
        Path("/tmp/bulk_insert_data").mkdir(parents=True, exist_ok=True)
        self._connect()
        self.milvus_sys = MilvusSys(alias='default')
        ms = MilvusSys()
        minio_port = "9000"
        self.minio_endpoint = f"{minio_host}:{minio_port}"
        self.bucket_name = ms.data_nodes[0]["infos"]["system_configurations"][
            "minio_bucket_name"
        ]

    @pytest.fixture(scope="function", autouse=True)
    def init_import_client(self, host, port, user, password):
        self.import_job_client = ImportJobClient(f"http://{host}:{port}", f"{user}:{password}")


class TestBulkInsertPerf(TestcaseBaseBulkInsert):

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("file_size", [1, 10, 15])  # file size in GB
    @pytest.mark.parametrize("file_nums", [1])
    @pytest.mark.parametrize("array_len", [100])
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    def test_bulk_insert_all_field_with_parquet(self, auto_id, dim, file_size, file_nums, array_len, enable_dynamic_field):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=200),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_parquet_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=3000,
            dim=dim,
            data_fields=data_fields,
            file_size=file_size,
            row_group_size=None,
            file_nums=file_nums,
            array_length=array_len,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)
        payload = {
            "collectionName": c_name,
            "files": [files],
        }

        # import data
        payload = {
            "collectionName": c_name,
            "files": [files],
        }
        t0 = time.time()
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id_list = [rsp["data"]["jobId"]]
        logging.info(f"bulk insert job ids:{job_id_list}")
        success, states = self.import_job_client.wait_import_job_completed(job_id_list, timeout=1800)
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("file_size", [1, 10, 15])  # file size in GB
    @pytest.mark.parametrize("file_nums", [1])
    @pytest.mark.parametrize("array_len", [100])
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    def test_bulk_insert_all_field_with_json(self, auto_id, dim, file_size, file_nums, array_len, enable_dynamic_field):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=200),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_new_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=3000,
            dim=dim,
            data_fields=data_fields,
            file_size=file_size,
            file_nums=file_nums,
            array_length=array_len,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        payload = {
            "collectionName": c_name,
            "files": [files],
        }
        t0 = time.time()
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id_list = [rsp["data"]["jobId"]]
        logging.info(f"bulk insert job ids:{job_id_list}")
        success, states = self.import_job_client.wait_import_job_completed(job_id_list, timeout=1800)
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("file_size", [1, 10, 15])  # file size in GB
    @pytest.mark.parametrize("file_nums", [1])
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    def test_bulk_insert_all_field_with_numpy(self, auto_id, dim, file_size, file_nums, enable_dynamic_field):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=3000,
            dim=dim,
            data_fields=data_fields,
            file_size=file_size,
            file_nums=file_nums,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        payload = {
            "collectionName": c_name,
            "files": [files],
        }
        t0 = time.time()
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id_list = [rsp["data"]["jobId"]]
        logging.info(f"bulk insert job ids:{job_id_list}")
        success, states = self.import_job_client.wait_import_job_completed(job_id_list, timeout=1800)
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success
