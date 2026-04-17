import csv
import json
import random
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pymilvus import Collection, utility
from sklearn import preprocessing

from base.testbase import TestBase
from utils.util_log import test_log as logger
from utils.utils import gen_collection_name

IMPORT_TIMEOUT = 360


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.float32):
            return float(obj)
        return super().default(obj)


@pytest.mark.BulkInsert
class TestCreateImportJob(TestBase):
    @pytest.mark.parametrize("insert_num", [3000])
    @pytest.mark.parametrize("import_task_num", [2])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    @pytest.mark.parametrize("file_format", ["parquet", "json"])
    def test_import_job_e2e(
        self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field, file_format
    ):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "word_count",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {
                        "fieldName": "book_describe",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256"},
                        "nullable": True,
                    },
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)
        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp["$meta"] = {}
                tmp["$meta"].update({f"dynamic_field_{i}": i})
            data.append(tmp)
        # dump data to file
        file_name = (
            f"bulk_insert_data_{uuid4()}.json" if file_format == "json" else f"bulk_insert_data_{uuid4()}.parquet"
        )
        file_path = f"/tmp/{file_name}"
        if file_format == "json":
            with open(file_path, "w") as f:
                json.dump(data, f, cls=NumpyEncoder)
        if file_format == "parquet":
            pa_schema = pa.schema(
                [
                    ("word_count", pa.int64(), False),  # not nullable
                    (
                        "book_describe",
                        pa.string(),
                        False,
                    ),  # pecifically set as not nullable to verify a certain corner case
                    ("book_intro", pa.list_(pa.float32()), False),  # not nullable
                ]
            )
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df, schema=pa_schema)
            pq.write_table(table, file_path)
            schema_info = pq.read_schema(file_path)
            logger.info(f"parquet schema: {schema_info}")
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        for _i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp["data"]["records"]:
            task_id = task["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        res = c.query(
            expr="",
            output_fields=["count(*)"],
        )
        assert res[0]["count(*)"] == insert_num * import_task_num
        # query data
        payload = {
            "collectionName": name,
            "filter": "book_id > 0",
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp["code"] == 0

    @pytest.mark.parametrize("insert_num", [3000])
    @pytest.mark.parametrize("import_task_num", [2])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    def test_import_job_with_coo_format(self, insert_num, import_task_num, auto_id, is_partition_key):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "word_count",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "SparseFloatVector"},
                ],
            },
            "indexParams": [
                {
                    "fieldName": "book_intro",
                    "indexName": "sparse_float_vector_index",
                    "metricType": "IP",
                    "params": {"index_type": "SPARSE_INVERTED_INDEX", "drop_ratio_build": "0.2"},
                },
            ],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)
        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": {"values": [random.random() for i in range(dim)], "indices": [i**2 for i in range(dim)]},
            }
            if not auto_id:
                tmp["book_id"] = i
            data.append(tmp)
        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        df = pd.DataFrame(data)
        logger.info(df)
        df.to_parquet(file_path, engine="pyarrow")
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        for _i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp["data"]["records"]:
            task_id = task["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        res = c.query(
            expr="",
            output_fields=["count(*)"],
        )
        assert res[0]["count(*)"] == insert_num * import_task_num
        # query data
        payload = {
            "collectionName": name,
            "filter": "book_id > 0",
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp["code"] == 0

    @pytest.mark.parametrize("insert_num", [3000])
    @pytest.mark.parametrize("import_task_num", [2])
    @pytest.mark.parametrize(
        "auto_id",
        [
            True,
        ],
    )
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    def test_import_with_longer_text_than_max_length(
        self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field
    ):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "word_count",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)
        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}" * 256,
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp.update({f"dynamic_field_{i}": i})
            data.append(tmp)
        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        for _i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp["data"]["records"]:
            task_id = task["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp["data"]["state"] == "Completed":
                    raise AssertionError("import job should not be completed")
                if rsp["data"]["state"] == "Failed":
                    assert True
                    finished = True
                logger.debug(f"job progress: {rsp}")
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")

    @pytest.mark.parametrize("insert_num", [5000])
    @pytest.mark.parametrize("import_task_num", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    def test_import_job_with_db(self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field):
        self.create_database(db_name="test_job")
        self.update_database(db_name="test_job")
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "word_count",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)
        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp.update({f"dynamic_field_{i}": i})
            data.append(tmp)
        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        for _i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp["data"]["records"]:
            task_id = task["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        res = c.query(
            expr="",
            output_fields=["count(*)"],
        )
        assert res[0]["count(*)"] == insert_num * import_task_num
        # query data
        payload = {
            "collectionName": name,
            "filter": "book_id > 0",
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp["code"] == 0

    @pytest.mark.parametrize("insert_num", [5000])
    @pytest.mark.parametrize("import_task_num", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [False])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    def test_import_job_with_partition(
        self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field
    ):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "word_count",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp.update({f"dynamic_field_{i}": i})
            data.append(tmp)
        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)
        # create partition
        partition_name = "test_partition"
        rsp = self.partition_client.partition_create(collection_name=name, partition_name=partition_name)
        # create import job
        payload = {
            "collectionName": name,
            "partitionName": partition_name,
            "files": [[file_name]],
        }
        for _i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp["data"]["records"]:
            task_id = task["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        res = c.query(
            expr="",
            output_fields=["count(*)"],
        )
        logger.info(f"count in collection: {res}")
        assert res[0]["count(*)"] == insert_num * import_task_num
        res = c.query(
            expr="",
            partition_names=[partition_name],
            output_fields=["count(*)"],
        )
        logger.info(f"count in partition {[partition_name]}: {res}")
        assert res[0]["count(*)"] == insert_num * import_task_num
        # query data
        payload = {
            "collectionName": name,
            "filter": "book_id > 0",
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp["code"] == 0

    def test_job_import_multi_json_file(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # upload file to storage
        file_nums = 2
        file_names = []
        for file_num in range(file_nums):
            data = [
                {
                    "book_id": i,
                    "word_count": i,
                    "book_describe": f"book_{i}",
                    "book_intro": [np.float32(random.random()) for _ in range(dim)],
                }
                for i in range(1000 * file_num, 1000 * (file_num + 1))
            ]

            # dump data to file
            file_name = f"bulk_insert_data_{file_num}_{uuid4()}.json"
            file_path = f"/tmp/{file_name}"
            # create dir for file path
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w") as f:
                json.dump(data, f, cls=NumpyEncoder)
            # upload file to minio storage
            self.storage_client.upload_file(file_path, file_name)
            file_names.append([file_name])

        # create import job
        payload = {
            "collectionName": name,
            "files": file_names,
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for job in rsp["data"]["records"]:
            job_id = job["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        time.sleep(10)
        # assert data count
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        assert c.num_entities == 2000
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp["data"]) == 100

    def test_job_import_multi_parquet_file(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # upload file to storage
        file_nums = 2
        file_names = []
        for file_num in range(file_nums):
            data = [
                {
                    "book_id": i,
                    "word_count": i,
                    "book_describe": f"book_{i}",
                    "book_intro": [np.float32(random.random()) for _ in range(dim)],
                }
                for i in range(1000 * file_num, 1000 * (file_num + 1))
            ]

            # dump data to file
            file_name = f"bulk_insert_data_{file_num}_{uuid4()}.parquet"
            file_path = f"/tmp/{file_name}"
            # create dir for file path
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame(data)
            df.to_parquet(file_path, index=False)
            # upload file to minio storage
            self.storage_client.upload_file(file_path, file_name)
            file_names.append([file_name])

        # create import job
        payload = {
            "collectionName": name,
            "files": file_names,
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for job in rsp["data"]["records"]:
            job_id = job["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        time.sleep(10)
        # assert data count
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        assert c.num_entities == 2000
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp["data"]) == 100

    def test_job_import_multi_numpy_file(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # upload file to storage
        file_nums = 2
        file_names = []
        for file_num in range(file_nums):
            data = [
                {
                    "book_id": i,
                    "word_count": i,
                    "book_describe": f"book_{i}",
                    "book_intro": [np.float32(random.random()) for _ in range(dim)],
                }
                for i in range(1000 * file_num, 1000 * (file_num + 1))
            ]

            file_list = []
            # dump data to file
            file_dir = f"bulk_insert_data_{file_num}_{uuid4()}"
            base_file_path = f"/tmp/{file_dir}"
            df = pd.DataFrame(data)
            # each column is a list and convert to a npy file
            for column in df.columns:
                file_path = f"{base_file_path}/{column}.npy"
                # create dir for file path
                Path(file_path).parent.mkdir(parents=True, exist_ok=True)
                file_name = f"{file_dir}/{column}.npy"
                np.save(file_path, np.array(df[column].values.tolist()))
                # upload file to minio storage
                self.storage_client.upload_file(file_path, file_name)
                file_list.append(file_name)
            file_names.append(file_list)
        # create import job
        payload = {
            "collectionName": name,
            "files": file_names,
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for job in rsp["data"]["records"]:
            job_id = job["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        time.sleep(10)
        # assert data count
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        assert c.num_entities == 2000
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp["data"]) == 100

    def test_job_import_multi_file_type(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # upload file to storage
        file_nums = 2
        file_names = []

        # numpy file
        for file_num in range(file_nums):
            data = [
                {
                    "book_id": i,
                    "word_count": i,
                    "book_describe": f"book_{i}",
                    "book_intro": [np.float32(random.random()) for _ in range(dim)],
                }
                for i in range(1000 * file_num, 1000 * (file_num + 1))
            ]

            file_list = []
            # dump data to file
            file_dir = f"bulk_insert_data_{file_num}_{uuid4()}"
            base_file_path = f"/tmp/{file_dir}"
            df = pd.DataFrame(data)
            # each column is a list and convert to a npy file
            for column in df.columns:
                file_path = f"{base_file_path}/{column}.npy"
                # create dir for file path
                Path(file_path).parent.mkdir(parents=True, exist_ok=True)
                file_name = f"{file_dir}/{column}.npy"
                np.save(file_path, np.array(df[column].values.tolist()))
                # upload file to minio storage
                self.storage_client.upload_file(file_path, file_name)
                file_list.append(file_name)
            file_names.append(file_list)
        # parquet file
        for file_num in range(2, file_nums + 2):
            data = [
                {
                    "book_id": i,
                    "word_count": i,
                    "book_describe": f"book_{i}",
                    "book_intro": [np.float32(random.random()) for _ in range(dim)],
                }
                for i in range(1000 * file_num, 1000 * (file_num + 1))
            ]

            # dump data to file
            file_name = f"bulk_insert_data_{file_num}_{uuid4()}.parquet"
            file_path = f"/tmp/{file_name}"
            # create dir for file path
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame(data)
            df.to_parquet(file_path, index=False)
            # upload file to minio storage
            self.storage_client.upload_file(file_path, file_name)
            file_names.append([file_name])
        # json file
        for file_num in range(4, file_nums + 4):
            data = [
                {
                    "book_id": i,
                    "word_count": i,
                    "book_describe": f"book_{i}",
                    "book_intro": [np.float32(random.random()) for _ in range(dim)],
                }
                for i in range(1000 * file_num, 1000 * (file_num + 1))
            ]

            # dump data to file
            file_name = f"bulk_insert_data_{file_num}_{uuid4()}.json"
            file_path = f"/tmp/{file_name}"
            with open(file_path, "w") as f:
                json.dump(data, f, cls=NumpyEncoder)
            # upload file to minio storage
            self.storage_client.upload_file(file_path, file_name)
            file_names.append([file_name])

        # create import job
        payload = {
            "collectionName": name,
            "files": file_names,
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for job in rsp["data"]["records"]:
            job_id = job["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        time.sleep(10)
        # assert data count
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        assert c.num_entities == 6000
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp["data"]) == 100

    @pytest.mark.parametrize("insert_round", [2])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.skip(reason="default storage is v3, cannot use collection to create v2 import")
    def test_job_import_binlog_file_type(self, nb, dim, insert_round, auto_id, is_partition_key, enable_dynamic_schema):
        """
        Insert a vector with a simple payload
        """
        # create a collection
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_schema,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "user_id",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "bool", "dataType": "Bool", "elementTypeParams": {}},
                    {"fieldName": "json", "dataType": "JSON", "elementTypeParams": {}},
                    {
                        "fieldName": "int_array",
                        "dataType": "Array",
                        "elementDataType": "Int64",
                        "elementTypeParams": {"max_capacity": "1024"},
                    },
                    {
                        "fieldName": "varchar_array",
                        "dataType": "Array",
                        "elementDataType": "VarChar",
                        "elementTypeParams": {"max_capacity": "1024", "max_length": "256"},
                    },
                    {
                        "fieldName": "bool_array",
                        "dataType": "Array",
                        "elementDataType": "Bool",
                        "elementTypeParams": {"max_capacity": "1024"},
                    },
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "image_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [
                {"fieldName": "text_emb", "indexName": "text_emb", "metricType": "L2"},
                {"fieldName": "image_emb", "indexName": "image_emb", "metricType": "L2"},
            ],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)
        # create restore collection
        restore_collection_name = f"{name}_restore"
        payload["collectionName"] = restore_collection_name
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp["code"] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "bool": random.choice([True, False]),
                        "json": {"key": i},
                        "int_array": [i],
                        "varchar_array": [f"varchar_{i}"],
                        "bool_array": [random.choice([True, False])],
                        "text_emb": preprocessing.normalize(
                            [np.array([np.float32(random.random()) for _ in range(dim)])]
                        )[0].tolist(),
                        "image_emb": preprocessing.normalize(
                            [np.array([np.float32(random.random()) for _ in range(dim)])]
                        )[0].tolist(),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "bool": random.choice([True, False]),
                        "json": {"key": i},
                        "int_array": [i],
                        "varchar_array": [f"varchar_{i}"],
                        "bool_array": [random.choice([True, False])],
                        "text_emb": preprocessing.normalize(
                            [np.array([np.float32(random.random()) for _ in range(dim)])]
                        )[0].tolist(),
                        "image_emb": preprocessing.normalize(
                            [np.array([np.float32(random.random()) for _ in range(dim)])]
                        )[0].tolist(),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp["code"] == 0
            assert rsp["data"]["insertCount"] == nb
        # flush data to generate binlog file
        c = Collection(name)
        c.flush()
        # wait for index building to complete, which ensures sort compaction is done
        for field_name in ["text_emb", "image_emb"]:
            utility.wait_for_index_building_complete(name, field_name)

        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 50})
        assert rsp["code"] == 0
        assert len(rsp["data"]) == 50
        # get collection id
        c = Collection(name)
        res = c.describe()
        collection_id = res["collection_id"]
        # get binlog files
        binlog_files = self.storage_client.get_collection_binlog(collection_id)
        files = []
        for file in binlog_files:
            files.append([file, ""])

        # create import job
        payload = {
            "collectionName": restore_collection_name,
            "files": files,
            "options": {"backup": "true", "storage_version": "2"},
        }
        if is_partition_key:
            payload["partitionName"] = "_default_0"
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] == 0
        # list import job
        payload = {
            "collectionName": restore_collection_name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for job in rsp["data"]["records"]:
            job_id = job["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        time.sleep(10)
        c_restore = Collection(restore_collection_name)
        # since we import both original and sorted segments, the number of entities should be 2x
        time.sleep(10)
        logger.info(f"c.num_entities: {c.num_entities}, c_restore.num_entities: {c_restore.num_entities}")
        assert c.num_entities * 2 == c_restore.num_entities

    def test_import_json_with_nullable_fields(self):
        """Test JSON import with nullable and default_value fields - fields should be auto-filled"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        default_int_value = 999
        default_varchar_value = "default_text"
        varchar_max_length = 256
        required_field_base = 100

        # Create collection with nullable and default_value fields
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "nullable_int", "dataType": "Int64", "nullable": True},
                    {"fieldName": "default_int", "dataType": "Int64", "defaultValue": default_int_value},
                    {
                        "fieldName": "nullable_varchar",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": str(varchar_max_length)},
                        "nullable": True,
                    },
                    {
                        "fieldName": "default_varchar",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": str(varchar_max_length)},
                        "defaultValue": default_varchar_value,
                    },
                    {"fieldName": "required_field", "dataType": "Int64"},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create JSON data with missing nullable and default fields
        data = []
        for i in range(num_entities):
            if i % 2 == 0:
                # Missing nullable and default fields (should be auto-filled)
                data.append(
                    {
                        "id": i,
                        "vector": [np.float32(random.random()) for _ in range(dim)],
                        "required_field": required_field_base + i,
                    }
                )
            else:
                # Provide all fields explicitly
                data.append(
                    {
                        "id": i,
                        "vector": [np.float32(random.random()) for _ in range(dim)],
                        "nullable_int": 200 + i,
                        "default_int": 300 + i,
                        "nullable_varchar": f"provided_value_{i}",
                        "default_varchar": f"custom_value_{i}",
                        "required_field": required_field_base + i,
                    }
                )

        # Save to JSON file
        file_name = f"test_nullable_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        # Import the file
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait for import to complete
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Query and verify nullable/default fields were filled correctly
        # Check even id (missing fields)
        res = c.query(expr="id == 0", output_fields=["*"])
        assert res[0]["nullable_int"] is None
        assert res[0]["default_int"] == default_int_value
        assert res[0]["nullable_varchar"] is None
        assert res[0]["default_varchar"] == default_varchar_value

        # Check odd id (provided fields)
        res = c.query(expr="id == 1", output_fields=["*"])
        assert res[0]["nullable_int"] == 201
        assert res[0]["default_int"] == 301
        assert res[0]["nullable_varchar"] == "provided_value_1"
        assert res[0]["default_varchar"] == "custom_value_1"

    def test_import_json_with_function_output_field(self):
        """Test JSON import with BM25 function output field - should fail with error"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        text_max_length = 256

        # Create collection with BM25 function output field
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": str(text_max_length), "enable_analyzer": True},
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "bm25_function",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_idx", "metricType": "BM25"},
                {"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"},
            ],
        }

        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create JSON data that includes the function output field
        data = []
        for i in range(num_entities):
            data.append(
                {
                    "id": i,
                    "text": f"sample text for BM25 document {i}",
                    "sparse_vector": {"indices": [1, 2, 3], "values": [0.1, 0.2, 0.3]},  # This should cause error
                    "dense_vector": [np.float32(random.random()) for _ in range(dim)],
                }
            )

        # Save to JSON file
        file_name = f"test_function_output_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        # Import should fail
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait and verify import fails
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert (
                    "not allowed to provide data for bm25 function output field" in reason
                    or "output by function" in reason
                    or "function" in reason
                )
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed for BM25 function output field")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_json_extra_fields_without_dynamic(self):
        """Test JSON import with extra fields when dynamic field is disabled - should be ignored"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        title_max_length = 256

        # Create collection without dynamic field
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {
                        "fieldName": "title",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": str(title_max_length)},
                    },
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create JSON data with extra fields
        data = []
        for i in range(num_entities):
            data.append(
                {
                    "id": i,
                    "vector": [np.float32(random.random()) for _ in range(dim)],
                    "title": f"Document {i}",
                    "extra_field1": f"ignored_value_{i}",
                    "extra_field2": 12345 + i,
                    "extra_field3": {"nested": f"data_{i}"},
                    "another_extra": [i, i + 1, i + 2],
                }
            )

        # Save to JSON file
        file_name = f"test_extra_fields_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        # Import should succeed, ignoring extra fields
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait for import to complete
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data imported successfully
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Verify only defined fields exist (sample check)
        res = c.query(expr="id in [0, 1, 2]", output_fields=["*"])
        for item in res:
            assert "id" in item
            assert "title" in item
            assert "extra_field1" not in item
            assert "extra_field2" not in item
            assert "another_extra" not in item

    def test_import_csv_nullable_fields_missing_in_header(self):
        """Test CSV import where nullable fields are missing from header - should succeed"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        text_max_length = 256
        default_value = 100

        # Create collection with nullable fields
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {
                        "fieldName": "required_text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": str(text_max_length)},
                    },
                    {"fieldName": "nullable_int", "dataType": "Int64", "nullable": True},
                    {"fieldName": "default_value", "dataType": "Int64", "defaultValue": default_value},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create CSV file without nullable and default_value columns
        file_name = f"test_csv_nullable_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "vector", "required_text"]  # Missing nullable_int and default_value
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                writer.writerow({"id": i, "vector": vector_str, "required_text": f"text_{i}"})

        self.storage_client.upload_file(file_path, file_name)

        # Import should succeed
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait for import to complete
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Verify nullable fields are null and default fields have default value
        res = c.query(expr="id == 0", output_fields=["*"])
        assert res[0]["nullable_int"] is None
        assert res[0]["default_value"] == default_value

    def test_import_csv_with_function_output_field(self):
        """Test CSV import with BM25 function output field - should fail with error"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        text_max_length = 256

        # Create collection with BM25 function
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": str(text_max_length), "enable_analyzer": True},
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "bm25_function",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_idx", "metricType": "BM25"},
                {"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"},
            ],
        }

        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create CSV file that includes the function output field
        file_name = f"test_csv_function_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            # Include sparse_vector which is a function output field
            fieldnames = ["id", "text", "sparse_vector", "vector"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                writer.writerow(
                    {
                        "id": str(i),
                        "text": f"sample document {i}",
                        "sparse_vector": '{"indices": [1, 2], "values": [0.1, 0.2]}',  # This should cause error
                        "vector": vector_str,
                    }
                )

        self.storage_client.upload_file(file_path, file_name)

        # Import should fail
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait and verify import fails
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert (
                    "not allowed to provide data for bm25 function output field" in reason
                    or "output by function" in reason
                    or "function" in reason
                )
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed for BM25 function output field in CSV")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_json_with_non_bm25_function_output_rejected_by_default(self, tei_endpoint):
        """Test JSON import with non-BM25 function output field is rejected by default"""
        name = gen_collection_name()
        dim = 768
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "text_emb",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["dense_vector"],
                        "params": {"provider": "TEI", "endpoint": tei_endpoint},
                    }
                ],
            },
            "indexParams": [{"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # Create JSON data with non-BM25 function output field
        data = []
        for i in range(num_entities):
            data.append(
                {"id": i, "text": f"sample text {i}", "dense_vector": [np.float32(random.random()) for _ in range(dim)]}
            )

        file_name = f"test_non_bm25_fn_output_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail because allowInsertNonBM25FunctionOutputs is not enabled
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "function output" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError(
                    "Import should have failed for non-BM25 function output field without property enabled"
                )
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_json_with_non_bm25_function_output_allowed_with_property(self, tei_endpoint):
        """Test JSON import with non-BM25 function output field succeeds after enabling property"""
        name = gen_collection_name()
        dim = 768
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "text_emb",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["dense_vector"],
                        "params": {"provider": "TEI", "endpoint": tei_endpoint},
                    }
                ],
            },
            "indexParams": [{"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # Enable allowInsertNonBM25FunctionOutputs
        rsp = self.collection_client.alter_collection_properties(
            name, {"collection.function.allowInsertNonBM25FunctionOutputs": "true"}
        )
        assert rsp["code"] == 0

        # Create JSON data with function output field
        data = []
        for i in range(num_entities):
            data.append(
                {"id": i, "text": f"sample text {i}", "dense_vector": [np.float32(random.random()) for _ in range(dim)]}
            )

        file_name = f"test_non_bm25_fn_output_allowed_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

    def test_import_json_without_non_bm25_function_output_field(self, tei_endpoint):
        """Test JSON import without non-BM25 function output field - should succeed with auto-generation"""
        name = gen_collection_name()
        dim = 768
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "text_emb",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["dense_vector"],
                        "params": {"provider": "TEI", "endpoint": tei_endpoint},
                    }
                ],
            },
            "indexParams": [{"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # Create JSON data WITHOUT function output field - should be auto-generated
        data = []
        for i in range(num_entities):
            data.append({"id": i, "text": f"sample text {i}"})

        file_name = f"test_json_no_non_bm25_fn_output_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed with TextEmbedding auto-generating dense vectors
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

    def test_import_csv_with_non_bm25_function_output_rejected_by_default(self, tei_endpoint):
        """Test CSV import with non-BM25 function output field is rejected by default"""
        name = gen_collection_name()
        dim = 768
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "text_emb",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["dense_vector"],
                        "params": {"provider": "TEI", "endpoint": tei_endpoint},
                    }
                ],
            },
            "indexParams": [{"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # Create CSV file with function output field
        file_name = f"test_csv_non_bm25_fn_output_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "text", "dense_vector"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                writer.writerow({"id": str(i), "text": f"sample text {i}", "dense_vector": vector_str})

        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail because allowInsertNonBM25FunctionOutputs is not enabled
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "function output" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError(
                    "Import should have failed for non-BM25 function output field without property enabled"
                )
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_csv_with_non_bm25_function_output_allowed_with_property(self, tei_endpoint):
        """Test CSV import with non-BM25 function output field succeeds after enabling property"""
        name = gen_collection_name()
        dim = 768
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "text_emb",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["dense_vector"],
                        "params": {"provider": "TEI", "endpoint": tei_endpoint},
                    }
                ],
            },
            "indexParams": [{"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # Enable allowInsertNonBM25FunctionOutputs
        rsp = self.collection_client.alter_collection_properties(
            name, {"collection.function.allowInsertNonBM25FunctionOutputs": "true"}
        )
        assert rsp["code"] == 0

        # Create CSV file with function output field
        file_name = f"test_csv_non_bm25_fn_allowed_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "text", "dense_vector"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                writer.writerow({"id": str(i), "text": f"sample text {i}", "dense_vector": vector_str})

        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

    def test_import_csv_without_non_bm25_function_output_field(self, tei_endpoint):
        """Test CSV import without non-BM25 function output field - should succeed with auto-generation"""
        name = gen_collection_name()
        dim = 768
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "text_emb",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["dense_vector"],
                        "params": {"provider": "TEI", "endpoint": tei_endpoint},
                    }
                ],
            },
            "indexParams": [{"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # Create CSV file WITHOUT function output field
        file_name = f"test_csv_no_non_bm25_fn_output_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "text"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                writer.writerow({"id": str(i), "text": f"sample text {i}"})

        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed with TextEmbedding auto-generating dense vectors
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

    def test_import_parquet_with_non_bm25_function_output_rejected_by_default(self, tei_endpoint):
        """Test Parquet import with non-BM25 function output field is rejected by default"""
        name = gen_collection_name()
        dim = 768
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "text_emb",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["dense_vector"],
                        "params": {"provider": "TEI", "endpoint": tei_endpoint},
                    }
                ],
            },
            "indexParams": [{"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # Create Parquet file with function output field
        data = {
            "id": list(range(num_entities)),
            "text": [f"sample text {i}" for i in range(num_entities)],
            "dense_vector": [[np.float32(random.random()) for _ in range(dim)] for _ in range(num_entities)],
        }
        df = pd.DataFrame(data)

        pa_schema = pa.schema([("id", pa.int64()), ("text", pa.string()), ("dense_vector", pa.list_(pa.float32()))])
        file_name = f"test_parquet_non_bm25_fn_output_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pandas(df, schema=pa_schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail because allowInsertNonBM25FunctionOutputs is not enabled
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "function output" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError(
                    "Import should have failed for non-BM25 function output field without property enabled"
                )
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_parquet_with_non_bm25_function_output_allowed_with_property(self, tei_endpoint):
        """Test Parquet import with non-BM25 function output field succeeds after enabling property"""
        name = gen_collection_name()
        dim = 768
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "text_emb",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["dense_vector"],
                        "params": {"provider": "TEI", "endpoint": tei_endpoint},
                    }
                ],
            },
            "indexParams": [{"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # Enable allowInsertNonBM25FunctionOutputs
        rsp = self.collection_client.alter_collection_properties(
            name, {"collection.function.allowInsertNonBM25FunctionOutputs": "true"}
        )
        assert rsp["code"] == 0

        # Create Parquet file with function output field
        data = {
            "id": list(range(num_entities)),
            "text": [f"sample text {i}" for i in range(num_entities)],
            "dense_vector": [[np.float32(random.random()) for _ in range(dim)] for _ in range(num_entities)],
        }
        df = pd.DataFrame(data)

        pa_schema = pa.schema([("id", pa.int64()), ("text", pa.string()), ("dense_vector", pa.list_(pa.float32()))])
        file_name = f"test_parquet_non_bm25_fn_allowed_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pandas(df, schema=pa_schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

    def test_import_parquet_without_non_bm25_function_output_field(self, tei_endpoint):
        """Test Parquet import without non-BM25 function output field - should succeed with auto-generation"""
        name = gen_collection_name()
        dim = 768
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "text_emb",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["dense_vector"],
                        "params": {"provider": "TEI", "endpoint": tei_endpoint},
                    }
                ],
            },
            "indexParams": [{"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # Create Parquet file WITHOUT function output field
        data = {"id": list(range(num_entities)), "text": [f"sample text {i}" for i in range(num_entities)]}
        df = pd.DataFrame(data)

        pa_schema = pa.schema([("id", pa.int64()), ("text", pa.string())])
        file_name = f"test_parquet_no_non_bm25_fn_output_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pandas(df, schema=pa_schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed with TextEmbedding auto-generating dense vectors
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

    def test_import_json_bm25_output_rejected_even_with_property(self):
        """Test JSON import with BM25 function output field is still rejected even with property enabled"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256", "enable_analyzer": True},
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "bm25_function",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_idx", "metricType": "BM25"},
                {"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"},
            ],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Enable property — should NOT affect BM25 output rejection
        rsp = self.collection_client.alter_collection_properties(
            name, {"collection.function.allowInsertNonBM25FunctionOutputs": "true"}
        )
        assert rsp["code"] == 0

        # Create JSON data with BM25 function output field
        data = []
        for i in range(num_entities):
            data.append(
                {
                    "id": i,
                    "text": f"sample text for BM25 {i}",
                    "sparse_vector": {"indices": [1, 2, 3], "values": [0.1, 0.2, 0.3]},
                    "dense_vector": [np.float32(random.random()) for _ in range(dim)],
                }
            )

        file_name = f"test_bm25_output_with_property_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should still fail for BM25 output
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "bm25" in reason or "function output" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed for BM25 function output field")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_json_without_function_output_field(self):
        """Test JSON import without function output field - BM25 should auto-generate"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256", "enable_analyzer": True},
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "bm25_function",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_idx", "metricType": "BM25"},
                {"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"},
            ],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create JSON data WITHOUT function output field
        data = []
        for i in range(num_entities):
            data.append(
                {
                    "id": i,
                    "text": f"sample text for document {i}",
                    "dense_vector": [np.float32(random.random()) for _ in range(dim)],
                }
            )

        file_name = f"test_no_fn_output_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed with BM25 auto-generating sparse vectors
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

    def test_import_csv_default_value_with_nullkey(self):
        """Test CSV import where default_value field has nullkey - should use default value"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        default_value = 999

        # Create collection with default_value field
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "default_int", "dataType": "Int64", "defaultValue": default_value},
                    {"fieldName": "nullable_int", "dataType": "Int64", "nullable": True},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create CSV with nullkey for default_value field
        file_name = f"test_csv_default_nullkey_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "vector", "default_int", "nullable_int"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Write rows with nullkey and provided values
            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                if i % 2 == 0:
                    # Use nullkey (empty string)
                    writer.writerow(
                        {
                            "id": str(i),
                            "vector": vector_str,
                            "default_int": "",  # Empty string as nullkey
                            "nullable_int": "",
                        }
                    )
                else:
                    # Provide explicit values
                    writer.writerow(
                        {
                            "id": str(i),
                            "vector": vector_str,
                            "default_int": str(123 + i),  # Provided value
                            "nullable_int": str(456 + i),
                        }
                    )

        self.storage_client.upload_file(file_path, file_name)

        # Import
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait for import
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Verify default_value field uses default when nullkey, nullable field is null
        res = c.query(expr="id == 0", output_fields=["*"])
        assert res[0]["default_int"] == default_value  # Should use default value
        assert res[0]["nullable_int"] is None  # Should be null

        res = c.query(expr="id == 1", output_fields=["*"])
        assert res[0]["default_int"] == 124  # Should use provided value (123 + 1)
        assert res[0]["nullable_int"] == 457  # Should use provided value (456 + 1)

    def test_import_parquet_extra_fields_ignored(self):
        """Test Parquet import with extra fields - should be ignored"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        text_max_length = 256

        # Create collection
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": str(text_max_length)},
                    },
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create DataFrame with extra columns
        data = {
            "id": list(range(num_entities)),
            "vector": [[random.random() for _ in range(dim)] for _ in range(num_entities)],
            "text": [f"text_{i}" for i in range(num_entities)],
            "extra_column1": list(range(num_entities, num_entities * 2)),  # Extra column
            "extra_column2": [f"extra_{i}" for i in range(num_entities)],  # Extra column
            "extra_column3": [[i, i + 1, i + 2] for i in range(num_entities)],  # Extra column
        }
        df = pd.DataFrame(data)

        # Save to Parquet
        file_name = f"test_parquet_extra_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        df.to_parquet(file_path, engine="pyarrow")
        self.storage_client.upload_file(file_path, file_name)

        # Import should succeed, ignoring extra columns
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait for import
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Verify only expected fields exist
        res = c.query(expr="id >= 0", limit=1, output_fields=["*"])
        assert "id" in res[0]
        assert "text" in res[0]
        assert "extra_column1" not in res[0]
        assert "extra_column2" not in res[0]

    def test_import_numpy_nullable_fields_without_files(self):
        """Test Numpy import where nullable field files are missing - should succeed"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        default_value = 888

        # Create collection with nullable fields
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "required_field", "dataType": "Int64"},
                    {"fieldName": "nullable_field", "dataType": "Int64", "nullable": True},
                    {"fieldName": "default_field", "dataType": "Int64", "defaultValue": default_value},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create numpy files (excluding nullable and default fields)
        file_dir = f"numpy_test_{uuid4()}"
        base_path = f"/tmp/{file_dir}"
        Path(base_path).mkdir(parents=True, exist_ok=True)

        # Only create files for required fields
        np.save(f"{base_path}/id.npy", np.array(list(range(num_entities))))
        np.save(f"{base_path}/vector.npy", np.random.random((num_entities, dim)).astype(np.float32))
        np.save(f"{base_path}/required_field.npy", np.array(list(range(100, 100 + num_entities))))
        # NOT creating nullable_field.npy and default_field.npy

        # Upload files
        file_list = []
        for field in ["id", "vector", "required_field"]:
            file_name = f"{file_dir}/{field}.npy"
            self.storage_client.upload_file(f"{base_path}/{field}.npy", file_name)
            file_list.append(file_name)

        # Import should succeed
        payload = {"collectionName": name, "files": [file_list]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait for import
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Verify nullable field is null and default field has default value
        res = c.query(expr="id == 0", output_fields=["*"])
        assert res[0]["nullable_field"] is None
        assert res[0]["default_field"] == default_value

    def test_import_json_array_varchar_max_length_validation(self):
        """Test JSON import with Array<Varchar> field - should validate max_length"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        max_length = 10
        max_capacity = 100

        # Create collection with Array<Varchar> field
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {
                        "fieldName": "varchar_array",
                        "dataType": "Array",
                        "elementDataType": "VarChar",
                        "elementTypeParams": {"max_capacity": str(max_capacity), "max_length": str(max_length)},
                    },
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create JSON data with varchar array exceeding max_length
        data = []
        for i in range(num_entities):
            data.append(
                {
                    "id": i,
                    "vector": [np.float32(random.random()) for _ in range(dim)],
                    "varchar_array": [
                        "short",
                        "also_ok",
                        "this_string_is_way_too_long_for_limit",
                    ],  # Last one exceeds max_length
                }
            )

        # Save to JSON file
        file_name = f"test_array_varchar_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        # Import should fail due to max_length violation
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait and verify import fails
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                assert (
                    "max_length" in rsp["data"].get("reason", "").lower()
                    or "length" in rsp["data"].get("reason", "").lower()
                )
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed due to max_length violation")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_parquet_array_float_nan_validation(self):
        """Test Parquet import with Array<Float> containing NaN - should fail"""
        name = gen_collection_name()
        dim = 128

        # Create collection with Array<Float> field
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {
                        "fieldName": "float_array",
                        "dataType": "Array",
                        "elementDataType": "Float",
                        "elementTypeParams": {"max_capacity": "100"},
                    },
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create DataFrame with NaN in float array
        data = {
            "id": [1, 2],
            "vector": [[random.random() for _ in range(dim)] for _ in range(2)],
            "float_array": [[1.0, 2.0, 3.0], [4.0, None, 6.0]],  # Second row has None
        }
        df = pd.DataFrame(data)

        # Save to Parquet
        file_name = f"test_parquet_nan_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"

        # Create parquet schema with proper array type
        pa_schema = pa.schema(
            [("id", pa.int64()), ("vector", pa.list_(pa.float32())), ("float_array", pa.list_(pa.float32()))]
        )
        table = pa.Table.from_pandas(df, schema=pa_schema)
        logger.info(f"table: {table}")
        pq.write_table(table, file_path)

        self.storage_client.upload_file(file_path, file_name)

        # Import should fail due to NaN
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait and verify import fails
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "nan" in reason or "infinite" in reason or "invalid" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed due to NaN value")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_csv_extra_columns_without_dynamic(self):
        """Test CSV import with extra columns when dynamic field is disabled - should be ignored"""
        name = gen_collection_name()
        dim = 128

        # Create collection without dynamic field
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "name", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create CSV file with extra columns
        file_name = f"test_csv_extra_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            # Include extra columns that are not in schema
            fieldnames = ["id", "vector", "name", "extra_col1", "extra_col2", "extra_col3"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(20):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                writer.writerow(
                    {
                        "id": i,
                        "vector": vector_str,
                        "name": f"name_{i}",
                        "extra_col1": f"extra_value_{i}",
                        "extra_col2": i * 100,
                        "extra_col3": f"ignored_{i}",
                    }
                )

        self.storage_client.upload_file(file_path, file_name)

        # Import should succeed, ignoring extra columns
        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Wait for import to complete
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data imported successfully
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == 20

        # Verify only expected fields exist
        res = c.query(expr="id == 0", output_fields=["*"])
        assert "id" in res[0]
        assert "name" in res[0]
        assert "extra_col1" not in res[0]
        assert "extra_col2" not in res[0]

    def test_import_json_required_field_missing(self):
        """Test JSON import with required field missing - should fail"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "required_text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create JSON data missing required_text field
        data = []
        for i in range(num_entities):
            data.append(
                {
                    "id": i,
                    "vector": [np.float32(random.random()) for _ in range(dim)],
                    # Missing required_text
                }
            )

        file_name = f"test_required_missing_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail due to missing required field
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed due to missing required field")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_csv_required_field_missing(self):
        """Test CSV import with required field missing from header - should fail"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "required_text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create CSV file missing required_text column
        file_name = f"test_csv_required_missing_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "vector"]  # Missing required_text
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                writer.writerow({"id": str(i), "vector": vector_str})

        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail due to missing required field
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed due to missing required field in CSV")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_parquet_required_field_missing(self):
        """Test Parquet import with required field missing - should fail"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "required_text", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create Parquet file missing required_text column
        data = {
            "id": list(range(num_entities)),
            "vector": [[np.float32(random.random()) for _ in range(dim)] for _ in range(num_entities)],
            # Missing required_text
        }
        df = pd.DataFrame(data)

        pa_schema = pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32()))])
        file_name = f"test_parquet_required_missing_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pandas(df, schema=pa_schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail due to missing required field
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed due to missing required field in Parquet")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_json_vector_dim_mismatch(self):
        """Test JSON import with wrong vector dimension - should fail"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create JSON data with wrong vector dimension (64 instead of 128)
        wrong_dim = 64
        data = []
        for i in range(num_entities):
            data.append({"id": i, "vector": [np.float32(random.random()) for _ in range(wrong_dim)]})

        file_name = f"test_dim_mismatch_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail due to dimension mismatch
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "dim" in reason or "dimension" in reason or "mismatch" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed due to vector dimension mismatch")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_csv_vector_dim_mismatch(self):
        """Test CSV import with wrong vector dimension - should fail"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create CSV file with wrong vector dimension
        wrong_dim = 64
        file_name = f"test_csv_dim_mismatch_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "vector"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(wrong_dim)]) + "]"
                writer.writerow({"id": str(i), "vector": vector_str})

        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail due to dimension mismatch
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "dim" in reason or "dimension" in reason or "mismatch" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed due to vector dimension mismatch in CSV")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_parquet_vector_dim_mismatch(self):
        """Test Parquet import with wrong vector dimension - should fail"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create Parquet file with wrong vector dimension
        wrong_dim = 64
        data = {
            "id": list(range(num_entities)),
            "vector": [[np.float32(random.random()) for _ in range(wrong_dim)] for _ in range(num_entities)],
        }
        df = pd.DataFrame(data)

        pa_schema = pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32()))])
        file_name = f"test_parquet_dim_mismatch_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pandas(df, schema=pa_schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail due to dimension mismatch
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "dim" in reason or "dimension" in reason or "mismatch" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed due to vector dimension mismatch in Parquet")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_parquet_with_nullable_and_default_fields(self):
        """Test Parquet import with nullable and default fields missing - should auto-fill"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000
        default_int_value = 999
        default_varchar_value = "default_text"

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "nullable_int", "dataType": "Int64", "nullable": True},
                    {"fieldName": "default_int", "dataType": "Int64", "defaultValue": default_int_value},
                    {
                        "fieldName": "nullable_varchar",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256"},
                        "nullable": True,
                    },
                    {
                        "fieldName": "default_varchar",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256"},
                        "defaultValue": default_varchar_value,
                    },
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create Parquet file with only required fields (missing nullable and default fields)
        data = {
            "id": list(range(num_entities)),
            "vector": [[np.float32(random.random()) for _ in range(dim)] for _ in range(num_entities)],
        }
        df = pd.DataFrame(data)

        pa_schema = pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32()))])
        file_name = f"test_parquet_nullable_default_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pandas(df, schema=pa_schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Verify nullable fields are null and default fields have default value
        res = c.query(expr="id == 0", output_fields=["*"])
        assert res[0]["nullable_int"] is None
        assert res[0]["default_int"] == default_int_value
        assert res[0]["nullable_varchar"] is None
        assert res[0]["default_varchar"] == default_varchar_value

    def test_import_json_extra_fields_with_dynamic(self):
        """Test JSON import with extra fields when dynamic field is enabled - should be stored in dynamic"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create JSON data with extra fields
        data = []
        for i in range(num_entities):
            data.append(
                {
                    "id": i,
                    "vector": [np.float32(random.random()) for _ in range(dim)],
                    "extra_str": f"dynamic_value_{i}",
                    "extra_int": 1000 + i,
                }
            )

        file_name = f"test_json_dynamic_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Verify dynamic fields are stored
        res = c.query(expr="id == 0", output_fields=["extra_str", "extra_int"])
        assert res[0]["extra_str"] == "dynamic_value_0"
        assert res[0]["extra_int"] == 1000

    def test_import_csv_extra_columns_with_dynamic(self):
        """Test CSV import with extra columns when dynamic field is enabled - should be stored in dynamic"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create CSV file with extra columns
        file_name = f"test_csv_dynamic_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "vector", "extra_str", "extra_int"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                writer.writerow(
                    {"id": str(i), "vector": vector_str, "extra_str": f"dynamic_value_{i}", "extra_int": str(1000 + i)}
                )

        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Verify dynamic fields are stored
        res = c.query(expr="id == 0", output_fields=["extra_str", "extra_int"])
        assert res[0]["extra_str"] == "dynamic_value_0"

    def test_import_parquet_extra_fields_with_dynamic(self):
        """Test Parquet import with $meta column when dynamic field is enabled - should be stored in dynamic.
        Note: Unlike JSON/CSV, Parquet does NOT auto-collect extra columns into dynamic field.
        Parquet only supports dynamic data via the special '$meta' column."""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create Parquet file with $meta column for dynamic fields
        # Parquet requires using the special $meta column (JSON string) for dynamic data
        data = {
            "id": list(range(num_entities)),
            "vector": [[np.float32(random.random()) for _ in range(dim)] for _ in range(num_entities)],
            "$meta": [
                json.dumps({"extra_str": f"dynamic_value_{i}", "extra_int": 1000 + i}) for i in range(num_entities)
            ],
        }
        df = pd.DataFrame(data)

        file_name = f"test_parquet_dynamic_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        df.to_parquet(file_path, engine="pyarrow")
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

        # Verify dynamic fields are stored via $meta
        res = c.query(expr="id == 0", output_fields=["extra_str", "extra_int"])
        assert res[0]["extra_str"] == "dynamic_value_0"
        assert res[0]["extra_int"] == 1000

    def test_import_csv_without_function_output_field(self):
        """Test CSV import without BM25 function output field - should succeed with auto-generation"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256", "enable_analyzer": True},
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "bm25_function",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_idx", "metricType": "BM25"},
                {"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"},
            ],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create CSV file WITHOUT function output field
        file_name = f"test_csv_no_fn_output_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "text", "dense_vector"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                writer.writerow({"id": str(i), "text": f"sample text for document {i}", "dense_vector": vector_str})

        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed with BM25 auto-generating sparse vectors
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

    def test_import_csv_bm25_output_rejected_even_with_property(self):
        """Test CSV import with BM25 function output field is still rejected even with property enabled"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256", "enable_analyzer": True},
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "bm25_function",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_idx", "metricType": "BM25"},
                {"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"},
            ],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Enable property — should NOT affect BM25 output rejection
        rsp = self.collection_client.alter_collection_properties(
            name, {"collection.function.allowInsertNonBM25FunctionOutputs": "true"}
        )
        assert rsp["code"] == 0

        # Create CSV file with BM25 function output field
        file_name = f"test_csv_bm25_with_property_{uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["id", "text", "sparse_vector", "dense_vector"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_entities):
                vector_str = "[" + ",".join([str(random.random()) for _ in range(dim)]) + "]"
                writer.writerow(
                    {
                        "id": str(i),
                        "text": f"sample text for BM25 {i}",
                        "sparse_vector": '{"indices": [1, 2], "values": [0.1, 0.2]}',
                        "dense_vector": vector_str,
                    }
                )

        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should still fail for BM25 output
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "bm25" in reason or "function output" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed for BM25 function output field in CSV")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_parquet_with_function_output_field(self):
        """Test Parquet import with BM25 function output field - should fail with error"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256", "enable_analyzer": True},
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "bm25_function",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_idx", "metricType": "BM25"},
                {"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"},
            ],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create Parquet file with BM25 function output field
        # Use sparse vector as a map column in parquet
        data = {
            "id": list(range(num_entities)),
            "text": [f"sample document {i}" for i in range(num_entities)],
            "sparse_vector": [{"indices": [1, 2, 3], "values": [0.1, 0.2, 0.3]} for _ in range(num_entities)],
            "dense_vector": [[random.random() for _ in range(dim)] for _ in range(num_entities)],
        }

        file_name = f"test_parquet_fn_output_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump([{k: data[k][i] for k in data} for i in range(num_entities)], f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should fail
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert (
                    "not allowed to provide data for bm25 function output field" in reason
                    or "output by function" in reason
                    or "function" in reason
                )
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed for BM25 function output field in Parquet")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")

    def test_import_parquet_without_function_output_field(self):
        """Test Parquet import without BM25 function output field - should succeed with auto-generation"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256", "enable_analyzer": True},
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "bm25_function",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_idx", "metricType": "BM25"},
                {"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"},
            ],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Create Parquet file WITHOUT function output field
        data = {
            "id": list(range(num_entities)),
            "text": [f"sample text for document {i}" for i in range(num_entities)],
            "dense_vector": [[np.float32(random.random()) for _ in range(dim)] for _ in range(num_entities)],
        }
        df = pd.DataFrame(data)

        pa_schema = pa.schema([("id", pa.int64()), ("text", pa.string()), ("dense_vector", pa.list_(pa.float32()))])
        file_name = f"test_parquet_no_fn_output_{uuid4()}.parquet"
        file_path = f"/tmp/{file_name}"
        table = pa.Table.from_pandas(df, schema=pa_schema)
        pq.write_table(table, file_path)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should succeed with BM25 auto-generating sparse vectors
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result, f"Import job failed: {res}"

        # Verify data
        c = Collection(name)
        c.load(_refresh=True)
        assert c.num_entities == num_entities

    def test_import_parquet_bm25_output_rejected_even_with_property(self):
        """Test Parquet import with BM25 function output field is still rejected even with property enabled"""
        name = gen_collection_name()
        dim = 128
        num_entities = 3000

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "256", "enable_analyzer": True},
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                    {"fieldName": "dense_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                ],
                "functions": [
                    {
                        "name": "bm25_function",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_idx", "metricType": "BM25"},
                {"fieldName": "dense_vector", "indexName": "dense_idx", "metricType": "L2"},
            ],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # Enable property — should NOT affect BM25 output rejection
        rsp = self.collection_client.alter_collection_properties(
            name, {"collection.function.allowInsertNonBM25FunctionOutputs": "true"}
        )
        assert rsp["code"] == 0

        # Create JSON data with BM25 function output field (use JSON for easier sparse vector encoding)
        data = []
        for i in range(num_entities):
            data.append(
                {
                    "id": i,
                    "text": f"sample text for BM25 {i}",
                    "sparse_vector": {"indices": [1, 2, 3], "values": [0.1, 0.2, 0.3]},
                    "dense_vector": [np.float32(random.random()) for _ in range(dim)],
                }
            )

        file_name = f"test_parquet_bm25_with_property_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        self.storage_client.upload_file(file_path, file_name)

        payload = {"collectionName": name, "files": [[file_name]]}
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]

        # Import should still fail for BM25 output
        finished = False
        t0 = time.time()
        while not finished:
            rsp = self.import_job_client.get_import_job_progress(job_id)
            if rsp["data"]["state"] == "Failed":
                reason = rsp["data"].get("reason", "").lower()
                assert "bm25" in reason or "function output" in reason
                finished = True
            elif rsp["data"]["state"] == "Completed":
                raise AssertionError("Import should have failed for BM25 function output field")
            time.sleep(5)
            if time.time() - t0 > IMPORT_TIMEOUT:
                raise AssertionError("Import job timeout")


@pytest.mark.L2
class TestImportJobAdvance(TestBase):
    def test_job_import_recovery_after_chaos(self, release_name):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # upload file to storage
        file_nums = 10
        batch_size = 1000
        file_names = []
        for file_num in range(file_nums):
            data = [
                {
                    "book_id": i,
                    "word_count": i,
                    "book_describe": f"book_{i}",
                    "book_intro": [np.float32(random.random()) for _ in range(dim)],
                }
                for i in range(batch_size * file_num, batch_size * (file_num + 1))
            ]

            # dump data to file
            file_name = f"bulk_insert_data_{file_num}_{uuid4()}.json"
            file_path = f"/tmp/{file_name}"
            # create dir for file path
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w") as f:
                json.dump(data, f, cls=NumpyEncoder)
            # upload file to minio storage
            self.storage_client.upload_file(file_path, file_name)
            file_names.append([file_name])

        # create import job
        payload = {
            "collectionName": name,
            "files": file_names,
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)
        assert job_id in [job["jobId"] for job in rsp["data"]["records"]]
        rsp = self.import_job_client.list_import_jobs(payload)
        # kill milvus by deleting pod
        cmd = f"kubectl delete pod -l 'app.kubernetes.io/instance={release_name}, app.kubernetes.io/name=milvus' "
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        output = result.stdout
        return_code = result.returncode
        logger.info(f"output: {output}, return_code, {return_code}")

        # get import job progress
        for job in rsp["data"]["records"]:
            job_id = job["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                try:
                    rsp = self.import_job_client.get_import_job_progress(job_id)
                    if rsp["data"]["state"] == "Completed":
                        finished = True
                    time.sleep(5)
                    if time.time() - t0 > IMPORT_TIMEOUT:
                        raise AssertionError("import job timeout")
                except Exception as e:
                    logger.error(f"get import job progress failed: {e}")
                    time.sleep(5)
        time.sleep(10)
        rsp = self.import_job_client.list_import_jobs(payload)
        # assert data count
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        assert c.num_entities == file_nums * batch_size
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp["data"]) == 100


@pytest.mark.L2
class TestCreateImportJobAdvance(TestBase):
    def test_job_import_with_multi_task_and_datanode(self, release_name):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # upload file to storage
        task_num = 48
        file_nums = 1
        batch_size = 100000
        file_names = []
        for file_num in range(file_nums):
            data = [
                {
                    "book_id": i,
                    "word_count": i,
                    "book_describe": f"book_{i}",
                    "book_intro": [np.float32(random.random()) for _ in range(dim)],
                }
                for i in range(batch_size * file_num, batch_size * (file_num + 1))
            ]

            # dump data to file
            file_name = f"bulk_insert_data_{file_num}_{uuid4()}.json"
            file_path = f"/tmp/{file_name}"
            # create dir for file path
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w") as f:
                json.dump(data, f, cls=NumpyEncoder)
            # upload file to minio storage
            self.storage_client.upload_file(file_path, file_name)
            file_names.append([file_name])
        for _i in range(task_num):
            # create import job
            payload = {
                "collectionName": name,
                "files": file_names,
            }
            rsp = self.import_job_client.create_import_jobs(payload)
            job_id = rsp["data"]["jobId"]
            # list import job
            payload = {
                "collectionName": name,
            }
            rsp = self.import_job_client.list_import_jobs(payload)
            assert job_id in [job["jobId"] for job in rsp["data"]["records"]]
        rsp = self.import_job_client.list_import_jobs(payload)
        # get import job progress
        for job in rsp["data"]["records"]:
            job_id = job["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                try:
                    rsp = self.import_job_client.get_import_job_progress(job_id)
                    if rsp["data"]["state"] == "Completed":
                        finished = True
                    time.sleep(5)
                    if time.time() - t0 > IMPORT_TIMEOUT:
                        raise AssertionError("import job timeout")
                except Exception as e:
                    logger.error(f"get import job progress failed: {e}")
                    time.sleep(5)
        time.sleep(10)
        rsp = self.import_job_client.list_import_jobs(payload)
        # assert data count
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        assert c.num_entities == file_nums * batch_size * task_num
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp["data"]) == 100

    def test_job_import_with_extremely_large_task_num(self, release_name):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # upload file to storage
        task_num = 1000
        file_nums = 2
        batch_size = 10
        file_names = []
        for file_num in range(file_nums):
            data = [
                {
                    "book_id": i,
                    "word_count": i,
                    "book_describe": f"book_{i}",
                    "book_intro": [np.float32(random.random()) for _ in range(dim)],
                }
                for i in range(batch_size * file_num, batch_size * (file_num + 1))
            ]

            # dump data to file
            file_name = f"bulk_insert_data_{file_num}_{uuid4()}.json"
            file_path = f"/tmp/{file_name}"
            # create dir for file path
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w") as f:
                json.dump(data, f, cls=NumpyEncoder)
            # upload file to minio storage
            self.storage_client.upload_file(file_path, file_name)
            file_names.append([file_name])
        for _i in range(task_num):
            # create import job
            payload = {
                "collectionName": name,
                "files": file_names,
            }
            rsp = self.import_job_client.create_import_jobs(payload)
            job_id = rsp["data"]["jobId"]
            # list import job
            payload = {
                "collectionName": name,
            }
            rsp = self.import_job_client.list_import_jobs(payload)
            assert job_id in [job["jobId"] for job in rsp["data"]["records"]]
        rsp = self.import_job_client.list_import_jobs(payload)
        # get import job progress
        for job in rsp["data"]["records"]:
            job_id = job["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                try:
                    rsp = self.import_job_client.get_import_job_progress(job_id)
                    if rsp["data"]["state"] == "Completed":
                        finished = True
                    time.sleep(5)
                    if time.time() - t0 > IMPORT_TIMEOUT:
                        raise AssertionError("import job timeout")
                except Exception as e:
                    logger.error(f"get import job progress failed: {e}")
                    time.sleep(5)
        time.sleep(10)
        rsp = self.import_job_client.list_import_jobs(payload)
        # assert data count
        c = Collection(name)
        c.load(_refresh=True, timeou=120)
        assert c.num_entities == file_nums * batch_size * task_num
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp["data"]) == 100


@pytest.mark.L1
class TestCreateImportJobNegative(TestBase):
    @pytest.mark.parametrize("insert_num", [2])
    @pytest.mark.parametrize("import_task_num", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    @pytest.mark.BulkInsert
    def test_create_import_job_with_json_dup_dynamic_key(
        self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field
    ):
        # create collection
        name = gen_collection_name()
        dim = 16
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "word_count",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "dynamic_key": i,
                "book_intro": [random.random() for _ in range(dim)],
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp.update({"$meta": {"dynamic_key": i + 1}})
            data.append(tmp)
        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        logger.info(f"data: {data}")
        with open(file_path, "w") as f:
            json.dump(data, f)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        for _i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp["data"]["records"]:
            task_id = task["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp["data"]["state"] == "Failed":
                    assert True
                    finished = True
                if rsp["data"]["state"] == "Completed":
                    raise AssertionError()
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")

    def test_import_job_with_empty_files(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[]],
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] == 1100 and "empty" in rsp["message"]

    def test_import_job_with_non_exist_files(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)

        # create import job
        payload = {
            "collectionName": name,
            "files": [["invalid_file.json"]],
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        time.sleep(5)
        rsp = self.import_job_client.get_import_job_progress(rsp["data"]["jobId"])
        assert rsp["data"]["state"] == "Failed"

    def test_import_job_with_non_exist_binlog_files(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        self.collection_client.collection_create(payload)
        self.wait_load_completed(name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [
                [
                    "invalid_bucket/invalid_root_path/insert_log/invalid_id/",
                ]
            ],
            "options": {"backup": "true"},
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] == 1100 and "invalid" in rsp["message"]

    def test_import_job_with_wrong_file_type(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = [
            {
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            for i in range(10000)
        ]

        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.txt"
        file_path = f"/tmp/{file_name}"

        json_data = json.dumps(data, cls=NumpyEncoder)

        # 将JSON数据保存到txt文件
        with open(file_path, "w") as file:
            file.write(json_data)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] == 2100 and "unexpected file type" in rsp["message"]

    def test_import_job_with_empty_rows(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = [
            {
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            for i in range(0)
        ]

        # dump data to file
        file_name = "bulk_insert_empty_data.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        job_id = rsp["data"]["jobId"]
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # wait import job to be completed
        res, result = self.import_job_client.wait_import_job_completed(job_id)
        assert result
        c = Collection(name)
        assert c.num_entities == 0

    def test_create_import_job_with_new_user(self):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        # create new user
        username = "test_user"
        password = "12345678"
        payload = {"userName": username, "password": password}
        self.user_client.user_create(payload)
        # try to describe collection with new user
        self.collection_client.api_key = f"{username}:{password}"
        try:
            rsp = self.collection_client.collection_describe(collection_name=name)
            logger.info(f"describe collection: {rsp}")
        except Exception as e:
            logger.error(f"describe collection failed: {e}")

        # upload file to storage
        data = []
        auto_id = True
        enable_dynamic_field = True
        for i in range(1):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp.update({f"dynamic_field_{i}": i})
            data.append(tmp)

        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        self.import_job_client.api_key = f"{username}:{password}"
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp["code"] == 1100 and "empty" in rsp["message"]

    @pytest.mark.parametrize("insert_num", [5000])
    @pytest.mark.parametrize("import_task_num", [2])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_get_job_progress_with_mismatch_db_name(
        self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field
    ):
        # create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "word_count",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp.update({f"dynamic_field_{i}": i})
            data.append(tmp)
        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        for _i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp["data"]["records"]:
            task_id = task["jobId"]
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp["data"]["state"] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    raise AssertionError("import job timeout")
        c = Collection(name)
        time.sleep(10)
        c.load(_refresh=True, timeou=120)
        res = c.query(
            expr="",
            output_fields=["count(*)"],
        )
        assert res[0]["count(*)"] == insert_num * import_task_num
        # query data
        payload = {
            "collectionName": name,
            "filter": "book_id > 0",
            "outputFields": ["*"],
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp["code"] == 0


@pytest.mark.L1
class TestListImportJob(TestBase):
    def test_list_job_e2e(self):
        # create two db
        self.create_database(db_name="db1")
        self.create_database(db_name="db2")

        # create collection
        insert_num = 5000
        import_task_num = 2
        auto_id = True
        is_partition_key = True
        enable_dynamic_field = True
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "word_count",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        for db_name in ["db1", "db2"]:
            rsp = self.collection_client.collection_create(payload, db_name=db_name)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp.update({f"dynamic_field_{i}": i})
            data.append(tmp)
        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        for db in ["db1", "db2"]:
            payload = {
                "collectionName": name,
                "files": [[file_name]],
            }
            for _i in range(import_task_num):
                rsp = self.import_job_client.create_import_jobs(payload, db_name=db)
        # list import job
        payload = {}
        for db_name in [None, "db1", "db2", "default"]:
            try:
                rsp = self.import_job_client.list_import_jobs(payload, db_name=db_name)
                logger.info(f"job num: {len(rsp['data']['records'])}")
            except Exception as e:
                logger.error(f"list import job failed: {e}")


@pytest.mark.L1
class TestGetImportJobProgress(TestBase):
    def test_list_job_e2e(self):
        # create two db
        self.create_database(db_name="db1")
        self.create_database(db_name="db2")

        # create collection
        insert_num = 5000
        import_task_num = 2
        auto_id = True
        is_partition_key = True
        enable_dynamic_field = True
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {
                        "fieldName": "word_count",
                        "dataType": "Int64",
                        "isPartitionKey": is_partition_key,
                        "elementTypeParams": {},
                    },
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}],
        }
        for db_name in ["db1", "db2"]:
            rsp = self.collection_client.collection_create(payload, db_name=db_name)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)],
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp.update({f"dynamic_field_{i}": i})
            data.append(tmp)
        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f, cls=NumpyEncoder)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)
        job_id_list = []
        # create import job
        for db in ["db1", "db2"]:
            payload = {
                "collectionName": name,
                "files": [[file_name]],
            }
            for _i in range(import_task_num):
                rsp = self.import_job_client.create_import_jobs(payload, db_name=db)
                job_id_list.append(rsp["data"]["jobId"])
        time.sleep(5)
        # get import job progress
        for job_id in job_id_list:
            try:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                logger.info(f"job progress: {rsp}")
            except Exception as e:
                logger.error(f"get import job progress failed: {e}")


@pytest.mark.L1
class TestGetImportJobProgressNegative(TestBase):
    def test_list_job_with_invalid_job_id(self):
        # get import job progress with invalid job id
        job_id_list = ["invalid_job_id", None]
        for job_id in job_id_list:
            try:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                logger.info(f"job progress: {rsp}")
            except Exception as e:
                logger.error(f"get import job progress failed: {e}")

    def test_list_job_with_job_id(self):
        # get import job progress with invalid job id
        job_id_list = ["invalid_job_id", None]
        for job_id in job_id_list:
            try:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                logger.info(f"job progress: {rsp}")
            except Exception as e:
                logger.error(f"get import job progress failed: {e}")

    def test_list_job_with_new_user(self):
        # create new user
        user_name = "test_user"
        password = "12345678"
        self.user_client.user_create(
            {
                "userName": user_name,
                "password": password,
            }
        )
