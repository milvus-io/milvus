import random
import json
import subprocess
import time
from sklearn import preprocessing
from pathlib import Path
import pandas as pd
import numpy as np
from pymilvus import Collection
from utils.utils import gen_collection_name
from utils.util_log import test_log as logger
import pytest
from base.testbase import TestBase
from uuid import uuid4

IMPORT_TIMEOUT = 360


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.float32):
            return float(obj)
        return super(NumpyEncoder, self).default(obj)



@pytest.mark.BulkInsert
class TestCreateImportJob(TestBase):

    @pytest.mark.parametrize("insert_num", [3000])
    @pytest.mark.parametrize("import_task_num", [2])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_job_e2e(self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field):
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
                    {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": is_partition_key, "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]
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
        for i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp['data']["records"]:
            task_id = task['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp['data']['state'] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"
        c = Collection(name)
        c.load(_refresh=True)
        time.sleep(10)
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
        assert rsp['code'] == 0

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
                    {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": is_partition_key, "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]
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
        for i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp['data']["records"]:
            task_id = task['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp['data']['state'] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"
        c = Collection(name)
        c.load(_refresh=True)
        time.sleep(10)
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
        assert rsp['code'] == 0

    @pytest.mark.parametrize("insert_num", [5000])
    @pytest.mark.parametrize("import_task_num", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [False])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    def test_import_job_with_partition(self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field):
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
                    {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": is_partition_key,
                     "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]
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
        for i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp['data']["records"]:
            task_id = task['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp['data']['state'] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"
        c = Collection(name)
        c.load(_refresh=True)
        time.sleep(10)
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
        assert rsp['code'] == 0

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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        file_nums = 2
        file_names = []
        for file_num in range(file_nums):
            data = [{
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]}
                for i in range(1000*file_num, 1000*(file_num+1))]

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
        for job in rsp['data']["records"]:
            job_id = job['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp['data']['state'] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"
        time.sleep(10)
        # assert data count
        c = Collection(name)
        assert c.num_entities == 2000
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 100

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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        file_nums = 2
        file_names = []
        for file_num in range(file_nums):
            data = [{
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]}
                for i in range(1000*file_num, 1000*(file_num+1))]

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
        for job in rsp['data']["records"]:
            job_id = job['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp['data']['state'] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"
        time.sleep(10)
        # assert data count
        c = Collection(name)
        assert c.num_entities == 2000
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 100

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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        file_nums = 2
        file_names = []
        for file_num in range(file_nums):
            data = [{
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]}
                for i in range(1000*file_num, 1000*(file_num+1))]

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
        for job in rsp['data']["records"]:
            job_id = job['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp['data']['state'] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"
        time.sleep(10)
        # assert data count
        c = Collection(name)
        assert c.num_entities == 2000
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 100

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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        file_nums = 2
        file_names = []

        # numpy file
        for file_num in range(file_nums):
            data = [{
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]}
                for i in range(1000*file_num, 1000*(file_num+1))]

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
        for file_num in range(2,file_nums+2):
            data = [{
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]}
                for i in range(1000*file_num, 1000*(file_num+1))]

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
        for file_num in range(4, file_nums+4):
            data = [{
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]}
                for i in range(1000*file_num, 1000*(file_num+1))]

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
        for job in rsp['data']["records"]:
            job_id = job['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp['data']['state'] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"
        time.sleep(10)
        # assert data count
        c = Collection(name)
        assert c.num_entities == 6000
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 100

    @pytest.mark.parametrize("insert_round", [2])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.skip("stats task will generate a new segment, "
                      "using collectionID as prefix will import twice as much data")
    def test_job_import_binlog_file_type(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema, bucket_name, root_path):
        # todo: copy binlog file to backup bucket
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
                    {"fieldName": "user_id", "dataType": "Int64", "isPartitionKey": is_partition_key,
                     "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "bool", "dataType": "Bool", "elementTypeParams": {}},
                    {"fieldName": "json", "dataType": "JSON", "elementTypeParams": {}},
                    {"fieldName": "int_array", "dataType": "Array", "elementDataType": "Int64",
                     "elementTypeParams": {"max_capacity": "1024"}},
                    {"fieldName": "varchar_array", "dataType": "Array", "elementDataType": "VarChar",
                     "elementTypeParams": {"max_capacity": "1024", "max_length": "256"}},
                    {"fieldName": "bool_array", "dataType": "Array", "elementDataType": "Bool",
                     "elementTypeParams": {"max_capacity": "1024"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "image_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "text_emb", "indexName": "text_emb", "metricType": "L2"},
                {"fieldName": "image_emb", "indexName": "image_emb", "metricType": "L2"}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        # create restore collection
        restore_collection_name = f"{name}_restore"
        payload["collectionName"] = restore_collection_name
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
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
                        "text_emb": preprocessing.normalize([np.array([np.float32(random.random()) for _ in range(dim)])])[
                            0].tolist(),
                        "image_emb": preprocessing.normalize([np.array([np.float32(random.random()) for _ in range(dim)])])[
                            0].tolist(),
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
                        "text_emb": preprocessing.normalize([np.array([np.float32(random.random()) for _ in range(dim)])])[
                            0].tolist(),
                        "image_emb": preprocessing.normalize([np.array([np.float32(random.random()) for _ in range(dim)])])[
                            0].tolist(),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # flush data to generate binlog file
        c = Collection(name)
        c.flush()
        time.sleep(2)

        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 50})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50
        # get collection id
        c = Collection(name)
        res = c.describe()
        collection_id = res["collection_id"]

        # create import job
        payload = {
            "collectionName": restore_collection_name,
            "files": [[f"/{root_path}/insert_log/{collection_id}/",
                       # f"{bucket_name}/{root_path}/delta_log/{collection_id}/"
                       ]],
            "options": {
                "backup": "true"
            }

        }
        if is_partition_key:
            payload["partitionName"] = "_default_0"
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp['code'] == 0
        # list import job
        payload = {
            "collectionName": restore_collection_name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for job in rsp['data']["records"]:
            job_id = job['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(job_id)
                if rsp['data']['state'] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"
        time.sleep(10)
        c_restore = Collection(restore_collection_name)
        assert c.num_entities == c_restore.num_entities


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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        file_nums = 10
        batch_size = 1000
        file_names = []
        for file_num in range(file_nums):
            data = [{
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]}
                for i in range(batch_size*file_num, batch_size*(file_num+1))]

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
        job_id = rsp['data']['jobId']
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)
        assert job_id in [job["jobId"] for job in rsp['data']["records"]]
        rsp = self.import_job_client.list_import_jobs(payload)
        # kill milvus by deleting pod
        cmd = f"kubectl delete pod -l 'app.kubernetes.io/instance={release_name}, app.kubernetes.io/name=milvus' "
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        output = result.stdout
        return_code = result.returncode
        logger.info(f"output: {output}, return_code, {return_code}")

        # get import job progress
        for job in rsp['data']["records"]:
            job_id = job['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                try:
                    rsp = self.import_job_client.get_import_job_progress(job_id)
                    if rsp['data']['state'] == "Completed":
                        finished = True
                    time.sleep(5)
                    if time.time() - t0 > IMPORT_TIMEOUT:
                        assert False, "import job timeout"
                except Exception as e:
                    logger.error(f"get import job progress failed: {e}")
                    time.sleep(5)
        time.sleep(10)
        rsp = self.import_job_client.list_import_jobs(payload)
        # assert data count
        c = Collection(name)
        assert c.num_entities == file_nums * batch_size
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 100


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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        task_num = 48
        file_nums = 1
        batch_size = 100000
        file_names = []
        for file_num in range(file_nums):
            data = [{
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]}
                for i in range(batch_size*file_num, batch_size*(file_num+1))]

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
        for i in range(task_num):
            # create import job
            payload = {
                "collectionName": name,
                "files": file_names,
            }
            rsp = self.import_job_client.create_import_jobs(payload)
            job_id = rsp['data']['jobId']
            # list import job
            payload = {
                "collectionName": name,
            }
            rsp = self.import_job_client.list_import_jobs(payload)
            assert job_id in [job["jobId"] for job in rsp['data']["records"]]
        rsp = self.import_job_client.list_import_jobs(payload)
        # get import job progress
        for job in rsp['data']["records"]:
            job_id = job['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                try:
                    rsp = self.import_job_client.get_import_job_progress(job_id)
                    if rsp['data']['state'] == "Completed":
                        finished = True
                    time.sleep(5)
                    if time.time() - t0 > IMPORT_TIMEOUT:
                        assert False, "import job timeout"
                except Exception as e:
                    logger.error(f"get import job progress failed: {e}")
                    time.sleep(5)
        time.sleep(10)
        rsp = self.import_job_client.list_import_jobs(payload)
        # assert data count
        c = Collection(name)
        assert c.num_entities == file_nums * batch_size * task_num
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 100

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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        task_num = 1000
        file_nums = 2
        batch_size = 10
        file_names = []
        for file_num in range(file_nums):
            data = [{
                "book_id": i,
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]}
                for i in range(batch_size*file_num, batch_size*(file_num+1))]

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
        for i in range(task_num):
            # create import job
            payload = {
                "collectionName": name,
                "files": file_names,
            }
            rsp = self.import_job_client.create_import_jobs(payload)
            job_id = rsp['data']['jobId']
            # list import job
            payload = {
                "collectionName": name,
            }
            rsp = self.import_job_client.list_import_jobs(payload)
            assert job_id in [job["jobId"] for job in rsp['data']["records"]]
        rsp = self.import_job_client.list_import_jobs(payload)
        # get import job progress
        for job in rsp['data']["records"]:
            job_id = job['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                try:
                    rsp = self.import_job_client.get_import_job_progress(job_id)
                    if rsp['data']['state'] == "Completed":
                        finished = True
                    time.sleep(5)
                    if time.time() - t0 > IMPORT_TIMEOUT:
                        assert False, "import job timeout"
                except Exception as e:
                    logger.error(f"get import job progress failed: {e}")
                    time.sleep(5)
        time.sleep(10)
        rsp = self.import_job_client.list_import_jobs(payload)
        # assert data count
        c = Collection(name)
        assert c.num_entities == file_nums * batch_size * task_num
        # assert import data can be queried
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 100


@pytest.mark.L1
class TestCreateImportJobNegative(TestBase):

    @pytest.mark.parametrize("insert_num", [2])
    @pytest.mark.parametrize("import_task_num", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    @pytest.mark.BulkInsert
    def test_create_import_job_with_json_dup_dynamic_key(self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field):
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
                    {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": is_partition_key, "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "dynamic_key": i,
                "book_intro": [random.random() for _ in range(dim)]
            }
            if not auto_id:
                tmp["book_id"] = i
            if enable_dynamic_field:
                tmp.update({f"$meta": {"dynamic_key": i+1}})
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
        for i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp['data']["records"]:
            task_id = task['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp['data']['state'] == "Failed":
                    assert True
                    finished = True
                if rsp['data']['state'] == "Completed":
                    assert False
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"

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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[]],
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp['code'] == 1100 and "empty" in rsp['message']

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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # create import job
        payload = {
            "collectionName": name,
            "files": [["invalid_file.json"]],
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        time.sleep(5)
        rsp = self.import_job_client.get_import_job_progress(rsp['data']['jobId'])
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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[
                f"invalid_bucket/invalid_root_path/insert_log/invalid_id/",
            ]],
            "options": {
                "backup": "true"
            }
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp['code'] == 1100 and "invalid" in rsp['message']

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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = [{
            "book_id": i,
            "word_count": i,
            "book_describe": f"book_{i}",
            "book_intro": [np.float32(random.random()) for _ in range(dim)]}
            for i in range(10000)]

        # dump data to file
        file_name = f"bulk_insert_data_{uuid4()}.txt"
        file_path = f"/tmp/{file_name}"

        json_data = json.dumps(data, cls=NumpyEncoder)

        # 将JSON数据保存到txt文件
        with open(file_path, 'w') as file:
            file.write(json_data)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [[file_name]],
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        assert rsp['code'] == 2100 and "unexpected file type" in rsp['message']

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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = [{
            "book_id": i,
            "word_count": i,
            "book_describe": f"book_{i}",
            "book_intro": [np.float32(random.random()) for _ in range(dim)]}
            for i in range(0)]

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
        job_id = rsp['data']['jobId']
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
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        # create new user
        username = "test_user"
        password = "12345678"
        payload = {
            "userName": username,
            "password": password
        }
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
                "book_intro": [np.float32(random.random()) for _ in range(dim)]
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
        assert rsp['code'] == 1100 and "empty" in rsp['message']



    @pytest.mark.parametrize("insert_num", [5000])
    @pytest.mark.parametrize("import_task_num", [2])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_get_job_progress_with_mismatch_db_name(self, insert_num, import_task_num, auto_id, is_partition_key, enable_dynamic_field):
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
                    {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": is_partition_key, "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]
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
        for i in range(import_task_num):
            rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp['data']["records"]:
            task_id = task['jobId']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp['data']['state'] == "Completed":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > IMPORT_TIMEOUT:
                    assert False, "import job timeout"
        c = Collection(name)
        c.load(_refresh=True)
        time.sleep(10)
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
        assert rsp['code'] == 0


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
                    {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": is_partition_key, "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        for db_name in ["db1", "db2"]:
            rsp = self.collection_client.collection_create(payload, db_name=db_name)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]
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
            for i in range(import_task_num):
                rsp = self.import_job_client.create_import_jobs(payload, db_name=db)
        # list import job
        payload = {
        }
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
                    {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": is_partition_key, "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        for db_name in ["db1", "db2"]:
            rsp = self.collection_client.collection_create(payload, db_name=db_name)

        # upload file to storage
        data = []
        for i in range(insert_num):
            tmp = {
                "word_count": i,
                "book_describe": f"book_{i}",
                "book_intro": [np.float32(random.random()) for _ in range(dim)]
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
            for i in range(import_task_num):
                rsp = self.import_job_client.create_import_jobs(payload, db_name=db)
                job_id_list.append(rsp['data']['jobId'])
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
        self.user_client.user_create({
            "userName": user_name,
            "password": password,
        })

