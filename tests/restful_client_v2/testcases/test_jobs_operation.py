import random
import json
import time
from utils.utils import gen_collection_name
import pytest
from base.testbase import TestBase


@pytest.mark.L0
class TestJobE2E(TestBase):

    def test_job_e2e(self):
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
            "book_intro": [random.random() for _ in range(dim)]}
            for i in range(10000)]

        # dump data to file
        file_name = "bulk_insert_data.json"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w") as f:
            json.dump(data, f)
        # upload file to minio storage
        self.storage_client.upload_file(file_path, file_name)

        # create import job
        payload = {
            "collectionName": name,
            "files": [file_name],
        }
        rsp = self.import_job_client.create_import_jobs(payload)
        # list import job
        payload = {
            "collectionName": name,
        }
        rsp = self.import_job_client.list_import_jobs(payload)

        # get import job progress
        for task in rsp['data']:
            task_id = task['taskID']
            finished = False
            t0 = time.time()

            while not finished:
                rsp = self.import_job_client.get_import_job_progress(task_id)
                if rsp['data']['state'] == "ImportCompleted":
                    finished = True
                time.sleep(5)
                if time.time() - t0 > 120:
                    assert False, "import job timeout"
        time.sleep(10)
        # query data
        payload = {
            "collectionName": name,
            "filter": f"book_id in {[i for i in range(1000)]}",
            "limit": 100,
            "offset": 0,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 100
