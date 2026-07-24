import io
import time

import pytest
from api.milvus import FileResourceClient
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name, gen_unique_str


class TestFileResourceOperations(TestBase):
    """End-to-end FileResource coverage using only REST v2 Milvus APIs."""

    def setup_method(self):
        self._file_resources_to_cleanup = []
        self._minio_objects_to_cleanup = []

    def teardown_method(self):
        try:
            super().teardown_method()
        finally:
            self.file_resource_client.api_key = self.api_key
            self._cleanup_file_resources()
            self._cleanup_minio_objects()

    def _upload_file(self, content, file_name="synonyms.txt"):
        resource_name = gen_unique_str("rest_file_resource")
        path = f"file-resource-rest/{resource_name}/{file_name}"
        self.storage_client.client.put_object(
            self.storage_client.bucket_name,
            path,
            io.BytesIO(content),
            len(content),
            content_type="text/plain; charset=utf-8",
        )
        self._minio_objects_to_cleanup.append(path)
        return resource_name, path

    def _add_file_resource(self, resource_name, path):
        rsp = self.file_resource_client.file_resource_add({"name": resource_name, "path": path})
        if rsp["code"] == 0 and resource_name not in self._file_resources_to_cleanup:
            self._file_resources_to_cleanup.append(resource_name)
        return rsp

    def _upload_and_register_synonyms(self):
        resource_name, path = self._upload_file(b"search, retrieval, query\n")
        rsp = self._add_file_resource(resource_name, path)
        assert rsp["code"] == 0, rsp
        return resource_name

    def _remove_file_resource(self, resource_name):
        rsp = self.file_resource_client.file_resource_remove({"name": resource_name})
        if rsp["code"] == 0 and resource_name in self._file_resources_to_cleanup:
            self._file_resources_to_cleanup.remove(resource_name)
        return rsp

    def _remove_file_resource_eventually(self, resource_name, timeout=30):
        deadline = time.time() + timeout
        last_rsp = None
        while time.time() < deadline:
            last_rsp = self._remove_file_resource(resource_name)
            if last_rsp["code"] == 0:
                return last_rsp
            if "is still in use" not in last_rsp.get("message", ""):
                raise AssertionError(last_rsp)
            time.sleep(0.5)
        raise AssertionError(f"file resource {resource_name!r} was not released: {last_rsp}")

    def _cleanup_file_resources(self):
        for resource_name in list(self._file_resources_to_cleanup):
            try:
                self._remove_file_resource_eventually(resource_name)
            except Exception:
                pass

    def _cleanup_minio_objects(self):
        for path in self._minio_objects_to_cleanup:
            try:
                self.storage_client.client.remove_object(self.storage_client.bucket_name, path)
            except Exception:
                pass

    @staticmethod
    def _wait_for_code(call, expected_code, timeout=20):
        deadline = time.time() + timeout
        last_rsp = None
        while time.time() < deadline:
            last_rsp = call()
            if last_rsp.get("code") == expected_code:
                return last_rsp
            time.sleep(1)
        raise AssertionError(f"expected code {expected_code}, last response: {last_rsp}")

    @staticmethod
    def _remote_analyzer_collection_payload(collection_name, resource_name, db_name=None):
        payload = {
            "collectionName": collection_name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {
                        "fieldName": "id",
                        "dataType": "Int64",
                        "isPrimary": True,
                        "elementTypeParams": {},
                    },
                    {
                        "fieldName": "text",
                        "dataType": "VarChar",
                        "elementTypeParams": {
                            "max_length": "1024",
                            "enable_analyzer": True,
                            "analyzer_params": {
                                "tokenizer": "standard",
                                "filter": [
                                    {
                                        "type": "synonym",
                                        "expand": True,
                                        "synonyms_file": {
                                            "type": "remote",
                                            "resource_name": resource_name,
                                            "file_name": "synonyms.txt",
                                        },
                                    }
                                ],
                            },
                        },
                    },
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                ],
                "functions": [
                    {
                        "name": "bm25",
                        "type": "BM25",
                        "inputFieldNames": ["text"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {},
                    }
                ],
            },
            "indexParams": [
                {
                    "fieldName": "sparse_vector",
                    "indexName": "sparse_vector",
                    "metricType": "BM25",
                    "params": {"index_type": "SPARSE_INVERTED_INDEX"},
                }
            ],
        }
        if db_name is not None:
            payload["dbName"] = db_name
        return payload

    @pytest.mark.tags(CaseLabel.L0)
    def test_file_resource_crud_idempotency_and_conflict(self):
        resource_name, path = self._upload_file(b"search, retrieval, query\n")
        _, other_path = self._upload_file(b"database, db\n")

        rsp = self._add_file_resource(resource_name, path)
        assert rsp["code"] == 0, rsp

        rsp = self.file_resource_client.file_resource_list()
        assert rsp["code"] == 0, rsp
        matches = [resource for resource in rsp["data"] if resource["name"] == resource_name]
        assert len(matches) == 1, rsp
        assert matches[0]["path"] == path
        assert isinstance(matches[0]["id"], int)

        rsp = self._add_file_resource(resource_name, path)
        assert rsp["code"] == 0, rsp

        rsp = self.file_resource_client.file_resource_add({"name": resource_name, "path": other_path})
        assert rsp["code"] == 1100, rsp
        assert "already exists" in rsp["message"], rsp

        rsp = self._remove_file_resource(resource_name)
        assert rsp["code"] == 0, rsp
        rsp = self._remove_file_resource(resource_name)
        assert rsp["code"] == 0, rsp

    @pytest.mark.tags(CaseLabel.L1)
    def test_file_resource_input_validation_and_missing_path(self):
        rsp = self.file_resource_client.file_resource_add({"name": "", "path": "path.txt"})
        assert rsp["code"] == 1802, rsp

        rsp = self.file_resource_client.file_resource_add({"name": gen_unique_str("resource"), "path": ""})
        assert rsp["code"] == 1802, rsp

        rsp = self.file_resource_client.file_resource_remove({"name": ""})
        assert rsp["code"] == 1802, rsp

        rsp = self.file_resource_client.file_resource_add(
            {"name": gen_unique_str("resource"), "path": f"missing/{gen_unique_str('object')}.txt"}
        )
        assert rsp["code"] == 1100, rsp
        assert "path not exist" in rsp["message"], rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_remote_synonym_search_and_reference_lifecycle(self):
        resource_name = self._upload_and_register_synonyms()
        collection_name = gen_collection_name("rest_remote_analyzer")
        payload = self._remote_analyzer_collection_payload(collection_name, resource_name)

        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_collection_load_completed(collection_name)

        rsp = self.vector_client.vector_insert(
            {
                "collectionName": collection_name,
                "data": [
                    {"id": 1, "text": "search engine"},
                    {"id": 2, "text": "distributed database"},
                ],
            }
        )
        assert rsp["code"] == 0, rsp

        rsp = self.vector_client.vector_search(
            {
                "collectionName": collection_name,
                "data": ["retrieval"],
                "annsField": "sparse_vector",
                "outputFields": ["id", "text"],
                "limit": 10,
            }
        )
        assert rsp["code"] == 0, rsp
        assert any(hit["id"] == 1 and "search engine" in hit["text"] for hit in rsp["data"]), rsp

        rsp = self._remove_file_resource(resource_name)
        assert rsp["code"] == 1100, rsp
        assert "is still in use" in rsp["message"], rsp

        rsp = self.collection_client.collection_drop({"collectionName": collection_name})
        assert rsp["code"] == 0, rsp
        self._remove_file_resource_eventually(resource_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_file_resource_released_after_collection_and_database_drop(self):
        resource_name = self._upload_and_register_synonyms()
        db_name = gen_unique_str("rest_file_resource_db")
        collection_name = gen_collection_name("rest_remote_analyzer_db")

        rsp = self.database_client.database_create({"dbName": db_name})
        assert rsp["code"] == 0, rsp
        payload = self._remote_analyzer_collection_payload(collection_name, resource_name, db_name=db_name)
        rsp = self.collection_client.collection_create(payload, db_name=db_name)
        assert rsp["code"] == 0, rsp

        rsp = self._remove_file_resource(resource_name)
        assert rsp["code"] == 1100, rsp
        assert "is still in use" in rsp["message"], rsp

        rsp = self.collection_client.collection_drop({"dbName": db_name, "collectionName": collection_name})
        assert rsp["code"] == 0, rsp
        rsp = self.database_client.database_drop({"dbName": db_name})
        assert rsp["code"] == 0, rsp
        self._remove_file_resource_eventually(resource_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_missing_remote_resource_returns_stable_error(self):
        collection_name = gen_collection_name("rest_missing_remote_analyzer")
        payload = self._remote_analyzer_collection_payload(collection_name, gen_unique_str("missing_resource"))

        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 1100, rsp
        assert "not found in local resource list" in rsp["message"], rsp

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_file_resource_management_requires_authentication(self):
        unauthenticated_client = FileResourceClient(self.endpoint, self.invalid_api_key)
        resource_name = gen_unique_str("unauthenticated_resource")

        rsp = unauthenticated_client.file_resource_add({"name": resource_name, "path": "missing.txt"})
        assert rsp["code"] == 1800, rsp
        rsp = unauthenticated_client.file_resource_list()
        assert rsp["code"] == 1800, rsp
        rsp = unauthenticated_client.file_resource_remove({"name": resource_name})
        assert rsp["code"] == 1800, rsp

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_file_resource_management_privileges(self):
        resource_name, path = self._upload_file(b"search, retrieval, query\n")
        user_name = gen_unique_str("rest_file_resource_user")
        role_name = gen_unique_str("rest_file_resource_role")
        password = "FileResource123!"
        privileges = ["ListFileResources", "AddFileResource", "RemoveFileResource"]
        user_created = False
        role_created = False
        role_bound = False
        granted = []
        try:
            rsp = self.user_client.user_create({"userName": user_name, "password": password})
            assert rsp["code"] == 0, rsp
            user_created = True
            rsp = self.role_client.role_create({"roleName": role_name})
            assert rsp["code"] == 0, rsp
            role_created = True
            rsp = self.user_client.user_grant({"userName": user_name, "roleName": role_name})
            assert rsp["code"] == 0, rsp
            role_bound = True

            limited_client = FileResourceClient(self.endpoint, f"{user_name}:{password}")

            def add_as_limited_user():
                add_rsp = limited_client.file_resource_add({"name": resource_name, "path": path})
                if add_rsp.get("code") == 0 and resource_name not in self._file_resources_to_cleanup:
                    self._file_resources_to_cleanup.append(resource_name)
                return add_rsp

            rsp = self._wait_for_code(add_as_limited_user, 65535)
            assert "PrivilegeAddFileResource: permission deny" in rsp["message"], rsp
            rsp = self._wait_for_code(limited_client.file_resource_list, 65535)
            assert "PrivilegeListFileResources: permission deny" in rsp["message"], rsp
            rsp = self._wait_for_code(lambda: limited_client.file_resource_remove({"name": resource_name}), 65535)
            assert "PrivilegeRemoveFileResource: permission deny" in rsp["message"], rsp

            for privilege in privileges:
                payload = {
                    "roleName": role_name,
                    "dbName": "*",
                    "collectionName": "*",
                    "privilege": privilege,
                }
                rsp = self.role_client.role_grant_v2(payload)
                assert rsp["code"] == 0, rsp
                granted.append(privilege)

            self._wait_for_code(limited_client.file_resource_list, 0)
            rsp = self._wait_for_code(add_as_limited_user, 0)
            assert rsp["code"] == 0, rsp
            rsp = self._wait_for_code(lambda: limited_client.file_resource_remove({"name": resource_name}), 0)
            assert rsp["code"] == 0, rsp
            self._file_resources_to_cleanup.remove(resource_name)
        finally:
            for privilege in granted:
                self.role_client.role_revoke_v2(
                    {
                        "roleName": role_name,
                        "dbName": "*",
                        "collectionName": "*",
                        "privilege": privilege,
                    }
                )
            if role_bound:
                self.user_client.user_revoke({"userName": user_name, "roleName": role_name})
            if user_created:
                self.user_client.user_drop({"userName": user_name})
            if role_created:
                self.role_client.role_drop({"roleName": role_name})
