import json
import time

import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

FM_DIM = 4
FM_ROWS = 2048


class TestFMIndexFilters(TestBase):
    """RESTful v2 FMINDEX build validation and indexed-field differential queries."""

    def _create_collection(self, scenario):
        name = f"{self.__class__.__name__}_{scenario}_{gen_collection_name()}"
        fields = [
            {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
            {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(FM_DIM)}},
        ]
        fields.insert(
            1,
            {
                "fieldName": "content",
                "dataType": "VarChar",
                "elementTypeParams": {"max_length": "600"},
            },
        )
        fields.insert(
            2,
            {
                "fieldName": "content_no_index",
                "dataType": "VarChar",
                "elementTypeParams": {"max_length": "600"},
            },
        )
        rsp = self.collection_client.collection_create(
            {
                "collectionName": name,
                "schema": {"autoId": False, "enableDynamicField": False, "fields": fields},
                "params": {"consistencyLevel": "Strong"},
            }
        )
        assert rsp["code"] == 0, rsp
        return name

    @staticmethod
    def _content_for_id(row_id):
        filler = "y" * 500
        case = row_id % 500
        if case == 0:
            return "stadium"
        if case == 1:
            return "x" * 250 + "school" + "x" * 250
        if case == 2:
            return filler[:-7] + "library"
        return filler

    def _insert_rows(self, name):
        rows = []
        for i in range(FM_ROWS):
            row = {
                "id": i,
                "vector": [float(i), 0.0, 0.0, 0.0],
            }
            text = self._content_for_id(i)
            row["content"] = text
            row["content_no_index"] = text
            rows.append(row)
        for start in range(0, FM_ROWS, 512):
            rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows[start : start + 512]})
            assert rsp["code"] == 0, rsp
            assert rsp["data"]["insertCount"] == len(rows[start : start + 512]), rsp
        rsp = self.collection_client.flush(name)
        assert rsp["code"] == 0, rsp

    def _index_and_load(self, name):
        index_params = [
            {
                "fieldName": "vector",
                "indexName": "vector_index",
                "indexType": "FLAT",
                "metricType": "L2",
                "params": {},
            }
        ]
        index_params.append(
            {
                "fieldName": "content",
                "indexName": "content_fmindex",
                "indexType": "FMINDEX",
                "params": {"fm_sa_sample_rate": 8, "fm_block_bytes": 64},
            }
        )
        rsp = self.index_client.index_create({"collectionName": name, "indexParams": index_params})
        assert rsp["code"] == 0, rsp
        for params in index_params:
            index = self._wait_index_ready(name, params["indexName"], timeout=180)
            assert index["indexState"] == "Finished", index
            assert index.get("failReason", "") == "", index
            assert index["fieldName"] == params["fieldName"], index
            assert index["indexName"] == params["indexName"], index
            assert index["indexType"] == params["indexType"], index
            assert index["totalRows"] == FM_ROWS, index
            assert index["indexedRows"] == FM_ROWS, index
            assert index["pendingRows"] == 0, index
            if params["indexType"] == "FMINDEX":
                reported = {item["key"]: item["value"] for item in index["indexParams"]}
                assert reported["index_type"] == "FMINDEX", index
                assert json.loads(reported["params"]) == params["params"], index
        rsp = self.collection_client.collection_load(collection_name=name)
        assert rsp["code"] == 0, rsp
        self.collection_client.wait_load_completed(name, timeout=180)
        describe = self.collection_client.collection_describe(name)
        assert describe["code"] == 0, describe
        assert describe["data"]["load"] == "LoadStateLoaded", describe

    def _wait_index_ready(self, name, index_name, timeout):
        start = time.time()
        while time.time() - start < timeout:
            rsp = self.index_client.index_describe(collection_name=name, index_name=index_name)
            assert rsp["code"] == 0, rsp
            assert len(rsp["data"]) == 1, rsp
            index = rsp["data"][0]
            if index["indexState"] == "Finished":
                return index
            if index["indexState"] == "Failed":
                raise AssertionError(index)
            time.sleep(1)
        raise AssertionError(f"index {index_name} of collection {name} not ready after {timeout}s")

    def _entity_request(self, operation, payload):
        # The VectorClient convenience wrappers retry successful empty results.
        # After flush/load with Strong consistency the first result is the
        # result under test, including a legitimate empty no-match response.
        payload["dbName"] = self.vector_client.db_name or "default"
        response = self.vector_client.post(
            f"{self.endpoint}/v2/vectordb/entities/{operation}",
            headers=self.vector_client.update_headers(),
            data=payload,
        )
        assert response.status_code == 200, response.text
        rsp = response.json()
        assert rsp["code"] == 0, rsp
        assert isinstance(rsp.get("data"), list), rsp
        return rsp

    def _query_ids(self, name, filter_expr):
        rsp = self._entity_request(
            "query",
            {
                "collectionName": name,
                "filter": filter_expr,
                "outputFields": ["id"],
                "limit": FM_ROWS,
                "consistencyLevel": "Strong",
            },
        )
        rows = rsp["data"]
        for row in rows:
            assert set(row) == {"id"}, row
        return sorted(row["id"] for row in rows)

    def _search_ids(self, name, filter_expr):
        rsp = self._entity_request(
            "search",
            {
                "collectionName": name,
                "data": [[1.0] * FM_DIM],
                "annsField": "vector",
                "filter": filter_expr,
                "outputFields": ["content_no_index"],
                "limit": FM_ROWS,
                "consistencyLevel": "Strong",
                "searchParams": {"metricType": "L2"},
            },
        )
        rows = rsp["data"]
        ids = [row["id"] for row in rows]
        assert len(ids) == len(set(ids)), rsp
        distances = [row["distance"] for row in rows]
        assert distances == sorted(distances), rsp
        for row in rows:
            assert set(row) == {"id", "distance", "content_no_index"}, row
            assert row.get("content_no_index") == self._content_for_id(row["id"]), row
        return sorted(ids)

    @pytest.mark.tags(CaseLabel.L0)
    def test_restful_fmindex_anchored_like(self):
        """
        target: verify RESTful v2 can build and query a real FMINDEX
        method: insert 2048 rows, build FMINDEX plus FLAT, then compare prefix/infix/suffix result PKs
        expected: all three anchored LIKE forms return the exact expected rows
        """
        name = self._create_collection("anchored_like")
        self._insert_rows(name)
        self._index_and_load(name)

        expected = {
            'content like "sta%"': [i for i in range(FM_ROWS) if i % 500 == 0],
            'content like "%ool%"': [i for i in range(FM_ROWS) if i % 500 == 1],
            'content like "%ary"': [i for i in range(FM_ROWS) if i % 500 == 2],
            'content like "zz_no_match%"': [],
        }
        for expr, ids in expected.items():
            assert self._query_ids(name, expr) == ids, expr
            assert self._search_ids(name, expr) == ids, expr
            twin_expr = expr.replace("content", "content_no_index", 1)
            assert self._query_ids(name, twin_expr) == ids, twin_expr

    @pytest.mark.tags(CaseLabel.L1)
    def test_restful_fmindex_invalid_params(self):
        """
        target: verify RESTful v2 returns server-side FMINDEX parameter errors
        method: create a VARCHAR collection and submit invalid sample/block parameters
        expected: both requests fail with the corresponding validation message
        """
        name = self._create_collection("invalid_params")
        cases = [
            ("invalid_sample_rate", {"fm_sa_sample_rate": 257}, "fm_sa_sample_rate for FM-index must be in"),
            ("invalid_block_bytes", {"fm_block_bytes": 24}, "fm_block_bytes for FM-index must be a power of two"),
        ]
        for case_name, params, message in cases:
            rsp = self.index_client.index_create(
                {
                    "collectionName": name,
                    "indexParams": [
                        {
                            "fieldName": "content",
                            "indexName": f"bad_{case_name}",
                            "indexType": "FMINDEX",
                            "params": params,
                        }
                    ],
                }
            )
            assert rsp["code"] == 1100, rsp
            assert message in rsp.get("message", ""), rsp
