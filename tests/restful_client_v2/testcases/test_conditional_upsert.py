import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name


class TestConditionalUpsert(TestBase):
    def _create_collection(self):
        name = gen_collection_name("cond_upsert")
        self.name = name
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "version", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "status", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "4"}},
                ]
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_index", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_collection_load_completed(name)
        return name

    def _insert_base_rows(self, name):
        rows = [
            {"id": 1, "version": 10, "status": "init_1", "vector": [1.0, 0.0, 0.0, 0.0]},
            {"id": 2, "version": 10, "status": "init_2", "vector": [0.0, 1.0, 0.0, 0.0]},
            {"id": 3, "version": 10, "status": "init_3", "vector": [0.0, 0.0, 1.0, 0.0]},
        ]
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        rsp = self.collection_client.flush(name)
        assert rsp["code"] == 0, rsp

    def _query_rows(self, name):
        rsp = self.vector_client.vector_query(
            {
                "collectionName": name,
                "filter": "id >= 0",
                "outputFields": ["id", "version", "status", "vector"],
            }
        )
        assert rsp["code"] == 0, rsp
        return {row["id"]: row for row in rsp["data"]}

    @pytest.mark.tags(CaseLabel.L0)
    def test_conditional_upsert_gt_and_gte(self):
        """
        target: verify RESTful v2 conditional upsert supports gt and gte with succIndex
        method: insert seed rows, run conditional upsert with gt (including missing row insert)
                and gte (partial update omitting vector), then verify stored rows
        expected: only qualifying rows are written, succIndex matches written positions,
                  omitted fields are preserved, and unconditional upsert omits succIndex
        """
        name = self._create_collection()
        self._insert_base_rows(name)

        # GT: id=1 newer (written), id=2 equal (skipped), id=3 stale (skipped), id=4 missing (inserted by default).
        rsp = self.vector_client.vector_upsert(
            {
                "collectionName": name,
                "data": [
                    {"id": 1, "version": 12, "status": "gt_1", "vector": [1.0, 1.0, 0.0, 0.0]},
                    {"id": 2, "version": 10, "status": "gt_2", "vector": [0.0, 2.0, 0.0, 0.0]},
                    {"id": 3, "version": 8, "status": "gt_3", "vector": [0.0, 0.0, 2.0, 0.0]},
                    {"id": 4, "version": 1, "status": "new_4", "vector": [0.0, 0.0, 0.0, 1.0]},
                ],
                "writeCondition": {"field": "version", "op": "gt"},
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["upsertCount"] == 2
        assert rsp["data"]["upsertIds"] == [1, 4]
        assert rsp["data"]["succIndex"] == [0, 3]

        rows = self._query_rows(name)
        assert rows[1]["version"] == 12 and rows[1]["status"] == "gt_1"
        assert rows[2]["version"] == 10 and rows[2]["status"] == "init_2"
        assert rows[3]["version"] == 10 and rows[3]["status"] == "init_3"
        assert rows[4]["version"] == 1 and rows[4]["status"] == "new_4"

        # GTE partial update (omitting vector): id=1 equal (written), id=2 stale (skipped), id=3 newer (written).
        rsp = self.vector_client.vector_upsert(
            {
                "collectionName": name,
                "data": [
                    {"id": 1, "version": 12, "status": "gte_1"},
                    {"id": 2, "version": 9, "status": "gte_2"},
                    {"id": 3, "version": 15, "status": "gte_3"},
                ],
                "writeCondition": {"field": "version", "op": "GTE"},
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["upsertCount"] == 2
        assert rsp["data"]["upsertIds"] == [1, 3]
        assert rsp["data"]["succIndex"] == [0, 2]

        rows = self._query_rows(name)
        assert rows[1]["status"] == "gte_1"
        assert rows[1]["vector"] == [1.0, 1.0, 0.0, 0.0]
        assert rows[2]["status"] == "init_2"
        assert rows[3]["version"] == 15 and rows[3]["status"] == "gte_3"
        assert rows[3]["vector"] == [0.0, 0.0, 1.0, 0.0]

        # Unconditional upsert keeps existing response shape without succIndex.
        rsp = self.vector_client.vector_upsert(
            {
                "collectionName": name,
                "partialUpdate": True,
                "data": [{"id": 2, "status": "unconditional_2"}],
            }
        )
        assert rsp["code"] == 0, rsp
        assert "succIndex" not in rsp["data"]

    @pytest.mark.tags(CaseLabel.L0)
    def test_conditional_upsert_all_skipped(self):
        """
        target: verify all-skipped conditional upsert succeeds with empty arrays and zero count
        method: upsert stale versions for all rows
        expected: code 0, upsertCount 0, upsertIds [], succIndex [], stored rows unchanged
        """
        name = self._create_collection()
        self._insert_base_rows(name)

        rsp = self.vector_client.vector_upsert(
            {
                "collectionName": name,
                "data": [
                    {"id": 1, "version": 5, "status": "stale_1"},
                    {"id": 2, "version": 10, "status": "stale_2"},
                ],
                "writeCondition": {"field": "version", "op": "gt"},
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["upsertCount"] == 0
        assert rsp["data"]["upsertIds"] == []
        assert rsp["data"]["succIndex"] == []

        rows = self._query_rows(name)
        assert rows[1]["status"] == "init_1"
        assert rows[2]["status"] == "init_2"

    @pytest.mark.tags(CaseLabel.L0)
    def test_conditional_upsert_missing_row_policies(self):
        """
        target: verify onMissing skip and error policies in RESTful v2 conditional upsert
        method: send mixed existing/missing batches with skip and error policies
        expected: skip omits missing rows; error rejects before submission and leaves existing rows unchanged
        """
        name = self._create_collection()
        self._insert_base_rows(name)

        # onMissing=skip: existing newer row written, missing row skipped.
        rsp = self.vector_client.vector_upsert(
            {
                "collectionName": name,
                "data": [
                    {"id": 1, "version": 20, "status": "updated_1"},
                    {"id": 99, "version": 5, "status": "missing_99"},
                ],
                "writeCondition": {"field": "version", "op": "gt", "onMissing": "SKIP"},
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["upsertCount"] == 1
        assert rsp["data"]["upsertIds"] == [1]
        assert rsp["data"]["succIndex"] == [0]

        rows = self._query_rows(name)
        assert rows[1]["version"] == 20 and rows[1]["status"] == "updated_1"
        assert 99 not in rows

        # onMissing=error: fails with parameter invalid and does not modify existing row.
        rsp = self.vector_client.vector_upsert(
            {
                "collectionName": name,
                "data": [
                    {"id": 2, "version": 25, "status": "should_not_commit"},
                    {"id": 100, "version": 1, "status": "missing_100"},
                ],
                "writeCondition": {"field": "version", "op": "gt", "onMissing": "error"},
            }
        )
        assert rsp["code"] == 1100, rsp

        rows = self._query_rows(name)
        assert rows[2]["version"] == 10 and rows[2]["status"] == "init_2"
        assert 100 not in rows

    @pytest.mark.tags(CaseLabel.L1)
    def test_conditional_upsert_validation_errors(self):
        """
        target: verify invalid writeCondition arguments and insert misuse are rejected
        method: send invalid op/onMissing/field to upsert and writeCondition to insert
        expected: all invalid requests return parameter invalid error (code 1100)
        """
        name = self._create_collection()
        self._insert_base_rows(name)

        invalid_conditions = [
            {"field": "version", "op": ""},
            {"field": "version", "op": "eq"},
            {"field": "version", "op": "gt", "onMissing": "unknown"},
            {"field": "nonexistent", "op": "gt"},
            {"field": "id", "op": "gt"},
        ]
        for cond in invalid_conditions:
            rsp = self.vector_client.vector_upsert(
                {
                    "collectionName": name,
                    "data": [{"id": 1, "version": 20, "status": "bad"}],
                    "writeCondition": cond,
                }
            )
            assert rsp["code"] == 1100, f"expected 1100 for {cond}, got {rsp}"

        # Insert endpoint rejects writeCondition.
        rsp = self.vector_client.vector_insert(
            {
                "collectionName": name,
                "data": [{"id": 10, "version": 1, "status": "bad", "vector": [1.0, 0.0, 0.0, 0.0]}],
                "writeCondition": {"field": "version", "op": "gt"},
            }
        )
        assert rsp["code"] == 1100, rsp
