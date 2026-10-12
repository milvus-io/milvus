import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

ROWS = [
    {"id": 1, "category": "A", "price": 30, "rating": 5.0, "vector": [0.8, 0.0]},
    {"id": 2, "category": "A", "price": 10, "rating": 2.0, "vector": [0.7, 0.0]},
    {"id": 3, "category": "B", "price": 20, "rating": 1.0, "vector": [1.0, 0.0]},
    {"id": 4, "category": "C", "price": 5, "rating": 4.0, "vector": [0.9, 0.0]},
    {"id": 5, "category": "A", "price": 25, "rating": 3.0, "vector": [0.6, 0.0]},
]
EXPECTED_DISTANCE_BY_ID = {1: 0.8, 2: 0.7, 3: 1.0, 4: 0.9, 5: 0.6}


class TestSearchOrderBy(TestBase):
    @pytest.fixture(scope="class", autouse=True)
    def prepare_shared_search_order_collection(self, request, init_class_config):
        collection_name = gen_collection_name(prefix=request.cls.__name__)
        request.cls.collection_name = collection_name
        collection_client, vector_client = self._class_scope_clients()

        def teardown():
            collection_client.collection_drop({"collectionName": collection_name})

        request.addfinalizer(teardown)
        payload = {
            "collectionName": collection_name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "category", "dataType": "VarChar", "elementTypeParams": {"max_length": "16"}},
                    {"fieldName": "price", "dataType": "Int64"},
                    {"fieldName": "rating", "dataType": "Double"},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
                ],
            },
            "indexParams": [
                {
                    "fieldName": "vector",
                    "indexName": "vector_index",
                    "indexType": "FLAT",
                    "metricType": "IP",
                }
            ],
        }
        rsp = collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        collection_client.wait_load_completed(collection_name, timeout=60)

        rsp = vector_client.vector_insert({"collectionName": collection_name, "data": ROWS})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(ROWS)
        rsp = collection_client.flush(collection_name)
        assert rsp["code"] == 0, rsp

    def _search(self, **overrides):
        payload = {
            "collectionName": self.collection_name,
            "data": [[1.0, 0.0]],
            "annsField": "vector",
            "limit": len(ROWS),
            "outputFields": ["id", "category", "price", "rating"],
            "consistencyLevel": "Strong",
        }
        payload.update(overrides)
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] == 0, rsp
        rows = rsp.get("data", [])
        self._assert_id_distance_association(rows)
        return rows

    def _create_tied_order_collection(self):
        collection_name = gen_collection_name(prefix=f"{self.__class__.__name__}Tied")
        collection_client, vector_client = self._class_scope_clients()
        rows = [
            {"id": 20, "price": 10, "vector": [1.0, 0.0]},
            {"id": 30, "price": 10, "vector": [0.9, 0.0]},
            {"id": 10, "price": 10, "vector": [0.8, 0.0]},
            {"id": 40, "price": 20, "vector": [0.7, 0.0]},
            {"id": 50, "price": 30, "vector": [0.6, 0.0]},
        ]
        rsp = collection_client.collection_create(
            {
                "collectionName": collection_name,
                "schema": {
                    "autoId": False,
                    "enableDynamicField": False,
                    "fields": [
                        {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                        {"fieldName": "price", "dataType": "Int64"},
                        {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
                    ],
                },
                "indexParams": [
                    {
                        "fieldName": "vector",
                        "indexName": "vector_index",
                        "indexType": "FLAT",
                        "metricType": "IP",
                    }
                ],
            }
        )
        assert rsp["code"] == 0, rsp
        rsp = vector_client.vector_insert({"collectionName": collection_name, "data": rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(rows)
        rsp = collection_client.flush(collection_name)
        assert rsp["code"] == 0, rsp
        collection_client.wait_load_completed(collection_name, timeout=60)
        return collection_name

    @staticmethod
    def _assert_id_distance_association(rows):
        for row in rows:
            assert row["id"] in EXPECTED_DISTANCE_BY_ID
            assert row["distance"] == pytest.approx(EXPECTED_DISTANCE_BY_ID[row["id"]])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize(
        "order_by,field_name,expected_values,expected_ids",
        [
            (["price"], "price", [5, 10, 20, 25, 30], [4, 2, 3, 5, 1]),
            (["rating:desc"], "rating", [5.0, 4.0, 3.0, 2.0, 1.0], [1, 4, 5, 2, 3]),
        ],
    )
    def test_search_order_by_single_field(self, order_by, field_name, expected_values, expected_ids):
        """
        target: verify REST search supports top-level orderByFields in ascending and descending order
        method: search deterministic candidates whose ANN, price, and rating orders are deliberately distinct
        expected: each scalar field produces its own exact requested value and ID order
        """
        rows = self._search(orderByFields=order_by)
        assert [row[field_name] for row in rows] == expected_values
        assert [row["id"] for row in rows] == expected_ids

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_order_by_with_filter(self):
        """
        target: verify REST search combines filter with top-level orderByFields
        method: request three category-A rows even though the global top-three ANN candidates include B and C
        expected: filtering happens before candidate selection, then price ordering returns ids 2, 5, and 1
        """
        rows = self._search(limit=3, filter='category == "A"', orderByFields=["price:asc"])
        assert len(rows) == 3
        assert all(row["category"] == "A" for row in rows)
        assert [row["id"] for row in rows] == [2, 5, 1]
        assert [row["price"] for row in rows] == [10, 25, 30]

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_order_by_sort_field_not_in_output(self):
        """
        target: verify REST search can sort by a field omitted from outputFields
        method: sort by price while requesting only id and category
        expected: ids follow price order and the response does not expose price or vector
        """
        rows = self._search(outputFields=["id", "category"], orderByFields=["price:asc"])
        expected_rows = sorted(ROWS, key=lambda row: row["price"])
        assert [(row["id"], row["category"]) for row in rows] == [(row["id"], row["category"]) for row in expected_rows]
        assert all(set(row) == {"id", "category", "distance"} for row in rows)

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_order_by_multi_fields(self):
        """
        target: verify REST search supports multiple top-level orderByFields
        method: sort by category and price so the secondary key differs from ANN, rating, and PK order within category A
        expected: both sort keys are applied and return ids 2, 5, 1, 3, and 4
        """
        rows = self._search(orderByFields=["category:asc", "price:asc"])
        assert [row["id"] for row in rows] == [2, 5, 1, 3, 4]
        assert [(row["category"], row["price"]) for row in rows] == [
            ("A", 10),
            ("A", 25),
            ("A", 30),
            ("B", 20),
            ("C", 5),
        ]

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_order_by_with_offset(self):
        """
        target: verify REST search applies offset after top-level orderByFields
        method: take the top three ANN candidates, sort by price ascending, skip one, and return two
        expected: pagination returns ids 3 and 1 after scalar ordering
        """
        rows = self._search(limit=2, offset=1, orderByFields=["price:asc"])
        assert [row["id"] for row in rows] == [3, 1]
        assert [row["price"] for row in rows] == [20, 30]

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_order_by_tied_keys_and_pagination(self):
        """
        target: verify explicit tie-break ordering and pagination across an equal-key block
        method: use equal prices whose ANN and primary-key orders differ, then sort by price and id
        expected: the full ordered result is stable and offset/limit returns the exact tied-block page
        """
        collection_name = self._create_tied_order_collection()
        try:
            payload = {
                "collectionName": collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "outputFields": ["id", "price"],
                "consistencyLevel": "Strong",
                "limit": 4,
                "orderByFields": ["price:asc", "id:asc"],
            }
            baseline_rsp = self.vector_client.vector_search(payload)
            assert baseline_rsp["code"] == 0, baseline_rsp
            baseline = baseline_rsp["data"]
            assert [row["id"] for row in baseline] == [10, 20, 30, 40]
            assert [row["price"] for row in baseline] == [10, 10, 10, 20]
            assert [row["distance"] for row in baseline] == pytest.approx([0.8, 1.0, 0.9, 0.7])
            assert len({row["id"] for row in baseline}) == len(baseline)
            assert {row["id"] for row in baseline} == {10, 20, 30, 40}

            page_payload = {**payload, "limit": 2, "offset": 2}
            for _ in range(3):
                page_rsp = self.vector_client.vector_search(page_payload)
                assert page_rsp["code"] == 0, page_rsp
                page = page_rsp["data"]
                assert [row["id"] for row in page] == [30, 40]
                assert [row["price"] for row in page] == [10, 20]
                assert [row["distance"] for row in page] == pytest.approx([0.9, 0.7])
                assert [row["id"] for row in page] == [row["id"] for row in baseline[2:4]]
        finally:
            self.collection_client.collection_drop({"collectionName": collection_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "search_params",
        [
            {"order_by_fields": "price:desc"},
            {"params": {"order_by_fields": "price:desc"}},
        ],
    )
    def test_search_order_by_with_legacy_parameter_rejected(self, search_params):
        """
        target: verify top-level orderByFields rejects ambiguous legacy order_by_fields
        method: provide the same semantic parameter at the top level and in either legacy location
        expected: REST rejects both ambiguous payload forms with code 1100
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": len(ROWS),
                "orderByFields": ["price:asc"],
                "searchParams": search_params,
            }
        )
        assert rsp["code"] == 1100, rsp
        assert "ambiguous order by" in rsp["message"], rsp

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "order_by,message",
        [
            (["unknown_field:asc"], "does not exist"),
            (["vector:asc"], "unsortable type"),
            (["price:invalid"], "invalid order direction"),
        ],
    )
    def test_search_order_by_invalid_params(self, order_by, message):
        """
        target: verify REST search surfaces server validation for invalid orderByFields
        method: send an unknown field, vector field, and invalid direction
        expected: each request fails with parameter error code 1100
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [[1.0, 0.0]],
                "annsField": "vector",
                "limit": len(ROWS),
                "orderByFields": order_by,
            }
        )
        assert rsp["code"] == 1100, rsp
        assert message in rsp["message"], rsp
