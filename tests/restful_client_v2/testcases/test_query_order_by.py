import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

DIM = 4
NB = 200


def _vec(seed):
    return [float(seed), 0.0, 0.0, 0.0]


def _rows(nb=NB):
    rows = []
    for i in range(nb):
        rows.append(
            {
                "id": i,
                "price": float(10 + (i % 10)),
                "score": (i * 17 + 11) % nb,
                "rating": float((i * 13) % 100) / 10,
                "category": f"category_{i % 5}",
                "nullable_score": None if i % 5 == 0 else i % 50,
                "json_field": {"value": i},
                "array_field": [i % 10, (i + 1) % 10],
                "vec": _vec(i),
            }
        )
    return rows


def _parse_order_by_field(order_by_field):
    parts = order_by_field.split(":")
    field_name = parts[0]
    direction = parts[1].lower() if len(parts) > 1 and parts[1] else "asc"
    descending = direction in ("desc", "descending")
    nulls_first = descending
    if len(parts) > 2:
        nulls_first = parts[2].lower() == "nulls_first"
    return field_name, descending, nulls_first


def _expected_rows(order_by_fields, limit, row_filter=None, offset=0):
    rows = [row for row in _rows() if row_filter is None or row_filter(row)]
    order_specs = [_parse_order_by_field(field) for field in order_by_fields]

    def order_key(row):
        key = []
        for field_name, descending, nulls_first in order_specs:
            value = row[field_name]
            if value is None:
                key.append((0 if nulls_first else 1, 0))
            else:
                key.append((1 if nulls_first else 0, -value if descending else value))
        key.append(row["id"])
        return tuple(key)

    rows = sorted(rows, key=order_key)
    return rows[offset : offset + limit]


class TestQueryOrderBy(TestBase):
    def setup_class(self):
        self.collection_name = self.__class__.__name__ + gen_collection_name()

    @pytest.fixture(scope="class", autouse=True)
    def prepare_shared_query_order_collection(self, request, init_class_config):
        collection_client, vector_client = self._class_scope_clients()

        def teardown():
            collection_client.collection_drop({"collectionName": self.collection_name})

        request.addfinalizer(teardown)
        self._create_query_order_collection(self.collection_name, collection_client, vector_client)

    def _create_query_order_collection(self, name, collection_client, vector_client):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "price", "dataType": "Double"},
                    {"fieldName": "score", "dataType": "Int64"},
                    {"fieldName": "rating", "dataType": "Double"},
                    {"fieldName": "category", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {"fieldName": "nullable_score", "dataType": "Int32", "nullable": True},
                    {"fieldName": "json_field", "dataType": "JSON"},
                    {
                        "fieldName": "array_field",
                        "dataType": "Array",
                        "elementDataType": "Int64",
                        "elementTypeParams": {"max_capacity": "8"},
                    },
                    {"fieldName": "vec", "dataType": "FloatVector", "elementTypeParams": {"dim": str(DIM)}},
                ],
            },
            "indexParams": [{"fieldName": "vec", "indexName": "vec_index", "metricType": "L2"}],
        }
        rsp = collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        collection_client.wait_load_completed(name, timeout=60)

        rows = _rows()
        rsp = vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(rows)
        rsp = collection_client.flush(name)
        assert rsp["code"] == 0, rsp

    def _shared_collection(self):
        return self.collection_name

    def _query(self, payload, timeout=1):
        rsp = self.vector_client.vector_query(payload, timeout=timeout)
        assert rsp["code"] == 0, rsp
        return rsp.get("data", [])

    @staticmethod
    def _assert_ordered(rows, field_name, descending=False):
        values = [row[field_name] for row in rows]
        for i in range(len(values) - 1):
            if descending:
                assert values[i] >= values[i + 1], f"{field_name} not descending at index {i}: {values}"
            else:
                assert values[i] <= values[i + 1], f"{field_name} not ascending at index {i}: {values}"

    @staticmethod
    def _assert_expected_ids(rows, expected_rows):
        assert [row["id"] for row in rows] == [row["id"] for row in expected_rows]

    @staticmethod
    def _assert_expected_values(rows, expected_rows, fields):
        actual = [tuple(row[field] for field in fields) for row in rows]
        expected = [tuple(row[field] for field in fields) for row in expected_rows]
        assert actual == expected

    @staticmethod
    def _assert_pk_tie_breaker(rows, fields):
        for current, following in zip(rows, rows[1:]):
            if all(current[field] == following[field] for field in fields):
                assert current["id"] < following["id"], rows

    @staticmethod
    def _assert_nulls_at_end(values):
        first_null = next((i for i, value in enumerate(values) if value is None), len(values))
        non_nulls = values[:first_null]
        nulls = values[first_null:]
        assert non_nulls, "Expected non-null values before nulls"
        assert nulls, "Expected null values at the end"
        assert all(value is None for value in nulls)
        return non_nulls

    @staticmethod
    def _assert_nulls_at_beginning(values):
        first_non_null = next((i for i, value in enumerate(values) if value is not None), len(values))
        nulls = values[:first_non_null]
        non_nulls = values[first_non_null:]
        assert nulls, "Expected null values at the beginning"
        assert non_nulls, "Expected non-null values after nulls"
        assert all(value is None for value in nulls)
        return non_nulls

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_order_by_single_field_asc(self):
        """
        target: verify REST query supports orderByFields with default ascending order
        method: query rows ordered by score without an explicit direction
        expected: returned rows are globally sorted by score ascending
        """
        rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "limit": 20,
                "outputFields": ["id", "score"],
                "orderByFields": ["score"],
            }
        )
        assert len(rows) == 20
        self._assert_ordered(rows, "score")
        expected = _expected_rows(["score"], limit=20)
        assert [row["score"] for row in rows] == [row["score"] for row in expected]
        self._assert_expected_ids(rows, expected)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_order_by_single_field_desc(self):
        """
        target: verify REST query supports descending orderByFields
        method: query rows ordered by score:desc
        expected: returned rows are globally sorted by score descending
        """
        rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "limit": 20,
                "outputFields": ["id", "score"],
                "orderByFields": ["score:desc"],
            }
        )
        assert len(rows) == 20
        self._assert_ordered(rows, "score", descending=True)
        expected = _expected_rows(["score:desc"], limit=20)
        assert [row["score"] for row in rows] == [row["score"] for row in expected]
        self._assert_expected_ids(rows, expected)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_order_by_multi_fields(self):
        """
        target: verify REST query supports multi-field orderByFields
        method: sort by price ascending and rating descending
        expected: primary and secondary sort orders are both respected
        """
        rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "limit": 80,
                "outputFields": ["id", "price", "rating"],
                "orderByFields": ["price:asc", "rating:desc"],
            }
        )
        assert len(rows) == 80
        secondary_sort_tested = False
        for i in range(len(rows) - 1):
            current = rows[i]
            following = rows[i + 1]
            assert current["price"] <= following["price"], rows
            if current["price"] == following["price"]:
                assert current["rating"] >= following["rating"], rows
                secondary_sort_tested = True
        assert secondary_sort_tested, "No duplicate price values found; secondary sort was not verified"
        expected = _expected_rows(["price:asc", "rating:desc"], limit=80)
        self._assert_expected_values(rows, expected, ["price", "rating"])
        self._assert_expected_ids(rows, expected)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_order_by_with_filter(self):
        """
        target: verify REST query applies filter before orderByFields
        method: filter a price range and sort by price ascending
        expected: all rows match the filter and are sorted by price
        """
        rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "price >= 12 && price <= 16",
                "limit": 50,
                "outputFields": ["id", "price"],
                "orderByFields": ["price:asc"],
            }
        )
        assert len(rows) == 50
        assert all(12 <= row["price"] <= 16 for row in rows)
        self._assert_ordered(rows, "price")
        expected = _expected_rows(
            ["price:asc"],
            limit=50,
            row_filter=lambda row: 12 <= row["price"] <= 16,
        )
        self._assert_expected_values(rows, expected, ["price"])
        self._assert_pk_tie_breaker(rows, ["price"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_order_by_default_limit(self):
        """
        target: verify REST query applies the default limit when orderByFields omits limit
        method: send orderByFields and omit limit
        expected: request succeeds with 100 ordered rows
        """
        rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "outputFields": ["id", "score"],
                "orderByFields": ["score:desc"],
            }
        )
        assert len(rows) == 100
        self._assert_ordered(rows, "score", descending=True)
        expected = _expected_rows(["score:desc"], limit=100)
        self._assert_expected_ids(rows, expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_with_offset(self):
        """
        target: verify REST query applies offset after ordering
        method: compare the second ordered page with a larger ordered baseline
        expected: offset page equals the matching slice of the baseline result
        """
        base_payload = {
            "collectionName": self._shared_collection(),
            "filter": "id >= 0",
            "limit": 20,
            "outputFields": ["id", "price"],
            "orderByFields": ["price:asc"],
        }
        baseline = self._query(base_payload)
        page_payload = {**base_payload, "limit": 10, "offset": 10}
        page = self._query(page_payload)
        repeated_page = self._query(page_payload)

        assert len(baseline) == 20
        assert len({row["price"] for row in baseline}) == 1
        assert [row["id"] for row in page] == [row["id"] for row in baseline[10:20]]
        assert [row["id"] for row in repeated_page] == [row["id"] for row in page]
        expected = _expected_rows(["price:asc"], limit=10, offset=10)
        self._assert_expected_ids(page, expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_nullable_default_order(self):
        """
        target: verify REST query default null ordering for orderByFields
        method: sort nullable_score ascending and descending
        expected: asc puts nulls last; desc puts nulls first
        """
        asc_rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "limit": NB,
                "outputFields": ["id", "nullable_score"],
                "orderByFields": ["nullable_score:asc"],
            }
        )
        assert len(asc_rows) == NB
        asc_non_nulls = self._assert_nulls_at_end([row["nullable_score"] for row in asc_rows])
        for i in range(len(asc_non_nulls) - 1):
            assert asc_non_nulls[i] <= asc_non_nulls[i + 1]

        desc_rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "limit": NB,
                "outputFields": ["id", "nullable_score"],
                "orderByFields": ["nullable_score:desc"],
            }
        )
        assert len(desc_rows) == NB
        desc_non_nulls = self._assert_nulls_at_beginning([row["nullable_score"] for row in desc_rows])
        for i in range(len(desc_non_nulls) - 1):
            assert desc_non_nulls[i] >= desc_non_nulls[i + 1]

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_nulls_override(self):
        """
        target: verify REST query supports explicit null ordering in orderByFields
        method: use asc:nulls_first and desc:nulls_last
        expected: explicit null placement overrides the default null ordering
        """
        nulls_first_rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "limit": NB,
                "outputFields": ["id", "nullable_score"],
                "orderByFields": ["nullable_score:asc:nulls_first"],
            }
        )
        assert len(nulls_first_rows) == NB
        non_nulls = self._assert_nulls_at_beginning([row["nullable_score"] for row in nulls_first_rows])
        for i in range(len(non_nulls) - 1):
            assert non_nulls[i] <= non_nulls[i + 1]

        nulls_last_rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "limit": NB,
                "outputFields": ["id", "nullable_score"],
                "orderByFields": ["nullable_score:desc:nulls_last"],
            }
        )
        assert len(nulls_last_rows) == NB
        non_nulls = self._assert_nulls_at_end([row["nullable_score"] for row in nulls_last_rows])
        for i in range(len(non_nulls) - 1):
            assert non_nulls[i] >= non_nulls[i + 1]

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_sort_field_not_in_output(self):
        """
        target: verify REST query can order by a field omitted from outputFields
        method: sort by price while outputting only id and category
        expected: price is absent from output and the returned ids are still price-sorted
        """
        rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "limit": 20,
                "outputFields": ["id", "category"],
                "orderByFields": ["price:asc"],
            }
        )
        assert len(rows) == 20
        assert all("price" not in row for row in rows)
        expected = _expected_rows(["price:asc"], limit=20)
        expected_prices = [row["price"] for row in expected]

        ids = [row["id"] for row in rows]
        verify_rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": f"id in {ids}",
                "limit": len(ids),
                "outputFields": ["id", "price"],
                "orderByFields": ["price:asc"],
            }
        )
        price_by_id = {row["id"]: row["price"] for row in verify_rows}
        prices_in_output_order = [price_by_id[row_id] for row_id in ids]
        assert prices_in_output_order == expected_prices
        for i in range(len(prices_in_output_order) - 1):
            assert prices_in_output_order[i] <= prices_in_output_order[i + 1]

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_empty_result(self):
        """
        target: verify REST query returns empty result when filter matches no rows
        method: query with impossible filter and orderByFields
        expected: request succeeds and data is empty
        """
        rows = self._query(
            {
                "collectionName": self._shared_collection(),
                "filter": "score > 999999",
                "limit": 10,
                "outputFields": ["id", "score"],
                "orderByFields": ["score:asc"],
            },
            timeout=0,
        )
        assert rows == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "order_by,message",
        [
            ("vec:asc", "not sortable"),
            ("json_field:asc", "not sortable"),
            ("array_field:asc", "not sortable"),
            ("unknown_field:asc", "does not exist"),
            ("score:invalid", "invalid order direction"),
            (":asc", "does not exist"),
        ],
    )
    def test_query_order_by_invalid_params(self, order_by, message):
        """
        target: verify REST query rejects invalid orderByFields
        method: send unsupported field types, unknown field, invalid direction, and empty field
        expected: each request fails with a validation error
        """
        rsp = self.vector_client.vector_query(
            {
                "collectionName": self._shared_collection(),
                "filter": "id >= 0",
                "limit": 10,
                "outputFields": ["id"],
                "orderByFields": [order_by],
            }
        )
        assert rsp["code"] == 1100, rsp
        assert message in rsp["message"], rsp
