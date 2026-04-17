import time

import pytest
from pymilvus import DataType

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
from utils.util_pymilvus import *

default_nb = 3200
default_dim = 8
default_batch = 1000
default_growing = 200

# Global collection names with unique suffix to avoid conflicts across parallel runs
VALID_COLLECTION_NAME = "test_query_order_valid_" + cf.gen_unique_str("_")
CUSTOM_PARTITION_NAME = "odd_partition"

# Data generation constants
PRICE_MIN = 10.0
RATING_MIN = 0.0
CATEGORIES = [
    "electronics",
    "tools",
    "books",
    "sports",
    "movies",
    "games",
    "furniture",
    "health",
    "toys",
    "jewelry",
    "cosmetics",
    "clothing",
    "garden",
    "food",
    "travel",
    "pet_supplies",
    "beauty",
    "appliances",
    "automotive",
    "music",
]


def build_query_order_schema(client):
    """Build schema for query order by tests."""
    schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("price", DataType.DOUBLE)
    schema.add_field("rating", DataType.DOUBLE)
    schema.add_field("category", DataType.VARCHAR, max_length=64)
    schema.add_field("int32_field", DataType.INT32)
    schema.add_field("bool_field", DataType.BOOL)
    schema.add_field("int8_field", DataType.INT8)
    schema.add_field("int16_field", DataType.INT16)
    schema.add_field("float_field", DataType.FLOAT)
    schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)
    schema.add_field("nullable_score", DataType.INT32, nullable=True)
    schema.add_field("nullable_varchar", DataType.VARCHAR, max_length=256, nullable=True)
    schema.add_field("json_field", DataType.JSON)
    schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT32, max_capacity=10)
    schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=default_dim)
    return schema


def gen_query_order_data(nb, start_id=0):
    """Generate deterministic test data for order-by testing.
    ~20% of rows have NULL nullable fields."""
    rows = []
    for i in range(nb):
        idx = start_id + i
        row = {
            "id": idx,
            "price": PRICE_MIN + (idx % 50),
            "rating": round(RATING_MIN + (idx % 50) * 0.1, 1),
            "category": CATEGORIES[idx % len(CATEGORIES)],
            "int32_field": idx % 500,
            "bool_field": idx % 2 == 0,
            "int8_field": idx % 127,
            "int16_field": idx % 1000,
            "float_field": round((idx % 1000) * 0.1, 1),
            "varchar_field": f"str_{idx:06d}",
            "json_field": {"val": idx},
            "array_field": [idx % 10, (idx + 1) % 10, (idx + 2) % 10],
            "dyn_age": idx % 80,
            "embeddings": list(cf.gen_vectors(1, default_dim)[0]),
        }
        if idx % 5 == 0:
            row["nullable_score"] = None
            row["nullable_varchar"] = None
        else:
            row["nullable_score"] = idx % 200
            row["nullable_varchar"] = f"nstr_{idx:06d}"
        rows.append(row)
    return rows


# =====================================================================
# Valid test cases — shared Collection A
# =====================================================================
@pytest.mark.xdist_group("TestMilvusClientQueryOrderValid")
class TestMilvusClientQueryOrderValid(TestMilvusClientV2Base):
    """Test cases for query with order_by_fields — valid scenarios."""

    @pytest.fixture(scope="module", autouse=True)
    def prepare_valid_collection(self, request):
        """Create shared collection: 3 sealed segments (3x1000) + 1 growing (200).
        INVERTED index on int32_field for index coverage."""
        client = self._client()
        collection_name = VALID_COLLECTION_NAME
        if client.has_collection(collection_name):
            client.drop_collection(collection_name)

        schema = build_query_order_schema(client)

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="embeddings", index_type="HNSW", metric_type="COSINE", M=16, efConstruction=200
        )
        index_params.add_index(field_name="int32_field", index_type="INVERTED")

        client.create_collection(
            collection_name=collection_name, schema=schema, index_params=index_params, consistency_level="Strong"
        )
        client.create_partition(collection_name, CUSTOM_PARTITION_NAME)

        # 3 batches → 3 sealed segments, even ids → _default, odd ids → custom partition
        for i in range(3):
            start_id = i * default_batch
            data = gen_query_order_data(default_batch, start_id=start_id)
            default_data = [r for r in data if r["id"] % 2 == 0]
            odd_data = [r for r in data if r["id"] % 2 == 1]
            if default_data:
                client.insert(collection_name=collection_name, data=default_data)
            if odd_data:
                client.insert(collection_name=collection_name, data=odd_data, partition_name=CUSTOM_PARTITION_NAME)
            client.flush(collection_name=collection_name)

        # 1 growing segment (no flush), same partition split
        growing_start = 3 * default_batch
        growing_data = gen_query_order_data(default_growing, start_id=growing_start)
        even_growing = [r for r in growing_data if r["id"] % 2 == 0]
        odd_growing = [r for r in growing_data if r["id"] % 2 == 1]
        if even_growing:
            client.insert(collection_name=collection_name, data=even_growing)
        if odd_growing:
            client.insert(collection_name=collection_name, data=odd_growing, partition_name=CUSTOM_PARTITION_NAME)

        # Wait until all 3200 rows are visible
        expected = 3 * default_batch + default_growing
        for _ in range(30):
            count = client.query(
                collection_name, filter="id >= 0", output_fields=["count(*)"], consistency_level="Strong"
            )
            if count[0]["count(*)"] >= expected:
                break
            time.sleep(1)
        else:
            log.warning(f"Only {count[0]['count(*)']} rows visible after 30s, expected {expected}")

        def teardown():
            try:
                if self.has_collection(client, VALID_COLLECTION_NAME):
                    self.drop_collection(client, VALID_COLLECTION_NAME)
            except Exception:
                pass

        request.addfinalizer(teardown)

    # ==================== 5.1 L0: Basic Functionality ====================

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_order_by_single_field_asc(self):
        """OB-001: Single field ascending"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=20,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_order_by_single_field_desc(self):
        """OB-002: Single field descending"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=20,
            order_by_fields=[{"field": "price", "order": "desc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] >= prices[i + 1], f"price not descending at index {i}: {prices[i]} < {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("direction", ["ascending", "descending"])
    def test_query_order_by_direction_variants(self, direction):
        """OB-003: Direction variants — ascending/descending"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=20,
            order_by_fields=[{"field": "price", "order": direction}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        prices = [r["price"] for r in res]
        is_asc = direction == "ascending"
        for i in range(len(prices) - 1):
            if is_asc:
                assert prices[i] <= prices[i + 1], f"price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"
            else:
                assert prices[i] >= prices[i + 1], f"price not descending at index {i}: {prices[i]} < {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_order_by_multi_fields(self):
        """OB-004: Multi-field sort — primary int32 asc, secondary rating desc"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "int32_field", "rating"],
            limit=50,
            order_by_fields=[
                {"field": "int32_field", "order": "asc"},
                {"field": "rating", "order": "desc"},
            ],
            consistency_level="Strong",
        )[0]

        assert len(res) == 50
        secondary_sort_tested = False
        for i in range(len(res) - 1):
            v1 = res[i]["int32_field"]
            v2 = res[i + 1]["int32_field"]
            assert v1 <= v2, f"int32_field not ascending at index {i}: {v1} > {v2}"
            if v1 == v2:
                assert res[i]["rating"] >= res[i + 1]["rating"], f"rating not descending within same int32 at index {i}"
                secondary_sort_tested = True
        assert secondary_sort_tested, "No duplicate int32_field values found — secondary sort not tested"

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_order_by_with_filter(self):
        """OB-005: ORDER BY combined with filter"""
        client = self._client()
        filter_expr = "price >= 20.0 && price <= 40.0"
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter=filter_expr,
            output_fields=["id", "price"],
            limit=200,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 200
        prices = [r["price"] for r in res]
        for p in prices:
            assert 20.0 <= p <= 40.0, f"price {p} not in filter range [20.0, 40.0]"
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("limit", [1, 5, 50, 100, 500])
    def test_query_order_by_different_limit(self, limit):
        """OB-006: ORDER BY with different limit values"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=limit,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == limit
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1]
        # First result should be the global minimum price
        assert prices[0] == PRICE_MIN, f"Expected min price {PRICE_MIN}, got {prices[0]}"

    # ==================== 5.2 L1: Parameter Format Variants ====================
    # OB-007 (Dict list format) is covered by OB-001

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_string_list_format(self):
        """OB-008: String list format — order_by_fields=["price:asc"]"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=20,
            order_by_fields=["price:asc"],
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_raw_string_format(self):
        """OB-009: Raw string via order_by kwarg"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price", "rating"],
            limit=20,
            order_by="price:asc,rating:desc",
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        for i in range(len(res) - 1):
            assert res[i]["price"] <= res[i + 1]["price"], f"price not ascending at index {i}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_dict_without_order_key(self):
        """OB-010: Dict without 'order' key — should default to ascending"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=20,
            order_by_fields=[{"field": "price"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("direction", ["ASC", "Desc", "DESCENDING", "Ascending"])
    def test_query_order_by_direction_case_variants(self, direction):
        """OB-011: Direction case variants — verify whether accepted or rejected"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=20,
            order_by_fields=[{"field": "price", "order": direction}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        prices = [r["price"] for r in res]
        is_asc = direction.lower() in ("asc", "ascending")
        for i in range(len(prices) - 1):
            if is_asc:
                assert prices[i] <= prices[i + 1]
            else:
                assert prices[i] >= prices[i + 1]

    # ==================== 5.3 L1: Data Type Coverage ====================

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "field_name,order",
        [
            ("int8_field", "asc"),
            ("int8_field", "desc"),
            ("int16_field", "asc"),
            ("int16_field", "desc"),
            ("int32_field", "asc"),
            ("int32_field", "desc"),
            ("id", "asc"),
            ("id", "desc"),
            ("float_field", "asc"),
            ("float_field", "desc"),
            ("price", "asc"),
            ("price", "desc"),
            ("category", "asc"),
            ("category", "desc"),
        ],
    )
    def test_query_order_by_scalar_types(self, field_name, order):
        """OB-012~018: Sort by various scalar data types"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", field_name],
            limit=50,
            order_by_fields=[{"field": field_name, "order": order}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 50
        values = [r[field_name] for r in res]
        for i in range(len(values) - 1):
            if order == "asc":
                assert values[i] <= values[i + 1], (
                    f"{field_name} not ascending at index {i}: {values[i]} > {values[i + 1]}"
                )
            else:
                assert values[i] >= values[i + 1], (
                    f"{field_name} not descending at index {i}: {values[i]} < {values[i + 1]}"
                )

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_bool_field(self):
        """OB-019: Bool field — not in user guide supported types, verify behavior"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "bool_field"],
            limit=100,
            order_by_fields=[{"field": "bool_field", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 100
        values = [r["bool_field"] for r in res]
        # If accepted: ASC should be False before True
        seen_true = False
        for v in values:
            if v is True:
                seen_true = True
            if v is False and seen_true:
                pytest.fail("bool_field ASC: found False after True")

    # ==================== 5.4 L2: Pagination ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_pagination(self):
        """OB-020~023: Pagination with ORDER BY (single field, PK as implicit tie-breaker)."""
        client = self._client()
        order_by = [{"field": "price", "order": "asc"}]

        # --- OB-020: Page 1 and Page 2 consistency ---
        page1 = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=10,
            offset=0,
            order_by_fields=order_by,
            consistency_level="Strong",
        )[0]
        page2 = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=10,
            offset=10,
            order_by_fields=order_by,
            consistency_level="Strong",
        )[0]
        assert len(page1) == 10
        assert len(page2) == 10
        ids1 = {r["id"] for r in page1}
        ids2 = {r["id"] for r in page2}
        assert ids1.isdisjoint(ids2), f"Overlap between pages: {ids1 & ids2}"
        assert page1[-1]["price"] <= page2[0]["price"], (
            f"Pages not contiguous: page1 last {page1[-1]['price']} > page2 first {page2[0]['price']}"
        )

        # --- OB-021: Multi-page full traversal ---
        page_size = 100
        all_ids = set()
        all_prices = []
        offset = 0
        while True:
            page = self.query(
                client,
                VALID_COLLECTION_NAME,
                filter="",
                output_fields=["id", "price"],
                limit=page_size,
                offset=offset,
                order_by_fields=order_by,
                consistency_level="Strong",
            )[0]
            if len(page) == 0:
                break
            page_ids = {r["id"] for r in page}
            assert page_ids.isdisjoint(all_ids), f"Duplicate ids at offset {offset}"
            all_ids.update(page_ids)
            all_prices.extend([r["price"] for r in page])
            offset += page_size
            if offset > default_nb + 1000:
                break
        assert len(all_ids) == default_nb, f"Expected {default_nb} total rows, got {len(all_ids)}"
        for i in range(len(all_prices) - 1):
            assert all_prices[i] <= all_prices[i + 1], (
                f"Global sort broken at index {i}: {all_prices[i]} > {all_prices[i + 1]}"
            )

        # --- OB-022: Offset exceeds total rows (but within topK cap 16384) ---
        # Data has 3200 rows, offset=4000 is beyond data but offset+limit=4010 < 16384
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=10,
            offset=4000,
            order_by_fields=order_by,
            consistency_level="Strong",
        )[0]
        assert len(res) == 0

        # --- OB-023: Limit exceeds total rows (but within topK cap 16384) ---
        # Data has 3200 rows, limit=16000 > total but < 16384
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=16000,
            order_by_fields=order_by,
            consistency_level="Strong",
        )[0]
        assert len(res) == default_nb
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1]
        log.info(f"OB-023: limit=16000, returned {len(res)} rows (total={default_nb})")

    # ==================== 5.5 L1: Nullable Field Handling ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_nullable_int32_asc(self):
        """OB-024: Nullable INT32 ASC — non-NULL ascending first, NULLs at end (NULLS LAST)"""
        client = self._client()
        # Use filter to limit scope: id < 200 → 160 non-NULL + 40 NULL
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="id < 200",
            output_fields=["id", "nullable_score"],
            limit=200,
            order_by_fields=[{"field": "nullable_score", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 200
        values = [r["nullable_score"] for r in res]
        # Find first NULL position
        first_null = next((i for i, v in enumerate(values) if v is None), len(values))
        non_nulls = values[:first_null]
        nulls = values[first_null:]
        # Must have both non-NULL and NULL values
        assert len(non_nulls) > 0, "Expected some non-NULL values"
        assert len(nulls) > 0, "Expected some NULL values"
        # Non-NULL values should be ascending
        for i in range(len(non_nulls) - 1):
            assert non_nulls[i] <= non_nulls[i + 1], (
                f"nullable_score not ascending at index {i}: {non_nulls[i]} > {non_nulls[i + 1]}"
            )
        # All values after first NULL should also be NULL
        for v in nulls:
            assert v is None, f"Expected NULL after first NULL, got {v}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_nullable_int32_desc(self):
        """OB-025: Nullable INT32 DESC — NULLs at beginning (NULLS FIRST), then non-NULL descending"""
        client = self._client()
        # Use filter to limit scope: id < 200 → 160 non-NULL + 40 NULL
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="id < 200",
            output_fields=["id", "nullable_score"],
            limit=200,
            order_by_fields=[{"field": "nullable_score", "order": "desc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 200
        values = [r["nullable_score"] for r in res]
        # NULLs should be at the beginning (NULLS FIRST in DESC, per PostgreSQL convention)
        first_non_null = next((i for i, v in enumerate(values) if v is not None), len(values))
        nulls = values[:first_non_null]
        non_nulls = values[first_non_null:]
        # Must have both
        assert len(nulls) > 0, "Expected some NULL values at beginning"
        assert len(non_nulls) > 0, "Expected some non-NULL values"
        # All leading values should be NULL
        for v in nulls:
            assert v is None, f"Expected NULL at beginning, got {v}"
        # Non-NULL values should be descending
        for i in range(len(non_nulls) - 1):
            assert non_nulls[i] >= non_nulls[i + 1], (
                f"nullable_score not descending at index {i}: {non_nulls[i]} < {non_nulls[i + 1]}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_nullable_varchar_asc(self):
        """OB-026: Nullable VarChar ASC — non-NULL lexicographic ascending, NULLs at end"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="id < 200",
            output_fields=["id", "nullable_varchar"],
            limit=200,
            order_by_fields=[{"field": "nullable_varchar", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 200
        values = [r["nullable_varchar"] for r in res]
        first_null = next((i for i, v in enumerate(values) if v is None), len(values))
        non_nulls = values[:first_null]
        nulls = values[first_null:]
        assert len(non_nulls) > 0, "Expected some non-NULL values"
        assert len(nulls) > 0, "Expected some NULL values"
        for i in range(len(non_nulls) - 1):
            assert non_nulls[i] <= non_nulls[i + 1], (
                f"nullable_varchar not ascending at index {i}: {non_nulls[i]} > {non_nulls[i + 1]}"
            )
        for v in nulls:
            assert v is None, f"Expected NULL after first NULL, got {v}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_nullable_varchar_desc(self):
        """OB-027: Nullable VarChar DESC — NULLs first, then non-NULL descending"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="id < 200",
            output_fields=["id", "nullable_varchar"],
            limit=200,
            order_by_fields=[{"field": "nullable_varchar", "order": "desc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 200
        values = [r["nullable_varchar"] for r in res]
        first_non_null = next((i for i, v in enumerate(values) if v is not None), len(values))
        nulls = values[:first_non_null]
        non_nulls = values[first_non_null:]
        assert len(nulls) > 0, "Expected some NULL values at beginning"
        assert len(non_nulls) > 0, "Expected some non-NULL values"
        for v in nulls:
            assert v is None, f"Expected NULL at beginning, got {v}"
        for i in range(len(non_nulls) - 1):
            assert non_nulls[i] >= non_nulls[i + 1], (
                f"nullable_varchar not descending at index {i}: {non_nulls[i]} < {non_nulls[i + 1]}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_all_null_values(self):
        """OB-028: Filter to only NULL rows, sort — all NULL, PK as implicit tie-breaker"""
        client = self._client()
        # idx % 5 == 0 rows have NULL nullable_score
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="nullable_score is null",
            output_fields=["id", "nullable_score"],
            limit=200,
            order_by_fields=[{"field": "nullable_score", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) > 0, "Expected some NULL rows"
        for r in res:
            assert r["nullable_score"] is None, f"Expected NULL nullable_score, got {r['nullable_score']}"
        # All values are NULL, PK (id) should serve as implicit tie-breaker (ascending)
        ids = [r["id"] for r in res]
        for i in range(len(ids) - 1):
            assert ids[i] < ids[i + 1], f"id not ascending among NULL rows at index {i}: {ids[i]} >= {ids[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_nulls_first_override(self):
        """OB-029: Explicit nulls_first override — ASC with nulls_first puts NULLs at beginning"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="id < 200",
            output_fields=["id", "nullable_score"],
            limit=200,
            order_by_fields=["nullable_score:asc:nulls_first"],
            consistency_level="Strong",
        )[0]

        assert len(res) == 200
        values = [r["nullable_score"] for r in res]
        # NULLs should be at the beginning (overriding ASC default of NULLS LAST)
        first_non_null = next((i for i, v in enumerate(values) if v is not None), len(values))
        nulls = values[:first_non_null]
        non_nulls = values[first_non_null:]
        assert len(nulls) > 0, "Expected NULLs at beginning with nulls_first"
        assert len(non_nulls) > 0, "Expected some non-NULL values"
        for v in nulls:
            assert v is None
        for i in range(len(non_nulls) - 1):
            assert non_nulls[i] <= non_nulls[i + 1], (
                f"nullable_score not ascending at index {i}: {non_nulls[i]} > {non_nulls[i + 1]}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_nulls_last_override(self):
        """OB-030: Explicit nulls_last override — DESC with nulls_last puts NULLs at end"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="id < 200",
            output_fields=["id", "nullable_score"],
            limit=200,
            order_by_fields=["nullable_score:desc:nulls_last"],
            consistency_level="Strong",
        )[0]

        assert len(res) == 200
        values = [r["nullable_score"] for r in res]
        # NULLs should be at the end (overriding DESC default of NULLS FIRST)
        first_null = next((i for i, v in enumerate(values) if v is None), len(values))
        non_nulls = values[:first_null]
        nulls = values[first_null:]
        assert len(non_nulls) > 0, "Expected some non-NULL values"
        assert len(nulls) > 0, "Expected NULLs at end with nulls_last"
        non_null_rows = res[:first_null]
        null_rows = res[first_null:]
        for i in range(len(non_null_rows) - 1):
            a, b = non_null_rows[i], non_null_rows[i + 1]
            assert a["nullable_score"] >= b["nullable_score"], (
                f"nullable_score not descending at index {i}: {a['nullable_score']} < {b['nullable_score']}"
            )
            # PK as tie-breaker when values are equal
            if a["nullable_score"] == b["nullable_score"]:
                assert a["id"] < b["id"], f"id not ascending as tie-breaker at index {i}: {a['id']} >= {b['id']}"
        for v in nulls:
            assert v is None, f"Expected NULL at end, got {v}"
        # Among NULL rows, PK (id) should be ascending as tie-breaker
        null_ids = [r["id"] for r in null_rows]
        for i in range(len(null_ids) - 1):
            assert null_ids[i] < null_ids[i + 1], (
                f"id not ascending among NULL rows at index {i}: {null_ids[i]} >= {null_ids[i + 1]}"
            )

    # ==================== 5.6 L1: Output Fields Relationship ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_sort_field_not_in_output(self):
        """OB-031: Sort field NOT in output_fields — sorted by price but price not in output"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "category"],
            limit=20,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        # price should NOT appear in output
        for r in res:
            assert "price" not in r, f"price should not be in output, got keys: {list(r.keys())}"
        # Verify sort correctness via second query
        ids = [r["id"] for r in res]
        verify = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter=f"id in {ids}",
            output_fields=["id", "price"],
            consistency_level="Strong",
        )[0]
        price_map = {r["id"]: r["price"] for r in verify}
        prices_in_order = [price_map[rid] for rid in ids]
        for i in range(len(prices_in_order) - 1):
            assert prices_in_order[i] <= prices_in_order[i + 1], (
                f"Sort by price not correct at index {i}: {prices_in_order[i]} > {prices_in_order[i + 1]}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_sort_field_in_output(self):
        """OB-032: Sort field IN output_fields — results sorted and price visible in output"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price", "category"],
            limit=20,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        for r in res:
            assert "price" in r, f"price should be in output, got keys: {list(r.keys())}"
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_output_fields_complete(self):
        """OB-061: All specified output_fields are present in every result row"""
        client = self._client()
        output_fields = ["id", "price", "rating", "category", "int32_field"]
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=output_fields,
            limit=20,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 20
        for r in res:
            for field in output_fields:
                assert field in r, f"Field '{field}' missing in result row, got keys: {list(r.keys())}"
        # Verify sorting is also correct
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    # ==================== 5.7 L1: No Filter / Empty Results ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_empty_filter_full_scan(self):
        """OB-033: Empty filter (full scan) — top-10 from entire collection"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=10,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 10
        prices = [r["price"] for r in res]
        assert prices[0] == PRICE_MIN, f"Expected min price {PRICE_MIN}, got {prices[0]}"
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1]

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_filter_matches_nothing(self):
        """OB-034: Filter matches nothing — returns empty list, no error"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="int32_field > 999999",
            output_fields=["id", "int32_field"],
            limit=10,
            order_by_fields=[{"field": "int32_field", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 0, f"Expected empty result, got {len(res)} rows"

    # ==================== 5.8 L1: Partition Support ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_order_by_within_partition(self):
        """OB-035: ORDER BY within partition — sorted results from specified partition only"""
        client = self._client()
        # odd_partition contains only odd ids (id % 2 == 1), total 1600 rows
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=50,
            partition_names=[CUSTOM_PARTITION_NAME],
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 50
        # All ids should be odd (from the custom partition)
        for r in res:
            assert r["id"] % 2 == 1, f"Expected odd id from {CUSTOM_PARTITION_NAME}, got {r['id']}"
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"price not ascending at index {i}: {prices[i]} > {prices[i + 1]}"

    # ==================== 5.9 L2: Segment & Index Coverage ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_multiple_sealed_segments(self):
        """OB-036: Multiple sealed segments — global sort across 3 sealed segments.
        Each segment has price range 10.0~59.0 (overlapping), verifies cross-segment merge sort."""
        client = self._client()
        # id < 3000 spans 3 sealed segments (0~999, 1000~1999, 2000~2999)
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="id < 3000",
            output_fields=["id", "price"],
            limit=3000,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 3000
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"Cross-segment sort broken at index {i}: {prices[i]} > {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_sealed_and_growing_mixed(self):
        """OB-037: Sealed + growing mixed — growing data (id >= 3000) merged into global sort"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=default_nb,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == default_nb
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"Sealed+growing sort broken at index {i}: {prices[i]} > {prices[i + 1]}"
        # Verify growing segment data (id >= 3000) is included
        ids = {r["id"] for r in res}
        growing_ids = set(range(3 * default_batch, 3 * default_batch + default_growing))
        assert growing_ids.issubset(ids), f"Growing segment data missing: {growing_ids - ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_indexed_vs_non_indexed(self):
        """OB-038: Indexed field (int32_field with INVERTED) vs non-indexed (price) — both correct"""
        client = self._client()
        # int32_field has INVERTED index
        res_indexed = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "int32_field"],
            limit=100,
            order_by_fields=[{"field": "int32_field", "order": "asc"}],
            consistency_level="Strong",
        )[0]
        # price has no index
        res_no_index = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=100,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res_indexed) == 100
        assert len(res_no_index) == 100

        int32_vals = [r["int32_field"] for r in res_indexed]
        for i in range(len(int32_vals) - 1):
            assert int32_vals[i] <= int32_vals[i + 1], f"Indexed int32_field not ascending at index {i}"

        prices = [r["price"] for r in res_no_index]
        for i in range(len(prices) - 1):
            assert prices[i] <= prices[i + 1], f"Non-indexed price not ascending at index {i}"

    # ==================== 5.10 L2: PK Deduplication ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_after_upsert(self):
        """OB-039: Upsert then ORDER BY — only latest version, sorted by updated values"""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        try:
            schema = build_query_order_schema(client)
            index_params = client.prepare_index_params()
            index_params.add_index(
                field_name="embeddings", index_type="HNSW", metric_type="COSINE", M=16, efConstruction=200
            )
            client.create_collection(
                collection_name=collection_name, schema=schema, index_params=index_params, consistency_level="Strong"
            )

            # Insert 100 rows
            data = gen_query_order_data(100, start_id=0)
            client.insert(collection_name=collection_name, data=data)
            client.flush(collection_name=collection_name)

            # Upsert rows 0~9 with high price values
            upsert_data = gen_query_order_data(10, start_id=0)
            for row in upsert_data:
                row["price"] = 999.0 + row["id"]
            client.upsert(collection_name=collection_name, data=upsert_data)
            client.flush(collection_name=collection_name)

            res = self.query(
                client,
                collection_name,
                filter="",
                output_fields=["id", "price"],
                limit=100,
                order_by_fields=[{"field": "price", "order": "desc"}],
                consistency_level="Strong",
            )[0]

            assert len(res) == 100
            prices = [r["price"] for r in res]
            for i in range(len(prices) - 1):
                assert prices[i] >= prices[i + 1], f"price not descending at index {i}"
            # Upserted rows (id 0~9) should be at the top with highest prices
            top_ids = {r["id"] for r in res[:10]}
            assert top_ids == set(range(10)), f"Upserted rows not at top: expected {{0..9}}, got {top_ids}"
            for r in res[:10]:
                assert r["price"] == 999.0 + r["id"], f"Upserted row {r['id']} has wrong price: {r['price']}"
        finally:
            if client.has_collection(collection_name):
                client.drop_collection(collection_name)

    # ==================== 5.10b L2: Empty Collection ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_empty_collection(self):
        """OB-060: ORDER BY on empty collection — should return empty result, not error"""
        client = self._client()
        collection_name = "test_query_order_empty_" + cf.gen_unique_str("_")
        try:
            schema = build_query_order_schema(client)
            index_params = client.prepare_index_params()
            index_params.add_index(
                field_name="embeddings", index_type="HNSW", metric_type="COSINE", M=16, efConstruction=200
            )
            client.create_collection(
                collection_name=collection_name, schema=schema, index_params=index_params, consistency_level="Strong"
            )

            res = self.query(
                client,
                collection_name,
                filter="",
                output_fields=["id", "price"],
                limit=10,
                order_by_fields=[{"field": "price", "order": "asc"}],
                consistency_level="Strong",
            )[0]

            assert len(res) == 0, f"Expected empty result on empty collection, got {len(res)} rows"
        finally:
            if client.has_collection(collection_name):
                client.drop_collection(collection_name)

    # ==================== 5.11 L2: Error Handling ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_vector_field(self):
        """OB-040: Unsupported type: FloatVector — error code 1100"""
        client = self._client()
        error = {
            ct.err_code: 1100,
            ct.err_msg: "order_by field 'embeddings' has type FloatVector which is not sortable",
        }
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id"],
            limit=10,
            order_by_fields=[{"field": "embeddings", "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_json_field(self):
        """OB-041: Unsupported type: JSON (no path) — error code 1100"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "order_by field 'json_field' has type JSON which is not sortable"}
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id"],
            limit=10,
            order_by_fields=[{"field": "json_field", "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_nonexistent_field(self):
        """OB-042: Non-existent field — error code 1100"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "order_by field 'no_such_field' does not exist in collection schema"}
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id"],
            limit=10,
            order_by_fields=[{"field": "no_such_field", "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_invalid_direction(self):
        """OB-043: Invalid direction — error code 1100"""
        client = self._client()
        error = {
            ct.err_code: 1100,
            ct.err_msg: "invalid order direction 'up' for field 'price', must be 'asc' or 'desc'",
        }
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id"],
            limit=10,
            order_by_fields=[{"field": "price", "order": "up"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_missing_limit(self):
        """OB-044: Missing limit — ORDER BY requires explicit limit"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "ORDER BY requires explicit limit"}
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id"],
            order_by_fields=[{"field": "price", "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_empty_list(self):
        """OB-045: Empty order_by list — should behave like normal query, no error"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=10,
            order_by_fields=[],
            consistency_level="Strong",
        )[0]

        assert len(res) == 10

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_empty_field_name(self):
        """OB-046: Empty field name — error"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "order_by field '' does not exist in collection schema"}
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id"],
            limit=10,
            order_by_fields=[{"field": "", "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_missing_field_key(self):
        """OB-062: Malformed dict missing 'field' key — error"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "order_by field '' does not exist in collection schema"}
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id"],
            limit=10,
            order_by_fields=[{"order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_array_field(self):
        """OB-047: Unsupported type: Array — error code 1100"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "order_by field 'array_field' has type Array which is not sortable"}
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id"],
            limit=10,
            order_by_fields=[{"field": "array_field", "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    # ==================== 5.12 L2: Edge Cases ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_all_values_identical(self):
        """OB-048: All values identical — int32_field==0 rows, PK as tie-breaker"""
        client = self._client()
        # int32_field = idx % 500, so int32_field==0 matches idx=0,500,1000,...
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="int32_field == 0",
            output_fields=["id", "int32_field"],
            limit=20,
            order_by_fields=[{"field": "int32_field", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) > 0
        for r in res:
            assert r["int32_field"] == 0
        # All values identical, PK (id) should be ascending as tie-breaker
        ids = [r["id"] for r in res]
        for i in range(len(ids) - 1):
            assert ids[i] < ids[i + 1], f"id not ascending as tie-breaker at index {i}: {ids[i]} >= {ids[i + 1]}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_three_sort_fields(self):
        """OB-049: 3 sort fields — correct cascading sort"""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "int32_field", "price", "rating"],
            limit=100,
            order_by_fields=[
                {"field": "int32_field", "order": "asc"},
                {"field": "price", "order": "asc"},
                {"field": "rating", "order": "desc"},
            ],
            consistency_level="Strong",
        )[0]

        assert len(res) == 100
        second_level_tested = False
        third_level_tested = False
        for i in range(len(res) - 1):
            a, b = res[i], res[i + 1]
            if a["int32_field"] < b["int32_field"]:
                continue
            assert a["int32_field"] == b["int32_field"], f"int32_field not ascending at index {i}"
            # Same int32_field, check price
            second_level_tested = True
            if a["price"] < b["price"]:
                continue
            assert a["price"] == b["price"], f"price not ascending at index {i} within same int32"
            # Same int32_field + price, check rating
            third_level_tested = True
            assert a["rating"] >= b["rating"], f"rating not descending at index {i} within same int32+price"
        assert second_level_tested, "No duplicate int32_field values found — second level sort not tested"
        assert third_level_tested, "No duplicate int32_field+price values found — third level sort not tested"

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_single_row(self):
        """OB-050: Single row in collection — returns 1 row, no error"""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        try:
            schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("price", DataType.DOUBLE)
            schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=default_dim)

            index_params = client.prepare_index_params()
            index_params.add_index(
                field_name="embeddings", index_type="HNSW", metric_type="COSINE", M=16, efConstruction=200
            )

            client.create_collection(
                collection_name=collection_name, schema=schema, index_params=index_params, consistency_level="Strong"
            )

            data = [{"id": 0, "price": 42.0, "embeddings": list(cf.gen_vectors(1, default_dim)[0])}]
            client.insert(collection_name=collection_name, data=data)
            client.flush(collection_name=collection_name)

            res = self.query(
                client,
                collection_name,
                filter="",
                output_fields=["id", "price"],
                limit=10,
                order_by_fields=[{"field": "price", "order": "asc"}],
                consistency_level="Strong",
            )[0]

            assert len(res) == 1
            assert res[0]["id"] == 0
            assert res[0]["price"] == 42.0
        finally:
            if client.has_collection(collection_name):
                client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_repeated_stability(self):
        """OB-051: Repeated query stability — same query 10 times, identical results"""
        client = self._client()
        order_by = [{"field": "price", "order": "asc"}]

        first_ids = None
        for attempt in range(10):
            res = self.query(
                client,
                VALID_COLLECTION_NAME,
                filter="",
                output_fields=["id", "price"],
                limit=50,
                order_by_fields=order_by,
                consistency_level="Strong",
            )[0]
            ids = [r["id"] for r in res]
            if first_ids is None:
                first_ids = ids
            else:
                assert ids == first_ids, f"Result changed on attempt {attempt + 1}"

    # ==================== 5.13 L2: Feature Interaction ====================

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "agg_field",
        [
            "count(*)",
            "sum(price)",
            "avg(price)",
            "min(price)",
            "max(price)",
        ],
    )
    def test_query_order_by_with_aggregation(self, agg_field):
        """OB-052: ORDER BY + aggregation — error: ORDER BY can only reference GROUP BY columns"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "ORDER BY can only reference GROUP BY columns"}
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="id < 200",
            output_fields=[agg_field],
            limit=10,
            order_by_fields=[{"field": "price", "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_dynamic_field(self):
        """OB-053: ORDER BY + dynamic field — not supported for query"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "order_by field 'dyn_age' does not exist in collection schema"}
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "dyn_age"],
            limit=10,
            order_by_fields=[{"field": "dyn_age", "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_json_path(self):
        """OB-054: ORDER BY + JSON path — not supported for query"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "does not exist in collection schema"}
        self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "json_field"],
            limit=10,
            order_by_fields=[{"field": 'json_field["val"]', "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_with_complex_filter(self):
        """OB-055: ORDER BY + complex filter (IN/LIKE) — filtered + sorted correctly"""
        client = self._client()
        filter_expr = 'price in [10.0, 20.0, 30.0] && category like "elec%"'
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter=filter_expr,
            output_fields=["id", "price", "category"],
            limit=100,
            order_by_fields=[{"field": "price", "order": "desc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) > 0
        for r in res:
            assert r["price"] in [10.0, 20.0, 30.0], f"price {r['price']} not in filter list"
            assert r["category"].startswith("elec"), f"category '{r['category']}' does not match LIKE 'elec%'"
        prices = [r["price"] for r in res]
        for i in range(len(prices) - 1):
            assert prices[i] >= prices[i + 1], f"price not descending at index {i}: {prices[i]} < {prices[i + 1]}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_with_iterator(self):
        """OB-056: ORDER BY + query iterator — not supported"""
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "ORDER BY with iterator is not supported"}
        self.query_iterator(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            batch_size=10,
            limit=50,
            order_by_fields=[{"field": "price", "order": "asc"}],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_group_by_positive(self):
        """OB-057: ORDER BY on a GROUP BY column with aggregation — should work correctly.
        OB-052 tests ORDER BY on non-GROUP BY column (expects error).
        This tests the positive case: ORDER BY references the GROUP BY column."""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["category", "count(*)"],
            limit=20,
            group_by_fields=["category"],
            order_by_fields=[{"field": "category", "order": "desc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) > 0
        # Each category should appear exactly once (aggregation GROUP BY)
        categories = [r["category"] for r in res]
        assert len(categories) == len(set(categories)), f"Duplicate categories in GROUP BY result: {categories}"
        # Categories should be sorted descending (not default order)
        for i in range(len(categories) - 1):
            assert categories[i] >= categories[i + 1], (
                f"category not descending at index {i}: '{categories[i]}' < '{categories[i + 1]}'"
            )
        # Each row should have a count
        for r in res:
            assert r["count(*)"] > 0, f"count(*) should be > 0 for category '{r['category']}'"

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_offset_last_row(self):
        """OB-058: ORDER BY + offset = total - 1 — should return exactly 1 row"""
        client = self._client()
        # Total rows = default_nb = 3200, offset = 3199, limit = 10
        # Should return only 1 row (the last one)
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="",
            output_fields=["id", "price"],
            limit=10,
            offset=default_nb - 1,
            order_by_fields=[{"field": "price", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) == 1, f"Expected 1 row at offset={default_nb - 1}, got {len(res)}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_order_by_float_field_with_duplicates(self):
        """OB-059: ORDER BY float field with many duplicate values — verify stable sort with PK tie-breaking.
        float_field = round((idx % 1000) * 0.1, 1), so 3200 rows produce many duplicates."""
        client = self._client()
        res = self.query(
            client,
            VALID_COLLECTION_NAME,
            filter="float_field == 0.5",
            output_fields=["id", "float_field"],
            limit=100,
            order_by_fields=[{"field": "float_field", "order": "asc"}],
            consistency_level="Strong",
        )[0]

        assert len(res) > 1, "Need multiple rows with same float value to test tie-breaking"
        # All float values should be identical
        for r in res:
            assert r["float_field"] == 0.5, f"Expected float_field=0.5, got {r['float_field']}"
        # PK (id) should be ascending as tie-breaker
        ids = [r["id"] for r in res]
        for i in range(len(ids) - 1):
            assert ids[i] < ids[i + 1], f"id not ascending as tie-breaker at index {i}: {ids[i]} >= {ids[i + 1]}"
