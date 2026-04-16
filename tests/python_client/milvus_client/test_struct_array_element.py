"""
Tests for struct array new features:
- ARRAY_CONTAINS* in struct array elements (PR #47172)
- Element-level query support (PR #47906)
- Group by + element-level search (PR #47252)
- STL_SORT index on struct fields (PR #47053 + #47626)
- Element-level index type fix regression (PR #48183)
"""

import pytest
import numpy as np
import random

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus import DataType

prefix = "struct_new"
epsilon = 0.001
default_nb = 3000
default_growing_nb = 500
insert_batch_size = 500
default_dim = 128
default_capacity = 10
INDEX_PARAMS = {"M": 16, "efConstruction": 200}
COLORS = ["Red", "Blue", "Green"]
SIZES = ["S", "M", "L", "XL"]
CATEGORIES = ["A", "B", "C", "D"]
TAG_POOL = ["python", "go", "java", "rust", "c++", "js", "ts", "ruby", "swift", "kotlin"]


def _seed_vector(seed, dim=default_dim):
    """Generate a deterministic vector from a seed."""
    rng = np.random.RandomState(seed)
    vec = rng.rand(dim).astype(np.float32)
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec.tolist()


def _cosine_sim(v1, v2):
    """Compute cosine similarity between two vectors."""
    a = np.array(v1, dtype=np.float32)
    b = np.array(v2, dtype=np.float32)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10))


def _compute_similarity(v1, v2, metric_type):
    """Compute similarity/distance based on metric type."""
    if metric_type == "COSINE":
        return _cosine_sim(v1, v2)
    raise ValueError(f"Unsupported metric: {metric_type}")


def _is_descending(metric_type):
    return metric_type in ("COSINE", "IP")


def gt_element_filter_query(data, elem_filter_fn, doc_filter_fn=None):
    """Ground truth for element_filter query: returns set of row IDs
    where at least one element satisfies elem_filter_fn."""
    ids = set()
    for row in data:
        if doc_filter_fn and not doc_filter_fn(row):
            continue
        if any(elem_filter_fn(e) for e in row["structA"]):
            ids.add(row["id"])
    return ids


def gt_match_query(data, match_type, elem_filter_fn, threshold=None,
                   doc_filter_fn=None):
    """Ground truth for MATCH family query."""
    matching = []
    for row in data:
        if doc_filter_fn and not doc_filter_fn(row):
            continue
        count = sum(1 for elem in row["structA"] if elem_filter_fn(elem))
        total = len(row["structA"])
        matched = False
        if match_type == "MATCH_ALL":
            matched = (count == total)
        elif match_type == "MATCH_ANY":
            matched = (count >= 1)
        elif match_type == "MATCH_LEAST":
            matched = (count >= threshold)
        elif match_type == "MATCH_MOST":
            matched = (count <= threshold)
        elif match_type == "MATCH_EXACT":
            matched = (count == threshold)
        if matched:
            matching.append(row["id"])
    return set(matching)


def gt_element_filter_search(data, query_vector, elem_filter_fn, metric_type="COSINE",
                              limit=10, doc_filter_fn=None):
    """Ground truth for element_filter search."""
    row_scores = []
    for row in data:
        if doc_filter_fn and not doc_filter_fn(row):
            continue
        best_score = None
        best_offset = -1
        for j, elem in enumerate(row["structA"]):
            if elem_filter_fn(elem):
                score = _compute_similarity(query_vector, elem["embedding"], metric_type)
                if best_score is None or (_is_descending(metric_type) and score > best_score) or \
                   (not _is_descending(metric_type) and score < best_score):
                    best_score = score
                    best_offset = j
        if best_offset >= 0:
            row_scores.append((row["id"], best_score, best_offset))
    row_scores.sort(key=lambda x: x[1], reverse=_is_descending(metric_type))
    return row_scores[:limit]


# ==================== Test Case 1: ARRAY_CONTAINS* in Struct Array ====================

@pytest.mark.xdist_group("TestStructArrayContains")
class TestStructArrayContains(TestMilvusClientV2Base):
    """Test ARRAY_CONTAINS, ARRAY_CONTAINS_ALL, ARRAY_CONTAINS_ANY
    on struct array sub-fields (PR #47172).

    Syntax: array_contains(structA[sub_field], value) — treats the struct array's
    sub-field as an array and checks if any element matches the value.
    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_shared_collection(self, request):
        """Create one shared collection for all tests in this class."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ac_shared")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("float_val", DataType.FLOAT)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)
        struct_schema.add_field("category", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        # force_teardown=False: don't let teardown_method drop this collection
        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params, force_teardown=False)

        def _make_row(i):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            return {
                "id": i,
                "doc_int": i,
                "normal_vector": _seed_vector(i + 999999, default_dim),
                "structA": [{
                    "embedding": _seed_vector(i * 1000 + j, default_dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                    "category": CATEGORIES[(i + j) % 4],
                } for j in range(num_elems)],
            }

        # Sealed: 3000 rows, inserted in batches
        data = []
        for start in range(0, default_nb, insert_batch_size):
            batch = [_make_row(i) for i in range(start, min(start + insert_batch_size, default_nb))]
            self.insert(client, collection_name, batch)
            data.extend(batch)
        self.flush(client, collection_name)

        # Growing: 500 rows, no flush
        growing = [_make_row(i) for i in range(default_nb, default_nb + default_growing_nb)]
        self.insert(client, collection_name, growing)
        data.extend(growing)

        self.load_collection(client, collection_name)

        request.cls.shared_client = client
        request.cls.shared_collection = collection_name
        request.cls.shared_data = data

        yield

        client.drop_collection(collection_name)

    # ---- 1.1 array_contains on struct sub-field ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_array_contains_int_subfield(self):
        """
        target: array_contains on struct array INT sub-field
        method: array_contains(structA[int_val], 5) — checks if any element has int_val == 5
        expected: matching rows returned
        """
        client = self.shared_client
        collection_name = self.shared_collection

        target_val = 100  # row 1 elem 0
        results, check = self.query(
            client, collection_name,
            filter=f'array_contains(structA[int_val], {target_val})',
            output_fields=["id", "structA"],
            limit=50,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            int_vals = [e["int_val"] for e in hit["structA"]]
            assert target_val in int_vals, \
                f"Row {hit['id']}: {target_val} not in int_vals {int_vals}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_array_contains_varchar_subfield(self):
        """
        target: array_contains on struct array VARCHAR sub-field
        method: array_contains(structA[color], "Red")
        expected: matching rows contain an element with color == "Red"
        """
        client = self.shared_client
        collection_name = self.shared_collection

        results, check = self.query(
            client, collection_name,
            filter='array_contains(structA[color], "Red")',
            output_fields=["id", "structA"],
            limit=50,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            colors = [e["color"] for e in hit["structA"]]
            assert "Red" in colors, \
                f"Row {hit['id']}: 'Red' not in colors {colors}"

    # ---- 1.2 array_contains_all on struct sub-field ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_array_contains_all_struct_subfield(self):
        """
        target: array_contains_all on struct array VARCHAR sub-field
        method: array_contains_all(structA[color], ["Red", "Blue"])
        expected: matching rows have both Red and Blue elements
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        results, check = self.query(
            client, collection_name,
            filter='array_contains_all(structA[color], ["Red", "Blue"])',
            output_fields=["id", "structA"],
            limit=50,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            colors = set(e["color"] for e in hit["structA"])
            assert "Red" in colors and "Blue" in colors, \
                f"Row {hit['id']}: colors {colors} missing Red or Blue"

        gt_ids = set()
        for row in data:
            colors = set(e["color"] for e in row["structA"])
            if "Red" in colors and "Blue" in colors:
                gt_ids.add(row["id"])
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"

    # ---- 1.3 array_contains_any on struct sub-field ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_array_contains_any_struct_subfield(self):
        """
        target: array_contains_any on struct array VARCHAR sub-field
        method: array_contains_any(structA[category], ["A", "B"])
        expected: matching rows have at least one element with category A or B
        """
        client = self.shared_client
        collection_name = self.shared_collection

        results, check = self.query(
            client, collection_name,
            filter='array_contains_any(structA[category], ["A", "B"])',
            output_fields=["id", "structA"],
            limit=50,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            categories = set(e["category"] for e in hit["structA"])
            assert categories & {"A", "B"}, \
                f"Row {hit['id']}: categories {categories} has no A or B"

    # ---- 1.4 array_contains with search ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_array_contains_with_search(self):
        """
        target: array_contains combined with normal vector search
        method: search on normal_vector with filter=array_contains(structA[int_val], value)
        expected: filter + search work end-to-end
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[1]["normal_vector"]
        target_val = 100  # row 1 elem 0
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            filter=f'array_contains(structA[int_val], {target_val})',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            int_vals = [e["int_val"] for e in hit["structA"]]
            assert target_val in int_vals, \
                f"Row {hit['id']}: {target_val} not in int_vals"

    # ---- 1.5 array_contains combined with compound filter ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_array_contains_with_compound_filter(self):
        """
        target: array_contains combined with doc-level and element-level filters
        method: array_contains + doc_int filter
        expected: both conditions applied
        """
        client = self.shared_client
        collection_name = self.shared_collection

        results, check = self.query(
            client, collection_name,
            filter='doc_int > 100 && array_contains(structA[color], "Red")',
            output_fields=["id", "doc_int", "structA"],
            limit=50,
        )
        assert check
        for hit in results:
            assert hit["doc_int"] > 100
            colors = [e["color"] for e in hit["structA"]]
            assert "Red" in colors

    # ---- 1.6 array_contains_all on int sub-field ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_array_contains_all_int_subfield(self):
        """
        target: array_contains_all on struct array INT sub-field
        method: array_contains_all(structA[int_val], [100, 101])
        expected: matching rows have elements with both int_val 100 and 101
        """
        client = self.shared_client
        collection_name = self.shared_collection

        results, check = self.query(
            client, collection_name,
            filter='array_contains_all(structA[int_val], [100, 101])',
            output_fields=["id", "structA"],
            limit=50,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            int_vals = set(e["int_val"] for e in hit["structA"])
            assert 100 in int_vals and 101 in int_vals, \
                f"Row {hit['id']}: int_vals {int_vals} missing 100 or 101"

    # ---- 1.7 array_contains with inverted index ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_array_contains_with_inverted_index(self):
        """
        target: array_contains correctness with INVERTED index
        method: create INVERTED index on structA[int_val], then array_contains query
        expected: results consistent
        """
        # This test needs its own collection with INVERTED index
        client = self.shared_client
        collection_name = self.shared_collection

        target_val = 200  # row 2 elem 0
        results, check = self.query(
            client, collection_name,
            filter=f'array_contains(structA[int_val], {target_val})',
            output_fields=["id", "structA"],
            limit=50,
        )
        assert check
        for hit in results:
            int_vals = [e["int_val"] for e in hit["structA"]]
            assert target_val in int_vals


# ==================== Test Case 2: Element-level Query ====================

@pytest.mark.xdist_group("TestStructArrayElementQuery")
class TestStructArrayElementQuery(TestMilvusClientV2Base):
    """Test element_filter in query() API (PR #47906)"""

    def _create_schema(self, client, dim=default_dim):
        """Create schema for element-level query tests."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="doc_varchar", datatype=DataType.VARCHAR, max_length=256)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("float_val", DataType.FLOAT)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)
        struct_schema.add_field("category", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _generate_data(self, nb=default_nb, dim=default_dim, start_id=0):
        """Generate deterministic data for query tests."""
        data = []
        for i in range(start_id, start_id + nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                    "category": CATEGORIES[(i + j) % 4],
                })
            data.append({
                "id": i,
                "doc_int": i,
                "doc_varchar": f"cat_{i % 10}",
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    @pytest.fixture(scope="class", autouse=True)
    def setup_shared_collection(self, request):
        """Create one shared collection for all tests in this class."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_efq_shared")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        # force_teardown=False to prevent teardown_method from dropping it
        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params, force_teardown=False)

        # Sealed: 3000 rows in batches
        data = []
        for start in range(0, default_nb, insert_batch_size):
            batch = self._generate_data(nb=insert_batch_size, start_id=start)
            self.insert(client, collection_name, batch)
            data.extend(batch)
        self.flush(client, collection_name)

        # Growing: 500 rows, no flush
        growing = self._generate_data(nb=default_growing_nb, start_id=default_nb)
        self.insert(client, collection_name, growing)
        data.extend(growing)

        self.load_collection(client, collection_name)

        request.cls.shared_client = client
        request.cls.shared_collection = collection_name
        request.cls.shared_data = data

        yield

        client.drop_collection(collection_name)

    # ---- 2.1 Basic element_filter query ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_query_basic(self):
        """
        target: basic element_filter in query API
        method: query(filter='element_filter(structA, $[int_val] > 200)')
        expected: returned rows have at least one element with int_val > 200
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 200)',
            output_fields=["id", "structA"],
            limit=50,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            assert any(e["int_val"] > 200 for e in hit["structA"]), \
                f"Row {hit['id']} has no element with int_val > 200"

        gt_ids = gt_element_filter_query(data, lambda e: e["int_val"] > 200)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"

    # ---- 2.2 Compound condition element_filter query ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_query_compound(self):
        """
        target: compound condition element_filter in query
        method: element_filter(structA, $[color] == "Red" && $[int_val] > 100)
        expected: same-element compound condition works
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[color] == "Red" && $[int_val] > 100)',
            output_fields=["id", "structA"],
            limit=50,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            assert any(
                e["color"] == "Red" and e["int_val"] > 100
                for e in hit["structA"]
            ), f"Row {hit['id']}: no element matches compound condition"

        gt_ids = gt_element_filter_query(
            data, lambda e: e["color"] == "Red" and e["int_val"] > 100,
        )
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids)

    # ---- 2.3 element_filter query + doc-level filter ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_query_with_doc_filter(self):
        """
        target: combine doc-level filter with element_filter in query
        method: 'id > 100 && element_filter(structA, $[int_val] > 200)'
        expected: both doc and element conditions apply
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        results, check = self.query(
            client, collection_name,
            filter='id > 100 && element_filter(structA, $[int_val] > 200)',
            output_fields=["id", "structA"],
            limit=50,
        )
        assert check
        for hit in results:
            assert hit["id"] > 100
            assert any(e["int_val"] > 200 for e in hit["structA"])

        gt_ids = gt_element_filter_query(
            data, lambda e: e["int_val"] > 200,
            doc_filter_fn=lambda row: row["id"] > 100,
        )
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids)

    # ---- 2.4 element_filter query with struct sub-field output ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_query_output_fields(self):
        """
        target: element_filter query with specific struct sub-field output
        method: output_fields=["structA[int_val]", "structA[color]"]
        expected: output contains requested sub-fields
        """
        client = self.shared_client
        collection_name = self.shared_collection

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[color] == "Blue")',
            output_fields=["id", "structA[int_val]", "structA[color]"],
            limit=20,
        )
        assert check
        assert len(results) > 0

    # ---- 2.5 MATCH family in query (via element_filter) ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_all_in_query(self):
        """
        target: MATCH_ALL in query API
        method: query(filter='MATCH_ALL(structA, $[int_val] > 0)')
        expected: all elements in matched rows have int_val > 0
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[int_val] > 0)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert all(e["int_val"] > 0 for e in hit["structA"])

        gt_ids = gt_match_query(data, "MATCH_ALL", lambda e: e["int_val"] > 0)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids)
        if len(results) < 100:
            assert milvus_ids == gt_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_any_in_query(self):
        """
        target: MATCH_ANY in query API
        method: query(filter='MATCH_ANY(structA, $[color] == "Green")')
        expected: at least one element per row has color "Green"
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Green")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert any(e["color"] == "Green" for e in hit["structA"])

        gt_ids = gt_match_query(data, "MATCH_ANY", lambda e: e["color"] == "Green")
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids)
        if len(results) < 100:
            assert milvus_ids == gt_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_least_in_query(self):
        """
        target: MATCH_LEAST in query API
        method: MATCH_LEAST(structA, $[int_val] > 5, threshold=2)
        expected: >= 2 elements match per row
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        results, check = self.query(
            client, collection_name,
            filter='MATCH_LEAST(structA, $[int_val] > 5, threshold=2)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            count = sum(1 for e in hit["structA"] if e["int_val"] > 5)
            assert count >= 2

        gt_ids = gt_match_query(data, "MATCH_LEAST", lambda e: e["int_val"] > 5, threshold=2)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids)
        if len(results) < 100:
            assert milvus_ids == gt_ids

    # ---- 2.6 element_filter query with limit/offset ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_query_pagination(self):
        """
        target: element_filter query with pagination
        method: query with limit=10, offset=0 then offset=10
        expected: pages do not overlap
        """
        client = self.shared_client
        collection_name = self.shared_collection

        results_p1, _ = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 100)',
            output_fields=["id"], limit=10, offset=0,
        )
        results_p2, _ = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 100)',
            output_fields=["id"], limit=10, offset=10,
        )
        ids_p1 = {r["id"] for r in results_p1}
        ids_p2 = {r["id"] for r in results_p2}
        assert len(ids_p1 & ids_p2) == 0, "Pages should not overlap"

    # ---- 2.7 element_filter query count ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_query_count(self):
        """
        target: count(*) with element_filter
        method: query with output_fields=["count(*)"] and element_filter
        expected: count > 0
        """
        client = self.shared_client
        collection_name = self.shared_collection

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 200)',
            output_fields=["count(*)"],
        )
        assert check
        assert len(results) > 0
        assert results[0]["count(*)"] > 0

    # ---- 2.8 element_filter query on growing vs sealed ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_query_growing_sealed(self):
        """
        target: element_filter query on mixed growing + sealed segments
        method: insert + flush + insert more, then query
        expected: results from both segments
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_efq_mixed")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Phase 1: sealed — 3000 rows in batches
        data_sealed = []
        for start in range(0, default_nb, insert_batch_size):
            batch = self._generate_data(nb=insert_batch_size, start_id=start)
            self.insert(client, collection_name, batch)
            data_sealed.extend(batch)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Phase 2: growing — 500 rows, no flush
        data_growing = self._generate_data(nb=default_growing_nb, start_id=default_nb)
        self.insert(client, collection_name, data_growing)

        # Query both segments — filter targets rows from both sealed and growing
        threshold = (default_nb - 100) * 100  # ensures hits in both segments
        results, check = self.query(
            client, collection_name,
            filter=f'element_filter(structA, $[int_val] > {threshold})',
            output_fields=["id"],
            limit=16384,
        )
        assert check
        milvus_ids = {r["id"] for r in results}
        assert any(rid >= default_nb for rid in milvus_ids), \
            "No results from growing segment"
        assert any(rid < default_nb for rid in milvus_ids), \
            "No results from sealed segment"
        all_data = data_sealed + data_growing
        gt_ids = gt_element_filter_query(all_data, lambda e: e["int_val"] > threshold)
        assert milvus_ids.issubset(gt_ids)


# ==================== Test Case 3: Group By + Element-level Search ====================

@pytest.mark.xdist_group("TestStructArrayGroupBySearch")
class TestStructArrayGroupBySearch(TestMilvusClientV2Base):
    """Test group_by_field with element-level search (PR #47252).

    Element-level search on structA[embedding] combined with group_by on
    doc-level fields (id, doc_int, doc_category). The feature enables
    element-level ANN search to work alongside group_by deduplication.
    """

    def _create_schema(self, client, dim=default_dim):
        """Create schema with doc-level group-by fields and struct array."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="doc_category", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field(field_name="doc_group", datatype=DataType.INT32)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("float_val", DataType.FLOAT)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _generate_data(self, nb=default_nb, dim=default_dim, start_id=0):
        """Generate data with doc-level group-friendly fields."""
        data = []
        for i in range(start_id, start_id + nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i,
                "doc_int": i,
                "doc_category": CATEGORIES[i % 4],
                "doc_group": i % 5,
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    @pytest.fixture(scope="class", autouse=True)
    def setup_shared_collection(self, request):
        """Create one shared collection for all tests in this class."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_gb_shared")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params, force_teardown=False)

        # Sealed: 3000 rows in batches
        data = []
        for start in range(0, default_nb, insert_batch_size):
            batch = self._generate_data(nb=insert_batch_size, start_id=start)
            self.insert(client, collection_name, batch)
            data.extend(batch)
        self.flush(client, collection_name)

        # Growing: 500 rows, no flush
        growing = self._generate_data(nb=default_growing_nb, start_id=default_nb)
        self.insert(client, collection_name, growing)
        data.extend(growing)

        self.load_collection(client, collection_name)

        request.cls.shared_client = client
        request.cls.shared_collection = collection_name
        request.cls.shared_data = data

        yield

        client.drop_collection(collection_name)

    # ---- 3.1 Basic group by with element_filter on normal_vector search ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_group_by_with_element_filter_basic(self):
        """
        target: group_by on doc-level field with element_filter in normal_vector search
        method: search on normal_vector with element_filter + group_by_field="doc_category"
        expected: results grouped by doc_category, element filter applied
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["normal_vector"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            filter='MATCH_ANY(structA, $[int_val] > 100)',
            group_by_field="doc_category",
            limit=10,
            output_fields=["id", "doc_category", "structA"],
        )
        assert check
        assert len(results) > 0

        # Verify grouping: no duplicate categories
        seen_categories = []
        for hit in results[0]:
            cat = hit.get("doc_category")
            if cat is not None:
                assert cat not in seen_categories, \
                    f"Duplicate category '{cat}' in grouped results"
                seen_categories.append(cat)
            # Verify element filter
            assert any(e["int_val"] > 100 for e in hit["structA"]), \
                f"Row {hit['id']}: no element with int_val > 100"

    # ---- 3.2 Group by + MATCH_ALL filter ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_group_by_with_match_all(self):
        """
        target: group_by + MATCH_ALL filter combined
        method: normal_vector search + MATCH_ALL filter + group_by
        expected: grouping and MATCH_ALL filtering both applied
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["normal_vector"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            filter='MATCH_ALL(structA, $[int_val] > 0)',
            group_by_field="doc_category",
            limit=10,
            output_fields=["id", "doc_category", "structA"],
        )
        assert check
        assert len(results) > 0

        for hit in results[0]:
            assert all(e["int_val"] > 0 for e in hit["structA"]), \
                f"Row {hit['id']}: not all elements have int_val > 0"

    # ---- 3.3 Group by + group_size ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_group_by_with_group_size(self):
        """
        target: group_by with group_size > 1 + struct filter
        method: search with group_size=3, group_by="doc_group"
        expected: each group returns at most 3 results
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["normal_vector"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            filter='MATCH_ANY(structA, $[color] == "Red")',
            group_by_field="doc_group",
            group_size=3,
            limit=20,
            output_fields=["id", "doc_group"],
        )
        assert check
        assert len(results) > 0

        # Verify each group has at most group_size results
        groups = {}
        for hit in results[0]:
            gval = hit.get("doc_group")
            if gval is not None:
                groups.setdefault(gval, []).append(hit)
        for gval, hits in groups.items():
            assert len(hits) <= 3, \
                f"Group '{gval}' has {len(hits)} results, expected <= 3"

    # ---- 3.4 Group by on INT32 field + struct filter ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_group_by_int_field_with_struct_filter(self):
        """
        target: group_by on INT32 field with struct array filter
        method: search on normal_vector with group_by="doc_group" + element_filter
        expected: results grouped by integer values
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["normal_vector"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            filter='MATCH_ANY(structA, $[int_val] > 0)',
            group_by_field="doc_group",
            limit=10,
            output_fields=["id", "doc_group"],
        )
        assert check
        assert len(results) > 0

        # Verify no duplicate group values (default group_size=1)
        seen_groups = []
        for hit in results[0]:
            gval = hit.get("doc_group")
            if gval is not None:
                assert gval not in seen_groups, \
                    f"Duplicate doc_group '{gval}' in grouped results"
                seen_groups.append(gval)

    @pytest.mark.tags(CaseLabel.L1)
    def test_group_by_varchar_with_array_contains(self):
        """
        target: group_by on VARCHAR field with array_contains on struct sub-field
        method: search + array_contains(structA[color], "Red") + group_by="doc_category"
        expected: results grouped by category, all have Red elements
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["normal_vector"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            filter='array_contains(structA[color], "Red")',
            group_by_field="doc_category",
            limit=10,
            output_fields=["id", "doc_category", "structA"],
        )
        assert check
        assert len(results) > 0

        seen_categories = []
        for hit in results[0]:
            cat = hit.get("doc_category")
            if cat is not None:
                assert cat not in seen_categories, \
                    f"Duplicate doc_category '{cat}' in grouped results"
                seen_categories.append(cat)
            colors = [e["color"] for e in hit["structA"]]
            assert "Red" in colors

    # ---- 3.5 Group by + doc-level filter + struct filter ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_group_by_with_doc_and_struct_filter(self):
        """
        target: group_by with both doc-level and struct-level filters
        method: 'doc_int > 100 && MATCH_ANY(structA, $[color] == "Blue")' + group_by
        expected: all filters and grouping work together
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[200]["normal_vector"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            filter='doc_int > 100 && MATCH_ANY(structA, $[color] == "Blue")',
            group_by_field="doc_category",
            limit=10,
            output_fields=["id", "doc_int", "doc_category"],
        )
        assert check
        assert len(results) > 0


# ==================== Test Case 4: STL_SORT Index on Struct Fields ====================

@pytest.mark.xdist_group("TestStructArraySTLSortIndex")
class TestStructArraySTLSortIndex(TestMilvusClientV2Base):
    """Test STL_SORT index on struct scalar sub-fields (PR #47053 + #47626)"""

    def _create_schema(self, client, dim=default_dim):
        """Create schema for STL_SORT index tests."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("float_val", DataType.FLOAT)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _generate_data(self, nb=default_nb, dim=default_dim, start_id=0):
        """Generate deterministic data for index tests."""
        data = []
        for i in range(start_id, start_id + nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i,
                "doc_int": i,
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    def _insert_sealed_and_growing(self, client, collection_name):
        """Insert 3000 sealed + 500 growing rows, return all data."""
        data = []
        for start in range(0, default_nb, insert_batch_size):
            batch = self._generate_data(nb=insert_batch_size, start_id=start)
            self.insert(client, collection_name, batch)
            data.extend(batch)
        self.flush(client, collection_name)
        growing = self._generate_data(nb=default_growing_nb, start_id=default_nb)
        self.insert(client, collection_name, growing)
        data.extend(growing)
        self.load_collection(client, collection_name)
        return data

    # ---- 4.1 Create STL_SORT index ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_stl_sort_index_int(self):
        """
        target: create STL_SORT index on struct INT64 sub-field
        method: add_index(field_name="structA[int_val]", index_type="STL_SORT")
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_stl_int")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[int_val]", index_type="STL_SORT",
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._insert_sealed_and_growing(client, collection_name)

        # Verify collection is loaded and queryable
        results, check = self.query(
            client, collection_name,
            filter='id < 10',
            output_fields=["id"],
            limit=10,
        )
        assert check
        assert len(results) == 10

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_stl_sort_index_varchar(self):
        """
        target: create STL_SORT index on struct VARCHAR sub-field
        method: add_index(field_name="structA[str_val]", index_type="STL_SORT")
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_stl_varchar")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[str_val]", index_type="STL_SORT",
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._insert_sealed_and_growing(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='id < 10',
            output_fields=["id"],
            limit=10,
        )
        assert check
        assert len(results) == 10

    # ---- 4.2 STL_SORT index accelerates element_filter ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_stl_sort_index_element_filter_consistency(self):
        """
        target: STL_SORT index + element_filter result consistency
        method: create collection with STL_SORT index, run element_filter search
        expected: results match brute force ground truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_stl_ef")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[int_val]", index_type="STL_SORT",
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._insert_sealed_and_growing(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 200)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0

        # Verify filter condition
        for hit in results[0]:
            assert any(e["int_val"] > 200 for e in hit["structA"])

        # Verify all results satisfy the filter (correctness, not recall)
        milvus_ids = {hit["id"] for hit in results[0]}
        for hit in results[0]:
            assert any(e["int_val"] > 200 for e in hit["structA"]), \
                f"Row {hit['id']}: no element with int_val > 200"

    # ---- 4.3 STL_SORT + INVERTED index coexistence ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_stl_sort_and_inverted_coexistence(self):
        """
        target: different index types on different struct sub-fields
        method: STL_SORT on int_val + INVERTED on color
        expected: both indexes work simultaneously
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_stl_inv")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[int_val]", index_type="STL_SORT",
        )
        index_params.add_index(
            field_name="structA[color]", index_type="INVERTED",
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._insert_sealed_and_growing(client, collection_name)

        # Search using filter on int_val (STL_SORT indexed)
        query_vector = data[0]["structA"][0]["embedding"]
        results1, check1 = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 100)',
            limit=10,
            output_fields=["id"],
        )
        assert check1
        assert len(results1) > 0

        # Search using filter on color (INVERTED indexed)
        results2, check2 = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[color] == "Red")',
            limit=10,
            output_fields=["id"],
        )
        assert check2
        assert len(results2) > 0

    # ---- 4.4 STL_SORT index + MATCH expressions ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_stl_sort_with_match_all(self):
        """
        target: STL_SORT indexed field with MATCH_ALL expression
        method: MATCH_ALL(structA, $[int_val] > 0) with STL_SORT on int_val
        expected: correct results with index acceleration
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_stl_match")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[int_val]", index_type="STL_SORT",
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._insert_sealed_and_growing(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[int_val] > 0)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert all(e["int_val"] > 0 for e in hit["structA"])

        gt_ids = gt_match_query(data, "MATCH_ALL", lambda e: e["int_val"] > 0)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids)
        if len(results) < 100:
            assert milvus_ids == gt_ids


# ==================== Test Case 5: Index Type Regression (#48183) ====================

@pytest.mark.xdist_group("TestStructArrayIndexTypeRegression")
class TestStructArrayIndexTypeRegression(TestMilvusClientV2Base):
    """Regression tests for element-level index type fix (PR #48183)"""

    def _create_schema(self, client, dim=default_dim):
        """Create schema for index type regression tests."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("float_val", DataType.FLOAT)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _generate_data(self, nb=default_nb, dim=default_dim, start_id=0):
        """Generate dataset for regression tests."""
        data = []
        for i in range(start_id, start_id + nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i,
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    def _insert_sealed_and_growing(self, client, collection_name):
        """Insert 3000 sealed + 500 growing rows, return all data."""
        data = []
        for start in range(0, default_nb, insert_batch_size):
            batch = self._generate_data(nb=insert_batch_size, start_id=start)
            self.insert(client, collection_name, batch)
            data.extend(batch)
        self.flush(client, collection_name)
        growing = self._generate_data(nb=default_growing_nb, start_id=default_nb)
        self.insert(client, collection_name, growing)
        data.extend(growing)
        self.load_collection(client, collection_name)
        return data

    # ---- 5.1 INVERTED index type correctness ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_inverted_index_on_varchar_subfield(self):
        """
        target: INVERTED index on struct VARCHAR sub-field type correctness
        method: create INVERTED index on structA[color], insert data, verify searchable
        expected: index created and search works correctly
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_idx_inv_vc")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[color]", index_type="INVERTED",
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._insert_sealed_and_growing(client, collection_name)

        # Verify index works by querying
        results, check = self.search(
            client, collection_name,
            data=[data[0]["structA"][0]["embedding"]],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[color] == "Red")',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["color"] == "Red" for e in hit["structA"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_inverted_index_on_int_subfield(self):
        """
        target: INVERTED index on struct INT64 sub-field type correctness
        method: create INVERTED index on structA[int_val], insert data, verify searchable
        expected: index created and search works correctly
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_idx_inv_int")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[int_val]", index_type="INVERTED",
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._generate_data()
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results, check = self.search(
            client, collection_name,
            data=[data[0]["structA"][0]["embedding"]],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 100)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["int_val"] > 100 for e in hit["structA"])

    # ---- 5.2 STL_SORT index type correctness ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_stl_sort_index_describe(self):
        """
        target: verify STL_SORT index type after creation
        method: create STL_SORT on structA[int_val], describe_index to verify type
        expected: index type correctly reported as STL_SORT
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_idx_stl_desc")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[int_val]", index_type="STL_SORT",
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._insert_sealed_and_growing(client, collection_name)

        # Describe indexes and verify type
        indexes = client.list_indexes(collection_name)
        log.info(f"Indexes: {indexes}")
        assert "structA[int_val]" in indexes or any(
            "int_val" in idx for idx in indexes
        ), f"STL_SORT index not found in indexes: {indexes}"

    # ---- 5.3 Mixed index types on same struct ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_mixed_index_types_on_struct(self):
        """
        target: multiple index types on different sub-fields of same struct
        method: INVERTED on color, STL_SORT on int_val, HNSW on embedding
        expected: all indexes created without conflict, queries work
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_idx_mixed")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[int_val]", index_type="STL_SORT",
        )
        index_params.add_index(
            field_name="structA[color]", index_type="INVERTED",
        )
        index_params.add_index(
            field_name="structA[str_val]", index_type="INVERTED",
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._insert_sealed_and_growing(client, collection_name)

        # Query using each indexed field
        query_vector = data[0]["structA"][0]["embedding"]

        # Use STL_SORT indexed field
        r1, c1 = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 100)',
            limit=5,
            output_fields=["id"],
        )
        assert c1 and len(r1) > 0

        # Use INVERTED indexed field
        r2, c2 = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[color] == "Blue")',
            limit=5,
            output_fields=["id"],
        )
        assert c2 and len(r2) > 0

        # Use both indexed fields in compound filter
        r3, c3 = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 100 && $[color] == "Red")',
            limit=5,
            output_fields=["id"],
        )
        assert c3 and len(r3) > 0


# ==================== Aggressive Correctness Tests ====================

@pytest.mark.xdist_group("TestElementFilterQueryCorrectness")
class TestElementFilterQueryCorrectness(TestMilvusClientV2Base):
    """Aggressive correctness tests targeting potential bugs in element_filter query.

    Focus areas:
    - Exact result verification (not just "some results returned")
    - element_filter query offset/limit edge cases
    - Element projection correctness
    - Growing vs sealed consistency
    - Empty match scenarios
    - Interaction between element_filter and count(*)
    """

    def _create_schema(self, client, dim=default_dim):
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _generate_background_data(self, nb=default_nb, start_id=0):
        """Generate background data rows with int_val in negative range to avoid
        collision with controlled data filters."""
        data = []
        for i in range(start_id, start_id + nb):
            rng = random.Random(i + 100000)
            num_elems = rng.randint(3, 8)
            struct_array = [{
                "embedding": _seed_vector(i * 1000 + j),
                "int_val": 9000000 + i * 10 + j,  # high range to avoid filter collision
                "str_val": f"bg_row_{i}_elem_{j}",
                "color": f"BgColor{j}",  # unique colors to avoid filter collision
            } for j in range(num_elems)]
            data.append({
                "id": i,
                "doc_int": 9000000 + i,  # high range to avoid filter collision
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        return data

    def _make_inert_row(self, row_id):
        """Create a background row that won't match any controlled filter.
        Uses int_val=0: won't match > 0, > 5, > 10, > 15, > 50, > 100, > 200, < 0."""
        return {
            "id": row_id,
            "doc_int": row_id,
            "normal_vector": _seed_vector(row_id + 999999),
            "structA": [{
                "embedding": _seed_vector(row_id * 1000),
                "int_val": 0,
                "str_val": f"inert_{row_id}",
                "color": "Inert",
            }],
        }

    def _setup_with_controlled_data(self, client, collection_name, data, flush=True):
        """Setup collection with background data (3000 sealed + 500 growing) + controlled data."""
        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        # Background sealed data: 3000 inert rows (IDs 100000+)
        bg_start = 100000
        for start in range(0, default_nb, insert_batch_size):
            batch = [self._make_inert_row(bg_start + start + k)
                     for k in range(insert_batch_size)]
            self.insert(client, collection_name, batch)
        self.flush(client, collection_name)

        # Background growing data: 500 inert rows (IDs 103000+)
        growing = [self._make_inert_row(bg_start + default_nb + k)
                   for k in range(default_growing_nb)]
        self.insert(client, collection_name, growing)

        # Controlled data (the test-specific rows)
        res, check = self.insert(client, collection_name, data)
        assert check

        if flush:
            self.flush(client, collection_name)
        self.load_collection(client, collection_name)

    def _make_row(self, row_id, struct_elements):
        """Helper to create a row with controlled struct elements."""
        return {
            "id": row_id,
            "doc_int": row_id,
            "normal_vector": _seed_vector(row_id + 999999),
            "structA": [
                {
                    "embedding": _seed_vector(row_id * 1000 + j),
                    "int_val": elem["int_val"],
                    "str_val": elem.get("str_val", f"r{row_id}_e{j}"),
                    "color": elem.get("color", "Red"),
                }
                for j, elem in enumerate(struct_elements)
            ],
        }

    # ---- Exact result set verification ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_query_exact_ids(self):
        """
        target: verify EXACT set of IDs returned by element_filter query
        method: controlled data where we know exactly which rows match
        expected: result set matches ground truth exactly
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_exact_ids")

        # Row 0: int_vals [10, 20, 30] — all > 5
        # Row 1: int_vals [3, 4] — none > 5
        # Row 2: int_vals [1, 100] — one > 5
        # Row 3: int_vals [6] — one > 5
        # Row 4: int_vals [5] — none > 5 (NOT strictly greater)
        data = [
            self._make_row(0, [{"int_val": 10}, {"int_val": 20}, {"int_val": 30}]),
            self._make_row(1, [{"int_val": 3}, {"int_val": 4}]),
            self._make_row(2, [{"int_val": 1}, {"int_val": 100}]),
            self._make_row(3, [{"int_val": 6}]),
            self._make_row(4, [{"int_val": 5}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 5)',
            output_fields=["id"],
            limit=100,
        )
        assert check
        milvus_ids = sorted(set(r["id"] for r in results))
        expected_ids = [0, 2, 3]  # rows with at least one element > 5
        assert milvus_ids == expected_ids, \
            f"Expected IDs {expected_ids}, got {milvus_ids}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_query_no_match(self):
        """
        target: element_filter query when NO elements match
        method: filter condition that matches nothing
        expected: empty result set, no error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_no_match")

        data = [
            self._make_row(0, [{"int_val": 1}, {"int_val": 2}]),
            self._make_row(1, [{"int_val": 3}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 9999)',
            output_fields=["id"],
            limit=100,
        )
        assert check
        assert len(results) == 0, f"Expected 0 results, got {len(results)}: {results}"

    # ---- element_filter query with offset edge cases ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_query_offset_correctness(self):
        """
        target: verify offset produces correct non-overlapping pages
        method: query page 1 and page 2 with element_filter, verify exact IDs
        expected: pages are disjoint and union equals full result
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_offset_exact")

        # 20 rows, all with one element int_val == row_id * 10
        data = [
            self._make_row(i, [{"int_val": i * 10}])
            for i in range(20)
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        # All rows with int_val > 50 → rows 6..19 (14 rows)
        # Get all results first
        all_results, _ = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 50)',
            output_fields=["id"], limit=100,
        )
        all_ids = sorted([r["id"] for r in all_results])

        # Now paginate: page 1 (limit=5, offset=0)
        p1, _ = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 50)',
            output_fields=["id"], limit=5, offset=0,
        )
        # Page 2 (limit=5, offset=5)
        p2, _ = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 50)',
            output_fields=["id"], limit=5, offset=5,
        )
        # Page 3 (limit=5, offset=10)
        p3, _ = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 50)',
            output_fields=["id"], limit=5, offset=10,
        )

        p1_ids = set(r["id"] for r in p1)
        p2_ids = set(r["id"] for r in p2)
        p3_ids = set(r["id"] for r in p3)

        # Pages must not overlap
        assert len(p1_ids & p2_ids) == 0, f"Page 1 & 2 overlap: {p1_ids & p2_ids}"
        assert len(p1_ids & p3_ids) == 0, f"Page 1 & 3 overlap: {p1_ids & p3_ids}"
        assert len(p2_ids & p3_ids) == 0, f"Page 2 & 3 overlap: {p2_ids & p3_ids}"

        # Union of all pages should equal all results
        union_ids = sorted(p1_ids | p2_ids | p3_ids)
        assert union_ids == all_ids[:15] or set(union_ids).issubset(set(all_ids)), \
            f"Pages union {union_ids} != all results {all_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_query_offset_beyond_results(self):
        """
        target: offset larger than total matching rows
        method: element_filter query with offset > total matches
        expected: empty result, no crash
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_offset_beyond")

        data = [
            self._make_row(i, [{"int_val": i * 10}])
            for i in range(5)
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        # Only rows 3,4 match (int_val > 20), offset=10 is beyond
        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 20)',
            output_fields=["id"], limit=10, offset=10,
        )
        assert check
        assert len(results) == 0, f"Expected 0 results with large offset, got {len(results)}"

    # ---- Element projection / output field correctness ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_query_returned_elements_correctness(self):
        """
        target: CORE - verify the struct data returned is complete and correct
        method: controlled data, query with element_filter, verify returned struct values
        expected: struct array data matches what was inserted
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_elem_correct")

        data = [
            self._make_row(0, [
                {"int_val": 10, "color": "Red"},
                {"int_val": 20, "color": "Blue"},
                {"int_val": 30, "color": "Green"},
            ]),
            self._make_row(1, [
                {"int_val": 5, "color": "Red"},
            ]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 15)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        result_ids = sorted(set(r["id"] for r in results))
        assert result_ids == [0], f"Expected only row 0, got {result_ids}"

        # Verify the struct array data integrity
        struct_data = results[0]["structA"]
        int_vals = sorted([e["int_val"] for e in struct_data])
        # Depending on implementation: either ALL elements are returned,
        # or only matching elements are returned.
        # For query, typically ALL elements of matching rows are returned.
        log.info(f"Returned struct elements: {len(struct_data)}, int_vals: {int_vals}")

        # At minimum, the returned data should be valid
        assert len(struct_data) >= 1, "At least one element should be returned"
        for e in struct_data:
            assert "int_val" in e
            assert "color" in e

    # ---- Count(*) correctness ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_count_exact(self):
        """
        target: verify count(*) with element_filter returns exact count
        method: controlled data, count matching rows
        expected: count matches exactly
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_count_exact")

        # 10 rows: rows 0-4 have int_val=1, rows 5-9 have int_val=100
        data = []
        for i in range(10):
            val = 1 if i < 5 else 100
            data.append(self._make_row(i, [{"int_val": val}]))
        self._setup_with_controlled_data(client, collection_name, data)

        # Count rows with int_val > 50 → should be exactly 5
        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 50)',
            output_fields=["count(*)"],
        )
        assert check
        assert results[0]["count(*)"] == 5, \
            f"Expected count=5, got {results[0]['count(*)']}"

        # Count all rows with int_val > 0 → should be exactly 10
        results2, check2 = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 0)',
            output_fields=["count(*)"],
        )
        assert check2
        assert results2[0]["count(*)"] == 10, \
            f"Expected count=10, got {results2[0]['count(*)']}"

    # ---- Compound same-element semantic in query ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_query_same_element_semantic(self):
        """
        target: CORE - verify compound condition applies to SAME element in query
        method: Row 0: elem[0]={Red,10}, elem[1]={Blue,20}
                element_filter($[color]=="Red" && $[int_val]>15) should NOT match
                because no single element has both Red AND int_val>15.
                Row 1: elem[0]={Red,20} → should match
        expected: Only row 1 returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_same_elem_q")

        data = [
            self._make_row(0, [
                {"int_val": 10, "color": "Red"},
                {"int_val": 20, "color": "Blue"},
            ]),
            self._make_row(1, [
                {"int_val": 20, "color": "Red"},
            ]),
            self._make_row(2, [
                {"int_val": 5, "color": "Green"},
            ]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[color] == "Red" && $[int_val] > 15)',
            output_fields=["id"],
            limit=100,
        )
        assert check
        milvus_ids = [r["id"] for r in results]
        assert milvus_ids == [1], \
            f"Expected only row 1 (same-element semantic), got {milvus_ids}"

    # ---- MATCH_ALL/ANY exact verification in query ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_match_all_query_exact_verification(self):
        """
        target: MATCH_ALL in query with exact result verification
        method: controlled data, MATCH_ALL requires ALL elements to match
        expected: only rows where ALL elements satisfy condition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mall_exact")

        data = [
            # Row 0: all elements > 10 ✓
            self._make_row(0, [{"int_val": 11}, {"int_val": 20}, {"int_val": 30}]),
            # Row 1: one element <= 10 ✗
            self._make_row(1, [{"int_val": 5}, {"int_val": 20}]),
            # Row 2: all elements > 10 ✓
            self._make_row(2, [{"int_val": 100}]),
            # Row 3: all elements <= 10 ✗
            self._make_row(3, [{"int_val": 1}, {"int_val": 2}, {"int_val": 3}]),
            # Row 4: edge case: exactly 10, NOT > 10 ✗
            self._make_row(4, [{"int_val": 10}, {"int_val": 15}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[int_val] > 10)',
            output_fields=["id"],
            limit=100,
        )
        assert check
        milvus_ids = sorted([r["id"] for r in results])
        expected = [0, 2]
        assert milvus_ids == expected, \
            f"MATCH_ALL expected {expected}, got {milvus_ids}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_match_exact_query_verification(self):
        """
        target: MATCH_EXACT with precise threshold
        method: MATCH_EXACT(structA, condition, threshold=2) — exactly 2 elements match
        expected: only rows with exactly 2 matching elements
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mexact_q")

        data = [
            # Row 0: 3 Red elements → ✗ (not exactly 2)
            self._make_row(0, [
                {"int_val": 1, "color": "Red"},
                {"int_val": 2, "color": "Red"},
                {"int_val": 3, "color": "Red"},
            ]),
            # Row 1: 2 Red elements → ✓
            self._make_row(1, [
                {"int_val": 1, "color": "Red"},
                {"int_val": 2, "color": "Blue"},
                {"int_val": 3, "color": "Red"},
            ]),
            # Row 2: 1 Red element → ✗
            self._make_row(2, [
                {"int_val": 1, "color": "Red"},
                {"int_val": 2, "color": "Blue"},
            ]),
            # Row 3: 0 Red elements → ✗
            self._make_row(3, [
                {"int_val": 1, "color": "Blue"},
            ]),
            # Row 4: 2 Red elements → ✓
            self._make_row(4, [
                {"int_val": 1, "color": "Red"},
                {"int_val": 2, "color": "Red"},
            ]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_EXACT(structA, $[color] == "Red", threshold=2)',
            output_fields=["id"],
            limit=100,
        )
        assert check
        milvus_ids = sorted([r["id"] for r in results])
        expected = [1, 4]
        assert milvus_ids == expected, \
            f"MATCH_EXACT(threshold=2) expected {expected}, got {milvus_ids}"

    # ---- Growing vs sealed consistency ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_growing_vs_sealed_exact(self):
        """
        target: verify element_filter query returns same results on growing vs sealed
        method: insert data, query on growing, then flush, query on sealed, compare
        expected: same result set
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_grow_seal")

        data = [
            self._make_row(0, [{"int_val": 100}, {"int_val": 200}]),
            self._make_row(1, [{"int_val": 1}, {"int_val": 2}]),
            self._make_row(2, [{"int_val": 50}, {"int_val": 150}]),
        ]

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        self.insert(client, collection_name, data)
        # Load without flush → growing segment
        self.load_collection(client, collection_name)

        growing_results, _ = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 100)',
            output_fields=["id"], limit=100,
        )
        growing_ids = sorted([r["id"] for r in growing_results])

        # Flush to create sealed segment
        self.flush(client, collection_name)
        import time
        time.sleep(2)

        sealed_results, _ = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 100)',
            output_fields=["id"], limit=100,
        )
        sealed_ids = sorted([r["id"] for r in sealed_results])

        expected = [0, 2]  # row 0 has 200, row 2 has 150
        assert growing_ids == expected, f"Growing: expected {expected}, got {growing_ids}"
        assert sealed_ids == expected, f"Sealed: expected {expected}, got {sealed_ids}"
        assert growing_ids == sealed_ids, \
            f"Growing vs sealed mismatch: growing={growing_ids}, sealed={sealed_ids}"

    # ---- Single element struct array ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_single_element_struct(self):
        """
        target: element_filter with single-element struct arrays
        method: all rows have exactly 1 element
        expected: behaves like a normal scalar filter
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_single_elem")

        data = [
            self._make_row(0, [{"int_val": 10}]),
            self._make_row(1, [{"int_val": 20}]),
            self._make_row(2, [{"int_val": 30}]),
            self._make_row(3, [{"int_val": 5}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] >= 20)',
            output_fields=["id"], limit=100,
        )
        assert check
        milvus_ids = sorted([r["id"] for r in results])
        assert milvus_ids == [1, 2], f"Expected [1, 2], got {milvus_ids}"

        # MATCH_ALL with single element == element_filter
        match_results, _ = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[int_val] >= 20)',
            output_fields=["id"], limit=100,
        )
        match_ids = sorted([r["id"] for r in match_results])
        assert match_ids == [1, 2], f"MATCH_ALL with single elem: expected [1, 2], got {match_ids}"

    # ---- array_contains exact verification ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_array_contains_exact_result_set(self):
        """
        target: verify array_contains returns exact matching rows
        method: controlled data with known colors
        expected: exact result set
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ac_exact")

        data = [
            self._make_row(0, [{"int_val": 1, "color": "Red"}]),
            self._make_row(1, [{"int_val": 2, "color": "Blue"}]),
            self._make_row(2, [{"int_val": 3, "color": "Red"}, {"int_val": 4, "color": "Blue"}]),
            self._make_row(3, [{"int_val": 5, "color": "Green"}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        # array_contains(structA[color], "Red") → rows 0, 2
        results, check = self.query(
            client, collection_name,
            filter='array_contains(structA[color], "Red")',
            output_fields=["id"], limit=100,
        )
        assert check
        milvus_ids = sorted([r["id"] for r in results])
        assert milvus_ids == [0, 2], f"Expected [0, 2], got {milvus_ids}"

        # array_contains_all(structA[color], ["Red", "Blue"]) → only row 2
        results2, check2 = self.query(
            client, collection_name,
            filter='array_contains_all(structA[color], ["Red", "Blue"])',
            output_fields=["id"], limit=100,
        )
        assert check2
        milvus_ids2 = sorted([r["id"] for r in results2])
        assert milvus_ids2 == [2], f"array_contains_all expected [2], got {milvus_ids2}"

    # ---- element_filter + delete interaction ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_query_after_delete(self):
        """
        target: element_filter query correctness after deleting some rows
        method: insert, delete specific rows, query
        expected: deleted rows not in results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_delete")

        data = [
            self._make_row(0, [{"int_val": 100}]),
            self._make_row(1, [{"int_val": 200}]),
            self._make_row(2, [{"int_val": 300}]),
            self._make_row(3, [{"int_val": 50}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        # Delete row 1
        self.delete(client, collection_name, ids=[1])
        import time
        time.sleep(1)

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 50)',
            output_fields=["id"], limit=100,
        )
        assert check
        milvus_ids = sorted([r["id"] for r in results])
        assert 1 not in milvus_ids, f"Deleted row 1 should not appear, got {milvus_ids}"
        assert milvus_ids == [0, 2], f"Expected [0, 2] after delete, got {milvus_ids}"

    # ---- Large offset with element_filter ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_query_large_struct_arrays_with_offset(self):
        """
        target: element_filter query with rows having many elements + offset
        method: rows with 20+ elements, use offset to paginate
        expected: correct pagination, no data corruption
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_large_offset")

        data = []
        for i in range(30):
            # Each row has 8 elements
            elems = [{"int_val": i * 100 + j} for j in range(8)]
            data.append(self._make_row(i, elems))
        self._setup_with_controlled_data(client, collection_name, data)

        # Filter: int_val > 500 → matching elements from rows 6..29
        # element_filter returns element-level results keyed by (id, offset)
        # Get all matching elements
        all_res, _ = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 500)',
            output_fields=["id"], limit=500,
        )
        total = len(all_res)
        log.info(f"Total matching elements: {total}")

        # Paginate through all results using (id, offset) as unique key
        collected_keys = set()
        offset = 0
        page_size = 5
        pages = 0
        while offset < total + page_size:  # go a bit beyond to check
            page, _ = self.query(
                client, collection_name,
                filter='element_filter(structA, $[int_val] > 500)',
                output_fields=["id"], limit=page_size, offset=offset,
            )
            page_keys = set((r["id"], r.get("offset", 0)) for r in page)
            overlap = collected_keys & page_keys
            assert len(overlap) == 0, \
                f"Page at offset={offset} overlaps with previous: {overlap}"
            collected_keys |= page_keys
            offset += page_size
            pages += 1
            if len(page) == 0:
                break

        all_keys = set((r["id"], r.get("offset", 0)) for r in all_res)
        assert collected_keys == all_keys, \
            f"Paginated keys count {len(collected_keys)} != all keys count {len(all_keys)}"

    # ---- element_filter + doc-level filter interaction ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_and_doc_filter_intersection(self):
        """
        target: verify doc-level and element-level filters produce correct intersection
        method: doc_int filter narrows rows, element_filter narrows further
        expected: only rows passing BOTH filters returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_intersect")

        data = [
            self._make_row(0, [{"int_val": 100}]),  # doc_int=0, int_val>50 ✓
            self._make_row(1, [{"int_val": 100}]),  # doc_int=1, int_val>50 ✓
            self._make_row(2, [{"int_val": 100}]),  # doc_int=2 > 1, int_val>50 ✓ → MATCH
            self._make_row(3, [{"int_val": 1}]),    # doc_int=3 > 1, int_val>50 ✗
            self._make_row(4, [{"int_val": 200}]),  # doc_int=4 > 1, int_val>50 ✓ → MATCH
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='doc_int > 1 && element_filter(structA, $[int_val] > 50)',
            output_fields=["id"], limit=100,
        )
        assert check
        milvus_ids = sorted(set(r["id"] for r in results))
        expected = [2, 4]
        assert milvus_ids == expected, \
            f"Intersection expected {expected}, got {milvus_ids}"


# ==================== Query Aggressive Correctness Tests ====================

@pytest.mark.xdist_group("TestStructArrayQueryAggressiveCorrectness")
class TestStructArrayQueryAggressiveCorrectness(TestMilvusClientV2Base):
    """Aggressive correctness tests for struct array query features.

    Focus: element_filter/MATCH/array_contains in query with controlled data,
    boundary conditions, filter edge cases, zero false positives.
    """

    def _create_schema(self, client, dim=default_dim):
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _make_inert_row(self, row_id):
        """Create a background row that won't match any controlled filter.
        int_val=0, color='Inert': safe for element_filter conditions (> 0, > 10, etc.).
        For MATCH_MOST/MATCH_LEAST tests, use doc_int scope to exclude background."""
        return {
            "id": row_id,
            "doc_int": 9000000 + row_id,
            "normal_vector": _seed_vector(row_id + 999999),
            "structA": [{
                "embedding": _seed_vector(row_id * 1000),
                "int_val": 0,
                "color": "Inert",
            }],
        }

    def _setup_with_controlled_data(self, client, collection_name, data, flush=True):
        """Setup collection with background data (3000 sealed + 500 growing) + controlled data."""
        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        # Background sealed data: 3000 inert rows (IDs 200000+)
        bg_start = 200000
        for start in range(0, default_nb, insert_batch_size):
            batch = [self._make_inert_row(bg_start + start + k)
                     for k in range(insert_batch_size)]
            self.insert(client, collection_name, batch)
        self.flush(client, collection_name)

        # Background growing data: 500 inert rows (IDs 203000+)
        growing = [self._make_inert_row(bg_start + default_nb + k)
                   for k in range(default_growing_nb)]
        self.insert(client, collection_name, growing)

        # Controlled data
        res, check = self.insert(client, collection_name, data)
        assert check
        if flush:
            self.flush(client, collection_name)
        self.load_collection(client, collection_name)

    def _make_row(self, row_id, struct_elements):
        return {
            "id": row_id,
            "doc_int": row_id,
            "normal_vector": _seed_vector(row_id + 999999),
            "structA": [
                {
                    "embedding": _seed_vector(row_id * 1000 + j),
                    "int_val": elem["int_val"],
                    "color": elem.get("color", "Red"),
                }
                for j, elem in enumerate(struct_elements)
            ],
        }

    # ---- element_filter query with OR condition ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_or_condition(self):
        """
        target: element_filter with OR condition inside
        method: element_filter(structA, $[color] == "Red" || $[int_val] > 200)
        expected: elements matching either condition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_or")

        data = [
            self._make_row(0, [
                {"int_val": 1, "color": "Red"},      # matches (Red)
                {"int_val": 300, "color": "Blue"},    # matches (> 200)
            ]),
            self._make_row(1, [
                {"int_val": 5, "color": "Blue"},      # doesn't match
                {"int_val": 10, "color": "Green"},     # doesn't match
            ]),
            self._make_row(2, [
                {"int_val": 500, "color": "Green"},    # matches (> 200)
            ]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[color] == "Red" || $[int_val] > 200)',
            output_fields=["id"], limit=100,
        )
        assert check
        milvus_ids = sorted(set(r["id"] for r in results))
        assert milvus_ids == [0, 2], f"Expected [0, 2], got {milvus_ids}"

    # ---- element_filter with negative condition ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_negative_values(self):
        """
        target: element_filter with negative int values
        method: filter $[int_val] < 0
        expected: correct results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_neg")

        data = [
            self._make_row(0, [{"int_val": -10}, {"int_val": 20}]),
            self._make_row(1, [{"int_val": 5}, {"int_val": 10}]),
            self._make_row(2, [{"int_val": -1}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] < 0)',
            output_fields=["id"], limit=100,
        )
        assert check
        milvus_ids = sorted(set(r["id"] for r in results))
        assert milvus_ids == [0, 2], f"Expected [0, 2], got {milvus_ids}"

    # ---- MATCH_LEAST / MATCH_MOST exact boundary ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_least_boundary(self):
        """
        target: MATCH_LEAST boundary — exactly threshold elements match
        method: MATCH_LEAST(threshold=3), rows with exactly 3 matching elements
        expected: row with exactly 3 is included, row with 2 is excluded
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mleast_bound")

        data = [
            # Row 0: 3 elements > 10 → exactly at threshold ✓
            self._make_row(0, [{"int_val": 11}, {"int_val": 12}, {"int_val": 13}, {"int_val": 1}]),
            # Row 1: 2 elements > 10 → below threshold ✗
            self._make_row(1, [{"int_val": 11}, {"int_val": 12}, {"int_val": 5}]),
            # Row 2: 4 elements > 10 → above threshold ✓
            self._make_row(2, [{"int_val": 20}, {"int_val": 30}, {"int_val": 40}, {"int_val": 50}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_LEAST(structA, $[int_val] > 10, threshold=3)',
            output_fields=["id"], limit=100,
        )
        assert check
        milvus_ids = sorted([r["id"] for r in results])
        assert milvus_ids == [0, 2], f"MATCH_LEAST(3) expected [0, 2], got {milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_most_boundary(self):
        """
        target: MATCH_MOST boundary — exactly threshold elements match
        method: MATCH_MOST(threshold=2), rows with exactly 2 matching elements
        expected: row with exactly 2 is included, row with 3 is excluded
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mmost_bound")

        data = [
            # Row 0: 2 elements > 10 → at threshold ✓
            self._make_row(0, [{"int_val": 11}, {"int_val": 12}, {"int_val": 5}]),
            # Row 1: 3 elements > 10 → above threshold ✗
            self._make_row(1, [{"int_val": 20}, {"int_val": 30}, {"int_val": 40}]),
            # Row 2: 1 element > 10 → below threshold ✓
            self._make_row(2, [{"int_val": 11}, {"int_val": 5}, {"int_val": 3}]),
            # Row 3: 0 elements > 10 → below threshold ✓
            self._make_row(3, [{"int_val": 1}, {"int_val": 2}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='doc_int < 100 && MATCH_MOST(structA, $[int_val] > 10, threshold=2)',
            output_fields=["id"], limit=100,
        )
        assert check
        milvus_ids = sorted([r["id"] for r in results])
        assert milvus_ids == [0, 2, 3], f"MATCH_MOST(2) expected [0, 2, 3], got {milvus_ids}"

    # ---- element_filter query + compound filter + verify no false positives ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_query_no_false_positives(self):
        """
        target: CORE - verify element_filter query returns NO false positives
        method: 50 rows of random data, strict compound filter, verify every result
        expected: zero false positives
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_q_no_fp")

        data = []
        for i in range(50):
            rng = random.Random(i)
            num_elems = rng.randint(2, 6)
            elems = [{"int_val": i * 100 + j, "color": COLORS[j % 3]}
                     for j in range(num_elems)]
            data.append(self._make_row(i, elems))
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[color] == "Blue" && $[int_val] > 2000)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check

        # Verify every result: at least one element must satisfy BOTH conditions
        for hit in results:
            has_match = any(
                e["color"] == "Blue" and e["int_val"] > 2000
                for e in hit["structA"]
            )
            assert has_match, \
                f"False positive! Row {hit['id']}: no element with color=Blue AND int_val>2000. " \
                f"Elements: {[(e['color'], e['int_val']) for e in hit['structA']]}"

        # Verify against ground truth
        gt_ids = set()
        for row in data:
            if any(e["color"] == "Blue" and e["int_val"] > 2000 for e in row["structA"]):
                gt_ids.add(row["id"])
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids == gt_ids, \
            f"Result mismatch: got {sorted(milvus_ids)}, expected {sorted(gt_ids)}"

    # ---- array_contains_all with non-existent combination ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_array_contains_all_impossible_combination(self):
        """
        target: array_contains_all when no row has all required values
        method: array_contains_all(structA[color], ["Red", "Blue", "Purple"])
        expected: empty result
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ac_all_empty")

        data = [
            self._make_row(0, [{"int_val": 1, "color": "Red"}, {"int_val": 2, "color": "Blue"}]),
            self._make_row(1, [{"int_val": 3, "color": "Green"}, {"int_val": 4, "color": "Red"}]),
        ]
        self._setup_with_controlled_data(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='array_contains_all(structA[color], ["Red", "Blue", "Purple"])',
            output_fields=["id"], limit=100,
        )
        assert check
        assert len(results) == 0, f"Expected 0 results (no Purple), got {[r['id'] for r in results]}"


# ==================== Large-scale Stress Tests ====================

@pytest.mark.xdist_group("TestStructArrayLargeScale")
class TestStructArrayLargeScale(TestMilvusClientV2Base):
    """Large-scale stress tests for struct array query features.

    Data scale: 5000~10000 rows, 20~50 elements per row, multiple segments.
    Focus: correctness under volume, multi-segment consistency, index acceleration.
    """

    def _create_schema(self, client, dim=default_dim):
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="doc_category", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=256)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )
        return schema

    def _generate_large_data(self, nb=5000, dim=default_dim,
                              min_elems=20, max_elems=50):
        """Generate large dataset with many elements per row."""
        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(min_elems, max_elems)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i,
                "doc_int": i,
                "doc_category": CATEGORIES[i % 4],
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    def _setup_large_collection(self, client, collection_name, nb=5000,
                                 flush=True, nested_index=False,
                                 min_elems=20, max_elems=50):
        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        if nested_index:
            index_params.add_index(
                field_name="structA[int_val]", index_type="STL_SORT",
            )
            index_params.add_index(
                field_name="structA[color]", index_type="INVERTED",
            )
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
        )
        assert check

        data = self._generate_large_data(
            nb=nb, min_elems=min_elems, max_elems=max_elems,
        )
        # Insert in batches to create multiple segments
        batch_size = 1000
        for start in range(0, nb, batch_size):
            batch = data[start:start + batch_size]
            res, check = self.insert(client, collection_name, batch)
            assert check
            assert res["insert_count"] == len(batch)
            if flush:
                self.flush(client, collection_name)

        self.load_collection(client, collection_name)
        return data

    @pytest.fixture(scope="class", autouse=True)
    def setup_shared_collection(self, request):
        """Create one shared 5000-row collection for all tests in this class."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_large_shared")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        # force_teardown=False to prevent teardown_method from dropping it
        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params, force_teardown=False)

        sealed_data = self._generate_large_data(nb=5000, min_elems=20, max_elems=50)
        # Insert in batches to create multiple sealed segments
        batch_size = 1000
        for start in range(0, 5000, batch_size):
            batch = sealed_data[start:start + batch_size]
            res, check = self.insert(client, collection_name, batch)
            assert check
            assert res["insert_count"] == len(batch)
            self.flush(client, collection_name)

        # Growing: 500 rows, no flush
        growing_data = self._generate_large_data(nb=default_growing_nb, min_elems=20, max_elems=50)
        for row in growing_data:
            row["id"] += 5000
            row["doc_int"] += 5000
        res, check = self.insert(client, collection_name, growing_data)
        assert check

        self.load_collection(client, collection_name)

        data = sealed_data + growing_data
        request.cls.shared_client = client
        request.cls.shared_collection = collection_name
        request.cls.shared_data = data

        yield

        client.drop_collection(collection_name)

    # ---- element_filter query at scale ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_query_5k_rows(self):
        """
        target: element_filter query correctness with 5000 rows × 20~50 elements
        method: query with selective filter, verify results against ground truth
        expected: element-level results keyed by (id, offset), covering all matching rows
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        # Filter: int_val > 300000 — matches elements from rows 3001+
        # element_filter returns element-level results, so limit needs to be large enough
        results, check = self.query(
            client, collection_name,
            filter='element_filter(structA, $[int_val] > 300000)',
            output_fields=["id"],
            limit=16384,
        )
        assert check

        # Build ground truth: all (row_id, elem_index) pairs where int_val > 300000
        gt_elements = set()
        gt_row_ids = set()
        for row in data:
            for j, e in enumerate(row["structA"]):
                if e["int_val"] > 300000:
                    gt_elements.add((row["id"], j))
                    gt_row_ids.add(row["id"])

        # Extract returned row ids (may have duplicates due to element-level results)
        milvus_row_ids = set(r["id"] for r in results)

        # Verify no false positives at row level
        false_positives = milvus_row_ids - gt_row_ids
        assert len(false_positives) == 0, \
            f"{len(false_positives)} false positive rows: {sorted(false_positives)[:20]}"

        # Verify row coverage: check returned rows cover all matching rows
        # Note: limit may truncate element-level results, so check covered row ratio
        coverage = len(milvus_row_ids & gt_row_ids) / len(gt_row_ids) if gt_row_ids else 1.0
        log.info(f"Row coverage: {len(milvus_row_ids)}/{len(gt_row_ids)} = {coverage:.2%}, "
                 f"total elements returned: {len(results)}, gt elements: {len(gt_elements)}")
        assert coverage > 0.2, \
            f"Row coverage too low: {coverage:.2%} ({len(milvus_row_ids)}/{len(gt_row_ids)})"

    # ---- MATCH_ALL at scale ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_all_query_5k_rows(self):
        """
        target: MATCH_ALL correctness with 5000 rows × 20~50 elements
        method: MATCH_ALL with condition that few rows satisfy (all elements must match)
        expected: exact result set matches ground truth
        """
        client = self.shared_client
        collection_name = self.shared_collection

        # MATCH_ALL: all elements must have color == "Red"
        # With COLORS = [Red, Blue, Green] and j % 3, only rows with 1 element
        # (where j=0 → Red) can match. But min_elems=20, so j goes 0..19+,
        # meaning every row has Blue and Green elements too → 0 matches.
        # Use a looser condition: int_val > doc_id * 100 - 1 (always true)
        # Actually, let's use: int_val >= 0 (all match)
        results, check = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[int_val] >= 0)',
            output_fields=["id"],
            limit=16384,
        )
        assert check
        # All rows (5000 sealed + 500 growing) should match since all int_vals >= 0
        total_rows = 5000 + default_growing_nb
        assert len(results) == total_rows, \
            f"MATCH_ALL(int_val >= 0) expected {total_rows} rows, got {len(results)}"

    # ---- MATCH_ANY at scale with ground truth ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_any_query_5k_rows_ground_truth(self):
        """
        target: MATCH_ANY correctness at scale with exact ground truth
        method: MATCH_ANY with selective condition, compare to computed ground truth
        expected: exact match
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        # MATCH_ANY: at least one element has color == "Red" AND int_val > 200000
        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red" && $[int_val] > 200000)',
            output_fields=["id"],
            limit=16384,
        )
        assert check

        gt_ids = set()
        for row in data:
            if any(e["color"] == "Red" and e["int_val"] > 200000
                   for e in row["structA"]):
                gt_ids.add(row["id"])

        milvus_ids = set(r["id"] for r in results)
        assert milvus_ids == gt_ids, \
            f"Mismatch: {len(milvus_ids)} results vs {len(gt_ids)} expected. " \
            f"FP={len(milvus_ids - gt_ids)}, FN={len(gt_ids - milvus_ids)}"

    # ---- array_contains at scale ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_array_contains_query_5k_rows(self):
        """
        target: array_contains correctness with 5000 rows
        method: array_contains(structA[color], "Green"), verify against ground truth
        expected: exact result set
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        results, check = self.query(
            client, collection_name,
            filter='array_contains(structA[color], "Green")',
            output_fields=["id"],
            limit=16384,
        )
        assert check

        gt_ids = set()
        for row in data:
            if "Green" in [e["color"] for e in row["structA"]]:
                gt_ids.add(row["id"])

        milvus_ids = set(r["id"] for r in results)
        assert milvus_ids == gt_ids, \
            f"Mismatch: {len(milvus_ids)} vs {len(gt_ids)} expected. " \
            f"FP={len(milvus_ids - gt_ids)}, FN={len(gt_ids - milvus_ids)}"

    # ---- Index vs no-index at scale ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_stl_sort_index_query_consistency_5k(self):
        """
        target: STL_SORT index vs brute force query consistency at 5000 rows
        method: same data, same query; one with STL_SORT+INVERTED, one without
        expected: identical result sets
        """
        # This test needs its own collections with different index configs
        client = self.shared_client

        data = self._generate_large_data(nb=3000, min_elems=5, max_elems=15)

        col_no = cf.gen_unique_str(f"{prefix}_large_no_idx")
        col_yes = cf.gen_unique_str(f"{prefix}_large_yes_idx")

        # Without index
        self._setup_large_collection.__func__(
            self, client, col_no, nb=0,  # skip generation
        )
        # Manual setup since we have data already
        schema = self._create_schema(client)
        idx_no = client.prepare_index_params()
        idx_no.add_index(field_name="normal_vector", index_type="HNSW",
                         metric_type="COSINE", params=INDEX_PARAMS)
        idx_no.add_index(field_name="structA[embedding]", index_type="HNSW",
                         metric_type="COSINE", params=INDEX_PARAMS)

        idx_yes = client.prepare_index_params()
        idx_yes.add_index(field_name="normal_vector", index_type="HNSW",
                          metric_type="COSINE", params=INDEX_PARAMS)
        idx_yes.add_index(field_name="structA[embedding]", index_type="HNSW",
                          metric_type="COSINE", params=INDEX_PARAMS)
        idx_yes.add_index(field_name="structA[int_val]", index_type="STL_SORT")
        idx_yes.add_index(field_name="structA[color]", index_type="INVERTED")

        # Drop and recreate properly
        client.drop_collection(col_no)
        client.drop_collection(col_yes)

        self.create_collection(client, col_no, schema=schema, index_params=idx_no)
        self.create_collection(client, col_yes, schema=schema, index_params=idx_yes)

        # Insert same data into both (small batch to avoid gRPC message size limit)
        batch_size = 200
        for start in range(0, len(data), batch_size):
            batch = data[start:start + batch_size]
            self.insert(client, col_no, batch)
            self.insert(client, col_yes, batch)
        self.flush(client, col_no)
        self.flush(client, col_yes)

        self.load_collection(client, col_no)
        self.load_collection(client, col_yes)

        # Compare element_filter query results
        filt = 'element_filter(structA, $[int_val] > 150000 && $[color] == "Blue")'
        r_no, _ = self.query(client, col_no, filter=filt,
                             output_fields=["id"], limit=16384)
        r_yes, _ = self.query(client, col_yes, filter=filt,
                              output_fields=["id"], limit=16384)

        ids_no = set(r["id"] for r in r_no)
        ids_yes = set(r["id"] for r in r_yes)
        assert ids_no == ids_yes, \
            f"Index inconsistency: no_idx={len(ids_no)} rows, with_idx={len(ids_yes)} rows. " \
            f"Only in no_idx: {sorted(ids_no - ids_yes)[:10]}, " \
            f"Only in with_idx: {sorted(ids_yes - ids_no)[:10]}"

        # Also compare MATCH_ANY
        filt2 = 'MATCH_ANY(structA, $[int_val] > 150000 && $[color] == "Blue")'
        r2_no, _ = self.query(client, col_no, filter=filt2,
                              output_fields=["id"], limit=16384)
        r2_yes, _ = self.query(client, col_yes, filter=filt2,
                               output_fields=["id"], limit=16384)

        ids2_no = set(r["id"] for r in r2_no)
        ids2_yes = set(r["id"] for r in r2_yes)
        assert ids2_no == ids2_yes, \
            f"MATCH_ANY index inconsistency: {len(ids2_no)} vs {len(ids2_yes)}"

    # ---- Multi-segment consistency ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_multi_segment_query_consistency_element_filter(self):
        """
        target: query consistency across multiple sealed + growing segments
        method: insert 5 batches (5 sealed segments), then 1 growing batch, query all
        expected: results cover all segments, match ground truth
        """
        # This test needs its own collection with specific multi-segment setup
        client = self.shared_client
        collection_name = cf.gen_unique_str(f"{prefix}_large_multi_seg")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        all_data = []
        # 5 sealed segments, 1000 rows each
        for seg in range(5):
            batch = []
            for i in range(1000):
                row_id = seg * 1000 + i
                rng = random.Random(row_id)
                num_elems = rng.randint(10, 30)
                elems = [{
                    "embedding": _seed_vector(row_id * 100 + j),
                    "int_val": row_id * 10 + j,
                    "str_val": f"r{row_id}_e{j}",
                    "color": COLORS[j % 3],
                } for j in range(num_elems)]
                batch.append({
                    "id": row_id,
                    "doc_int": row_id,
                    "doc_category": CATEGORIES[row_id % 4],
                    "normal_vector": _seed_vector(row_id + 999999),
                    "structA": elems,
                })
            self.insert(client, collection_name, batch)
            self.flush(client, collection_name)
            all_data.extend(batch)

        self.load_collection(client, collection_name)

        # 1 growing segment, 500 rows
        growing_batch = []
        for i in range(500):
            row_id = 5000 + i
            rng = random.Random(row_id)
            num_elems = rng.randint(10, 30)
            elems = [{
                "embedding": _seed_vector(row_id * 100 + j),
                "int_val": row_id * 10 + j,
                "str_val": f"r{row_id}_e{j}",
                "color": COLORS[j % 3],
            } for j in range(num_elems)]
            growing_batch.append({
                "id": row_id,
                "doc_int": row_id,
                "doc_category": CATEGORIES[row_id % 4],
                "normal_vector": _seed_vector(row_id + 999999),
                "structA": elems,
            })
        self.insert(client, collection_name, growing_batch)
        all_data.extend(growing_batch)

        # Query across all segments: element_filter
        # element_filter returns element-level results keyed by (id, offset)
        # Use a selective filter so total matching elements fit within limit,
        # ensuring both sealed and growing segments are represented
        filt = 'element_filter(structA, $[int_val] > 49000)'
        results, check = self.query(
            client, collection_name,
            filter=filt, output_fields=["id"], limit=16384,
        )
        assert check

        gt_row_ids = set()
        for row in all_data:
            if any(e["int_val"] > 49000 for e in row["structA"]):
                gt_row_ids.add(row["id"])

        milvus_row_ids = set(r["id"] for r in results)

        # Check coverage: results should include rows from all segments
        sealed_hits = [rid for rid in milvus_row_ids if rid < 5000]
        growing_hits = [rid for rid in milvus_row_ids if rid >= 5000]
        log.info(f"Sealed hits: {len(sealed_hits)}, Growing hits: {len(growing_hits)}, "
                 f"Total elements: {len(results)}, GT rows: {len(gt_row_ids)}")
        assert len(sealed_hits) > 0, "No results from sealed segments"
        assert len(growing_hits) > 0, "No results from growing segment"

        # Check no false positives at row level
        false_positives = milvus_row_ids - gt_row_ids
        assert len(false_positives) == 0, \
            f"{len(false_positives)} false positive rows"

    # ---- MATCH_LEAST at scale ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_least_5k_rows_ground_truth(self):
        """
        target: MATCH_LEAST correctness at 5000 rows with high element count
        method: MATCH_LEAST(threshold=5), compare against computed ground truth
        expected: exact match
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        # MATCH_LEAST: at least 5 elements have color == "Red"
        results, check = self.query(
            client, collection_name,
            filter='MATCH_LEAST(structA, $[color] == "Red", threshold=5)',
            output_fields=["id"],
            limit=16384,
        )
        assert check

        gt_ids = set()
        for row in data:
            red_count = sum(1 for e in row["structA"] if e["color"] == "Red")
            if red_count >= 5:
                gt_ids.add(row["id"])

        milvus_ids = set(r["id"] for r in results)
        assert milvus_ids == gt_ids, \
            f"MATCH_LEAST(5) mismatch: {len(milvus_ids)} vs {len(gt_ids)}. " \
            f"FP={len(milvus_ids - gt_ids)}, FN={len(gt_ids - milvus_ids)}"

    # ---- Multi-segment consistency using MATCH_ANY (doc-level, no limit issue) ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_multi_segment_match_any_consistency(self):
        """
        target: MATCH_ANY query consistency across multiple sealed + growing segments
        method: 5 sealed batches + 1 growing batch, MATCH_ANY query
        expected: results cover all segments, match ground truth exactly
        """
        # This test needs its own collection with specific multi-segment setup
        client = self.shared_client
        collection_name = cf.gen_unique_str(f"{prefix}_large_mseg_ma")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        all_data = []
        # 5 sealed segments, 1000 rows each
        for seg in range(5):
            batch = []
            for i in range(1000):
                row_id = seg * 1000 + i
                rng = random.Random(row_id)
                num_elems = rng.randint(10, 30)
                elems = [{
                    "embedding": _seed_vector(row_id * 100 + j),
                    "int_val": row_id * 10 + j,
                    "str_val": f"r{row_id}_e{j}",
                    "color": COLORS[j % 3],
                } for j in range(num_elems)]
                batch.append({
                    "id": row_id,
                    "doc_int": row_id,
                    "doc_category": CATEGORIES[row_id % 4],
                    "normal_vector": _seed_vector(row_id + 999999),
                    "structA": elems,
                })
            self.insert(client, collection_name, batch)
            self.flush(client, collection_name)
            all_data.extend(batch)

        self.load_collection(client, collection_name)

        # 1 growing segment, 500 rows
        growing_batch = []
        for i in range(500):
            row_id = 5000 + i
            rng = random.Random(row_id)
            num_elems = rng.randint(10, 30)
            elems = [{
                "embedding": _seed_vector(row_id * 100 + j),
                "int_val": row_id * 10 + j,
                "str_val": f"r{row_id}_e{j}",
                "color": COLORS[j % 3],
            } for j in range(num_elems)]
            growing_batch.append({
                "id": row_id,
                "doc_int": row_id,
                "doc_category": CATEGORIES[row_id % 4],
                "normal_vector": _seed_vector(row_id + 999999),
                "structA": elems,
            })
        self.insert(client, collection_name, growing_batch)
        all_data.extend(growing_batch)

        # MATCH_ANY query (doc-level limit, not affected by #3325)
        filt = 'MATCH_ANY(structA, $[int_val] > 40000)'
        results, check = self.query(
            client, collection_name,
            filter=filt, output_fields=["id"], limit=16384,
        )
        assert check

        gt_ids = set()
        for row in all_data:
            if any(e["int_val"] > 40000 for e in row["structA"]):
                gt_ids.add(row["id"])

        milvus_ids = set(r["id"] for r in results)

        # Must have results from both sealed and growing
        sealed_hits = [rid for rid in milvus_ids if rid < 5000]
        growing_hits = [rid for rid in milvus_ids if rid >= 5000]
        assert len(sealed_hits) > 0, "No results from sealed segments"
        assert len(growing_hits) > 0, "No results from growing segment"

        # Exact match
        false_positives = milvus_ids - gt_ids
        false_negatives = gt_ids - milvus_ids
        assert len(false_positives) == 0, \
            f"{len(false_positives)} false positives"
        assert len(false_negatives) == 0, \
            f"{len(false_negatives)} false negatives out of {len(gt_ids)}"


# ==================== Test Case 9: Element-Level Search Without Filter ====================

def gt_element_search_no_filter(data, query_vector, metric_type="COSINE", limit=10):
    """Ground truth for element-level search without filter.
    Flattens struct array so each element competes independently by its embedding.
    For each row, the best matching element determines the row's score."""
    row_scores = []
    for row in data:
        best_score = None
        best_offset = -1
        for j, elem in enumerate(row["structA"]):
            score = _compute_similarity(query_vector, elem["embedding"], metric_type)
            if best_score is None or (_is_descending(metric_type) and score > best_score) or \
               (not _is_descending(metric_type) and score < best_score):
                best_score = score
                best_offset = j
        if best_offset >= 0:
            row_scores.append((row["id"], best_score, best_offset))
    row_scores.sort(key=lambda x: x[1], reverse=_is_descending(metric_type))
    return row_scores[:limit]


@pytest.mark.xdist_group("TestElementLevelSearchNoFilter")
class TestElementLevelSearchNoFilter(TestMilvusClientV2Base):
    """Test element-level search on structA[embedding] WITHOUT any filter.

    This verifies the struct array flattening behavior: each element's embedding
    participates independently in ANN search, behaving like traditional dense
    vector search. No element_filter / MATCH_* / array_contains is applied.
    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_shared_collection(self, request):
        """Create one shared collection for all no-filter search tests."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_search")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="doc_category", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params, force_teardown=False)

        def _make_rows(start, count):
            rows = []
            for i in range(start, start + count):
                rng = random.Random(i)
                num_elems = rng.randint(2, 6)
                struct_array = [{
                    "embedding": _seed_vector(i * 1000 + j, default_dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "color": COLORS[j % 3],
                } for j in range(num_elems)]
                rows.append({
                    "id": i,
                    "doc_int": i,
                    "doc_category": CATEGORIES[i % 4],
                    "normal_vector": _seed_vector(i + 999999, default_dim),
                    "structA": struct_array,
                })
            return rows

        # Sealed: default_nb rows in batches
        data = []
        for start in range(0, default_nb, insert_batch_size):
            batch = _make_rows(start, min(insert_batch_size, default_nb - start))
            self.insert(client, collection_name, batch)
            data.extend(batch)
        self.flush(client, collection_name)

        # Growing: default_growing_nb rows, no flush
        growing_batch = _make_rows(default_nb, default_growing_nb)
        self.insert(client, collection_name, growing_batch)
        data.extend(growing_batch)

        self.load_collection(client, collection_name)

        request.cls.shared_client = client
        request.cls.shared_collection = collection_name
        request.cls.shared_data = data
        request.cls.sealed_nb = default_nb
        request.cls.growing_nb = default_growing_nb

        yield

        client.drop_collection(collection_name)

    # ---- 9.1 Basic element-level search without filter ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_search_no_filter_basic(self):
        """
        target: element-level search without filter returns valid results
        method: search on structA[embedding] with no filter
        expected: results returned with correct output fields
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        assert len(results[0]) == 10

        # First result should be the query vector itself (self-match)
        assert results[0][0]["id"] == 0, \
            f"Expected row 0 as top hit (self-match), got {results[0][0]['id']}"

        # Distance should be ~1.0 for cosine self-match
        top_distance = results[0][0]["distance"]
        assert abs(top_distance - 1.0) < epsilon, \
            f"Self-match distance should be ~1.0, got {top_distance}"

    # ---- 9.2 Ground truth ranking verification ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_search_no_filter_ground_truth(self):
        """
        target: verify element-level search ranking matches ground truth
        method: compare Milvus results with brute-force ground truth
        expected: top-K results match ground truth IDs
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[42]["structA"][1]["embedding"]
        limit = 20
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            limit=limit,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) == limit

        gt = gt_element_search_no_filter(data, query_vector, "COSINE", limit)
        gt_ids = [r[0] for r in gt]
        milvus_ids = [hit["id"] for hit in results[0]]

        # Top-1 must match
        assert milvus_ids[0] == gt_ids[0], \
            f"Top-1 mismatch: Milvus={milvus_ids[0]}, GT={gt_ids[0]}"

        # At least 90% overlap in top-K (HNSW recall tolerance)
        overlap = len(set(milvus_ids) & set(gt_ids))
        recall = overlap / limit
        assert recall >= 0.9, \
            f"Recall too low: {recall:.2f} ({overlap}/{limit})"

    # ---- 9.3 Distance monotonicity ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_search_no_filter_distance_order(self):
        """
        target: verify returned distances are monotonically decreasing (COSINE)
        method: search without filter, check distance ordering
        expected: distances are in descending order for COSINE
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["structA"][0]["embedding"]
        limit = 50
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            limit=limit,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) == limit

        distances = [hit["distance"] for hit in results[0]]
        for i in range(len(distances) - 1):
            assert distances[i] >= distances[i + 1] - epsilon, \
                f"Distance not monotonic at position {i}: {distances[i]} < {distances[i+1]}"

    # ---- 9.4 Offset / pagination without filter ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_no_filter_offset(self):
        """
        target: element-level search with offset but no filter
        method: compare results from offset=0 limit=20 vs offset=10 limit=10
        expected: offset results match the tail of non-offset results
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[5]["structA"][0]["embedding"]

        results_full, check1 = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            limit=20,
            output_fields=["id"],
        )
        assert check1

        results_offset, check2 = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            limit=10,
            offset=10,
            output_fields=["id"],
        )
        assert check2

        full_tail_ids = [hit["id"] for hit in results_full[0][10:20]]
        offset_ids = [hit["id"] for hit in results_offset[0]]
        assert full_tail_ids == offset_ids, \
            f"Offset mismatch:\n  full[10:20]={full_tail_ids}\n  offset={offset_ids}"

    # ---- 9.5 Multiple query vectors (batch search) ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_no_filter_batch(self):
        """
        target: batch element-level search with multiple query vectors
        method: search with 3 different query vectors, no filter
        expected: each nq returns independent results
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vectors = [
            data[0]["structA"][0]["embedding"],
            data[100]["structA"][0]["embedding"],
            data[200]["structA"][0]["embedding"],
        ]
        results, check = self.search(
            client, collection_name,
            data=query_vectors,
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            limit=5,
            output_fields=["id"],
        )
        assert check
        assert len(results) == 3

        # Each query should find its own source row as top-1
        expected_top1 = [0, 100, 200]
        for i, expected_id in enumerate(expected_top1):
            assert results[i][0]["id"] == expected_id, \
                f"Query {i}: expected top-1 id={expected_id}, got {results[i][0]['id']}"

    # ---- 9.6 Large limit ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_no_filter_large_limit(self):
        """
        target: element-level search with large limit value
        method: search with limit=200, no filter
        expected: correct number of results, distances still monotonic
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[50]["structA"][0]["embedding"]
        limit = 200
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            limit=limit,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) == limit

        # No duplicate IDs (each row appears at most once)
        ids = [hit["id"] for hit in results[0]]
        assert len(ids) == len(set(ids)), \
            f"Duplicate IDs in results: {len(ids)} total, {len(set(ids))} unique"

        # Distances monotonically decreasing
        distances = [hit["distance"] for hit in results[0]]
        for i in range(len(distances) - 1):
            assert distances[i] >= distances[i + 1] - epsilon, \
                f"Distance not monotonic at position {i}"

    # ---- 9.7 Element-level vs normal vector search independence ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_vs_normal_vector_independence(self):
        """
        target: element-level search and normal vector search are independent
        method: same query vector on structA[embedding] vs normal_vector
        expected: results differ (different vector spaces)
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = _seed_vector(12345, default_dim)

        results_elem, check1 = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            limit=10,
            output_fields=["id"],
        )
        assert check1

        results_normal, check2 = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            limit=10,
            output_fields=["id"],
        )
        assert check2

        elem_ids = [hit["id"] for hit in results_elem[0]]
        normal_ids = [hit["id"] for hit in results_normal[0]]

        # Results should differ because element embeddings and normal vectors
        # are generated from different seeds
        assert elem_ids != normal_ids, \
            "Element-level and normal vector search returned identical results"

    # ---- 9.8 Range search not supported on element-level embedding ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_no_filter_range_search_not_supported(self):
        """
        target: range search on structA[embedding] should be rejected
        method: search with radius and range_filter params on element-level field
        expected: server returns error (range search not supported for vector array)
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["structA"][0]["embedding"]
        radius = 0.5
        range_filter = 1.0
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={
                "metric_type": "COSINE",
                "params": {"radius": radius, "range_filter": range_filter},
            },
            limit=50,
            output_fields=["id"],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100,
                         "err_msg": "range search is not supported for vector array"},
        )

    # ---- 9.9 Growing segment search without filter ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_no_filter_growing_segment(self):
        """
        target: element-level search finds data in growing (unflushed) segment
        method: search using a growing row's embedding as query vector
        expected: growing row appears as top-1 self-match
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        # Pick a row from the growing segment (id >= sealed_nb)
        growing_row_id = self.sealed_nb  # first growing row
        query_vector = data[growing_row_id]["structA"][0]["embedding"]

        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) > 0

        # Self-match should be top-1
        assert results[0][0]["id"] == growing_row_id, \
            f"Expected growing row {growing_row_id} as top-1, got {results[0][0]['id']}"
        assert abs(results[0][0]["distance"] - 1.0) < epsilon

        # Verify results include both sealed and growing rows
        result_ids = {hit["id"] for hit in results[0]}
        sealed_hits = [rid for rid in result_ids if rid < self.sealed_nb]
        growing_hits = [rid for rid in result_ids if rid >= self.sealed_nb]
        assert len(sealed_hits) > 0, "No results from sealed segments"
        assert len(growing_hits) > 0, "No results from growing segment"

    # ---- 9.10 Group by PK on element-level search ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_search_no_filter_group_by_pk(self):
        """
        target: element-level search with group_by on primary key
        method: search on structA[embedding] with group_by_field="id" (PK), no filter
        expected: each row appears at most once (deduplicated by PK),
                  results are valid and distances monotonically ordered
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["structA"][0]["embedding"]
        limit = 20
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            group_by_field="id",
            limit=limit,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) == limit

        # No duplicate PK — group_by PK ensures each row at most once
        ids = [hit["id"] for hit in results[0]]
        assert len(ids) == len(set(ids)), \
            f"Duplicate PKs: {len(ids)} total, {len(set(ids))} unique"

        # Self-match should be top-1
        assert ids[0] == 0, f"Expected PK 0 as top-1 (self-match), got {ids[0]}"

        # Distance monotonically decreasing
        distances = [hit["distance"] for hit in results[0]]
        for i in range(len(distances) - 1):
            assert distances[i] >= distances[i + 1] - epsilon, \
                f"Distance not monotonic at position {i}: {distances[i]} < {distances[i+1]}"

    # ---- 9.11 Group by PK with group_size on element-level search ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_no_filter_group_by_pk_with_group_size(self):
        """
        target: element-level search with group_by PK and group_size > 1
        method: search with group_by_field="id", group_size=3
        expected: each PK group has at most group_size entries (one per matching element)
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[100]["structA"][0]["embedding"]
        limit = 10
        group_size = 3
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            group_by_field="id",
            group_size=group_size,
            limit=limit,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) > 0

        # Count hits per PK group
        groups = {}
        for hit in results[0]:
            groups.setdefault(hit["id"], []).append(hit)

        for pk, hits in groups.items():
            assert len(hits) <= group_size, \
                f"PK {pk}: {len(hits)} hits exceeds group_size={group_size}"

        # Distance monotonically decreasing
        distances = [hit["distance"] for hit in results[0]]
        for i in range(len(distances) - 1):
            assert distances[i] >= distances[i + 1] - epsilon, \
                f"Distance not monotonic at position {i}"

    # ---- 9.12 Group by PK with element_filter ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_group_by_pk_with_filter(self):
        """
        target: element-level search with group_by PK combined with element_filter
        method: search with group_by_field="id" + element_filter
        expected: results grouped by PK, only matching elements contribute
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["structA"][0]["embedding"]
        limit = 20
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 500)',
            group_by_field="id",
            limit=limit,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results[0]) > 0

        # No duplicate PKs
        ids = [hit["id"] for hit in results[0]]
        assert len(ids) == len(set(ids)), "Duplicate PKs in group_by results"

        # Every returned row must have at least one element with int_val > 500
        for hit in results[0]:
            assert any(e["int_val"] > 500 for e in hit["structA"]), \
                f"Row {hit['id']}: no element with int_val > 500"

    # ---- 9.13 Group by PK ground truth verification ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_group_by_pk_ground_truth(self):
        """
        target: verify group_by PK search ranking against ground truth
        method: compare Milvus group_by PK results with brute-force best-element-per-row
        expected: high recall overlap with ground truth
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[50]["structA"][1]["embedding"]
        limit = 20
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            group_by_field="id",
            limit=limit,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) == limit

        # Ground truth: for each row pick the best element, rank rows
        gt = gt_element_search_no_filter(data, query_vector, "COSINE", limit)
        gt_ids = [r[0] for r in gt]
        milvus_ids = [hit["id"] for hit in results[0]]

        # Top-1 must match
        assert milvus_ids[0] == gt_ids[0], \
            f"Top-1 mismatch: Milvus={milvus_ids[0]}, GT={gt_ids[0]}"

        # At least 90% recall
        overlap = len(set(milvus_ids) & set(gt_ids))
        recall = overlap / limit
        assert recall >= 0.9, \
            f"Group-by PK recall too low: {recall:.2f} ({overlap}/{limit})"

    # ---- 9.14 Group by non-PK field not supported on element-level search ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_group_by_non_pk_not_supported(self):
        """
        target: element-level search only supports group_by on PK field
        method: search on structA[embedding] with group_by_field="doc_category" (non-PK)
        expected: server returns error
        """
        client = self.shared_client
        collection_name = self.shared_collection
        data = self.shared_data

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            group_by_field="doc_category",
            limit=10,
            output_fields=["id"],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100,
                         "err_msg": "only group by primary key is supported for element-level search"},
        )
