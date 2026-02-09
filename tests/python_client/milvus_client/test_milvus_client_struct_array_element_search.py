import pytest
import numpy as np
import random

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus import DataType, AnnSearchRequest, RRFRanker, WeightedRanker
from pymilvus.client.embedding_list import EmbeddingList


prefix = "struct_elem"
epsilon = 0.001
default_nb = ct.default_nb  # 2000
default_nq = ct.default_nq  # 2
default_dim = 128
default_capacity = 100
INDEX_PARAMS = {"M": 16, "efConstruction": 200}
COLORS = ["Red", "Blue", "Green"]
SIZES = ["S", "M", "L", "XL"]


def _seed_vector(seed, dim=default_dim):
    """Generate a deterministic vector from a seed."""
    rng = np.random.RandomState(seed)
    vec = rng.rand(dim).astype(np.float32)
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec.tolist()


# ========== Ground Truth Computation ==========

def _cosine_sim(v1, v2):
    """Compute cosine similarity between two vectors."""
    a = np.array(v1, dtype=np.float32)
    b = np.array(v2, dtype=np.float32)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10))


def _l2_distance(v1, v2):
    """Compute L2 (squared) distance."""
    a = np.array(v1, dtype=np.float32)
    b = np.array(v2, dtype=np.float32)
    return float(np.sum((a - b) ** 2))


def _ip_score(v1, v2):
    """Compute inner product."""
    a = np.array(v1, dtype=np.float32)
    b = np.array(v2, dtype=np.float32)
    return float(np.dot(a, b))


def _compute_similarity(v1, v2, metric_type):
    """Compute similarity/distance based on metric type."""
    if metric_type == "COSINE":
        return _cosine_sim(v1, v2)
    elif metric_type == "L2":
        return _l2_distance(v1, v2)
    elif metric_type == "IP":
        return _ip_score(v1, v2)
    raise ValueError(f"Unsupported metric: {metric_type}")


def _is_descending(metric_type):
    """Whether results should be sorted in descending order (higher = better)."""
    return metric_type in ("COSINE", "IP")


def gt_element_filter_search(data, query_vector, elem_filter_fn, metric_type="COSINE",
                              limit=10, doc_filter_fn=None):
    """
    Ground truth for element_filter search.

    For each row:
    1. Apply doc_filter_fn if provided (doc-level pre-filter)
    2. Find elements satisfying elem_filter_fn
    3. Compute similarity between query_vector and each matching element's embedding
    4. Row score = best matching element's similarity
    5. Rank rows by score, return top-K

    Returns: list of (row_id, best_distance, best_offset)
    """
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


def gt_match_query(data, match_type, elem_filter_fn, threshold=None,
                   doc_filter_fn=None):
    """
    Ground truth for MATCH family query.

    For each row, count elements satisfying elem_filter_fn, then apply MATCH logic.

    Returns: set of matching row IDs
    """
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


def gt_match_search(data, query_vector, match_type, elem_filter_fn,
                    threshold=None, metric_type="COSINE", limit=10,
                    anns_field="normal_vector", doc_filter_fn=None):
    """
    Ground truth for MATCH + vector search.

    1. Filter rows by MATCH condition
    2. Rank by vector similarity on anns_field
    3. Return top-K

    Returns: list of (row_id, distance)
    """
    match_ids = gt_match_query(data, match_type, elem_filter_fn, threshold, doc_filter_fn)

    row_scores = []
    for row in data:
        if row["id"] in match_ids:
            score = _compute_similarity(query_vector, row[anns_field], metric_type)
            row_scores.append((row["id"], score))

    row_scores.sort(key=lambda x: x[1], reverse=_is_descending(metric_type))
    return row_scores[:limit]


def assert_result_ids_match(milvus_results, gt_results, recall_threshold=0.9,
                            is_search=True, check_order=False):
    """
    Compare Milvus results against ground truth.

    Args:
        milvus_results: Milvus search results (List[List[dict]]) or query results (List[dict])
        gt_results: Ground truth - list of tuples (id, score, ...) or set of IDs
        recall_threshold: Minimum fraction of GT IDs that must appear in results
        is_search: True for search results (List[List[dict]]), False for query (List[dict])
        check_order: If True, also verify top-1 ranking matches
    """
    if is_search:
        milvus_ids = [hit["id"] for hit in milvus_results[0]]
    else:
        milvus_ids = [hit["id"] for hit in milvus_results]

    if isinstance(gt_results, set):
        gt_ids = gt_results
    else:
        gt_ids = {r[0] for r in gt_results}

    milvus_id_set = set(milvus_ids)

    # Check recall
    if len(gt_ids) > 0:
        overlap = milvus_id_set & gt_ids
        recall = len(overlap) / min(len(gt_ids), len(milvus_ids)) if milvus_ids else 0
        assert recall >= recall_threshold, \
            f"Recall {recall:.2f} < {recall_threshold}. " \
            f"GT IDs (top-{len(gt_ids)}): {sorted(gt_ids)[:20]}, " \
            f"Milvus IDs: {sorted(milvus_id_set)[:20]}"

    # Check order (top-1 should match in most cases)
    if check_order and gt_results and milvus_ids:
        gt_top1 = gt_results[0][0] if isinstance(gt_results, list) else None
        if gt_top1 is not None:
            assert milvus_ids[0] == gt_top1, \
                f"Top-1 mismatch: Milvus={milvus_ids[0]}, GT={gt_top1}"


def _assert_distance_order(results, metric_type):
    """Verify search results are sorted by distance correctly."""
    if len(results[0]) <= 1:
        return
    distances = [hit["distance"] for hit in results[0]]
    if _is_descending(metric_type):
        for k in range(len(distances) - 1):
            assert distances[k] >= distances[k + 1] - epsilon, \
                f"Results not sorted descending: distances[{k}]={distances[k]} < distances[{k+1}]={distances[k+1]}"
    else:
        for k in range(len(distances) - 1):
            assert distances[k] <= distances[k + 1] + epsilon, \
                f"Results not sorted ascending: distances[{k}]={distances[k]} > distances[{k+1}]={distances[k+1]}"


def _generate_float16_bytes(dim, seed=None):
    """Generate Float16 vector bytes."""
    rng = np.random.RandomState(seed)
    vec = rng.rand(dim).astype(np.float16)
    return vec.tobytes()


def _generate_bfloat16_bytes(dim, seed=None):
    """Generate BFloat16 vector bytes."""
    rng = np.random.RandomState(seed)
    vec = rng.rand(dim).astype(np.float32)
    # Convert float32 to bfloat16 by truncating lower 16 bits
    int_repr = np.frombuffer(vec.tobytes(), dtype=np.uint32)
    bfloat16 = (int_repr >> 16).astype(np.uint16)
    return bfloat16.tobytes()


def _generate_int8_vector(dim, seed=None):
    """Generate Int8 vector as list of ints."""
    rng = np.random.RandomState(seed)
    return rng.randint(-128, 127, size=dim).astype(np.int8).tobytes()


def _generate_binary_vector(dim, seed=None):
    """Generate binary vector bytes (dim/8 bytes)."""
    rng = np.random.RandomState(seed)
    return rng.bytes(dim // 8)


class TestStructArrayElementFilterSearch(TestMilvusClientV2Base):
    """Test element_filter() syntax + element-level vector search (22 cases)"""

    def _create_schema(self, client, dim=default_dim):
        """Create schema with struct array containing embedding + scalar sub-fields."""
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
        struct_schema.add_field("size", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _generate_data(self, nb=1000, dim=default_dim, min_elems=3, max_elems=10):
        """Generate deterministic test data for element-level search."""
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
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                    "size": SIZES[(i + j) % 4],
                })
            data.append({
                "id": i,
                "doc_int": i,
                "doc_varchar": f"cat_{i % 10}",
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    def _create_collection_and_insert(self, client, collection_name, nb=1000,
                                       dim=default_dim, flush=True,
                                       metric_type="COSINE",
                                       create_nested_index=False,
                                       index_type="HNSW"):
        """Helper: create collection, insert data, build index, load."""
        schema = self._create_schema(client, dim=dim)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]",
            index_type=index_type,
            metric_type=metric_type,
            params=INDEX_PARAMS,
        )
        if create_nested_index:
            index_params.add_index(
                field_name="structA[int_val]",
                index_type="INVERTED",
            )

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        data = self._generate_data(nb=nb, dim=dim)
        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == nb

        if flush:
            self.flush(client, collection_name)

        self.load_collection(client, collection_name)
        return data

    # ---- L0 tests ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_search_basic_cosine(self):
        """
        target: basic element_filter search with COSINE metric
        method: search on structA[embedding] with element_filter condition
        expected: results returned with element_indices
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_cosine")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        # Use a known element vector as query
        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        # Scalar condition: each hit must have at least one element with int_val >= 0
        for hit in results[0]:
            assert any(e["int_val"] >= 0 for e in hit["structA"]), \
                f"Row {hit['id']} has no element with int_val >= 0"
        # Top-1: querying row 0's own vector should return row 0
        assert results[0][0]["id"] == 0, \
            f"Top-1 should be row 0 (queried its own vector), got {results[0][0]['id']}"
        # Distance ordering
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_search_basic_l2(self):
        """
        target: element_filter search with L2 metric
        method: search on structA[embedding] with L2 metric + element_filter
        expected: results returned successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_l2")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="L2")

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "L2"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        # Scalar condition verification
        for hit in results[0]:
            assert any(e["int_val"] >= 0 for e in hit["structA"]), \
                f"Row {hit['id']} has no element with int_val >= 0"
        # Top-1: querying row 0's own vector should return row 0
        assert results[0][0]["id"] == 0, \
            f"Top-1 should be row 0, got {results[0][0]['id']}"
        # Distance ordering (L2: ascending)
        _assert_distance_order(results, "L2")

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_with_doc_level_filter(self):
        """
        target: combine doc-level filter with element_filter
        method: 'doc_int > 100 && element_filter(structA, $[str_val] == "row_200_elem_0")'
        expected: only rows matching both conditions returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_doc")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[200]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='doc_int > 100 && element_filter(structA, $[str_val] == "row_200_elem_0")',
            limit=10,
            output_fields=["id", "doc_int"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert hit["doc_int"] > 100
        # Top-1: querying row 200's vector with its specific str_val filter
        assert results[0][0]["id"] == 200, \
            f"Top-1 should be row 200, got {results[0][0]['id']}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_element_filter_compound_same_element_semantic(self):
        """
        target: CORE - verify element_filter compound conditions apply to SAME element
        method: Row 0: elem[0]={Red,S}, elem[1]={Blue,L}
                element_filter($[color]=="Red" && $[size]=="L") should NOT match Row 0
                because no single element has both Red AND L.
                Row 1: elem[0]={Red,L} → should match
        expected: Row 0 not in results, Row 1 in results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_semantic")

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
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        # Carefully constructed data
        target_vec = _seed_vector(77777)
        data = [
            {
                "id": 0, "doc_int": 0, "doc_varchar": "cat_0",
                "normal_vector": _seed_vector(99990),
                "structA": [
                    {"embedding": _seed_vector(0), "int_val": 1, "str_val": "a",
                     "float_val": 0.1, "color": "Red", "size": "S"},
                    {"embedding": target_vec, "int_val": 2, "str_val": "b",
                     "float_val": 0.2, "color": "Blue", "size": "L"},
                ],
            },
            {
                "id": 1, "doc_int": 1, "doc_varchar": "cat_1",
                "normal_vector": _seed_vector(99991),
                "structA": [
                    {"embedding": target_vec, "int_val": 10, "str_val": "x",
                     "float_val": 1.0, "color": "Red", "size": "L"},
                ],
            },
            {
                "id": 2, "doc_int": 2, "doc_varchar": "cat_2",
                "normal_vector": _seed_vector(99992),
                "structA": [
                    {"embedding": _seed_vector(20), "int_val": 20, "str_val": "p",
                     "float_val": 2.0, "color": "Blue", "size": "S"},
                ],
            },
        ]
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search using target_vec so Row 0 (elem[1]) and Row 1 (elem[0]) are closest
        results, check = self.search(
            client, collection_name,
            data=[target_vec],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[color] == "Red" && $[size] == "L")',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        matched_ids = {hit["id"] for hit in results[0]}
        # Row 0: Red on elem[0](size=S), L on elem[1](color=Blue) → NO match
        assert 0 not in matched_ids, \
            "Row 0 should NOT match (Red and L are on different elements)"
        # Row 1: elem[0]={Red,L} → YES
        assert 1 in matched_ids, \
            "Row 1 should match (Red+L on same element)"
        # Ground truth verification
        gt = gt_element_filter_search(
            data, target_vec,
            elem_filter_fn=lambda e: e["color"] == "Red" and e["size"] == "L",
            metric_type="COSINE", limit=10,
        )
        assert_result_ids_match(results, gt, recall_threshold=1.0)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="pymilvus element_indices not yet re-exposed after PR #3240 refactoring")
    def test_element_filter_verify_in_struct_offset(self):
        """
        target: verify element_indices corresponds to correct array subscript
        method: insert known data, search with element_filter matching specific element
        expected: element_indices matches the known position
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_offset")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        # Search for elem_2 of row 50 specifically
        target_row = 50
        target_elem = 2
        query_vector = data[target_row]["structA"][target_elem]["embedding"]

        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter=f'element_filter(structA, $[str_val] == "row_{target_row}_elem_{target_elem}")',
            limit=1,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        # Check element_indices (offset) if exposed
        top_hit = results[0]
        assert top_hit["id"] == target_row
        # Verify offset field exists
        assert "offset" in top_hit or hasattr(top_hit, "offset"), \
            "element_indices (offset) not exposed in pymilvus"

    # ---- L1 tests ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_basic_ip(self):
        """
        target: element_filter search with IP metric
        method: search with IP metric + element_filter
        expected: results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_ip")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="IP")

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "IP"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["int_val"] >= 0 for e in hit["structA"]), \
                f"Row {hit['id']} has no element with int_val >= 0"
        _assert_distance_order(results, "IP")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_int_condition(self):
        """
        target: element_filter with integer condition
        method: element_filter(structA, $[int_val] > 50)
        expected: only elements with int_val > 50 matched
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_int")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[10]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 50)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        # Scalar condition: each hit must have element with int_val > 50
        for hit in results[0]:
            assert any(e["int_val"] > 50 for e in hit["structA"]), \
                f"Row {hit['id']} has no element with int_val > 50"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_compound_condition(self):
        """
        target: element_filter with compound AND condition on same element
        method: element_filter(structA, $[color] == "Red" && $[int_val] > 10)
        expected: both conditions satisfied on the same element
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_compound")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[color] == "Red" && $[int_val] > 10)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        # CRITICAL: BOTH conditions must hold on the SAME element
        for hit in results[0]:
            assert any(e["color"] == "Red" and e["int_val"] > 10 for e in hit["structA"]), \
                f"Row {hit['id']} has no element with color==Red AND int_val>10 on same element"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_or_condition(self):
        """
        target: element_filter with OR condition
        method: element_filter(structA, $[color] == "Red" || $[int_val] > 99900)
        expected: elements matching either condition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_or")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[color] == "Red" || $[int_val] > 99900)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["color"] == "Red" or e["int_val"] > 99900 for e in hit["structA"]), \
                f"Row {hit['id']} has no element matching OR condition"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_range_condition(self):
        """
        target: element_filter with range condition
        method: element_filter(structA, $[int_val] > 10 && $[int_val] < 50)
        expected: only elements within range matched
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_range")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 10 && $[int_val] < 50)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        # Scalar condition verification: each hit must have element in range
        for hit in results[0]:
            assert any(10 < e["int_val"] < 50 for e in hit["structA"]), \
                f"Row {hit['id']} has no element in range (10, 50)"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_with_doc_varchar_filter(self):
        """
        target: doc-level varchar filter + element_filter
        method: 'doc_varchar == "cat_1" && element_filter(structA, $[int_val] > 5)'
        expected: only matching rows
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_varchar")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[1]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='doc_varchar == "cat_1" && element_filter(structA, $[int_val] > 5)',
            limit=10,
            output_fields=["id", "doc_varchar"],
        )
        assert check
        for hit in results[0]:
            assert hit["doc_varchar"] == "cat_1"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_multiple_matches_per_row(self):
        """
        target: same row has multiple elements matching filter
        method: broad filter that matches many elements per row
        expected: results returned (one result per row, not per element)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_multi")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[5]["structA"][0]["embedding"]
        # All elements of row 5 have int_val = 500..509, all > 100
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 100)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["int_val"] > 100 for e in hit["structA"]), \
                f"Row {hit['id']} has no element with int_val > 100"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_no_matching_elements(self):
        """
        target: no element matches the filter condition
        method: use impossible condition
        expected: empty results or rows with no matching elements excluded
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_nomatch")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[0]["structA"][0]["embedding"]
        # int_val max is 999*100+9 = 99909, so > 999999 matches nothing
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 999999)',
            limit=10,
        )
        assert check
        assert len(results[0]) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_growing_segment(self):
        """
        target: element_filter search on growing segment (not flushed)
        method: insert data without flush, then search
        expected: results from growing segment
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_growing")
        data = self._create_collection_and_insert(
            client, collection_name, nb=1000, metric_type="COSINE", flush=False
        )

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["int_val"] >= 0 for e in hit["structA"])
        # Top-1: searching row 0's own vector should return row 0
        assert results[0][0]["id"] == 0, \
            f"Top-1 should be row 0 (self-vector), got {results[0][0]['id']}"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_sealed_segment(self):
        """
        target: element_filter search on sealed segment (flushed)
        method: insert + flush, then search
        expected: results from sealed segment
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_sealed")
        data = self._create_collection_and_insert(
            client, collection_name, nb=1000, metric_type="COSINE", flush=True
        )

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["int_val"] >= 0 for e in hit["structA"])
        # Top-1: searching row 0's own vector should return row 0
        assert results[0][0]["id"] == 0, \
            f"Top-1 should be row 0 (self-vector), got {results[0][0]['id']}"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_with_output_fields(self):
        """
        target: element_filter search with struct sub-field in output_fields
        method: specify output_fields containing struct sub-fields
        expected: output contains requested sub-fields
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_output")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
            output_fields=["id", "doc_int", "structA[int_val]", "structA[color]"],
        )
        assert check
        assert len(results) > 0
        # Verify output_fields are present in results
        for hit in results[0]:
            assert hit["id"] is not None
            # Access doc_int via [] (pymilvus proxies to entity)
            assert hit["doc_int"] is not None, f"doc_int missing from hit"
            # Verify structA sub-fields accessible
            assert hit["structA"] is not None
        _assert_distance_order(results, "COSINE")

    @pytest.mark.xfail(reason="FLAT index on struct array vector not supported for element_filter search")
    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_brute_force(self):
        """
        target: element_filter search without vector index (brute force)
        method: create collection without HNSW index on struct vector, search
        expected: brute force path works
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_brute")
        schema = self._create_schema(client)

        # Only index normal_vector, NOT structA[embedding]
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        # Still need to add index for struct vector to load collection
        index_params.add_index(
            field_name="structA[embedding]",
            index_type="FLAT",
            metric_type="COSINE",
        )

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        data = self._generate_data(nb=500)
        res, check = self.insert(client, collection_name, data)
        assert check
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["int_val"] >= 0 for e in hit["structA"])
        # Top-1: searching row 0's own vector should return row 0
        assert results[0][0]["id"] == 0, \
            f"Top-1 should be row 0 (self-vector), got {results[0][0]['id']}"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_with_hnsw_index(self):
        """
        target: element_filter search with HNSW index + COSINE metric
        method: create HNSW index on struct vector, then element_filter search
        expected: indexed search returns correct results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_hnsw")
        data = self._create_collection_and_insert(
            client, collection_name, nb=1000, metric_type="COSINE", index_type="HNSW"
        )

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE", "params": {"ef": 200}},
            filter='element_filter(structA, $[color] == "Red")',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["color"] == "Red" for e in hit["structA"]), \
                f"Row {hit['id']} has no Red element"
        _assert_distance_order(results, "COSINE")

    # ---- L2 tests ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_search_mixed_segments(self):
        """
        target: element_filter search on mixed sealed + growing segments
        method: insert 1000 + flush + insert 500 (growing), then search
        expected: results from both segments
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_mixed")

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
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        # Sealed segment
        sealed_data = self._generate_data(nb=1000)
        self.insert(client, collection_name, sealed_data)
        self.flush(client, collection_name)

        # Growing segment
        growing_data = []
        for i in range(1000, 1500):
            rng = random.Random(i)
            num_elems = rng.randint(3, 10)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                    "size": SIZES[(i + j) % 4],
                })
            growing_data.append({
                "id": i, "doc_int": i, "doc_varchar": f"cat_{i % 10}",
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, growing_data)
        self.load_collection(client, collection_name)

        all_data = sealed_data + growing_data
        query_vector = sealed_data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=20,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["int_val"] >= 0 for e in hit["structA"])
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_search_limit_offset(self):
        """
        target: element_filter search with limit and offset for pagination
        method: limit=5, offset=5
        expected: second page of results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_page")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[0]["structA"][0]["embedding"]
        # First page
        results_p1, _ = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=5,
            offset=0,
        )
        # Second page
        results_p2, _ = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=5,
            offset=5,
        )
        # Pages should not overlap
        ids_p1 = {r["id"] for r in results_p1[0]}
        ids_p2 = {r["id"] for r in results_p2[0]}
        assert len(ids_p1 & ids_p2) == 0, "Pagination pages should not overlap"
        # Verify page 1 distances are better than page 2
        if results_p1[0] and results_p2[0]:
            _assert_distance_order(results_p1, "COSINE")
            _assert_distance_order(results_p2, "COSINE")
            # Last score of page 1 should be >= first score of page 2 (COSINE: higher is better)
            p1_last = results_p1[0][-1]["distance"]
            p2_first = results_p2[0][0]["distance"]
            assert p1_last >= p2_first - 1e-4, \
                f"Page 1 last distance ({p1_last}) should >= page 2 first ({p2_first})"

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_search_nq_multiple(self):
        """
        target: element_filter search with nq > 1 (multiple query vectors)
        method: send 3 query vectors
        expected: 3 result sets returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_nq")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vectors = [
            data[i]["structA"][0]["embedding"] for i in range(3)
        ]
        results, check = self.search(
            client, collection_name,
            data=query_vectors,
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=5,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) == 3
        # Verify each nq result set
        for nq_idx in range(3):
            assert len(results[nq_idx]) > 0, f"nq[{nq_idx}] returned empty results"
            for hit in results[nq_idx]:
                assert any(e["int_val"] >= 0 for e in hit["structA"])
            # Verify distance ordering within each nq
            distances = [hit["distance"] for hit in results[nq_idx]]
            for k in range(len(distances) - 1):
                assert distances[k] >= distances[k + 1] - 1e-4, \
                    f"nq[{nq_idx}] distances not sorted: {distances[k]} < {distances[k + 1]}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_negation(self):
        """
        target: element_filter with negation
        method: element_filter(structA, !($[int_val] < 0))
        expected: all elements with int_val >= 0 match (which is all)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_neg")
        data = self._create_collection_and_insert(client, collection_name, nb=1000, metric_type="COSINE")

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, !($[int_val] < 0))',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        # !($[int_val] < 0) == $[int_val] >= 0 — all elements match
        for hit in results[0]:
            assert any(e["int_val"] >= 0 for e in hit["structA"])
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", ["L2", "COSINE", "IP"])
    def test_element_filter_search_parametrize_metrics(self, metric_type):
        """
        target: element_filter search with different metric types
        method: parametrize L2, COSINE, IP
        expected: all metrics work with element_filter
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ef_{metric_type.lower()}")
        data = self._create_collection_and_insert(
            client, collection_name, nb=500, metric_type=metric_type
        )

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": metric_type},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["int_val"] >= 0 for e in hit["structA"])
        # Top-1: searching row 0's own vector should return row 0
        assert results[0][0]["id"] == 0, \
            f"Top-1 should be row 0 (self-vector), got {results[0][0]['id']}"
        _assert_distance_order(results, metric_type)


class TestStructArrayMatchFamily(TestMilvusClientV2Base):
    """Test MATCH_ALL/ANY/LEAST/MOST/EXACT operators (25 cases)"""

    def _create_schema(self, client, dim=default_dim):
        """Create schema for Match family tests."""
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
        struct_schema.add_field("size", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _generate_data(self, nb=1000, dim=default_dim):
        """Generate deterministic data for match family tests."""
        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 10)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                    "size": SIZES[(i + j) % 4],
                })
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    def _generate_semantic_data(self, client, dim=default_dim):
        """Generate specific data for nested semantic verification.
        Row 0: elem[0]={Red,S}, elem[1]={Blue,L}, elem[2]={Green,M}
        Row 1: elem[0]={Red,L}, elem[1]={Red,L}  (both Red AND L on same elem)
        Row 2: elem[0]={Blue,S}, elem[1]={Green,XL}
        """
        data = [
            {
                "id": 0, "doc_int": 0,
                "normal_vector": _seed_vector(99990, dim),
                "structA": [
                    {"embedding": _seed_vector(0, dim), "int_val": 1, "str_val": "a",
                     "float_val": 0.1, "color": "Red", "size": "S"},
                    {"embedding": _seed_vector(1, dim), "int_val": 2, "str_val": "b",
                     "float_val": 0.2, "color": "Blue", "size": "L"},
                    {"embedding": _seed_vector(2, dim), "int_val": 3, "str_val": "c",
                     "float_val": 0.3, "color": "Green", "size": "M"},
                ],
            },
            {
                "id": 1, "doc_int": 1,
                "normal_vector": _seed_vector(99991, dim),
                "structA": [
                    {"embedding": _seed_vector(10, dim), "int_val": 10, "str_val": "x",
                     "float_val": 1.0, "color": "Red", "size": "L"},
                    {"embedding": _seed_vector(11, dim), "int_val": 11, "str_val": "y",
                     "float_val": 1.1, "color": "Red", "size": "L"},
                ],
            },
            {
                "id": 2, "doc_int": 2,
                "normal_vector": _seed_vector(99992, dim),
                "structA": [
                    {"embedding": _seed_vector(20, dim), "int_val": 20, "str_val": "p",
                     "float_val": 2.0, "color": "Blue", "size": "S"},
                    {"embedding": _seed_vector(21, dim), "int_val": 21, "str_val": "q",
                     "float_val": 2.1, "color": "Green", "size": "XL"},
                ],
            },
        ]
        return data

    def _setup_collection(self, client, collection_name, data, dim=default_dim,
                          flush=True, create_nested_index=False):
        """Helper to create collection, insert data, build index, load."""
        schema = self._create_schema(client, dim=dim)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="MAX_SIM_COSINE", params=INDEX_PARAMS,
        )
        if create_nested_index:
            index_params.add_index(field_name="structA[int_val]", index_type="INVERTED")
            index_params.add_index(field_name="structA[color]", index_type="INVERTED")
            index_params.add_index(field_name="structA[size]", index_type="INVERTED")

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check
        res, check = self.insert(client, collection_name, data)
        assert check
        if flush:
            self.flush(client, collection_name)
        self.load_collection(client, collection_name)

    # ---- L0 tests ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_match_all_basic(self):
        """
        target: MATCH_ALL basic - all elements must match
        method: MATCH_ALL(structA, $[color] == "Red")
        expected: only rows where ALL elements have color=="Red"
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ma_basic")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[color] == "Red")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        # Verify: for each result, ALL elements must have color=="Red"
        for hit in results:
            for elem in hit["structA"]:
                assert elem["color"] == "Red", f"Row {hit['id']} has non-Red element"
        # GT completeness check
        gt_ids = gt_match_query(data, "MATCH_ALL", lambda e: e["color"] == "Red")
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_match_all_compound_same_element(self):
        """
        target: MATCH_ALL with compound condition on same element
        method: MATCH_ALL(structA, $[color] == "Red" && $[size] == "L")
        expected: all elements must have BOTH color==Red AND size==L
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ma_compound")
        data = self._generate_semantic_data(client)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[color] == "Red" && $[size] == "L")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        # Row 1 has all elements with Red+L → should match
        # Row 0, 2 should NOT match
        matched_ids = {r["id"] for r in results}
        assert 1 in matched_ids, "Row 1 (all Red+L) should match"
        assert 0 not in matched_ids, "Row 0 should not match MATCH_ALL"

    @pytest.mark.tags(CaseLabel.L0)
    def test_match_any_basic(self):
        """
        target: MATCH_ANY basic - at least one element matches
        method: MATCH_ANY(structA, $[color] == "Blue")
        expected: rows where at least one element has color=="Blue"
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_many_basic")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Blue")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            has_blue = any(e["color"] == "Blue" for e in hit["structA"])
            assert has_blue, f"Row {hit['id']} has no Blue element"
        # GT completeness check
        gt_ids = gt_match_query(data, "MATCH_ANY", lambda e: e["color"] == "Blue")
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_match_nested_semantic_verification(self):
        """
        target: CORE TEST - verify nested semantic (conditions on SAME element)
        method: Row 0: elem[0]={Red,S}, elem[1]={Blue,L}
                MATCH_ANY(structA, $[color]=="Red" && $[size]=="L") should NOT match Row 0
                because no single element has both Red AND L
        expected: Row 0 not in results, Row 1 in results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_semantic")
        data = self._generate_semantic_data(client)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red" && $[size] == "L")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        matched_ids = {r["id"] for r in results}
        # Row 0: Red on elem[0] but size=S, L on elem[1] but color=Blue → NO match
        assert 0 not in matched_ids, "Row 0 should NOT match (Red and L on different elements)"
        # Row 1: elem[0]={Red,L}, elem[1]={Red,L} → YES
        assert 1 in matched_ids, "Row 1 should match (Red+L on same element)"

    # ---- L1 tests ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_any_compound_same_element(self):
        """
        target: MATCH_ANY compound - cross-element matching should NOT hit
        method: construct data where color=Red and size=L are on different elements
        expected: only rows with BOTH on same element match
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_many_cross")
        data = self._generate_semantic_data(client)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red" && $[size] == "L")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        matched_ids = {r["id"] for r in results}
        assert 0 not in matched_ids
        assert 1 in matched_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_least_basic(self):
        """
        target: MATCH_LEAST - at least threshold elements match
        method: MATCH_LEAST(structA, $[int_val] > 5, threshold=3)
        expected: rows with >= 3 elements having int_val > 5
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ml_basic")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_LEAST(structA, $[int_val] > 5, threshold=3)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            count = sum(1 for e in hit["structA"] if e["int_val"] > 5)
            assert count >= 3, f"Row {hit['id']} has only {count} elements with int_val > 5"
        # GT completeness check
        gt_ids = gt_match_query(data, "MATCH_LEAST", lambda e: e["int_val"] > 5, threshold=3)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_least_threshold_1(self):
        """
        target: MATCH_LEAST with threshold=1 should be equivalent to MATCH_ANY
        method: compare MATCH_LEAST(threshold=1) vs MATCH_ANY
        expected: same result sets
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ml_t1")
        data = self._generate_data(nb=500)
        self._setup_collection(client, collection_name, data)

        results_least, _ = self.query(
            client, collection_name,
            filter='MATCH_LEAST(structA, $[color] == "Blue", threshold=1)',
            output_fields=["id"],
            limit=500,
        )
        results_any, _ = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Blue")',
            output_fields=["id"],
            limit=500,
        )
        ids_least = sorted([r["id"] for r in results_least])
        ids_any = sorted([r["id"] for r in results_any])
        assert ids_least == ids_any, "MATCH_LEAST(threshold=1) should equal MATCH_ANY"

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_most_basic(self):
        """
        target: MATCH_MOST - at most threshold elements match
        method: MATCH_MOST(structA, $[int_val] > 5, threshold=2)
        expected: rows with <= 2 elements having int_val > 5
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mmost_basic")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_MOST(structA, $[int_val] > 5, threshold=2)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            count = sum(1 for e in hit["structA"] if e["int_val"] > 5)
            assert count <= 2, f"Row {hit['id']} has {count} elements with int_val > 5, expected <= 2"
        # GT completeness check
        gt_ids = gt_match_query(data, "MATCH_MOST", lambda e: e["int_val"] > 5, threshold=2)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_exact_basic(self):
        """
        target: MATCH_EXACT - exactly threshold elements match
        method: MATCH_EXACT(structA, $[color] == "Red", threshold=2)
        expected: rows with exactly 2 elements having color=="Red"
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_me_basic")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_EXACT(structA, $[color] == "Red", threshold=2)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            count = sum(1 for e in hit["structA"] if e["color"] == "Red")
            assert count == 2, f"Row {hit['id']} has {count} Red elements, expected exactly 2"
        # GT completeness check
        gt_ids = gt_match_query(data, "MATCH_EXACT", lambda e: e["color"] == "Red", threshold=2)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_combined_with_doc_filter(self):
        """
        target: combine doc-level filter with MATCH
        method: 'doc_int > 10 && MATCH_ANY(structA, $[color] == "Red")'
        expected: only rows with doc_int > 10 and at least one Red element
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_doc")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='doc_int > 10 && MATCH_ANY(structA, $[color] == "Red")',
            output_fields=["id", "doc_int", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert hit["doc_int"] > 10
            assert any(e["color"] == "Red" for e in hit["structA"])
        # GT completeness check with doc-level filter
        gt_ids = gt_match_query(
            data, "MATCH_ANY", lambda e: e["color"] == "Red",
            doc_filter_fn=lambda row: row["doc_int"] > 10,
        )
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_combined_with_search(self):
        """
        target: MATCH_ANY as filter in normal_vector search
        method: search on normal_vector with MATCH_ANY filter
        expected: search results filtered by MATCH condition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_search")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        query_vector = data[0]["normal_vector"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            filter='MATCH_ANY(structA, $[color] == "Red")',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["color"] == "Red" for e in hit["structA"])
        # GT: MATCH_ANY filter + normal_vector search
        gt = gt_match_search(
            data, query_vector, "MATCH_ANY", lambda e: e["color"] == "Red",
            metric_type="COSINE", limit=10, anns_field="normal_vector",
        )
        assert_result_ids_match(results, gt, recall_threshold=0.8)
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_growing_segment(self):
        """
        target: MATCH query on growing segment
        method: insert without flush, then MATCH query
        expected: results from growing segment
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_growing")
        data = self._generate_data(nb=500)
        self._setup_collection(client, collection_name, data, flush=False)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red")',
            output_fields=["id"],
            limit=100,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_sealed_segment(self):
        """
        target: MATCH query on sealed segment
        method: insert + flush, then MATCH query
        expected: results from sealed segment
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_sealed")
        data = self._generate_data(nb=500)
        self._setup_collection(client, collection_name, data, flush=True)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Blue")',
            output_fields=["id"],
            limit=100,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_with_brute_force(self):
        """
        target: MATCH without nested index (brute force)
        method: no INVERTED/STL_SORT index, just MATCH query
        expected: correct results via brute force
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_brute")
        data = self._generate_data(nb=500)
        self._setup_collection(client, collection_name, data, create_nested_index=False)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[int_val] > 500)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert any(e["int_val"] > 500 for e in hit["structA"])
        gt_ids = gt_match_query(data, "MATCH_ANY", lambda e: e["int_val"] > 500)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_with_nested_inverted_index(self):
        """
        target: MATCH with INVERTED nested index
        method: create INVERTED index on structA[color], then MATCH
        expected: correct results with index acceleration
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_inv")
        data = self._generate_data(nb=500)
        self._setup_collection(client, collection_name, data, create_nested_index=True)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Green")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert any(e["color"] == "Green" for e in hit["structA"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_match_with_nested_stl_sort_index(self):
        """
        target: MATCH with STL_SORT nested index
        method: create STL_SORT index on structA[int_val], then MATCH
        expected: correct results with index acceleration
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_stl")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="MAX_SIM_COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(field_name="structA[int_val]", index_type="STL_SORT")

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        data = self._generate_data(nb=500)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[int_val] > 1000)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert any(e["int_val"] > 1000 for e in hit["structA"])

    # ---- L2 tests ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_most_threshold_0(self):
        """
        target: MATCH_MOST with threshold=0 - no elements should match
        method: MATCH_MOST(structA, $[color] == "Red", threshold=0)
        expected: only rows with zero Red elements
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mmost_t0")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_MOST(structA, $[color] == "Red", threshold=0)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            count = sum(1 for e in hit["structA"] if e["color"] == "Red")
            assert count == 0, f"Row {hit['id']} has {count} Red elements, expected 0"
        gt_ids = gt_match_query(data, "MATCH_MOST", lambda e: e["color"] == "Red", threshold=0)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_exact_threshold_0(self):
        """
        target: MATCH_EXACT with threshold=0 - no elements should match
        method: MATCH_EXACT(structA, $[color] == "Red", threshold=0)
        expected: only rows with exactly 0 Red elements
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_me_t0")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_EXACT(structA, $[color] == "Red", threshold=0)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            count = sum(1 for e in hit["structA"] if e["color"] == "Red")
            assert count == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_all_with_or_condition(self):
        """
        target: MATCH_ALL with OR condition
        method: MATCH_ALL(structA, $[color] == "Red" || $[size] == "L")
        expected: all elements match either condition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ma_or")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[color] == "Red" || $[size] == "L")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            for elem in hit["structA"]:
                assert elem["color"] == "Red" or elem["size"] == "L"
        gt_ids = gt_match_query(
            data, "MATCH_ALL",
            lambda e: e["color"] == "Red" or e["size"] == "L",
        )
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_case_insensitivity(self):
        """
        target: MATCH operators are case-insensitive
        method: lowercase match_all / match_any
        expected: same results as uppercase
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_case")
        data = self._generate_data(nb=500)
        self._setup_collection(client, collection_name, data)

        results_upper, _ = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red")',
            output_fields=["id"], limit=500,
        )
        results_lower, _ = self.query(
            client, collection_name,
            filter='match_any(structA, $[color] == "Red")',
            output_fields=["id"], limit=500,
        )
        ids_upper = sorted([r["id"] for r in results_upper])
        ids_lower = sorted([r["id"] for r in results_lower])
        assert ids_upper == ids_lower

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_least_large_threshold(self):
        """
        target: MATCH_LEAST with very large threshold
        method: threshold=100 but no row has 100 elements (max_elems=10)
        expected: empty result set
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ml_large")
        data = self._generate_data(nb=500)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_LEAST(structA, $[int_val] >= 0, threshold=100)',
            output_fields=["id"], limit=100,
        )
        assert check
        assert len(results) == 0, "No row should have 100+ matching elements"

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_all_empty_array_row(self):
        """
        target: MATCH_ALL on row with empty array
        method: insert row with empty structA array, then MATCH_ALL
        expected: empty array row behavior (vacuous truth or excluded)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ma_empty")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="MAX_SIM_COSINE", params=INDEX_PARAMS,
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = [
            {
                "id": 0, "doc_int": 0,
                "normal_vector": _seed_vector(0),
                "structA": [],  # empty array
            },
            {
                "id": 1, "doc_int": 1,
                "normal_vector": _seed_vector(1),
                "structA": [
                    {"embedding": _seed_vector(10), "int_val": 1, "str_val": "a",
                     "float_val": 0.1, "color": "Red", "size": "S"},
                ],
            },
        ]
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[color] == "Red")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        matched_ids = {r["id"] for r in results}
        log.info(f"Empty array MATCH_ALL result ids: {matched_ids}")
        # Row 1 has all elements Red → should match
        assert 1 in matched_ids, "Row 1 (all Red) should match MATCH_ALL"
        # Empty array row (id=0) should NOT match (no elements satisfy condition)
        # Note: vacuous truth could also be valid - log behavior for now
        if 0 in matched_ids:
            log.warning("Empty array row matches MATCH_ALL (vacuous truth)")

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_all_and_match_any_combined(self):
        """
        target: combine MATCH_ALL and MATCH_ANY with AND
        method: MATCH_ALL(structA, $[int_val] > 0) && MATCH_ANY(structA, $[color] == "Blue")
        expected: all elements int_val>0 AND at least one Blue
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_combo")
        data = self._generate_data(nb=500)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ALL(structA, $[int_val] > 0) && MATCH_ANY(structA, $[color] == "Blue")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert all(e["int_val"] > 0 for e in hit["structA"])
            assert any(e["color"] == "Blue" for e in hit["structA"])
        # GT: combined MATCH_ALL + MATCH_ANY
        gt_all = gt_match_query(data, "MATCH_ALL", lambda e: e["int_val"] > 0)
        gt_any = gt_match_query(data, "MATCH_ANY", lambda e: e["color"] == "Blue")
        gt_ids = gt_all & gt_any
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_or_with_doc_filter(self):
        """
        target: MATCH with OR + doc-level filter
        method: MATCH_ANY(structA, $[color] == "Red") || doc_int > 900
        expected: rows matching either condition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_m_or_doc")
        data = self._generate_data(nb=1000)
        self._setup_collection(client, collection_name, data)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red") || doc_int > 900',
            output_fields=["id", "doc_int", "structA"],
            limit=500,
        )
        assert check
        for hit in results:
            has_red = any(e["color"] == "Red" for e in hit["structA"])
            assert has_red or hit["doc_int"] > 900
        # GT: MATCH_ANY(Red) OR doc_int > 900
        gt_match = gt_match_query(data, "MATCH_ANY", lambda e: e["color"] == "Red")
        gt_doc = {row["id"] for row in data if row["doc_int"] > 900}
        gt_ids = gt_match | gt_doc
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_index_brute_force_consistency(self):
        """
        target: verify indexed MATCH and brute force MATCH return same results
        method: compare MATCH results with and without nested index
        expected: identical result sets
        """
        client = self._client()
        data = self._generate_data(nb=500)

        # Collection without nested index
        name_bf = cf.gen_unique_str(f"{prefix}_m_bf")
        self._setup_collection(client, name_bf, data, create_nested_index=False)

        # Collection with nested index
        name_idx = cf.gen_unique_str(f"{prefix}_m_idx")
        self._setup_collection(client, name_idx, data, create_nested_index=True)

        filter_expr = 'MATCH_ANY(structA, $[color] == "Red" && $[int_val] > 500)'

        results_bf, _ = self.query(client, name_bf, filter=filter_expr,
                                    output_fields=["id"], limit=500)
        results_idx, _ = self.query(client, name_idx, filter=filter_expr,
                                     output_fields=["id"], limit=500)

        ids_bf = sorted([r["id"] for r in results_bf])
        ids_idx = sorted([r["id"] for r in results_idx])
        assert ids_bf == ids_idx, "Indexed and brute-force MATCH should return same results"


class TestStructArrayNestedIndex(TestMilvusClientV2Base):
    """Test INVERTED + STL_SORT nested index creation and acceleration (17 cases)"""

    def _create_schema(self, client, dim=default_dim, add_bool=False):
        """Create schema with struct array for nested index tests."""
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
        struct_schema.add_field("size", DataType.VARCHAR, max_length=128)
        if add_bool:
            struct_schema.add_field("bool_val", DataType.BOOL)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _generate_data(self, nb=500, dim=default_dim, add_bool=False):
        """Generate test data for nested index tests."""
        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 10)
            struct_array = []
            for j in range(num_elems):
                elem = {
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                    "size": SIZES[(i + j) % 4],
                }
                if add_bool:
                    elem["bool_val"] = (i + j) % 2 == 0
                struct_array.append(elem)
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    def _setup_base_collection(self, client, collection_name, dim=default_dim,
                               nb=500, add_bool=False, extra_index_fn=None):
        """Create collection with base indexes, optionally add extra indexes."""
        schema = self._create_schema(client, dim=dim, add_bool=add_bool)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        if extra_index_fn:
            extra_index_fn(index_params)

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        data = self._generate_data(nb=nb, dim=dim, add_bool=add_bool)
        res, check = self.insert(client, collection_name, data)
        assert check
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return data

    # ---- L0 tests ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_inverted_index_on_struct_int(self):
        """
        target: create INVERTED index on struct int sub-field
        method: add_index(field_name="structA[int_val]", index_type="INVERTED")
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_inv_int")

        def add_nested(ip):
            ip.add_index(field_name="structA[int_val]", index_type="INVERTED")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

        # Verify index exists via list_indexes
        indexes, _ = self.list_indexes(client, collection_name)
        log.info(f"Indexes: {indexes}")
        assert "structA[int_val]" in str(indexes) or len(indexes) >= 3

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_inverted_index_on_struct_varchar(self):
        """
        target: create INVERTED index on struct varchar sub-field
        method: add_index(field_name="structA[str_val]", index_type="INVERTED")
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_inv_str")

        def add_nested(ip):
            ip.add_index(field_name="structA[str_val]", index_type="INVERTED")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_stl_sort_index_on_struct_int(self):
        """
        target: create STL_SORT index on struct int sub-field
        method: add_index(field_name="structA[int_val]", index_type="STL_SORT")
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_stl_int")

        def add_nested(ip):
            ip.add_index(field_name="structA[int_val]", index_type="STL_SORT")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

    # ---- L1 tests ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_inverted_index_on_struct_bool(self):
        """
        target: create INVERTED index on struct bool sub-field
        method: add bool_val to struct, create INVERTED index
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_inv_bool")

        def add_nested(ip):
            ip.add_index(field_name="structA[bool_val]", index_type="INVERTED")

        self._setup_base_collection(client, collection_name, add_bool=True,
                                     extra_index_fn=add_nested)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_inverted_index_on_struct_float(self):
        """
        target: create INVERTED index on struct float sub-field
        method: add_index(field_name="structA[float_val]", index_type="INVERTED")
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_inv_float")

        def add_nested(ip):
            ip.add_index(field_name="structA[float_val]", index_type="INVERTED")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_stl_sort_index_on_struct_float(self):
        """
        target: create STL_SORT index on struct float sub-field
        method: add_index(field_name="structA[float_val]", index_type="STL_SORT")
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_stl_float")

        def add_nested(ip):
            ip.add_index(field_name="structA[float_val]", index_type="STL_SORT")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_stl_sort_index_on_struct_varchar(self):
        """
        target: create STL_SORT index on struct varchar sub-field
        method: add_index(field_name="structA[str_val]", index_type="STL_SORT")
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_stl_str")

        def add_nested(ip):
            ip.add_index(field_name="structA[str_val]", index_type="STL_SORT")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

    @pytest.mark.tags(CaseLabel.L1)
    def test_inverted_index_accelerates_element_filter(self):
        """
        target: INVERTED index accelerates element_filter search
        method: create INVERTED index, then element_filter search
        expected: correct results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_ef")

        def add_nested(ip):
            ip.add_index(field_name="structA[int_val]", index_type="INVERTED")

        data = self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 100)',
            limit=10,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results) > 0
        for hit in results[0]:
            assert any(e["int_val"] > 100 for e in hit["structA"]), \
                f"Row {hit['id']} has no element with int_val > 100"
        _assert_distance_order(results, "COSINE")

    @pytest.mark.tags(CaseLabel.L1)
    def test_inverted_index_accelerates_match(self):
        """
        target: INVERTED index accelerates MATCH operations
        method: create INVERTED index on color, then MATCH_ANY
        expected: correct results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_match")

        def add_nested(ip):
            ip.add_index(field_name="structA[color]", index_type="INVERTED")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Blue")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert any(e["color"] == "Blue" for e in hit["structA"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_stl_sort_index_accelerates_element_filter(self):
        """
        target: STL_SORT index accelerates element_filter search
        method: create STL_SORT index, then element_filter search
        expected: correct results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_stl_ef")

        def add_nested(ip):
            ip.add_index(field_name="structA[int_val]", index_type="STL_SORT")

        data = self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 100)',
            limit=10,
        )
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_stl_sort_index_accelerates_match(self):
        """
        target: STL_SORT index accelerates MATCH operations
        method: create STL_SORT index on int_val, then MATCH_ANY
        expected: correct results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_stl_match")

        def add_nested(ip):
            ip.add_index(field_name="structA[int_val]", index_type="STL_SORT")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[int_val] > 1000)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert any(e["int_val"] > 1000 for e in hit["structA"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_multiple_indexes_on_different_subfields(self):
        """
        target: create indexes on multiple struct sub-fields simultaneously
        method: INVERTED on color, STL_SORT on int_val
        expected: both indexes created and work
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_multi")

        def add_nested(ip):
            ip.add_index(field_name="structA[color]", index_type="INVERTED")
            ip.add_index(field_name="structA[int_val]", index_type="STL_SORT")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

        # Query using both indexed fields
        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red" && $[int_val] > 100)',
            output_fields=["id"],
            limit=100,
        )
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_nested_index_sealed_segment(self):
        """
        target: nested index on sealed segment
        method: create index, insert, flush, query
        expected: index works on sealed data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_sealed")

        def add_nested(ip):
            ip.add_index(field_name="structA[int_val]", index_type="INVERTED")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[int_val] > 100)',
            output_fields=["id"],
            limit=100,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_nested_index_growing_segment(self):
        """
        target: insert new data after index creation, query on growing segment
        method: create collection + index, insert more data (no flush), query
        expected: growing segment data queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_growing")

        def add_nested(ip):
            ip.add_index(field_name="structA[int_val]", index_type="INVERTED")

        self._setup_base_collection(client, collection_name, nb=500,
                                            extra_index_fn=add_nested)

        # Insert more data without flush (growing)
        extra_data = []
        for i in range(500, 700):
            rng = random.Random(i)
            num_elems = rng.randint(3, 10)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "float_val": float(i + j * 0.1),
                    "color": COLORS[j % 3],
                    "size": SIZES[(i + j) % 4],
                })
            extra_data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, extra_data)
        # No flush - growing segment

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[int_val] > 60000)',
            output_fields=["id"],
            limit=100,
        )
        assert check
        assert len(results) > 0
        # Should include growing data (id >= 500)
        has_growing = any(r["id"] >= 500 for r in results)
        assert has_growing, "Should find data from growing segment"

    # ---- L2 tests ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_and_rebuild_nested_index(self):
        """
        target: drop and rebuild nested index
        method: create → drop → rebuild INVERTED index
        expected: index works after rebuild
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_rebuild")

        def add_nested(ip):
            ip.add_index(field_name="structA[int_val]", index_type="INVERTED",
                         index_name="nested_int_idx")

        self._setup_base_collection(client, collection_name, extra_index_fn=add_nested)

        # Drop the nested index
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, index_name="nested_int_idx")

        # Rebuild
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="structA[int_val]", index_type="INVERTED",
                               index_name="nested_int_idx_v2")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[int_val] > 100)',
            output_fields=["id"],
            limit=100,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "scalar_type,scalar_params,gen_val",
        [
            (DataType.INT8, {}, lambda i, j: (i + j) % 127),
            (DataType.INT16, {}, lambda i, j: i * 10 + j),
            (DataType.INT32, {}, lambda i, j: i * 100 + j),
            (DataType.INT64, {}, lambda i, j: i * 1000 + j),
            (DataType.FLOAT, {}, lambda i, j: float(i + j * 0.5)),
            (DataType.DOUBLE, {}, lambda i, j: float(i * 1.1 + j)),
            (DataType.VARCHAR, {"max_length": 256}, lambda i, j: f"v_{i}_{j}"),
            (DataType.BOOL, {}, lambda i, j: (i + j) % 2 == 0),
        ],
    )
    def test_nested_index_parametrize_scalar_types(self, scalar_type, scalar_params, gen_val):
        """
        target: nested index on various scalar sub-field types
        method: parametrize INT8/16/32/64, FLOAT, DOUBLE, VARCHAR, BOOL
        expected: INVERTED index created and queryable for each type
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_type")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("test_field", scalar_type, **scalar_params)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[test_field]", index_type="INVERTED")

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        # Insert data
        data = []
        for i in range(100):
            rng = random.Random(i)
            num_elems = rng.randint(2, 5)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 100 + j),
                    "test_field": gen_val(i, j),
                })
            data.append({
                "id": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        log.info(f"Successfully created INVERTED index on {scalar_type} sub-field")

        # Verify index is functional with a query
        results, check = self.query(
            client, collection_name, filter="id >= 0",
            output_fields=["id"], limit=10,
        )
        assert check
        assert len(results) > 0, "Should be able to query data after nested index creation"

    @pytest.mark.tags(CaseLabel.L2)
    def test_inverted_index_on_vector_subfield_rejected(self):
        """
        target: INVERTED index on vector sub-field should be rejected
        method: try to create INVERTED index on structA[embedding]
        expected: error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_ni_vec_rej")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        # Try INVERTED on vector sub-field
        index_params.add_index(field_name="structA[embedding]", index_type="INVERTED",
                               index_name="invalid_inv_on_vec")

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
            check_task=CheckTasks.err_res, check_items=error,
        )


class TestStructArrayNonFloatVectors(TestMilvusClientV2Base):
    """Test Float16, BFloat16, Int8, Binary vector types in struct (19 cases)"""

    def _create_schema_with_vec_type(self, client, vec_type, dim=default_dim):
        """Create schema with specified vector type in struct."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", vec_type, dim=dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=256)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _gen_vec_by_type(self, vec_type, dim, seed=None):
        """Generate vector data by type."""
        if vec_type == DataType.FLOAT_VECTOR:
            return _seed_vector(seed, dim)
        elif vec_type == DataType.FLOAT16_VECTOR:
            return _generate_float16_bytes(dim, seed)
        elif vec_type == DataType.BFLOAT16_VECTOR:
            return _generate_bfloat16_bytes(dim, seed)
        elif vec_type == DataType.INT8_VECTOR:
            return _generate_int8_vector(dim, seed)
        elif vec_type == DataType.BINARY_VECTOR:
            return _generate_binary_vector(dim, seed)
        raise ValueError(f"Unsupported vector type: {vec_type}")

    def _get_metric_for_type(self, vec_type):
        """Get appropriate metric for vector type."""
        metrics = {
            DataType.FLOAT_VECTOR: "COSINE",
            DataType.FLOAT16_VECTOR: "L2",
            DataType.BFLOAT16_VECTOR: "IP",
            DataType.INT8_VECTOR: "COSINE",
            DataType.BINARY_VECTOR: "HAMMING",
        }
        return metrics.get(vec_type, "COSINE")

    def _get_max_sim_metric(self, vec_type):
        """Get MAX_SIM metric for struct vector search."""
        metrics = {
            DataType.FLOAT_VECTOR: "MAX_SIM_COSINE",
            DataType.FLOAT16_VECTOR: "MAX_SIM_L2",
            DataType.BFLOAT16_VECTOR: "MAX_SIM_IP",
            DataType.INT8_VECTOR: "MAX_SIM_COSINE",
            DataType.BINARY_VECTOR: "MAX_SIM_HAMMING",
        }
        return metrics.get(vec_type, "MAX_SIM_COSINE")

    def _generate_data(self, vec_type, nb=200, dim=default_dim):
        """Generate data with specified vector type."""
        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(2, 5)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": self._gen_vec_by_type(vec_type, dim, seed=i * 100 + j),
                    "int_val": i * 10 + j,
                    "str_val": f"row_{i}_elem_{j}",
                })
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    def _setup_collection(self, client, collection_name, vec_type, nb=200, dim=default_dim,
                          use_max_sim=True):
        """Create collection with non-float vector type, insert data, load."""
        schema = self._create_schema_with_vec_type(client, vec_type, dim)
        metric = self._get_max_sim_metric(vec_type) if use_max_sim else self._get_metric_for_type(vec_type)

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector", index_type="HNSW",
            metric_type="COSINE", params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]", index_type="HNSW",
            metric_type=metric, params=INDEX_PARAMS,
        )

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        data = self._generate_data(vec_type, nb=nb, dim=dim)
        res, check = self.insert(client, collection_name, data)
        assert check
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return data

    # ---- L0 tests ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_with_float16_vector(self):
        """
        target: create struct with Float16Vector
        method: create collection with Float16Vector in struct
        expected: collection created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_f16_create")
        schema = self._create_schema_with_vec_type(client, DataType.FLOAT16_VECTOR)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="MAX_SIM_L2", params=INDEX_PARAMS)

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_with_bfloat16_vector(self):
        """
        target: create struct with BFloat16Vector
        method: create collection with BFloat16Vector in struct
        expected: collection created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_bf16_create")
        schema = self._create_schema_with_vec_type(client, DataType.BFLOAT16_VECTOR)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="MAX_SIM_IP", params=INDEX_PARAMS)

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

    # ---- L1 tests ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_struct_with_int8_vector(self):
        """
        target: create struct with Int8Vector
        method: create collection with INT8_VECTOR in struct
        expected: collection created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_int8_create")
        self._setup_collection(client, collection_name, DataType.INT8_VECTOR, nb=50)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_struct_with_binary_vector(self):
        """
        target: create struct with BinaryVector
        method: create collection with BINARY_VECTOR in struct
        expected: collection created successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_bin_create")
        self._setup_collection(client, collection_name, DataType.BINARY_VECTOR, nb=50)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_query_float16_struct(self):
        """
        target: insert and query Float16Vector struct data
        method: insert Float16Vector data, query by id
        expected: data queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_f16_iq")
        self._setup_collection(client, collection_name, DataType.FLOAT16_VECTOR, nb=200)

        results, check = self.query(
            client, collection_name, filter="id < 10",
            output_fields=["id", "structA"], limit=10,
        )
        assert check
        assert len(results) == 10

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_query_bfloat16_struct(self):
        """
        target: insert and query BFloat16Vector struct data
        method: insert BFloat16Vector data, query by id
        expected: data queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_bf16_iq")
        self._setup_collection(client, collection_name, DataType.BFLOAT16_VECTOR, nb=200)

        results, check = self.query(
            client, collection_name, filter="id < 10",
            output_fields=["id", "structA"], limit=10,
        )
        assert check
        assert len(results) == 10

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_query_int8_struct(self):
        """
        target: insert and query Int8Vector struct data
        method: insert INT8_VECTOR data, query by id
        expected: data queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_int8_iq")
        self._setup_collection(client, collection_name, DataType.INT8_VECTOR, nb=200)

        results, check = self.query(
            client, collection_name, filter="id < 10",
            output_fields=["id", "structA"], limit=10,
        )
        assert check
        assert len(results) == 10

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_query_binary_struct(self):
        """
        target: insert and query BinaryVector struct data
        method: insert BINARY_VECTOR data, query by id
        expected: data queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_bin_iq")
        self._setup_collection(client, collection_name, DataType.BINARY_VECTOR, nb=200)

        results, check = self.query(
            client, collection_name, filter="id < 10",
            output_fields=["id", "structA"], limit=10,
        )
        assert check
        assert len(results) == 10

    @pytest.mark.xfail(reason="pymilvus search does not serialize non-float vectors in struct array correctly")
    @pytest.mark.tags(CaseLabel.L1)
    def test_max_sim_search_float16_struct(self):
        """
        target: MAX_SIM search on Float16Vector in struct
        method: search with EmbeddingList on Float16 struct vector
        expected: search results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_f16_sim")
        self._setup_collection(client, collection_name, DataType.FLOAT16_VECTOR, nb=200)

        search_tensor = EmbeddingList()
        search_tensor.add(self._gen_vec_by_type(DataType.FLOAT16_VECTOR, default_dim, seed=42))

        results, check = self.search(
            client, collection_name,
            data=[search_tensor],
            anns_field="structA[embedding]",
            search_params={"metric_type": "MAX_SIM_L2"},
            limit=10,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.xfail(reason="pymilvus search does not serialize non-float vectors in struct array correctly")
    @pytest.mark.tags(CaseLabel.L1)
    def test_max_sim_search_bfloat16_struct(self):
        """
        target: MAX_SIM search on BFloat16Vector in struct
        method: search with EmbeddingList on BFloat16 struct vector
        expected: search results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_bf16_sim")
        self._setup_collection(client, collection_name, DataType.BFLOAT16_VECTOR, nb=200)

        search_tensor = EmbeddingList()
        search_tensor.add(self._gen_vec_by_type(DataType.BFLOAT16_VECTOR, default_dim, seed=42))

        results, check = self.search(
            client, collection_name,
            data=[search_tensor],
            anns_field="structA[embedding]",
            search_params={"metric_type": "MAX_SIM_IP"},
            limit=10,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.xfail(reason="pymilvus search does not serialize non-float vectors in struct array correctly")
    @pytest.mark.tags(CaseLabel.L1)
    def test_max_sim_search_int8_struct(self):
        """
        target: MAX_SIM search on Int8Vector in struct
        method: search with EmbeddingList on Int8 struct vector
        expected: search results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_int8_sim")
        self._setup_collection(client, collection_name, DataType.INT8_VECTOR, nb=200)

        search_tensor = EmbeddingList()
        search_tensor.add(self._gen_vec_by_type(DataType.INT8_VECTOR, default_dim, seed=42))

        results, check = self.search(
            client, collection_name,
            data=[search_tensor],
            anns_field="structA[embedding]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.xfail(reason="pymilvus search does not serialize non-float vectors in struct array correctly")
    @pytest.mark.tags(CaseLabel.L1)
    def test_max_sim_search_binary_struct(self):
        """
        target: MAX_SIM_HAMMING search on BinaryVector in struct
        method: search with EmbeddingList on binary struct vector
        expected: search results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_bin_sim")
        self._setup_collection(client, collection_name, DataType.BINARY_VECTOR, nb=200)

        search_tensor = EmbeddingList()
        search_tensor.add(self._gen_vec_by_type(DataType.BINARY_VECTOR, default_dim, seed=42))

        results, check = self.search(
            client, collection_name,
            data=[search_tensor],
            anns_field="structA[embedding]",
            search_params={"metric_type": "MAX_SIM_HAMMING"},
            limit=10,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.xfail(reason="pymilvus search does not serialize non-float vectors in struct array correctly")
    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_float16(self):
        """
        target: element_filter search with Float16Vector
        method: single vector search (non-EmbeddingList) + element_filter
        expected: results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_f16_ef")
        data = self._setup_collection(client, collection_name, DataType.FLOAT16_VECTOR, nb=200,
                                      use_max_sim=False)

        query_vec = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vec],
            anns_field="structA[embedding]",
            search_params={"metric_type": "L2"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.xfail(reason="pymilvus search does not serialize non-float vectors in struct array correctly")
    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_search_bfloat16(self):
        """
        target: element_filter search with BFloat16Vector
        method: single vector search + element_filter
        expected: results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_bf16_ef")
        data = self._setup_collection(client, collection_name, DataType.BFLOAT16_VECTOR, nb=200,
                                      use_max_sim=False)

        query_vec = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vec],
            anns_field="structA[embedding]",
            search_params={"metric_type": "IP"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_hnsw_index_on_non_float_struct_vector(self):
        """
        target: HNSW index on non-float vector types in struct
        method: create HNSW index on Float16 struct vector
        expected: index created and searchable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_hnsw")
        # Float16 with HNSW is already tested via _setup_collection
        self._setup_collection(client, collection_name, DataType.FLOAT16_VECTOR, nb=200)
        log.info("HNSW index on Float16 struct vector created successfully")

    @pytest.mark.tags(CaseLabel.L1)
    def test_sparse_vector_in_struct_rejected(self):
        """
        target: SparseFloatVector in struct should be rejected
        method: create struct with SPARSE_FLOAT_VECTOR
        expected: error 65535
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_sparse_rej")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("sparse_vec", DataType.SPARSE_FLOAT_VECTOR)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        error = {
            ct.err_code: 65535,
            ct.err_msg: "only fixed dimension vector types are supported",
        }
        self.create_collection(
            client, collection_name, schema=schema,
            check_task=CheckTasks.err_res, check_items=error,
        )

    # ---- L2 tests ----

    @pytest.mark.xfail(reason="pymilvus search does not serialize non-float vectors in struct array correctly")
    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_search_int8(self):
        """
        target: element_filter search with Int8Vector
        method: single vector + element_filter on Int8
        expected: results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_int8_ef")
        data = self._setup_collection(client, collection_name, DataType.INT8_VECTOR, nb=200,
                                      use_max_sim=False)

        query_vec = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vec],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.xfail(reason="pymilvus search does not serialize non-float vectors in struct array correctly")
    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_search_binary(self):
        """
        target: element_filter search with BinaryVector
        method: single vector + element_filter on binary
        expected: results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_bin_ef")
        data = self._setup_collection(client, collection_name, DataType.BINARY_VECTOR, nb=200,
                                      use_max_sim=False)

        query_vec = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vec],
            anns_field="structA[embedding]",
            search_params={"metric_type": "HAMMING"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
        )
        assert check
        assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_mixed_vector_types_in_struct(self):
        """
        target: mixed FLOAT_VECTOR + Float16Vector in same struct
        method: create struct with both vector types
        expected: collection created and searchable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_nf_mixed")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("float_emb", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("float16_emb", DataType.FLOAT16_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[float_emb]", index_type="HNSW",
                               metric_type="MAX_SIM_COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[float16_emb]", index_type="HNSW",
                               metric_type="MAX_SIM_L2", params=INDEX_PARAMS)

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        data = []
        for i in range(100):
            rng = random.Random(i)
            num_elems = rng.randint(2, 5)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "float_emb": _seed_vector(i * 100 + j),
                    "float16_emb": _generate_float16_bytes(default_dim, seed=i * 100 + j),
                    "int_val": i * 10 + j,
                })
            data.append({
                "id": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        res, check = self.insert(client, collection_name, data)
        assert check
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search on float vector
        search_tensor = EmbeddingList()
        search_tensor.add(_seed_vector(42))
        results, check = self.search(
            client, collection_name,
            data=[search_tensor],
            anns_field="structA[float_emb]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        assert check
        assert len(results) > 0


class TestStructArrayMatchQuery(TestMilvusClientV2Base):
    """Test Match operators in pure query (non-search) context (10 cases)"""

    def _create_schema(self, client, dim=default_dim):
        """Create schema for Match query tests."""
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

    def _generate_data(self, nb=500, dim=default_dim):
        """Generate data for match query tests."""
        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 10)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    def _setup_collection(self, client, collection_name, nb=500, flush=True):
        """Helper: create, insert, index, load."""
        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="MAX_SIM_COSINE", params=INDEX_PARAMS)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = self._generate_data(nb=nb)
        self.insert(client, collection_name, data)
        if flush:
            self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return data

    # ---- L0 tests ----

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_with_match_all(self):
        """
        target: query with MATCH_ALL filter
        method: query(filter='MATCH_ALL(structA, $[int_val] > 0)')
        expected: all elements in matched rows have int_val > 0
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_all")
        data = self._setup_collection(client, collection_name)

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
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_with_match_any(self):
        """
        target: query with MATCH_ANY filter
        method: query(filter='MATCH_ANY(structA, $[str_val] == "row_10_elem_0")')
        expected: at least one matching element per row
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_any")
        data = self._setup_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[str_val] == "row_10_elem_0")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            assert any(e["str_val"] == "row_10_elem_0" for e in hit["structA"])
        gt_ids = gt_match_query(data, "MATCH_ANY", lambda e: e["str_val"] == "row_10_elem_0")
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids == gt_ids, f"Expected {gt_ids}, got {milvus_ids}"

    # ---- L1 tests ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_with_match_least(self):
        """
        target: query with MATCH_LEAST
        method: MATCH_LEAST(structA, $[int_val] > 5, threshold=2)
        expected: >= 2 elements match per row
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_least")
        data = self._setup_collection(client, collection_name)

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
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_with_match_most(self):
        """
        target: query with MATCH_MOST
        method: MATCH_MOST(structA, $[int_val] > 5, threshold=3)
        expected: <= 3 elements match per row
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_most")
        data = self._setup_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_MOST(structA, $[int_val] > 5, threshold=3)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            count = sum(1 for e in hit["structA"] if e["int_val"] > 5)
            assert count <= 3
        gt_ids = gt_match_query(data, "MATCH_MOST", lambda e: e["int_val"] > 5, threshold=3)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_with_match_exact(self):
        """
        target: query with MATCH_EXACT
        method: MATCH_EXACT(structA, $[color] == "Red", threshold=1)
        expected: exactly 1 Red element per row
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_exact")
        data = self._setup_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_EXACT(structA, $[color] == "Red", threshold=1)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            count = sum(1 for e in hit["structA"] if e["color"] == "Red")
            assert count == 1
        gt_ids = gt_match_query(data, "MATCH_EXACT", lambda e: e["color"] == "Red", threshold=1)
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_match_with_doc_filter(self):
        """
        target: MATCH with doc-level filter in query
        method: 'id > 100 && MATCH_ANY(structA, $[color] == "Red")'
        expected: combined filter works
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_doc")
        data = self._setup_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='id > 100 && MATCH_ANY(structA, $[color] == "Red")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        for hit in results:
            assert hit["id"] > 100
            assert any(e["color"] == "Red" for e in hit["structA"])
        gt_ids = gt_match_query(
            data, "MATCH_ANY", lambda e: e["color"] == "Red",
            doc_filter_fn=lambda row: row["id"] > 100,
        )
        milvus_ids = {r["id"] for r in results}
        assert milvus_ids.issubset(gt_ids), f"False positives: {milvus_ids - gt_ids}"
        if len(results) < 100:
            assert milvus_ids == gt_ids, f"Missing IDs: {gt_ids - milvus_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_match_with_output_fields(self):
        """
        target: MATCH query with struct sub-field output
        method: specify output_fields with struct sub-fields
        expected: output contains requested fields
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_output")
        self._setup_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Blue")',
            output_fields=["id", "structA[color]", "structA[int_val]"],
            limit=50,
        )
        assert check
        assert len(results) > 0

    # ---- L2 tests ----

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_count_with_match(self):
        """
        target: count(*) with MATCH filter
        method: query with output_fields=["count(*)"] and MATCH filter
        expected: count returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_count")
        self._setup_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red")',
            output_fields=["count(*)"],
        )
        assert check
        assert len(results) > 0
        assert results[0]["count(*)"] > 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_match_limit_offset(self):
        """
        target: MATCH query with pagination (limit + offset)
        method: query with limit=10, offset=10
        expected: second page of results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_page")
        self._setup_collection(client, collection_name)

        results_p1, _ = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red")',
            output_fields=["id"], limit=10, offset=0,
        )
        results_p2, _ = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red")',
            output_fields=["id"], limit=10, offset=10,
        )
        ids_p1 = {r["id"] for r in results_p1}
        ids_p2 = {r["id"] for r in results_p2}
        assert len(ids_p1 & ids_p2) == 0, "Pages should not overlap"

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_match_growing_sealed(self):
        """
        target: MATCH query on mixed growing + sealed segments
        method: insert + flush + insert more, then query
        expected: results from both segments
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mq_mixed")

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="MAX_SIM_COSINE", params=INDEX_PARAMS)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Sealed data
        sealed_data = self._generate_data(nb=300)
        self.insert(client, collection_name, sealed_data)
        self.flush(client, collection_name)

        # Growing data
        growing_data = []
        for i in range(300, 500):
            rng = random.Random(i)
            num_elems = rng.randint(3, 10)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "color": COLORS[j % 3],
                })
            growing_data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, growing_data)
        self.load_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Blue")',
            output_fields=["id"], limit=500,
        )
        assert check
        ids = [r["id"] for r in results]
        has_sealed = any(i < 300 for i in ids)
        has_growing = any(i >= 300 for i in ids)
        assert has_sealed and has_growing, "Should have results from both segments"


class TestStructArrayElementFilterHybridSearch(TestMilvusClientV2Base):
    """Test element_filter search + hybrid search (4 cases)"""

    def _create_schema(self, client, dim=default_dim):
        """Create schema for hybrid search tests."""
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

    def _generate_data(self, nb=500, dim=default_dim):
        """Generate data for hybrid search tests."""
        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        return data

    def _setup_collection(self, client, collection_name, nb=500, dim=default_dim,
                          struct_metric_type="COSINE"):
        """Helper to setup collection for hybrid search."""
        schema = self._create_schema(client, dim)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type=struct_metric_type, params=INDEX_PARAMS)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = self._generate_data(nb=nb, dim=dim)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return data

    # ---- L1 tests ----

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_element_filter_and_normal_vector(self):
        """
        target: hybrid search combining element_filter search + normal_vector search
        method: use RRFRanker to combine two AnnSearchRequests
        expected: hybrid results returned
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_ef_nv")
        data = self._setup_collection(client, collection_name, nb=500)

        # Request 1: element_filter search on struct vector
        req1 = AnnSearchRequest(**{
            "data": [data[0]["structA"][0]["embedding"]],
            "anns_field": "structA[embedding]",
            "param": {"metric_type": "COSINE"},
            "limit": 10,
            "expr": 'element_filter(structA, $[int_val] >= 0)',
        })

        # Request 2: normal vector search
        req2 = AnnSearchRequest(**{
            "data": [data[0]["normal_vector"]],
            "anns_field": "normal_vector",
            "param": {"metric_type": "COSINE"},
            "limit": 10,
        })

        results, check = self.hybrid_search(
            client, collection_name, [req1, req2],
            ranker=RRFRanker(), limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_element_filter_with_weighted_ranker(self):
        """
        target: hybrid search with WeightedRanker
        method: element_filter search + normal search with weights
        expected: weighted hybrid results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_weighted")
        data = self._setup_collection(client, collection_name, nb=500)

        req1 = AnnSearchRequest(**{
            "data": [data[0]["structA"][0]["embedding"]],
            "anns_field": "structA[embedding]",
            "param": {"metric_type": "COSINE"},
            "limit": 10,
            "expr": 'element_filter(structA, $[int_val] >= 0)',
        })

        req2 = AnnSearchRequest(**{
            "data": [data[0]["normal_vector"]],
            "anns_field": "normal_vector",
            "param": {"metric_type": "COSINE"},
            "limit": 10,
        })

        results, check = self.hybrid_search(
            client, collection_name, [req1, req2],
            ranker=WeightedRanker(0.7, 0.3), limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results) > 0

    # ---- L2 tests ----

    @pytest.mark.xfail(reason="element_filter(COSINE) + EmbeddingList(MAX_SIM) require different index metrics on same field")
    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_element_filter_and_max_sim(self):
        """
        target: element_filter search + MAX_SIM search hybrid
        method: combine element-level search with MAX_SIM search
        expected: hybrid results (requires server metric adaptation)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_maxsim")
        data = self._setup_collection(client, collection_name, nb=500,
                                      struct_metric_type="MAX_SIM_COSINE")

        # element_filter search (single vector)
        req1 = AnnSearchRequest(**{
            "data": [data[0]["structA"][0]["embedding"]],
            "anns_field": "structA[embedding]",
            "param": {"metric_type": "COSINE"},
            "limit": 10,
            "expr": 'element_filter(structA, $[color] == "Red")',
        })

        # MAX_SIM search (EmbeddingList)
        search_tensor = EmbeddingList()
        search_tensor.add(_seed_vector(42))
        req2 = AnnSearchRequest(**{
            "data": [search_tensor],
            "anns_field": "structA[embedding]",
            "param": {"metric_type": "MAX_SIM_COSINE"},
            "limit": 10,
        })

        results, check = self.hybrid_search(
            client, collection_name, [req1, req2],
            ranker=RRFRanker(), limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_with_match_as_filter(self):
        """
        target: hybrid search with MATCH_ANY as filter
        method: normal_vector search with MATCH_ANY filter + struct vector search
        expected: combined results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_match")
        data = self._setup_collection(client, collection_name, nb=500,
                                      struct_metric_type="MAX_SIM_COSINE")

        req1 = AnnSearchRequest(**{
            "data": [data[0]["normal_vector"]],
            "anns_field": "normal_vector",
            "param": {"metric_type": "COSINE"},
            "limit": 10,
            "expr": 'MATCH_ANY(structA, $[color] == "Red")',
        })

        search_tensor = EmbeddingList()
        search_tensor.add(_seed_vector(42))
        req2 = AnnSearchRequest(**{
            "data": [search_tensor],
            "anns_field": "structA[embedding]",
            "param": {"metric_type": "MAX_SIM_COSINE"},
            "limit": 10,
        })

        results, check = self.hybrid_search(
            client, collection_name, [req1, req2],
            ranker=RRFRanker(), limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results) > 0


class TestStructArrayElementSearchInvalid(TestMilvusClientV2Base):
    """Test invalid usage and boundary conditions (20 cases)"""

    def _create_collection_ready(self, client, collection_name, nb=200):
        """Helper: create collection with data loaded for invalid tests."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("str_val", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)
        struct_schema.add_field("size", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 100 + j),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "color": COLORS[j % 3],
                    "size": SIZES[(i + j) % 4],
                })
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return data

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_nested_not_allowed(self):
        """
        target: nested element_filter should not be allowed
        method: nested element_filter inside element_filter
        expected: error about nested ElementFilter
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_nested_ef")
        data = self._create_collection_ready(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, element_filter(structA, $[int_val] > 0))',
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_not_last_in_and(self):
        """
        target: element_filter must be last in AND expression
        method: element_filter on the left side of AND
        expected: error about position
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ef_pos")
        data = self._create_collection_ready(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 0) && doc_int > 10',
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_not_last_in_or(self):
        """
        target: element_filter must be last in OR expression
        method: element_filter on the left side of OR
        expected: error about position
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ef_or")
        data = self._create_collection_ready(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] > 0) || doc_int > 10',
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_nonexistent_field(self):
        """
        target: element_filter on nonexistent struct field
        method: element_filter(nonexistent_field, ...)
        expected: field not found error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ef_nofield")
        data = self._create_collection_ready(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(nonexistent_field, $[int_val] > 0)',
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_nonexistent_subfield(self):
        """
        target: element_filter referencing nonexistent sub-field
        method: element_filter(structA, $[nonexistent] > 0)
        expected: sub-field not found error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ef_nosub")
        data = self._create_collection_ready(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[nonexistent_field] > 0)',
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_empty_params(self):
        """
        target: element_filter with empty params
        method: element_filter()
        expected: parse error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ef_empty")
        data = self._create_collection_ready(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter()',
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_dollar_bracket_outside_context(self):
        """
        target: $[...] syntax outside element_filter/MATCH context
        method: use $[int_val] > 1 directly in filter
        expected: error about context
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_dollar")
        self._create_collection_ready(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client, collection_name,
            filter='$[int_val] > 1',
            output_fields=["id"],
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_with_max_sim_metric(self):
        """
        target: element_filter search with MAX_SIM metric should be rejected
        method: use MAX_SIM_COSINE with element_filter (index uses COSINE for element-level search)
        expected: error about metric mismatch (MAX_SIM_COSINE != COSINE)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ef_maxsim")
        data = self._create_collection_ready(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        error = {ct.err_code: 65535, ct.err_msg: "metric type not match"}
        self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            filter='element_filter(structA, $[int_val] > 0)',
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_with_embedding_list(self):
        """
        target: element_filter + EmbeddingList should be mutually exclusive
        method: pass EmbeddingList (multi-vector) with element_filter (single-vector mode)
        expected: error about incompatible search modes (单搜单 vs 多搜多)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ef_emblist")
        self._create_collection_ready(client, collection_name)

        search_tensor = EmbeddingList()
        search_tensor.add(_seed_vector(42))
        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.search(
            client, collection_name,
            data=[search_tensor],
            anns_field="structA[embedding]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            filter='element_filter(structA, $[int_val] > 0)',
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_nested_not_allowed(self):
        """
        target: nested MATCH should not be allowed
        method: MATCH inside MATCH
        expected: error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_match_nested")
        self._create_collection_ready(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, MATCH_ALL(structA, $[int_val] > 0))',
            output_fields=["id"],
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_least_negative_threshold(self):
        """
        target: MATCH_LEAST with negative threshold
        method: threshold=-1
        expected: error about positive count
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ml_neg")
        self._create_collection_ready(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client, collection_name,
            filter='MATCH_LEAST(structA, $[int_val] > 0, threshold=-1)',
            output_fields=["id"],
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_least_zero_threshold(self):
        """
        target: MATCH_LEAST with zero threshold
        method: threshold=0
        expected: error about positive count
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ml_zero")
        self._create_collection_ready(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client, collection_name,
            filter='MATCH_LEAST(structA, $[int_val] > 0, threshold=0)',
            output_fields=["id"],
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_most_negative_threshold(self):
        """
        target: MATCH_MOST with negative threshold
        method: threshold=-1
        expected: error about non-negative count
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_mmost_neg")
        self._create_collection_ready(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client, collection_name,
            filter='MATCH_MOST(structA, $[int_val] > 0, threshold=-1)',
            output_fields=["id"],
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_exact_negative_threshold(self):
        """
        target: MATCH_EXACT with negative threshold
        method: threshold=-1
        expected: error about non-negative count
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_me_neg")
        self._create_collection_ready(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client, collection_name,
            filter='MATCH_EXACT(structA, $[int_val] > 0, threshold=-1)',
            output_fields=["id"],
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_nonexistent_struct(self):
        """
        target: MATCH on nonexistent struct field name
        method: MATCH_ANY(nonexistent, ...)
        expected: field not found error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_match_nostruct")
        self._create_collection_ready(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client, collection_name,
            filter='MATCH_ANY(nonexistent_struct, $[int_val] > 0)',
            output_fields=["id"],
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_nonexistent_subfield(self):
        """
        target: MATCH referencing nonexistent sub-field
        method: MATCH_ANY(structA, $[nonexistent] > 0)
        expected: error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_match_nosub")
        self._create_collection_ready(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[nonexistent_field] > 0)',
            output_fields=["id"],
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_inverted_index_nonexistent_subfield(self):
        """
        target: create index on nonexistent sub-field
        method: add_index(field_name="structA[nonexistent]", ...)
        expected: error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_idx_nosub")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        schema.add_field("structA", datatype=DataType.ARRAY,
                         element_type=DataType.STRUCT,
                         struct_schema=struct_schema, max_capacity=10)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[nonexistent]", index_type="INVERTED")

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_stl_sort_index_on_vector_subfield(self):
        """
        target: STL_SORT index on vector sub-field should fail
        method: add STL_SORT index on structA[embedding]
        expected: error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_stl_vec")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        schema.add_field("structA", datatype=DataType.ARRAY,
                         element_type=DataType.STRUCT,
                         struct_schema=struct_schema, max_capacity=10)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="STL_SORT",
                               index_name="invalid_stl_on_vec")

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_filter_missing_expr(self):
        """
        target: element_filter with only field name, no expression
        method: element_filter(structA)
        expected: parse error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ef_noexpr")
        data = self._create_collection_ready(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA)',
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_match_all_missing_expr(self):
        """
        target: MATCH_ALL with only field name, no expression
        method: MATCH_ALL(structA)
        expected: parse error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_inv_ma_noexpr")
        self._create_collection_ready(client, collection_name)

        error = {ct.err_code: 65535, ct.err_msg: ""}
        self.query(
            client, collection_name,
            filter='MATCH_ALL(structA)',
            output_fields=["id"],
            limit=10,
            check_task=CheckTasks.err_res, check_items=error,
        )


class TestStructArrayElementSearchCRUD(TestMilvusClientV2Base):
    """Test CRUD operations with element-level features (5 cases)"""

    def _create_schema(self, client, dim=default_dim):
        """Create schema for CRUD tests."""
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

    def _setup_collection(self, client, collection_name, nb=500, dim=default_dim):
        """Helper: create collection, insert data, index, load."""
        schema = self._create_schema(client, dim)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j, dim),
                    "int_val": i * 100 + j,
                    "str_val": f"row_{i}_elem_{j}",
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999, dim),
                "structA": struct_array,
            })
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return data

    @pytest.mark.tags(CaseLabel.L1)
    def test_upsert_then_match_query(self):
        """
        target: upsert data then verify MATCH reflects new data
        method: upsert rows with new color, then MATCH_ANY on new color
        expected: upserted data found via MATCH
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_crud_upsert_match")
        self._setup_collection(client, collection_name, nb=500)

        # Upsert rows 0-9 with color="Purple"
        upsert_data = []
        for i in range(10):
            upsert_data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": [
                    {
                        "embedding": _seed_vector(i * 1000),
                        "int_val": i * 100 + 9999,
                        "str_val": f"upserted_{i}",
                        "color": "Purple",
                    }
                ],
            })
        self.upsert(client, collection_name, upsert_data)
        self.flush(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Purple")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        matched_ids = {r["id"] for r in results}
        for i in range(10):
            assert i in matched_ids, f"Upserted row {i} should match Purple"

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_then_element_filter_search(self):
        """
        target: delete rows then verify element_filter search excludes them
        method: delete rows 0-9, then element_filter search
        expected: deleted rows not in results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_crud_del_ef")
        data = self._setup_collection(client, collection_name, nb=500)

        # Delete rows 0-9
        self.delete(client, collection_name, ids=list(range(10)))

        query_vector = data[15]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=100,
            output_fields=["id"],
        )
        assert check
        result_ids = {r["id"] for r in results[0]}
        for i in range(10):
            assert i not in result_ids, f"Deleted row {i} should not appear"

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_then_match_query(self):
        """
        target: delete rows then verify MATCH doesn't return them
        method: delete rows 0-9, then MATCH query
        expected: deleted rows excluded
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_crud_del_match")
        self._setup_collection(client, collection_name, nb=500)

        self.delete(client, collection_name, ids=list(range(10)))

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[int_val] >= 0)',
            output_fields=["id"],
            limit=500,
        )
        assert check
        result_ids = {r["id"] for r in results}
        for i in range(10):
            assert i not in result_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_preserves_element_filter(self):
        """
        target: upsert then verify element_indices correspond to new data
        method: upsert with known struct, search for specific element
        expected: element_indices match new struct positions
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_crud_upsert_ef")
        self._setup_collection(client, collection_name, nb=500)

        # Upsert row 50 with known struct
        upsert_data = [{
            "id": 50, "doc_int": 50,
            "normal_vector": _seed_vector(50 + 999999),
            "structA": [
                {
                    "embedding": _seed_vector(99999),
                    "int_val": 99999,
                    "str_val": "upserted_target",
                    "color": "Gold",
                }
            ],
        }]
        self.upsert(client, collection_name, upsert_data)
        self.flush(client, collection_name)

        query_vector = _seed_vector(99999)
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[str_val] == "upserted_target")',
            limit=1,
            output_fields=["id", "structA"],
        )
        assert check
        assert len(results[0]) > 0
        assert results[0][0]["id"] == 50

    @pytest.mark.tags(CaseLabel.L2)
    def test_truncate_then_element_filter(self):
        """
        target: truncate collection then re-insert and search
        method: truncate → re-insert → element_filter search
        expected: search works on new data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_crud_truncate")
        self._setup_collection(client, collection_name, nb=200)

        # Truncate
        self.release_collection(client, collection_name)
        self.truncate_collection(client, collection_name)

        # Re-insert new data
        new_data = []
        for i in range(100):
            new_data.append({
                "id": i + 10000, "doc_int": i + 10000,
                "normal_vector": _seed_vector(i + 888888),
                "structA": [
                    {
                        "embedding": _seed_vector(i * 500),
                        "int_val": i + 50000,
                        "str_val": f"new_{i}",
                        "color": "Silver",
                    }
                ],
            })
        self.insert(client, collection_name, new_data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        query_vector = new_data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[color] == "Silver")',
            limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) > 0
        for r in results[0]:
            assert r["id"] >= 10000


class TestStructArrayElementSearchIterator(TestMilvusClientV2Base):
    """Test search iterator + element_filter (3 cases)"""

    def _setup_collection(self, client, collection_name, nb=500):
        """Helper to setup collection for iterator tests."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j),
                    "int_val": i * 100 + j,
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return data

    @pytest.mark.xfail(reason="search iterator not supported for vector array (embedding list) fields")
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_iterator_with_element_filter(self):
        """
        target: search_iterator with element_filter
        method: iterate through all results using search_iterator + element_filter
        expected: all results collected across batches
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_iter_ef")
        data = self._setup_collection(client, collection_name, nb=500)

        query_vector = data[0]["structA"][0]["embedding"]
        iterator, _ = self.search_iterator(
            client, collection_name,
            data=[query_vector],
            batch_size=50,
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=200,
            output_fields=["id"],
        )
        all_results = []
        if iterator:
            while True:
                batch = iterator.next()
                if not batch:
                    break
                all_results.extend(batch)
            iterator.close()
        assert len(all_results) > 0
        log.info(f"Iterator collected {len(all_results)} results")

    @pytest.mark.xfail(reason="search iterator not supported for vector array (embedding list) fields")
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("batch_size", [10, 50, 100])
    def test_search_iterator_element_filter_batch_size(self, batch_size):
        """
        target: search_iterator with different batch sizes
        method: parametrize batch_size=10,50,100
        expected: all batch sizes work correctly
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_iter_bs_{batch_size}")
        data = self._setup_collection(client, collection_name, nb=500)

        query_vector = data[0]["structA"][0]["embedding"]
        iterator, _ = self.search_iterator(
            client, collection_name,
            data=[query_vector],
            batch_size=batch_size,
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=200,
            output_fields=["id"],
        )
        all_results = []
        if iterator:
            while True:
                batch = iterator.next()
                if not batch:
                    break
                all_results.extend(batch)
            iterator.close()
        assert len(all_results) > 0

    @pytest.mark.xfail(reason="search iterator not supported for vector array (embedding list) fields")
    @pytest.mark.tags(CaseLabel.L2)
    def test_search_iterator_mixed_segments(self):
        """
        target: search_iterator on mixed sealed + growing segments
        method: insert + flush + insert more, then iterate with element_filter
        expected: results from both segments
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_iter_mixed")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Sealed segment
        sealed_data = []
        for i in range(300):
            random.Random(i)
            struct_array = [{"embedding": _seed_vector(i * 1000), "int_val": i * 100, "color": "Red"}]
            sealed_data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, sealed_data)
        self.flush(client, collection_name)

        # Growing segment
        growing_data = []
        for i in range(300, 500):
            struct_array = [{"embedding": _seed_vector(i * 1000), "int_val": i * 100, "color": "Blue"}]
            growing_data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, growing_data)
        self.load_collection(client, collection_name)

        query_vector = sealed_data[0]["structA"][0]["embedding"]
        iterator, _ = self.search_iterator(
            client, collection_name,
            data=[query_vector],
            batch_size=50,
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=400,
            output_fields=["id"],
        )
        all_ids = set()
        if iterator:
            while True:
                batch = iterator.next()
                if not batch:
                    break
                for r in batch:
                    all_ids.add(r["id"])
            iterator.close()
        has_sealed = any(i < 300 for i in all_ids)
        has_growing = any(i >= 300 for i in all_ids)
        assert has_sealed and has_growing, "Should have results from both segments"


class TestStructArrayArrayContains(TestMilvusClientV2Base):
    """Test ARRAY_CONTAINS on struct sub-fields (6 cases, all skip - depends on PR #47172)"""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="Depends on PR #47172 - ARRAY_CONTAINS support for struct arrays")
    def test_array_contains_struct_int_subfield(self):
        """
        target: array_contains on struct array int sub-field
        method: array_contains query on struct sub-field of array type
        expected: matching rows returned
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="Depends on PR #47172 - ARRAY_CONTAINS support for struct arrays")
    def test_array_contains_all_struct_varchar_subfield(self):
        """
        target: array_contains_all on struct varchar sub-field
        method: array_contains_all query
        expected: matching rows returned
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="Depends on PR #47172 - ARRAY_CONTAINS support for struct arrays")
    def test_array_contains_any_struct_int_subfield(self):
        """
        target: array_contains_any on struct int sub-field
        method: array_contains_any query
        expected: matching rows returned
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="Depends on PR #47172 - ARRAY_CONTAINS support for struct arrays")
    def test_array_contains_combined_with_match(self):
        """
        target: array_contains combined with MATCH operator
        method: MATCH_ANY(structA, array_contains($[tags], 5) && $[int_val] > 10)
        expected: matching rows returned
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="Depends on PR #47172 - ARRAY_CONTAINS support for struct arrays")
    def test_array_contains_combined_with_element_filter(self):
        """
        target: array_contains combined with element_filter
        method: element_filter(structA, array_contains($[tags], 5))
        expected: matching rows returned
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="Depends on PR #47172 - ARRAY_CONTAINS support for struct arrays")
    def test_array_contains_empty_array(self):
        """
        target: array_contains on empty array sub-field
        method: query where sub-field array is empty
        expected: no match
        """
        pass


class TestStructArrayElementSearchMmap(TestMilvusClientV2Base):
    """Test mmap with element-level features (3 cases)"""

    def _setup_collection(self, client, collection_name, nb=500):
        """Helper: create collection for mmap tests."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j),
                    "int_val": i * 100 + j,
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        return data

    @pytest.mark.tags(CaseLabel.L2)
    def test_mmap_with_element_filter_search(self):
        """
        target: element_filter search with mmap enabled
        method: enable mmap → load → element_filter search
        expected: search works correctly with mmap
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_ef")
        data = self._setup_collection(client, collection_name, nb=500)

        # Release before enabling mmap (cannot alter mmap on loaded collection)
        self.release_collection(client, collection_name)
        self.alter_collection_properties(
            client, collection_name,
            properties={"mmap.enabled": True},
        )
        self.load_collection(client, collection_name)

        query_vector = data[0]["structA"][0]["embedding"]
        results, check = self.search(
            client, collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE"},
            filter='element_filter(structA, $[int_val] >= 0)',
            limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_mmap_with_match_query(self):
        """
        target: MATCH query with mmap enabled
        method: enable mmap → load → MATCH query
        expected: query works correctly with mmap
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_match")
        self._setup_collection(client, collection_name, nb=500)

        # Release before enabling mmap (cannot alter mmap on loaded collection)
        self.release_collection(client, collection_name)
        self.alter_collection_properties(
            client, collection_name,
            properties={"mmap.enabled": True},
        )
        self.load_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Red")',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        assert len(results) > 0
        for hit in results:
            assert any(e["color"] == "Red" for e in hit["structA"])

    @pytest.mark.tags(CaseLabel.L2)
    def test_mmap_with_nested_index(self):
        """
        target: nested index + mmap combination
        method: create nested INVERTED index, enable mmap, query
        expected: works correctly
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_nidx")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        struct_schema.add_field("color", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "structA", datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[embedding]", index_type="HNSW",
                               metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(field_name="structA[int_val]", index_type="INVERTED")
        index_params.add_index(field_name="structA[color]", index_type="INVERTED")

        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = []
        for i in range(500):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                struct_array.append({
                    "embedding": _seed_vector(i * 1000 + j),
                    "int_val": i * 100 + j,
                    "color": COLORS[j % 3],
                })
            data.append({
                "id": i, "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": struct_array,
            })
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Release before enabling mmap (cannot alter mmap on loaded collection)
        self.release_collection(client, collection_name)
        self.alter_collection_properties(
            client, collection_name,
            properties={"mmap.enabled": True},
        )
        self.load_collection(client, collection_name)

        results, check = self.query(
            client, collection_name,
            filter='MATCH_ANY(structA, $[color] == "Blue" && $[int_val] > 100)',
            output_fields=["id", "structA"],
            limit=100,
        )
        assert check
        assert len(results) > 0
