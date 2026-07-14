import random

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import AnnSearchRequest, DataType, RRFRanker, WeightedRanker
from pymilvus.client.embedding_list import EmbeddingList

prefix = "struct_elem"
default_nb = ct.default_nb
default_dim = ct.default_dim
default_capacity = 100
INDEX_PARAMS = {"M": 16, "efConstruction": 200}
HNSW_SEARCH_PARAMS = {"ef": 1000}
COLORS = ["Red", "Blue", "Green"]


def _seed_vector(seed, dim=default_dim):
    rng = np.random.RandomState(seed)
    vec = rng.rand(dim).astype(np.float32)
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec.tolist()


def _cosine_sim(v1, v2):
    a = np.array(v1, dtype=np.float32)
    b = np.array(v2, dtype=np.float32)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10))


def gt_element_search_no_filter(data, query_vector, limit=10):
    element_scores = []
    for row in data:
        for offset, elem in enumerate(row["structA"]):
            element_scores.append((row["id"], _cosine_sim(query_vector, elem["embedding"]), offset))

    element_scores.sort(key=lambda x: x[1], reverse=True)
    return element_scores[:limit]


def _assert_distance_order(results):
    if len(results[0]) <= 1:
        return
    distances = [hit["distance"] for hit in results[0]]
    for i in range(len(distances) - 1):
        assert distances[i] >= distances[i + 1] - 0.001


class StructArrayElementSearchBase(TestMilvusClientV2Base):
    def _cosine_vector(self, cosine, dim=default_dim):
        vec = [0.0] * dim
        vec[0] = float(cosine)
        if dim > 1:
            vec[1] = float(np.sqrt(max(0.0, 1.0 - cosine * cosine)))
        return vec

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
            "structA",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )
        return schema

    def _generate_data(self, nb=500, dim=default_dim):
        data = []
        for i in range(nb):
            rng = random.Random(i)
            num_elems = rng.randint(3, 8)
            struct_array = []
            for j in range(num_elems):
                if i == 0 and j == 0:
                    emb = [1.0] + [0.0] * (dim - 1)
                else:
                    emb = _seed_vector(i * 1000 + j, dim)
                struct_array.append(
                    {
                        "embedding": emb,
                        "int_val": i * 100 + j,
                        "color": COLORS[j % len(COLORS)],
                    }
                )
            data.append(
                {
                    "id": i,
                    "doc_int": i,
                    "normal_vector": _seed_vector(i + 999999, dim),
                    "structA": struct_array,
                }
            )
        return data

    def _setup_collection(self, client, collection_name, nb=500, dim=default_dim):
        schema = self._create_schema(client, dim)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="structA[embedding]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = self._generate_data(nb=nb, dim=dim)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return data

    def _setup_multi_struct_collection(self, client, collection_name, metric_type="COSINE"):
        """Create deterministic data for hybrid collapse and validation checks."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="doc_int", datatype=DataType.INT64)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_a = client.create_struct_field_schema()
        struct_a.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_a.add_field("embedding_alt", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_a.add_field("tag", DataType.VARCHAR, max_length=32)
        schema.add_field(
            "structA",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_a,
            max_capacity=10,
        )

        struct_b = client.create_struct_field_schema()
        struct_b.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_b.add_field("tag", DataType.VARCHAR, max_length=32)
        schema.add_field(
            "structB",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_b,
            max_capacity=10,
        )

        index_params = client.prepare_index_params()
        for field_name in ["normal_vector", "structA[embedding]", "structA[embedding_alt]", "structB[embedding]"]:
            index_params.add_index(field_name=field_name, index_type="FLAT", metric_type=metric_type)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        def _struct_a(scores):
            return [
                {
                    "embedding": self._cosine_vector(score),
                    "embedding_alt": self._cosine_vector(score),
                    "tag": f"a_{idx}",
                }
                for idx, score in enumerate(scores)
            ]

        def _struct_b(scores):
            return [
                {"embedding": self._cosine_vector(score), "tag": f"b_{idx}"}
                for idx, score in enumerate(scores)
            ]

        score_rows = [
            (1, [0.99, 0.10, 0.10]),
            (2, [0.90, 0.89, 0.88]),
            (3, [0.70, 0.69, 0.68]),
        ]
        data = [
            {
                "id": row_id,
                "doc_int": row_id * 10,
                "normal_vector": self._cosine_vector(0.20 + row_id * 0.01),
                "structA": _struct_a(scores),
                "structB": _struct_b(scores),
            }
            for row_id, scores in score_rows
        ]
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return data

    def _setup_embedding_list_collection(self, client, collection_name, sealed_nb=default_nb, growing_nb=200):
        """Create an embedding-list StructArray collection for hybrid validation paths."""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("doc_int", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("int_val", DataType.INT64)
        schema.add_field(
            "structA",
            DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=default_capacity,
        )

        index_params = client.prepare_index_params()
        index_params.add_index("normal_vector", index_type="HNSW", metric_type="COSINE", params=INDEX_PARAMS)
        index_params.add_index(
            "structA[embedding]", index_type="HNSW", metric_type="MAX_SIM_COSINE", params=INDEX_PARAMS
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        def _gen_row(i):
            rng = random.Random(i)
            return {
                "id": i,
                "doc_int": i,
                "normal_vector": _seed_vector(i + 999999),
                "structA": [
                    {"embedding": _seed_vector(i * 100 + k), "int_val": i * 10 + k}
                    for k in range(rng.randint(3, 6))
                ],
            }

        data = [_gen_row(i) for i in range(sealed_nb)]
        for start in range(0, sealed_nb, 1000):
            self.insert(client, collection_name, data[start : start + 1000])
        self.flush(client, collection_name)
        growing = [_gen_row(sealed_nb + i) for i in range(growing_nb)]
        self.insert(client, collection_name, growing)
        data.extend(growing)
        self.load_collection(client, collection_name)
        return data

    def _generate_hybrid_collapse_data(self, nb=default_nb, candidate_count=64, dim=default_dim):
        data = [
            {
                "id": i,
                "doc_int": i,
                "normal_vector": self._cosine_vector(-1.0, dim),
                "structA": [
                    {
                        "embedding": self._cosine_vector(-1.0 + offset * 0.01, dim),
                        "int_val": -(offset + 1),
                        "color": "drop",
                    }
                    for offset in range(3 + i % 4)
                ],
            }
            for i in range(nb)
        ]

        for rank in range(candidate_count):
            row_id = nb + rank
            elem_score = 1.0 - rank * 0.025
            normal_score = -1.0 if rank == 0 else 1.0 - (rank - 1) * 0.025
            struct_array = [
                {
                    "embedding": self._cosine_vector(elem_score, dim),
                    "int_val": rank * 10,
                    "color": "keep",
                }
            ]
            if rank == 0:
                struct_array.append(
                    {
                        "embedding": self._cosine_vector(0.99, dim),
                        "int_val": rank * 10 + 1,
                        "color": "keep",
                    }
                )
            elif rank % 7 == 0:
                struct_array.append(
                    {
                        "embedding": self._cosine_vector(elem_score - 0.01, dim),
                        "int_val": rank * 10 + 1,
                        "color": "keep",
                    }
                )
            struct_array.append({"embedding": self._cosine_vector(-1.0, dim), "int_val": -1, "color": "drop"})
            struct_array.append({"embedding": self._cosine_vector(-0.9, dim), "int_val": -2, "color": "drop"})
            data.append(
                {
                    "id": row_id,
                    "doc_int": row_id,
                    "normal_vector": self._cosine_vector(normal_score, dim),
                    "structA": struct_array,
                }
            )

        return data

    def _gt_hybrid_collapse_rrf(self, data, query_vector, sub_limit, hybrid_limit, rrf_k):
        element_hits = []
        for row in data:
            for offset, elem in enumerate(row["structA"]):
                element_hits.append(
                    {
                        "id": row["id"],
                        "score": _cosine_sim(query_vector, elem["embedding"]),
                        "offset": offset,
                    }
                )
        element_hits.sort(key=lambda hit: (-hit["score"], hit["id"], hit["offset"]))
        element_hits = element_hits[:sub_limit]

        best_by_id = {}
        for order, hit in enumerate(element_hits):
            current = best_by_id.get(hit["id"])
            if current is None or hit["score"] > current["score"]:
                best_by_id[hit["id"]] = {
                    "id": hit["id"],
                    "score": hit["score"],
                    "order": order,
                }
        element_rows = sorted(best_by_id.values(), key=lambda hit: (-hit["score"], hit["order"]))

        normal_rows = [{"id": row["id"], "score": _cosine_sim(query_vector, row["normal_vector"])} for row in data]
        normal_rows.sort(key=lambda hit: (-hit["score"], hit["id"]))
        normal_rows = normal_rows[:sub_limit]

        rrf_scores = {}
        for hits in (element_rows, normal_rows):
            for rank, hit in enumerate(hits):
                rrf_scores[hit["id"]] = rrf_scores.get(hit["id"], 0.0) + 1 / (rrf_k + rank + 1)

        hybrid_rows = [{"id": row_id, "score": score} for row_id, score in rrf_scores.items()]
        hybrid_rows.sort(key=lambda hit: (-hit["score"], hit["id"]))
        return element_hits, normal_rows, hybrid_rows[:hybrid_limit]


class TestStructArrayElementSearchNoFilter(StructArrayElementSearchBase):
    """Plain element-level search on structA[embedding]."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_no_filter_basic(self):
        """
        target: element-level search on struct vector without element_filter
        method: search structA[embedding] with a single plain vector
        expected: search succeeds and returns the exact self element as top-1
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_plain_elem")
        data = self._setup_collection(client, collection_name)

        results, check = self.search(
            client,
            collection_name,
            data=[data[0]["structA"][0]["embedding"]],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE", "params": HNSW_SEARCH_PARAMS},
            limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) == 10
        assert results[0][0]["id"] == 0
        _assert_distance_order(results)

    @pytest.mark.tags(CaseLabel.L1)
    def test_element_search_no_filter_ground_truth(self):
        """
        target: verify element-level search ranking
        method: compare top result with exact flattened-element ground truth
        expected: the top returned row matches the best matching element
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_plain_gt")
        data = self._setup_collection(client, collection_name)
        query_vector = data[42]["structA"][1]["embedding"]

        results, check = self.search(
            client,
            collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE", "params": HNSW_SEARCH_PARAMS},
            limit=10,
            output_fields=["id"],
        )
        assert check
        gt = gt_element_search_no_filter(data, query_vector, limit=10)
        assert results[0][0]["id"] == gt[0][0]
        _assert_distance_order(results)

    @pytest.mark.tags(CaseLabel.L2)
    def test_element_search_no_filter_nq_multiple(self):
        """
        target: element-level search supports multiple plain query vectors
        method: search structA[embedding] with three vectors
        expected: one result set is returned for each query
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_plain_batch")
        data = self._setup_collection(client, collection_name)
        query_vectors = [data[i]["structA"][0]["embedding"] for i in range(3)]

        results, check = self.search(
            client,
            collection_name,
            data=query_vectors,
            anns_field="structA[embedding]",
            search_params={"metric_type": "COSINE", "params": HNSW_SEARCH_PARAMS},
            limit=5,
            output_fields=["id"],
        )
        assert check
        assert len(results) == len(query_vectors)
        for i, hits in enumerate(results):
            assert len(hits) == 5
            assert hits[0]["id"] == i


class TestStructArrayElementHybridSearchNoFilter(StructArrayElementSearchBase):
    """Plain element-level search combined with a normal vector field in hybrid_search."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_element_search_and_normal_vector_rrf(self):
        """
        target: hybrid search combining element-level struct vector search and normal_vector search
        method: use RRFRanker to combine two AnnSearchRequests
        expected: hybrid results returned and the shared self-match row ranks first
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_elem_nv")
        data = self._setup_collection(client, collection_name)

        req1 = AnnSearchRequest(
            **{
                "data": [data[0]["structA"][0]["embedding"]],
                "anns_field": "structA[embedding]",
                "param": {"metric_type": "COSINE", "params": HNSW_SEARCH_PARAMS},
                "limit": 10,
            }
        )
        req2 = AnnSearchRequest(
            **{
                "data": [data[0]["normal_vector"]],
                "anns_field": "normal_vector",
                "param": {"metric_type": "COSINE", "params": HNSW_SEARCH_PARAMS},
                "limit": 10,
            }
        )

        results, check = self.hybrid_search(
            client,
            collection_name,
            [req1, req2],
            ranker=RRFRanker(),
            limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) == 10
        assert results[0][0]["id"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_element_search_and_normal_vector_weighted(self):
        """
        target: hybrid search with WeightedRanker over element-level and normal vector fields
        method: combine structA[embedding] and normal_vector requests with explicit weights
        expected: hybrid results returned and the shared self-match row ranks first
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_elem_weighted")
        data = self._setup_collection(client, collection_name)

        req1 = AnnSearchRequest(
            **{
                "data": [data[0]["structA"][0]["embedding"]],
                "anns_field": "structA[embedding]",
                "param": {"metric_type": "COSINE", "params": HNSW_SEARCH_PARAMS},
                "limit": 10,
            }
        )
        req2 = AnnSearchRequest(
            **{
                "data": [data[0]["normal_vector"]],
                "anns_field": "normal_vector",
                "param": {"metric_type": "COSINE", "params": HNSW_SEARCH_PARAMS},
                "limit": 10,
            }
        )

        results, check = self.hybrid_search(
            client,
            collection_name,
            [req1, req2],
            ranker=WeightedRanker(0.7, 0.3),
            limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) == 10
        assert results[0][0]["id"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", ["FLAT", "HNSW"])
    def test_hybrid_element_search_collapse_correctness(self, index_type):
        """
        target: verify element-level hybrid search collapses element hits to row hits before rerank
        method: insert deterministic rows, compute brute-force ground truth, and run with FLAT/HNSW
        expected: hybrid result matches element-collapse + normal-vector RRF ground truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_collapse_{index_type.lower()}")
        query_vector = self._cosine_vector(1.0)
        search_params = {"metric_type": "COSINE"}
        if index_type == "HNSW":
            search_params["params"] = HNSW_SEARCH_PARAMS

        schema = self._create_schema(client)
        index_params = client.prepare_index_params()
        normal_index = {
            "field_name": "normal_vector",
            "index_type": index_type,
            "metric_type": "COSINE",
        }
        struct_index = {
            "field_name": "structA[embedding]",
            "index_type": index_type,
            "metric_type": "COSINE",
        }
        if index_type == "HNSW":
            normal_index["params"] = INDEX_PARAMS
            struct_index["params"] = INDEX_PARAMS
        index_params.add_index(**normal_index)
        index_params.add_index(**struct_index)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        sub_limit = 20
        hybrid_limit = 10
        rrf_k = 60
        data = self._generate_hybrid_collapse_data()
        assert len(data) > default_nb
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        element_gt, normal_gt, hybrid_gt = self._gt_hybrid_collapse_rrf(
            data,
            query_vector,
            sub_limit,
            hybrid_limit,
            rrf_k,
        )

        element_results, check = self.search(
            client,
            collection_name,
            data=[query_vector],
            anns_field="structA[embedding]",
            search_params=search_params,
            limit=sub_limit,
            output_fields=["id"],
        )
        assert check
        element_ids = [hit["id"] for hit in element_results[0]]
        element_gt_ids = [hit["id"] for hit in element_gt]
        assert len(element_gt_ids) > len(set(element_gt_ids))
        assert element_ids == element_gt_ids

        normal_results, check = self.search(
            client,
            collection_name,
            data=[query_vector],
            anns_field="normal_vector",
            search_params=search_params,
            limit=sub_limit,
            output_fields=["id"],
        )
        assert check
        assert [hit["id"] for hit in normal_results[0]] == [hit["id"] for hit in normal_gt]

        req1 = AnnSearchRequest(
            **{
                "data": [query_vector],
                "anns_field": "structA[embedding]",
                "param": search_params,
                "limit": sub_limit,
            }
        )
        req2 = AnnSearchRequest(
            **{
                "data": [query_vector],
                "anns_field": "normal_vector",
                "param": search_params,
                "limit": sub_limit,
            }
        )

        results, check = self.hybrid_search(
            client,
            collection_name,
            [req1, req2],
            ranker=RRFRanker(rrf_k),
            limit=hybrid_limit,
            output_fields=["id"],
        )
        assert check
        hits = results[0]
        ids = [hit["id"] for hit in hits]
        expected_ids = [hit["id"] for hit in hybrid_gt]
        assert ids == expected_ids
        assert len(ids) == len(set(ids)), f"hybrid result should be row-level, got duplicate ids: {ids}"
        assert all("offset" not in hit for hit in hits), f"hybrid result should not expose element offsets: {hits}"

        expected_scores = {hit["id"]: hit["score"] for hit in hybrid_gt}
        for hit in hits:
            assert abs(hit["distance"] - expected_scores[hit["id"]]) < 1e-5, (
                f"unexpected RRF score for id={hit['id']}: got {hit['distance']}, expected {expected_scores[hit['id']]}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_element_scope_topk_sum_changes_collapse_order(self):
        """Verify element_scope changes row-level collapse across different StructArray fields."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_scope_topk_sum")
        self._setup_multi_struct_collection(client, collection_name)
        query_vector = self._cosine_vector(1.0)

        def _req(field_name, params=None):
            search_params = {"metric_type": "COSINE"}
            if params is not None:
                search_params["params"] = params
            return AnnSearchRequest(
                **{
                    "data": [query_vector],
                    "anns_field": field_name,
                    "param": search_params,
                    "limit": 9,
                }
            )

        default_results, check = self.hybrid_search(
            client,
            collection_name,
            [_req("structA[embedding]"), _req("structB[embedding]")],
            ranker=RRFRanker(),
            limit=3,
            output_fields=["id"],
        )
        assert check
        default_ids = [hit["id"] for hit in default_results[0]]
        assert default_ids[0] == 1

        element_scope = {"element_scope": {"collapse": {"strategy": "topk_sum", "topk": 3}}}
        scoped_results, check = self.hybrid_search(
            client,
            collection_name,
            [_req("structA[embedding]", element_scope), _req("structB[embedding]", element_scope)],
            ranker=RRFRanker(),
            limit=3,
            output_fields=["id"],
        )
        assert check
        scoped_ids = [hit["id"] for hit in scoped_results[0]]
        assert scoped_ids[0] == 2
        assert scoped_ids != default_ids
        assert len(scoped_ids) == len(set(scoped_ids))

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_same_struct_element_scope_not_supported(self):
        """element_scope is invalid for a same-StructArray element-level hybrid search."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_scope_same_struct")
        self._setup_multi_struct_collection(client, collection_name)
        query_vector = self._cosine_vector(1.0)
        req1 = AnnSearchRequest(
            **{
                "data": [query_vector],
                "anns_field": "structA[embedding]",
                "param": {
                    "metric_type": "COSINE",
                    "params": {"element_scope": {"collapse": {"strategy": "max"}}},
                },
                "limit": 9,
            }
        )
        req2 = AnnSearchRequest(
            **{
                "data": [query_vector],
                "anns_field": "structA[embedding_alt]",
                "param": {"metric_type": "COSINE"},
                "limit": 9,
            }
        )
        self.hybrid_search(
            client,
            collection_name,
            [req1, req2],
            ranker=RRFRanker(),
            limit=3,
            output_fields=["id"],
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "element_scope is not allowed for same-struct element-level hybrid search",
            },
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_element_scope_sum_l2_not_supported(self):
        """Sum-family element collapse is invalid for negatively related metrics."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_scope_l2")
        self._setup_multi_struct_collection(client, collection_name, metric_type="L2")
        query_vector = self._cosine_vector(1.0)
        req1 = AnnSearchRequest(
            **{
                "data": [query_vector],
                "anns_field": "structA[embedding]",
                "param": {
                    "metric_type": "L2",
                    "params": {"element_scope": {"collapse": {"strategy": "topk_sum", "topk": 2}}},
                },
                "limit": 9,
            }
        )
        req2 = AnnSearchRequest(
            **{
                "data": [query_vector],
                "anns_field": "structB[embedding]",
                "param": {"metric_type": "L2"},
                "limit": 9,
            }
        )
        self.hybrid_search(
            client,
            collection_name,
            [req1, req2],
            ranker=RRFRanker(),
            limit=3,
            output_fields=["id"],
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "element_scope.collapse.strategy topk_sum is only supported",
            },
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_embedding_list_range_not_supported(self):
        """Embedding-list-level hybrid search rejects range-search parameters."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_emblist_radius")
        data = self._setup_embedding_list_collection(client, collection_name)

        tensor = EmbeddingList()
        tensor.add(_seed_vector(7))
        tensor.add(_seed_vector(8))
        req1 = AnnSearchRequest(
            **{
                "data": [tensor],
                "anns_field": "structA[embedding]",
                "param": {"metric_type": "MAX_SIM_COSINE", "params": {"radius": 0.1}},
                "limit": 10,
            }
        )
        req2 = AnnSearchRequest(
            **{
                "data": [data[0]["normal_vector"]],
                "anns_field": "normal_vector",
                "param": {"metric_type": "COSINE"},
                "limit": 10,
            }
        )
        self.hybrid_search(
            client,
            collection_name,
            [req1, req2],
            ranker=RRFRanker(),
            limit=10,
            output_fields=["id"],
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "range search is not supported for vector array (embedding-list) fields in hybrid search",
            },
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_embedding_list_group_by_not_supported(self):
        """Embedding-list-level hybrid search rejects group-by."""
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_hyb_emblist_gb")
        data = self._setup_embedding_list_collection(client, collection_name)

        tensor = EmbeddingList()
        tensor.add(_seed_vector(3))
        tensor.add(_seed_vector(4))
        req1 = AnnSearchRequest(
            **{
                "data": [tensor],
                "anns_field": "structA[embedding]",
                "param": {"metric_type": "MAX_SIM_COSINE"},
                "limit": 10,
            }
        )
        req2 = AnnSearchRequest(
            **{
                "data": [data[0]["normal_vector"]],
                "anns_field": "normal_vector",
                "param": {"metric_type": "COSINE"},
                "limit": 10,
            }
        )
        self.hybrid_search(
            client,
            collection_name,
            [req1, req2],
            ranker=RRFRanker(),
            limit=10,
            output_fields=["id"],
            group_by_field="id",
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "group by search is not supported for vector array (embedding-list) fields in hybrid search",
            },
        )
