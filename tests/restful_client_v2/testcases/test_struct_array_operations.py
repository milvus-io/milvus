"""
RESTful v2 tests for struct array (DataType_ArrayOfStruct) support.

Coverage:
  - Schema creation with struct array containing Array (scalar) + ArrayOfVector
    (vector) sub-fields; describe returns structFields.
  - Schema validation — rejects nullable / defaultValue / primary / partition /
    clustering sub-fields; rejects non Array|ArrayOfVector sub-field data types;
    rejects empty/duplicated sub-fields.
  - Insert + query roundtrip of struct array rows.
  - Struct sub-vector search in both element-level and embedding-list shapes
    (auto-detected by the handler).

Two indexing paths are exercised:
  - Two-step (TestStructSubVectorSearch): create collection without sub-vector
    index, then call /v2/vectordb/indexes/create with the qualified
    `structName[subName]` name.
  - One-step (TestStructSubVectorSearchOneStep): pass both top-level and
    `structName[subName]` entries in collection_create's indexParams, aligned
    with pymilvus `index_params.add_index(field_name="struct[sub]")`.
"""

import random
import pytest
import numpy as np

from base.testbase import TestBase
from utils.utils import gen_collection_name
from utils.util_log import test_log as logger


DEFAULT_DIM = 8
DEFAULT_SUB_CAPACITY = 16


def _rand_vector(dim=DEFAULT_DIM):
    return [random.random() for _ in range(dim)]


def _build_struct_schema_payload(name, dim=DEFAULT_DIM,
                                 max_capacity=DEFAULT_SUB_CAPACITY,
                                 include_index_params=False,
                                 metric_type="COSINE",
                                 sub_metric_type="COSINE",
                                 enable_dynamic_field=False):
    """Build a standard create-collection payload with a struct array field.

    - top-level: id (Int64 PK), vec (FloatVector dim)
    - struct: my_struct{ sub_int: Array<Int32>, sub_vec: ArrayOfVector<FloatVector dim> }

    When include_index_params=True, builds indexes for BOTH the top-level
    vector and the struct sub-vector (structName[subName]) in a single
    create call — Milvus requires every vector field to have an index
    before the collection can be loaded. The sub-vector index metric
    determines the permitted search shape: a plain metric (e.g. COSINE)
    enables element-level search, while a MAX_SIM_* metric (e.g.
    MAX_SIM_COSINE) enables embedding-list search.
    """
    payload = {
        "collectionName": name,
        "schema": {
            "autoId": False,
            "enableDynamicField": enable_dynamic_field,
            "fields": [
                {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                {
                    "fieldName": "vec",
                    "dataType": "FloatVector",
                    "elementTypeParams": {"dim": f"{dim}"},
                },
            ],
            "structFields": [
                {
                    "fieldName": "my_struct",
                    "fields": [
                        {
                            "fieldName": "sub_int",
                            "dataType": "Array",
                            "elementDataType": "Int32",
                            "elementTypeParams": {"max_capacity": max_capacity},
                        },
                        {
                            "fieldName": "sub_vec",
                            "dataType": "ArrayOfVector",
                            "elementDataType": "FloatVector",
                            "elementTypeParams": {
                                "dim": dim,
                                "max_capacity": max_capacity,
                            },
                        },
                    ],
                }
            ],
        },
    }
    if include_index_params:
        payload["indexParams"] = [
            {"fieldName": "vec", "indexName": "vec_idx", "metricType": metric_type},
            {
                "fieldName": "my_struct[sub_vec]",
                "indexName": "sub_vec_idx",
                "metricType": sub_metric_type,
            },
        ]
    return payload


def _gen_struct_row(row_id, num_elems, dim=DEFAULT_DIM):
    struct_elems = []
    for j in range(num_elems):
        struct_elems.append({
            "sub_int": row_id * 100 + j,
            "sub_vec": _rand_vector(dim),
        })
    return {
        "id": row_id,
        "vec": _rand_vector(dim),
        "my_struct": struct_elems,
    }


@pytest.mark.L0
class TestStructArrayCollection(TestBase):
    """Create + describe collections that contain a struct array field."""

    def test_create_struct_array_collection(self):
        """
        target: create a collection whose schema includes a struct array field
                with an Array scalar sub-field and an ArrayOfVector sub-field
        expected: create + describe succeed; describe response surfaces
                  structFields with the two declared sub-fields
        """
        name = gen_collection_name()
        payload = _build_struct_schema_payload(name, include_index_params=True)
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0

        rsp = self.collection_client.collection_describe(name)
        assert rsp["code"] == 0
        data = rsp["data"]
        struct_fields = data.get("structFields", [])
        assert len(struct_fields) == 1, f"expected 1 struct field, got {struct_fields}"

        struct_field = struct_fields[0]
        assert struct_field["name"] == "my_struct"
        assert struct_field["type"] == "ArrayOfStruct"

        sub_fields = struct_field.get("fields", [])
        sub_names = sorted(s["name"] for s in sub_fields)
        assert sub_names == ["sub_int", "sub_vec"], sub_names

        by_name = {s["name"]: s for s in sub_fields}
        assert by_name["sub_int"]["type"] == "Array"
        assert by_name["sub_int"]["elementType"] == "Int32"
        assert by_name["sub_vec"]["elementType"] == "FloatVector"


@pytest.mark.L1
class TestStructArraySchemaValidation(TestBase):
    """Negative tests for struct-array sub-field constraints."""

    def _create_with_bad_sub_field(self, name, bad_sub_field):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "vec",
                        "dataType": "FloatVector",
                        "elementTypeParams": {"dim": f"{DEFAULT_DIM}"},
                    },
                ],
                "structFields": [
                    {
                        "fieldName": "bad_struct",
                        "fields": [bad_sub_field],
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "vec", "indexName": "vec_idx", "metricType": "L2"},
            ],
        }
        return self.collection_client.collection_create(payload)

    def test_reject_nullable_sub_field(self):
        """nullable sub-field must be rejected."""
        name = gen_collection_name()
        rsp = self._create_with_bad_sub_field(name, {
            "fieldName": "sub",
            "dataType": "Array",
            "elementDataType": "Int32",
            "elementTypeParams": {"max_capacity": DEFAULT_SUB_CAPACITY},
            "nullable": True,
        })
        assert rsp["code"] != 0, rsp
        assert "nullable" in rsp.get("message", "").lower()

    def test_reject_default_value_sub_field(self):
        """sub-field with defaultValue must be rejected."""
        name = gen_collection_name()
        rsp = self._create_with_bad_sub_field(name, {
            "fieldName": "sub",
            "dataType": "Array",
            "elementDataType": "Int32",
            "elementTypeParams": {"max_capacity": DEFAULT_SUB_CAPACITY},
            "defaultValue": 1,
        })
        assert rsp["code"] != 0, rsp
        assert "default" in rsp.get("message", "").lower()

    def test_reject_primary_key_sub_field(self):
        """sub-field with isPrimary=True must be rejected."""
        name = gen_collection_name()
        rsp = self._create_with_bad_sub_field(name, {
            "fieldName": "sub",
            "dataType": "Array",
            "elementDataType": "Int64",
            "elementTypeParams": {"max_capacity": DEFAULT_SUB_CAPACITY},
            "isPrimary": True,
        })
        assert rsp["code"] != 0, rsp

    def test_reject_partition_key_sub_field(self):
        """sub-field with isPartitionKey=True must be rejected."""
        name = gen_collection_name()
        rsp = self._create_with_bad_sub_field(name, {
            "fieldName": "sub",
            "dataType": "Array",
            "elementDataType": "Int64",
            "elementTypeParams": {"max_capacity": DEFAULT_SUB_CAPACITY},
            "isPartitionKey": True,
        })
        assert rsp["code"] != 0, rsp

    def test_reject_clustering_key_sub_field(self):
        """sub-field with isClusteringKey=True must be rejected."""
        name = gen_collection_name()
        rsp = self._create_with_bad_sub_field(name, {
            "fieldName": "sub",
            "dataType": "Array",
            "elementDataType": "Int64",
            "elementTypeParams": {"max_capacity": DEFAULT_SUB_CAPACITY},
            "isClusteringKey": True,
        })
        assert rsp["code"] != 0, rsp

    def test_reject_non_array_sub_field(self):
        """sub-field whose dataType is not Array/ArrayOfVector must be rejected."""
        name = gen_collection_name()
        rsp = self._create_with_bad_sub_field(name, {
            "fieldName": "sub",
            "dataType": "Int32",
        })
        assert rsp["code"] != 0, rsp

    def test_reject_empty_sub_fields(self):
        """struct with empty fields list must be rejected."""
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "vec",
                        "dataType": "FloatVector",
                        "elementTypeParams": {"dim": f"{DEFAULT_DIM}"},
                    },
                ],
                "structFields": [
                    {"fieldName": "empty_struct", "fields": []}
                ],
            },
            "indexParams": [
                {"fieldName": "vec", "indexName": "vec_idx", "metricType": "L2"},
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] != 0, rsp

    def test_reject_duplicated_sub_field_name(self):
        """duplicated sub-field name must be rejected."""
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "vec",
                        "dataType": "FloatVector",
                        "elementTypeParams": {"dim": f"{DEFAULT_DIM}"},
                    },
                ],
                "structFields": [
                    {
                        "fieldName": "dup_struct",
                        "fields": [
                            {
                                "fieldName": "s",
                                "dataType": "Array",
                                "elementDataType": "Int32",
                                "elementTypeParams": {"max_capacity": DEFAULT_SUB_CAPACITY},
                            },
                            {
                                "fieldName": "s",
                                "dataType": "Array",
                                "elementDataType": "Int32",
                                "elementTypeParams": {"max_capacity": DEFAULT_SUB_CAPACITY},
                            },
                        ],
                    }
                ],
            },
            "indexParams": [
                {"fieldName": "vec", "indexName": "vec_idx", "metricType": "L2"},
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] != 0, rsp


@pytest.mark.L0
class TestStructArrayInsertQuery(TestBase):
    """Insert + query roundtrip with struct array rows."""

    def _create_and_load(self, name, dim=DEFAULT_DIM):
        payload = _build_struct_schema_payload(name, dim=dim, include_index_params=True)
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.collection_client.wait_load_completed(name, timeout=60)

    def test_insert_and_query_struct_rows(self):
        """
        target: insert struct array rows and read them back via query
        expected: query returns the same struct elements in order
        """
        name = gen_collection_name()
        self._create_and_load(name)

        nb = 10
        rows = [_gen_struct_row(i, num_elems=random.randint(1, 4)) for i in range(nb)]
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == nb

        rsp = self.vector_client.vector_query({
            "collectionName": name,
            "filter": "id >= 0",
            "outputFields": ["id", "my_struct"],
            "limit": nb,
        })
        assert rsp["code"] == 0, rsp
        got = sorted(rsp["data"], key=lambda r: r["id"])
        assert len(got) == nb

        for orig, back in zip(rows, got):
            assert back["id"] == orig["id"]
            elems_back = back["my_struct"]
            assert len(elems_back) == len(orig["my_struct"]), \
                f"row {orig['id']} elem count mismatch: {len(elems_back)} vs {len(orig['my_struct'])}"
            for eb, eo in zip(elems_back, orig["my_struct"]):
                assert int(eb["sub_int"]) == eo["sub_int"]
                # Compare vectors with float tolerance.
                np.testing.assert_allclose(eb["sub_vec"], eo["sub_vec"], rtol=1e-5, atol=1e-5)

    def test_insert_reject_missing_sub_field(self):
        """struct element missing a declared sub-field must be rejected."""
        name = gen_collection_name()
        self._create_and_load(name)

        bad_row = {
            "id": 1,
            "vec": _rand_vector(),
            "my_struct": [
                {"sub_int": 1},  # sub_vec missing
            ],
        }
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": [bad_row]})
        assert rsp["code"] != 0, rsp
        assert "sub_vec" in rsp.get("message", "") or "missing" in rsp.get("message", "").lower()

    def test_insert_reject_struct_value_not_array(self):
        """struct field value must be a JSON array of objects."""
        name = gen_collection_name()
        self._create_and_load(name)

        bad_row = {
            "id": 1,
            "vec": _rand_vector(),
            "my_struct": {"sub_int": 1, "sub_vec": _rand_vector()},
        }
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": [bad_row]})
        assert rsp["code"] != 0, rsp


@pytest.mark.L0
class TestStructSubVectorSearch(TestBase):
    """Sub-vector search via RESTful v2 (element-level + embedding-list shapes)."""

    def _setup_collection_with_sub_index(self, name, dim=DEFAULT_DIM, nb=50,
                                         sub_metric="COSINE"):
        """Create a struct-array collection, index both top-level vec and the
        struct sub-vector, load, and insert nb rows. `sub_metric` selects the
        search mode: plain metric → element-level; MAX_SIM_* → embedding list."""
        payload = _build_struct_schema_payload(name, dim=dim, include_index_params=False)
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp

        # Top-level vector index.
        rsp = self.index_client.index_create({
            "collectionName": name,
            "indexParams": [
                {"fieldName": "vec", "indexName": "vec_idx", "metricType": "COSINE"},
            ],
        })
        assert rsp["code"] == 0, rsp

        # Struct sub-vector index. The REST handler matches annsField by
        # canonical "<struct>[<sub>]" form. The metric decides the
        # search shape: plain (COSINE/L2/IP) → element-level, MAX_SIM_* →
        # embedding-list.
        rsp = self.index_client.index_create({
            "collectionName": name,
            "indexParams": [
                {
                    "fieldName": "my_struct[sub_vec]",
                    "indexName": "sub_vec_idx",
                    "metricType": sub_metric,
                },
            ],
        })
        assert rsp["code"] == 0, rsp

        rsp = self.collection_client.collection_load(collection_name=name)
        assert rsp["code"] == 0, rsp
        self.collection_client.wait_load_completed(name, timeout=60)

        rows = [_gen_struct_row(i, num_elems=random.randint(2, 5), dim=dim) for i in range(nb)]
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        # Flush to make data sealed so index is applied.
        self.collection_client.flush(name)
        self.collection_client.wait_load_completed(name, timeout=60)
        return rows

    def test_element_level_sub_vector_search(self):
        """
        target: search struct sub-vector with element-level shape
                (one vector per nq query)
        expected: search returns up to `limit` results per query
        """
        name = gen_collection_name()
        nq = 2
        self._setup_collection_with_sub_index(name)

        search_payload = {
            "collectionName": name,
            "annsField": "my_struct[sub_vec]",
            "data": [_rand_vector() for _ in range(nq)],
            "limit": 5,
            "outputFields": ["id"],
        }
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp["code"] == 0, rsp
        # Element-level returns `nq` top-k lists concatenated. When nq>1 the
        # response typically groups results per query; here we only assert at
        # least one hit was returned.
        assert len(rsp["data"]) > 0, rsp

    def test_embedding_list_sub_vector_search(self):
        """
        target: search struct sub-vector with embedding-list shape
                (list of vectors per nq query)
        expected: request is routed via convertEmbListQueries2Placeholder;
                  search succeeds with non-empty results
        """
        name = gen_collection_name()
        nq = 2
        per_query_vecs = 3
        self._setup_collection_with_sub_index(name, sub_metric="MAX_SIM_COSINE")

        search_payload = {
            "collectionName": name,
            "annsField": "my_struct[sub_vec]",
            "data": [
                [_rand_vector() for _ in range(per_query_vecs)]
                for _ in range(nq)
            ],
            "limit": 5,
            "outputFields": ["id"],
        }
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp["code"] == 0, rsp
        assert len(rsp["data"]) > 0, rsp

    def test_sub_vector_search_dim_mismatch(self):
        """sub-vector search with wrong dim must fail the request parsing."""
        name = gen_collection_name()
        self._setup_collection_with_sub_index(name)

        search_payload = {
            "collectionName": name,
            "annsField": "my_struct[sub_vec]",
            "data": [_rand_vector(DEFAULT_DIM + 1)],
            "limit": 5,
        }
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp["code"] != 0, rsp

    def test_embedding_list_search_dim_mismatch(self):
        """embedding-list search with wrong dim per vector must fail parsing."""
        name = gen_collection_name()
        self._setup_collection_with_sub_index(name, sub_metric="MAX_SIM_COSINE")

        search_payload = {
            "collectionName": name,
            "annsField": "my_struct[sub_vec]",
            "data": [[_rand_vector(DEFAULT_DIM + 1), _rand_vector(DEFAULT_DIM + 1)]],
            "limit": 5,
        }
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp["code"] != 0, rsp


@pytest.mark.L0
class TestStructSubVectorSearchOneStep(TestBase):
    """Verify collection_create accepts struct sub-vector indexParams directly.

    Covers the fix that registers `structName[subName]` into the handler's
    fieldNames map — users can build the top-level and sub-vector index in a
    single /collections/create call (pymilvus parity).
    """

    def _create_load_insert(self, name, sub_metric, nb=50):
        payload = _build_struct_schema_payload(
            name, include_index_params=True, sub_metric_type=sub_metric,
        )
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.collection_client.wait_load_completed(name, timeout=60)

        rows = [_gen_struct_row(i, num_elems=random.randint(2, 4)) for i in range(nb)]
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        self.collection_client.flush(name)
        self.collection_client.wait_load_completed(name, timeout=60)

    def test_create_with_sub_vector_index_element_level(self):
        """
        target: create a collection whose indexParams includes a struct
                sub-vector with a plain metric (COSINE) in one request
        expected: create succeeds, collection auto-loads, element-level
                  sub-vector search works
        """
        name = gen_collection_name()
        self._create_load_insert(name, sub_metric="COSINE")
        rsp = self.vector_client.vector_search({
            "collectionName": name,
            "annsField": "my_struct[sub_vec]",
            "data": [_rand_vector()],
            "limit": 5,
        })
        assert rsp["code"] == 0, rsp
        assert len(rsp["data"]) > 0, rsp

    def test_create_with_sub_vector_index_embedding_list(self):
        """
        target: create a collection whose indexParams includes a struct
                sub-vector with a MAX_SIM_* metric in one request
        expected: create succeeds, collection auto-loads, embedding-list
                  sub-vector search works
        """
        name = gen_collection_name()
        self._create_load_insert(name, sub_metric="MAX_SIM_COSINE")
        rsp = self.vector_client.vector_search({
            "collectionName": name,
            "annsField": "my_struct[sub_vec]",
            "data": [[_rand_vector(), _rand_vector(), _rand_vector()]],
            "limit": 5,
        })
        assert rsp["code"] == 0, rsp
        assert len(rsp["data"]) > 0, rsp

    def test_create_with_unknown_sub_field_rejected(self):
        """
        target: indexParams referencing a non-existent struct sub-field
                (my_struct[no_such_sub]) must be rejected by the handler
        expected: error "hasn't defined in schema"
        """
        name = gen_collection_name()
        payload = _build_struct_schema_payload(name, include_index_params=False)
        payload["indexParams"] = [
            {"fieldName": "vec", "indexName": "vec_idx", "metricType": "L2"},
            {
                "fieldName": "my_struct[no_such_sub]",
                "indexName": "bad",
                "metricType": "L2",
            },
        ]
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] != 0, rsp
        assert "hasn't defined in schema" in rsp.get("message", ""), rsp
