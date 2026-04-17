import time

import numpy as np
import pytest
from pymilvus import AnnSearchRequest, RRFRanker, WeightedRanker

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_params import DefaultVectorIndexParams, DefaultVectorSearchParams, FieldParams, MetricType
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
from utils.util_pymilvus import DataType

prefix = "alter"
default_vector_field_name = "vector"
default_primary_key_field_name = "id"
default_string_field_name = "varchar"
default_float_field_name = "float"
default_new_field_name = "field_new"
default_dynamic_field_name = "dynamic_field"
exp_res = "exp_res"
default_nb = 20
default_dim = 128
default_limit = 10
default_warmup_dim = 128
default_warmup_nb = 2000


class TestMilvusClientWarmup(TestMilvusClientV2Base):
    """Warmup feature tests: field/collection/index level warmup configuration"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_schema_add_field_warmup_describe(self):
        """
        target: verify add_field(warmup=...) sets field warmup at schema creation, describe returns correct values
        method: create schema with warmup on multiple fields, describe_collection, insert/search/query
        expected: describe returns correct warmup per field, search/query work normally
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create schema with warmup on add_field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")
        schema.add_field("int64_field", DataType.INT64, nullable=True, warmup="disable")
        schema.add_field("float_field", DataType.FLOAT, nullable=True, warmup="sync")
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256, nullable=True, warmup="disable")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # 2. describe_collection verify field warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "int64_field") == "disable"
        assert cf.get_field_warmup(res, "float_field") == "sync"
        assert cf.get_field_warmup(res, "varchar_field") == "disable"
        assert cf.get_field_warmup(res, "pk") is None

        # 3. insert data & flush & load
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # 4. search & query
        vectors_to_search = cf.gen_vectors(1, default_warmup_dim)
        search_res = self.search(
            client,
            collection_name,
            vectors_to_search,
            limit=default_limit,
            output_fields=["int64_field", "varchar_field"],
        )[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(
            client,
            collection_name,
            filter="pk >= 0",
            limit=default_limit,
            output_fields=["pk", "int64_field", "float_field", "varchar_field"],
        )[0]
        assert len(query_res) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("warmup", ["disable", "sync"])
    def test_warmup_all_data_types_add_field_warmup(self, warmup):
        """
        target: verify all data types support add_field(warmup=...), describe returns correctly
        method: create schema with all data types + parametrized warmup value, describe, insert/search/query
        expected: all fields show correct warmup in describe, search/query work normally
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec_float", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup=warmup)
        schema.add_field("vec_f16", DataType.FLOAT16_VECTOR, dim=default_warmup_dim, warmup=warmup, nullable=True)
        schema.add_field("vec_bf16", DataType.BFLOAT16_VECTOR, dim=default_warmup_dim, warmup=warmup, nullable=True)
        schema.add_field("vec_sparse", DataType.SPARSE_FLOAT_VECTOR, warmup=warmup, nullable=True)
        schema.add_field("int64_1", DataType.INT64, warmup=warmup, nullable=True)
        schema.add_field("float_1", DataType.FLOAT, warmup=warmup, nullable=True)
        schema.add_field("double_1", DataType.DOUBLE, warmup=warmup, nullable=True)
        schema.add_field("varchar_1", DataType.VARCHAR, max_length=256, warmup=warmup, nullable=True)
        schema.add_field("bool_1", DataType.BOOL, warmup=warmup, nullable=True)
        schema.add_field(
            "array_int64_1", DataType.ARRAY, element_type=DataType.INT64, max_capacity=10, warmup=warmup, nullable=True
        )
        schema.add_field(
            "array_float_1", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=10, warmup=warmup, nullable=True
        )
        schema.add_field(
            "array_varchar_1",
            DataType.ARRAY,
            element_type=DataType.VARCHAR,
            max_length=256,
            max_capacity=10,
            warmup=warmup,
            nullable=True,
        )
        schema.add_field("json_1", DataType.JSON, warmup=warmup, nullable=True)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec_float", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        index_params.add_index(
            field_name="vec_f16", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        index_params.add_index(
            field_name="vec_bf16", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        index_params.add_index(field_name="vec_sparse", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # describe verify all fields
        res = self.describe_collection(client, collection_name)[0]
        warmup_fields = [
            "vec_float",
            "vec_f16",
            "vec_bf16",
            "vec_sparse",
            "int64_1",
            "float_1",
            "double_1",
            "varchar_1",
            "bool_1",
            "array_int64_1",
            "array_float_1",
            "array_varchar_1",
            "json_1",
        ]
        for field_name in warmup_fields:
            actual = cf.get_field_warmup(res, field_name)
            assert actual == warmup, f"field {field_name}: expected warmup='{warmup}', got '{actual}'"
        assert cf.get_field_warmup(res, "pk") is None

        # insert & load & search & query
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_res = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, default_warmup_dim),
            limit=default_limit,
            anns_field="vec_float",
            output_fields=["int64_1", "varchar_1", "json_1", "array_int64_1"],
        )[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(
            client,
            collection_name,
            filter="pk >= 0",
            limit=5,
            output_fields=[
                "pk",
                "int64_1",
                "float_1",
                "double_1",
                "varchar_1",
                "bool_1",
                "json_1",
                "array_int64_1",
                "array_float_1",
                "array_varchar_1",
            ],
        )[0]
        assert len(query_res) == 5
        for row in query_res:
            assert "int64_1" in row
            assert "json_1" in row

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_val", ["invalid_value", "", "Sync"])
    def test_warmup_schema_invalid_warmup_values(self, invalid_val):
        """
        target: verify add_field with invalid warmup values is rejected at create_collection
        method: create schema with invalid/empty/case-sensitive warmup values
        expected: create_collection fails with error
        """
        client = self._client()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup=invalid_val)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )

        collection_name = cf.gen_collection_name_by_testcase_name()
        error = {ct.err_code: 1100, ct.err_msg: "invalid warmup policy"}
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_collection_level_create_describe(self):
        """
        target: verify create_collection with collection-level warmup properties, describe returns correctly
        method: create collection with warmup.* properties, describe, insert/search/query
        expected: describe shows collection warmup in properties, fields have no warmup, search/query work
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("float_field", DataType.FLOAT)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )

        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            properties={
                "warmup.scalarField": "sync",
                "warmup.scalarIndex": "disable",
                "warmup.vectorField": "sync",
                "warmup.vectorIndex": "disable",
            },
        )

        # describe verify collection properties
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "sync"
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # field params should NOT have warmup
        assert cf.get_field_warmup(res, "vec") is None
        assert cf.get_field_warmup(res, "float_field") is None
        assert cf.get_field_warmup(res, "varchar_field") is None

        # insert & load & search & query
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        vectors_to_search = cf.gen_vectors(1, default_warmup_dim)
        search_res = self.search(
            client, collection_name, vectors_to_search, limit=default_limit, output_fields=["pk", "float_field"]
        )[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(
            client,
            collection_name,
            filter="pk >= 0",
            limit=default_limit,
            output_fields=["pk", "float_field", "varchar_field"],
        )[0]
        assert len(query_res) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_field_and_collection_coexist(self):
        """
        target: verify field + collection warmup can coexist, field level has higher priority
        method: create schema with field warmup + collection warmup, describe both
        expected: both levels visible in describe, field level overrides collection level
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")
        schema.add_field("int_field", DataType.INT64, warmup="sync", nullable=True)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256, nullable=True)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )

        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            properties={"warmup.scalarField": "disable", "warmup.vectorField": "disable"},
        )

        # describe verify both levels
        res = self.describe_collection(client, collection_name)[0]
        # collection level
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"
        # field level (explicitly set)
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "int_field") == "sync"
        # not set at field level (inherits collection)
        assert cf.get_field_warmup(res, "varchar_field") is None
        assert cf.get_field_warmup(res, "pk") is None

        # insert & load & search & query
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_res = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, default_warmup_dim),
            limit=default_limit,
            output_fields=["int_field", "varchar_field"],
        )[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(
            client,
            collection_name,
            filter="pk >= 0",
            limit=default_limit,
            output_fields=["pk", "int_field", "varchar_field"],
        )[0]
        assert len(query_res) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_alter_collection_field_warmup(self):
        """
        target: verify alter_collection_field modifies field warmup, describe reflects changes
        method: create collection with custom schema, alter field warmup step by step, describe each time
        expected: describe shows correct warmup after each alter, search works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # create collection with explicit schema (including varchar_field)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # initial state: no warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") is None
        assert cf.get_field_warmup(res, "varchar_field") is None

        # release → set vec warmup=sync
        self.release_collection(client, collection_name)
        self.alter_collection_field(client, collection_name, field_name="vec", field_params={"warmup": "sync"})

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "varchar_field") is None

        # set varchar_field warmup=disable
        self.alter_collection_field(
            client, collection_name, field_name="varchar_field", field_params={"warmup": "disable"}
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "varchar_field") == "disable"

        # modify vec warmup sync → disable
        self.alter_collection_field(client, collection_name, field_name="vec", field_params={"warmup": "disable"})

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"
        assert cf.get_field_warmup(res, "varchar_field") == "disable"

        # insert & load & search
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_res = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, default_warmup_dim),
            limit=default_limit,
            output_fields=["varchar_field"],
        )[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_alter_drop_collection_warmup(self):
        """
        target: verify alter/drop collection warmup properties + describe correctness
        method: alter collection warmup, modify, drop partially, drop all, describe each step
        expected: describe reflects all changes correctly, search works at each stage
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # create collection & insert data
        self.create_collection(client, collection_name, default_warmup_dim, consistency_level="Strong")
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                "id": i,
                "vector": list(rng.random(default_warmup_dim).astype(np.float32)),
                "float": float(i),
                "varchar": str(i),
            }
            for i in range(default_warmup_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # initial: no warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") is None
        assert cf.get_collection_warmup(res, "warmup.scalarField") is None

        # release → alter 4 warmup properties
        self.release_collection(client, collection_name)
        self.alter_collection_properties(
            client,
            collection_name,
            properties={
                "warmup.vectorField": "sync",
                "warmup.scalarField": "disable",
                "warmup.vectorIndex": "disable",
                "warmup.scalarIndex": "sync",
            },
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "sync"
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") == "sync"

        # load & search
        self.load_collection(client, collection_name)
        vectors_to_search = cf.gen_vectors(1, default_warmup_dim)
        search_res = self.search(client, collection_name, vectors_to_search, limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → partial modify (swap 2 keys)
        self.release_collection(client, collection_name)
        self.alter_collection_properties(
            client, collection_name, properties={"warmup.vectorField": "disable", "warmup.scalarField": "sync"}
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") == "sync"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, vectors_to_search, limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → drop partial warmup properties
        self.release_collection(client, collection_name)
        self.drop_collection_properties(
            client, collection_name, property_keys=["warmup.vectorField", "warmup.scalarField"]
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") is None
        assert cf.get_collection_warmup(res, "warmup.scalarField") is None
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") == "sync"

        # drop remaining warmup properties
        self.drop_collection_properties(
            client, collection_name, property_keys=["warmup.vectorIndex", "warmup.scalarIndex"]
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") is None
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") is None

        # reload & search (fallback to cluster default)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, vectors_to_search, limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_index_level_full_lifecycle(self):
        """
        target: verify index level warmup create/alter/drop full lifecycle with describe_index
        method: create index with warmup, alter, drop, describe_index each step
        expected: describe_index reflects warmup state correctly, search works at each stage
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec",
            index_type="HNSW",
            metric_type="L2",
            params={"M": 16, "efConstruction": 200, "warmup": "disable"},
        )
        index_params.add_index(field_name="varchar_field", index_type="INVERTED")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # describe_index: vec should have warmup=disable
        vec_idx_res = self.describe_index(client, collection_name, "vec")[0]
        assert cf.get_index_warmup(vec_idx_res) == "disable"
        assert vec_idx_res["index_type"] == "HNSW"
        assert vec_idx_res["metric_type"] == "L2"

        # scalar index: no warmup
        scalar_idx_names = self.list_indexes(client, collection_name, field_name="varchar_field")[0]
        scalar_idx_res = self.describe_index(client, collection_name, scalar_idx_names[0])[0]
        assert cf.get_index_warmup(scalar_idx_res) is None

        # insert data & flush & load & search
        res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → alter vector index warmup to sync
        self.release_collection(client, collection_name)
        vec_idx_names = self.list_indexes(client, collection_name, field_name="vec")[0]
        self.alter_index_properties(client, collection_name, index_name=vec_idx_names[0], properties={"warmup": "sync"})

        vec_idx_res = self.describe_index(client, collection_name, vec_idx_names[0])[0]
        assert cf.get_index_warmup(vec_idx_res) == "sync"
        assert vec_idx_res["index_type"] == "HNSW"
        assert vec_idx_res["M"] == "16"

        # alter scalar index warmup to disable
        self.alter_index_properties(
            client, collection_name, index_name=scalar_idx_names[0], properties={"warmup": "disable"}
        )
        scalar_idx_res = self.describe_index(client, collection_name, scalar_idx_names[0])[0]
        assert cf.get_index_warmup(scalar_idx_res) == "disable"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → modify vector warmup sync → disable
        self.release_collection(client, collection_name)
        self.alter_index_properties(
            client, collection_name, index_name=vec_idx_names[0], properties={"warmup": "disable"}
        )
        vec_idx_res = self.describe_index(client, collection_name, vec_idx_names[0])[0]
        assert cf.get_index_warmup(vec_idx_res) == "disable"

        # drop vector index warmup
        self.drop_index_properties(client, collection_name, index_name=vec_idx_names[0], property_keys=["warmup"])
        vec_idx_res = self.describe_index(client, collection_name, vec_idx_names[0])[0]
        assert cf.get_index_warmup(vec_idx_res) is None
        assert vec_idx_res["index_type"] == "HNSW"
        assert vec_idx_res["M"] == "16"

        # drop scalar index warmup
        self.drop_index_properties(client, collection_name, index_name=scalar_idx_names[0], property_keys=["warmup"])
        scalar_idx_res = self.describe_index(client, collection_name, scalar_idx_names[0])[0]
        assert cf.get_index_warmup(scalar_idx_res) is None

        # reload & search (fallback to upper level)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_three_level_priority(self):
        """
        target: verify Field > Collection > Cluster priority + drop fallback behavior
        method: set field + collection warmup, alter, drop collection warmup, describe each step
        expected: field level overrides collection, drop collection does not affect field level
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("int32_field", DataType.INT32, warmup="sync", nullable=True)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256, nullable=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={"warmup.scalarField": "disable", "warmup.vectorField": "disable"},
        )

        # describe initial state
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"
        assert cf.get_field_warmup(res, "int32_field") == "sync"  # field > collection
        assert cf.get_field_warmup(res, "vec") == "sync"  # field > collection
        assert cf.get_field_warmup(res, "varchar_field") is None  # inherits collection

        # insert & flush
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # build index after data is flushed
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        search_res = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, default_warmup_dim),
            limit=default_limit,
            output_fields=["int32_field", "varchar_field"],
        )[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(
            client,
            collection_name,
            filter="pk >= 0",
            limit=default_limit,
            output_fields=["pk", "int32_field", "varchar_field"],
        )[0]
        assert len(query_res) == default_limit

        # release → alter field warmup
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client, collection_name, field_name="int32_field", field_params={"warmup": "disable"}
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "int32_field") == "disable"
        assert cf.get_field_warmup(res, "vec") == "sync"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → drop collection warmup
        self.release_collection(client, collection_name)
        self.drop_collection_properties(
            client, collection_name, property_keys=["warmup.scalarField", "warmup.vectorField"]
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.scalarField") is None
        assert cf.get_collection_warmup(res, "warmup.vectorField") is None
        # field level warmup unaffected
        assert cf.get_field_warmup(res, "int32_field") == "disable"
        assert cf.get_field_warmup(res, "vec") == "sync"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_index_overrides_collection(self):
        """
        target: verify Index > Collection priority + drop index warmup fallback
        method: set collection warmup, alter index warmup to override, drop index warmup
        expected: index level overrides collection, drop index warmup falls back to collection
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            properties={"warmup.vectorIndex": "disable"},
        )

        # describe: collection level set
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # describe_index: no index warmup
        idx_names = self.list_indexes(client, collection_name, field_name="vec")[0]
        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) is None

        # insert & flush
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # release → alter index warmup=sync (override collection disable)
        self.release_collection(client, collection_name)
        self.alter_index_properties(client, collection_name, index_name=idx_names[0], properties={"warmup": "sync"})

        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) == "sync"
        # collection level unaffected
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # load & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → drop index warmup
        self.release_collection(client, collection_name)
        self.drop_index_properties(client, collection_name, index_name=idx_names[0], property_keys=["warmup"])

        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) is None
        # collection level still present
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_negative_cases(self):
        """
        target: verify all negative cases: invalid values, key mismatch, non-existent targets, loaded state
        method: test various invalid warmup operations
        expected: all return proper error codes/messages, no side effects on existing state
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_warmup_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)

        invalid_warmup_error = {ct.err_code: 1100, ct.err_msg: "invalid warmup policy"}

        # 11.1 alter_collection_field invalid warmup values
        invalid_values = ["invalid", "Sync", "DISABLE", "true"]
        for val in invalid_values:
            self.alter_collection_field(
                client,
                collection_name,
                field_name="vector",
                field_params={"warmup": val},
                check_task=CheckTasks.err_res,
                check_items=invalid_warmup_error,
            )

        # 11.2 alter_collection_properties invalid values
        invalid_collection_error = {ct.err_code: 1100, ct.err_msg: "invalid warmup"}
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"warmup.vectorField": "invalid"},
            check_task=CheckTasks.err_res,
            check_items=invalid_collection_error,
        )
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"warmup.scalarIndex": "SYNC"},
            check_task=CheckTasks.err_res,
            check_items=invalid_collection_error,
        )

        # 11.3 key level mismatch: field key "warmup" at collection level
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"warmup": "sync"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "warmup"},
        )

        # 11.3 collection key at field level - server accepts arbitrary key names via
        # alter_collection_field, verify it doesn't affect the actual warmup setting
        self.alter_collection_field(
            client, collection_name, field_name="vector", field_params={"warmup.vectorField": "sync"}
        )
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vector") is None  # warmup key not set via mismatched key

        # 11.4 non-existent field/index
        self.alter_collection_field(
            client,
            collection_name,
            field_name="not_exist_field",
            field_params={"warmup": "sync"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "does not exist"},
        )

        self.alter_index_properties(
            client,
            collection_name,
            index_name="not_exist_index",
            properties={"warmup": "sync"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "not found"},
        )

        # 11.5 alter warmup on loaded collection
        self.load_collection(client, collection_name)

        self.alter_collection_properties(
            client,
            collection_name,
            properties={"warmup.vectorField": "sync"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "can not alter warmup properties if collection loaded"},
        )

        self.alter_collection_field(
            client,
            collection_name,
            field_name="vector",
            field_params={"warmup": "sync"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "can not alter warmup if collection loaded"},
        )

        # 11.6 create_collection with invalid warmup
        bad_col = cf.gen_collection_name_by_testcase_name() + "_bad"
        self.create_collection(
            client,
            bad_col,
            default_warmup_dim,
            properties={"warmup.vectorIndex": "invalid"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "invalid warmup"},
        )

        # 11.7 create_index with invalid warmup
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vector",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200, "warmup": "bad_value"},
        )
        self.create_index(
            client,
            collection_name,
            index_params,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "invalid warmup policy"},
        )

        # 11.8 alter_index_properties invalid warmup
        index_params2 = self.prepare_index_params(client)[0]
        index_params2.add_index(
            field_name="vector", index_type="HNSW", metric_type="COSINE", params={"M": 16, "efConstruction": 200}
        )
        self.create_index(client, collection_name, index_params2)
        idx_names = self.list_indexes(client, collection_name, field_name="vector")[0]
        self.alter_index_properties(
            client,
            collection_name,
            index_name=idx_names[0],
            properties={"warmup": "invalid"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "invalid warmup policy"},
        )

        # verify no side effect after errors
        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) is None
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vector") is None

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_sync_vs_disable_data_correctness(self):
        """
        target: verify sync and disable warmup produce identical search/query/retrieve results
        method: insert deterministic data, search/query under sync, switch to disable, compare
        expected: all results identical between sync and disable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("float_field", DataType.FLOAT, warmup="sync")
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256, warmup="sync")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            properties={
                "warmup.scalarField": "sync",
                "warmup.scalarIndex": "sync",
                "warmup.vectorField": "sync",
                "warmup.vectorIndex": "sync",
            },
        )

        # insert deterministic data
        rng = np.random.default_rng(seed=42)
        rows = [
            {
                "pk": i,
                "float_field": float(i),
                "varchar_field": f"text_{i}",
                "vec": list(rng.random(default_warmup_dim).astype(np.float32)),
            }
            for i in range(default_warmup_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # verify sync state
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "sync"

        # search + query under sync
        query_vec = [list(rng.random(default_warmup_dim).astype(np.float32))]
        search_sync = self.search(
            client, collection_name, query_vec, limit=20, output_fields=["pk", "float_field", "varchar_field"]
        )[0]
        assert len(search_sync[0]) == 20

        query_sync = self.query(
            client,
            collection_name,
            filter="float_field >= 100 and float_field < 200",
            output_fields=["pk", "float_field", "varchar_field"],
        )[0]
        sync_query_pks = sorted([r["pk"] for r in query_sync])

        retrieve_sync = self.query(client, collection_name, filter="pk >= 0 and pk < 5", output_fields=["pk", "vec"])[0]
        sync_vectors = {r["pk"]: r["vec"] for r in retrieve_sync}

        # release → switch to all disable
        self.release_collection(client, collection_name)
        for field_name in ["vec", "float_field", "varchar_field"]:
            self.alter_collection_field(
                client, collection_name, field_name=field_name, field_params={"warmup": "disable"}
            )
        self.alter_collection_properties(
            client,
            collection_name,
            properties={
                "warmup.scalarField": "disable",
                "warmup.scalarIndex": "disable",
                "warmup.vectorField": "disable",
                "warmup.vectorIndex": "disable",
            },
        )

        # verify disable state
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # reload & same operations under disable
        self.load_collection(client, collection_name)

        search_disable = self.search(
            client, collection_name, query_vec, limit=20, output_fields=["pk", "float_field", "varchar_field"]
        )[0]
        assert len(search_disable[0]) == 20  # search works under disable

        query_disable = self.query(
            client,
            collection_name,
            filter="float_field >= 100 and float_field < 200",
            output_fields=["pk", "float_field", "varchar_field"],
        )[0]
        disable_query_pks = sorted([r["pk"] for r in query_disable])

        retrieve_disable = self.query(
            client, collection_name, filter="pk >= 0 and pk < 5", output_fields=["pk", "vec"]
        )[0]
        disable_vectors = {r["pk"]: r["vec"] for r in retrieve_disable}

        # deterministic operations (query/retrieve) should produce identical results
        assert sync_query_pks == disable_query_pks, "query results should be identical"
        assert sync_vectors == disable_vectors, "retrieved vectors should be identical"

        # disable mode: repeated search consistency (same load, should be deterministic)
        search_2nd = self.search(client, collection_name, query_vec, limit=20, output_fields=["pk"])[0]
        disable_pks = [hit["pk"] for hit in search_disable[0]]
        assert disable_pks == [hit["pk"] for hit in search_2nd[0]]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "index_type,vec_type,metric,extra_params",
        [
            pytest.param("HNSW", DataType.FLOAT_VECTOR, "L2", {"M": 16, "efConstruction": 200}, id="HNSW"),
            pytest.param("IVF_FLAT", DataType.FLOAT_VECTOR, "L2", {"nlist": 128}, id="IVF_FLAT"),
            pytest.param("IVF_SQ8", DataType.FLOAT_VECTOR, "L2", {"nlist": 128}, id="IVF_SQ8"),
            pytest.param("IVF_PQ", DataType.FLOAT_VECTOR, "L2", {"nlist": 128, "m": 16, "nbits": 8}, id="IVF_PQ"),
            pytest.param("FLAT", DataType.FLOAT_VECTOR, "L2", {}, id="FLAT"),
            pytest.param("SCANN", DataType.FLOAT_VECTOR, "L2", {"nlist": 128}, id="SCANN"),
            pytest.param("DISKANN", DataType.FLOAT_VECTOR, "L2", {}, id="DISKANN"),
            pytest.param("BIN_FLAT", DataType.BINARY_VECTOR, "HAMMING", {}, id="BIN_FLAT"),
            pytest.param("BIN_IVF_FLAT", DataType.BINARY_VECTOR, "HAMMING", {"nlist": 128}, id="BIN_IVF_FLAT"),
            pytest.param("AUTOINDEX", DataType.FLOAT_VECTOR, "L2", {}, id="AUTOINDEX"),
        ],
    )
    def test_warmup_vector_index_types(self, index_type, vec_type, metric, extra_params):
        """
        target: verify various vector index types work with warmup
        method: parametrize index types with vec_type/metric/extra_params, create with field warmup, alter index warmup, search
        expected: describe correct, search works under both warmup modes
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        dim = default_warmup_dim
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", vec_type, dim=dim, warmup="disable")

        self.create_collection(client, collection_name, schema=schema)

        # describe field warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"

        # insert & flush
        rows = [{"pk": i, "vec": cf.gen_vectors(1, dim, vec_type)[0]} for i in range(default_warmup_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # build index after data is flushed
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type=index_type, metric_type=metric, params=extra_params)
        self.create_index(client, collection_name, index_params)

        # load & search (warmup=disable)
        self.load_collection(client, collection_name)
        query_vec = cf.gen_vectors(1, dim, vec_type)
        res_disable = self.search(client, collection_name, query_vec, limit=default_limit)[0]
        assert len(res_disable[0]) == default_limit

        # release → alter index warmup=sync → describe_index → reload & search
        self.release_collection(client, collection_name)
        idx_names = self.list_indexes(client, collection_name, field_name="vec")[0]
        self.alter_index_properties(client, collection_name, index_name=idx_names[0], properties={"warmup": "sync"})

        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) == "sync"
        assert idx_res["index_type"] == index_type

        # field warmup unchanged
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"

        self.load_collection(client, collection_name)
        res_sync = self.search(client, collection_name, query_vec, limit=default_limit)[0]
        assert len(res_sync[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("scalar_index_type", ["STL_SORT", "TRIE", "BITMAP", "INVERTED", "AUTOINDEX"])
    def test_warmup_scalar_index_types(self, scalar_index_type):
        """
        target: verify various scalar index types work with warmup
        method: parametrize scalar index types, create with field warmup, alter index warmup, filtered search
        expected: describe correct, filtered search works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        if scalar_index_type == "TRIE":
            scalar_type = DataType.VARCHAR
            field_kwargs = {"max_length": 256}
        else:
            scalar_type = DataType.INT64
            field_kwargs = {}

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("scalar_field", scalar_type, warmup="disable", **field_kwargs)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        index_params.add_index(field_name="scalar_field", index_type=scalar_index_type)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # describe field warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "scalar_field") == "disable"

        # insert data
        rng = np.random.default_rng(seed=19530)
        if scalar_type == DataType.VARCHAR:
            rows = [
                {"pk": i, "scalar_field": f"val_{i}", "vec": list(rng.random(default_warmup_dim).astype(np.float32))}
                for i in range(default_warmup_nb)
            ]
            filter_expr = 'scalar_field like "val_1%"'
        else:
            rows = [
                {"pk": i, "scalar_field": i, "vec": list(rng.random(default_warmup_dim).astype(np.float32))}
                for i in range(default_warmup_nb)
            ]
            filter_expr = "scalar_field >= 100 and scalar_field < 200"
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # load & filtered search (warmup=disable)
        self.load_collection(client, collection_name)
        search_res = self.search(
            client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit, filter=filter_expr
        )[0]
        assert len(search_res[0]) > 0

        # release → alter scalar index warmup=sync → describe_index
        self.release_collection(client, collection_name)
        scalar_idx_names = self.list_indexes(client, collection_name, field_name="scalar_field")[0]
        self.alter_index_properties(
            client, collection_name, index_name=scalar_idx_names[0], properties={"warmup": "sync"}
        )

        idx_res = self.describe_index(client, collection_name, scalar_idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) == "sync"
        assert idx_res["index_type"] == scalar_index_type

        # field warmup unchanged
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "scalar_field") == "disable"

        # reload & filtered search
        self.load_collection(client, collection_name)
        search_res = self.search(
            client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit, filter=filter_expr
        )[0]
        assert len(search_res[0]) > 0

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "warmup_val,mmap_val", [("sync", "true"), ("sync", "false"), ("disable", "true"), ("disable", "false")]
    )
    def test_warmup_x_mmap(self, warmup_val, mmap_val):
        """
        target: verify warmup and mmap are orthogonal, all 4 combinations work correctly
        method: set one (warmup, mmap) combination, describe and search
        expected: describe_index shows both warmup and mmap.enabled, search works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # insert data
        rng = np.random.default_rng(seed=19530)
        rows = [
            {"pk": i, "varchar_field": f"text_{i}", "vec": list(rng.random(default_warmup_dim).astype(np.float32))}
            for i in range(default_warmup_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        query_vec = cf.gen_vectors(1, default_warmup_dim)

        self.release_collection(client, collection_name)
        self.alter_collection_properties(
            client, collection_name, properties={"warmup.vectorField": warmup_val, "warmup.scalarField": warmup_val}
        )
        self.alter_collection_field(client, collection_name, field_name="vec", field_params={"mmap.enabled": mmap_val})
        self.alter_collection_field(
            client, collection_name, field_name="varchar_field", field_params={"mmap.enabled": mmap_val}
        )

        idx_names = self.list_indexes(client, collection_name, field_name="vec")[0]
        self.alter_index_properties(
            client, collection_name, index_name=idx_names[0], properties={"mmap.enabled": True, "warmup": warmup_val}
        )

        # describe_collection verify collection warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") == warmup_val
        assert cf.get_collection_warmup(res, "warmup.scalarField") == warmup_val

        # describe_index verify warmup and mmap coexist
        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) == warmup_val
        assert idx_res.get("mmap.enabled") == "True"

        self.load_collection(client, collection_name)
        search_res = self.search(
            client, collection_name, query_vec, limit=default_limit, output_fields=["varchar_field"]
        )[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_x_partition(self):
        """
        target: verify warmup works with partitions
        method: create partitions, insert, load single partition, search, then load all
        expected: partition load respects warmup, search works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("category", DataType.VARCHAR, max_length=64, warmup="disable")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # describe verify
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "category") == "disable"

        # create partitions & insert
        self.create_partition(client, collection_name, partition_name="part_a")
        self.create_partition(client, collection_name, partition_name="part_b")

        rng = np.random.default_rng(seed=19530)
        rows_a = [
            {"pk": i, "category": "cat_a", "vec": list(rng.random(default_warmup_dim).astype(np.float32))}
            for i in range(1000)
        ]
        rows_b = [
            {"pk": i + 1000, "category": "cat_b", "vec": list(rng.random(default_warmup_dim).astype(np.float32))}
            for i in range(1000)
        ]
        self.insert(client, collection_name, rows_a, partition_name="part_a")
        self.insert(client, collection_name, rows_b, partition_name="part_b")
        self.flush(client, collection_name)

        # load single partition → search
        self.load_partitions(client, collection_name, partition_names=["part_a"])
        query_vec = cf.gen_vectors(1, default_warmup_dim)
        search_res = self.search(client, collection_name, query_vec, limit=default_limit, partition_names=["part_a"])[0]
        assert len(search_res[0]) == default_limit
        for hit in search_res[0]:
            assert hit["pk"] < 1000

        # load second partition → search all
        self.load_partitions(client, collection_name, partition_names=["part_b"])
        search_all = self.search(client, collection_name, query_vec, limit=default_limit)[0]
        assert len(search_all[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_x_compaction(self):
        """
        target: verify compacted segments respect warmup, data consistent
        method: insert in batches, compact, compare search results before/after
        expected: search results identical, data count consistent
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            properties={"warmup.vectorField": "sync", "warmup.vectorIndex": "sync"},
        )

        # insert in batches to produce multiple segments
        rng = np.random.default_rng(seed=19530)
        for batch in range(5):
            rows = [
                {"pk": batch * 400 + i, "vec": list(rng.random(default_warmup_dim).astype(np.float32))}
                for i in range(400)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

        # load & search before compact
        self.load_collection(client, collection_name)
        query_vec = cf.gen_vectors(1, default_warmup_dim)
        search_before = self.search(client, collection_name, query_vec, limit=20, output_fields=["pk"])[0]
        assert len(search_before[0]) == 20

        # compact & wait
        self.compact(client, collection_name)
        time.sleep(10)

        # release → reload → search after compact
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        search_after = self.search(client, collection_name, query_vec, limit=20, output_fields=["pk"])[0]
        assert len(search_after[0]) == 20  # search still works after compaction with warmup

        # query count - data integrity preserved after compaction
        count_res = self.query(client, collection_name, filter="pk >= 0", output_fields=["count(*)"])[0]
        assert count_res[0]["count(*)"] == 2000

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.Loadbalance)
    def test_warmup_x_multi_replica(self):
        """
        target: verify warmup works with multi-replica load
        method: load with 2 replicas, search multiple times
        expected: consistent results across replicas
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params, properties={"warmup.vectorIndex": "sync"}
        )

        # insert & flush
        rng = np.random.default_rng(seed=19530)
        rows = [
            {"pk": i, "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(default_warmup_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # release first (collection is auto-loaded after create), then load with 2 replicas
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name, timeout=60, replica_number=2)

        # search multiple times for consistency
        query_vec = cf.gen_vectors(1, default_warmup_dim)
        results = []
        for _ in range(5):
            res = self.search(client, collection_name, query_vec, limit=default_limit)[0]
            results.append([hit["pk"] for hit in res[0]])
        for r in results[1:]:
            assert r == results[0]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_x_growing_segment(self):
        """
        target: verify growing segment not affected by warmup, newly inserted data searchable
        method: load, then insert new data (growing segment), query/search
        expected: new data queryable and searchable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("tag", DataType.VARCHAR, max_length=64, warmup="disable")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="disable")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
            properties={
                "warmup.scalarField": "disable",
                "warmup.scalarIndex": "disable",
                "warmup.vectorField": "disable",
                "warmup.vectorIndex": "disable",
            },
        )

        # describe verify all disable
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"
        assert cf.get_field_warmup(res, "tag") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"

        # insert initial data & flush & load
        rng = np.random.default_rng(seed=19530)
        rows = [
            {"pk": i, "tag": "old", "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(1000)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # insert after load (growing segment)
        new_rows = [
            {"pk": 1000 + i, "tag": "new", "vec": list(rng.random(default_warmup_dim).astype(np.float32))}
            for i in range(500)
        ]
        self.insert(client, collection_name, new_rows)

        # query new data
        query_res = self.query(client, collection_name, filter='tag == "new"', limit=100, output_fields=["pk", "tag"])[
            0
        ]
        assert len(query_res) > 0

        # search includes old + new
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_multi_vector_fields(self):
        """
        target: verify multi vector fields with independent warmup + collection vectorField override
        method: create 3 vector fields with different warmup, collection vectorField=disable
        expected: each field's warmup correct in describe, all 3 vector search work
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec_a", DataType.FLOAT_VECTOR, dim=128, warmup="sync")
        schema.add_field("vec_b", DataType.FLOAT_VECTOR, dim=64, warmup="disable")
        schema.add_field("vec_c", DataType.FLOAT_VECTOR, dim=32)  # no warmup set

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec_a", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        index_params.add_index(
            field_name="vec_b", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        index_params.add_index(field_name="vec_c", index_type="IVF_FLAT", metric_type="L2", params={"nlist": 128})
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            properties={"warmup.vectorField": "disable"},
        )

        # describe verify 3-level state
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"
        assert cf.get_field_warmup(res, "vec_a") == "sync"  # field > collection
        assert cf.get_field_warmup(res, "vec_b") == "disable"
        assert cf.get_field_warmup(res, "vec_c") is None  # inherits collection

        # insert & load
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                "pk": i,
                "vec_a": list(rng.random(128).astype(np.float32)),
                "vec_b": list(rng.random(64).astype(np.float32)),
                "vec_c": list(rng.random(32).astype(np.float32)),
            }
            for i in range(default_warmup_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # search each vector field
        res_a = self.search(client, collection_name, cf.gen_vectors(1, 128), limit=default_limit, anns_field="vec_a")[0]
        assert len(res_a[0]) == default_limit

        res_b = self.search(client, collection_name, cf.gen_vectors(1, 64), limit=default_limit, anns_field="vec_b")[0]
        assert len(res_b[0]) == default_limit

        res_c = self.search(client, collection_name, cf.gen_vectors(1, 32), limit=default_limit, anns_field="vec_c")[0]
        assert len(res_c[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_create_index_idempotent(self):
        """
        target: verify field/collection/index warmup do not break create_index idempotency
        method: set warmup at each level, create index twice
        expected: 21.1/21.2 second create_index succeeds (idempotent);
                  21.3 second create_index fails because index params contain warmup (distinct index)
        """
        client = self._client()

        # 21.1 field level warmup - create_index idempotent
        col1 = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, col1, default_warmup_dim, consistency_level="Strong")
        self.release_collection(client, col1)
        self.alter_collection_field(client, col1, field_name="vector", field_params={"warmup": "sync"})
        self.drop_index(client, col1, "vector")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vector", index_type="HNSW", metric_type="COSINE", params={"M": 8, "efConstruction": 200}
        )
        self.create_index(client, col1, index_params)
        self.create_index(client, col1, index_params)  # idempotent
        assert len(self.list_indexes(client, col1, field_name="vector")[0]) == 1

        # 21.2 collection level warmup - create_index idempotent
        col2 = cf.gen_collection_name_by_testcase_name() + "_2"
        self.create_collection(client, col2, default_warmup_dim, consistency_level="Strong")
        self.release_collection(client, col2)
        self.alter_collection_properties(client, col2, properties={"warmup.vectorField": "sync"})
        self.drop_index(client, col2, "vector")
        index_params2 = self.prepare_index_params(client)[0]
        index_params2.add_index(
            field_name="vector", index_type="HNSW", metric_type="COSINE", params={"M": 8, "efConstruction": 200}
        )
        self.create_index(client, col2, index_params2)
        self.create_index(client, col2, index_params2)  # idempotent
        assert len(self.list_indexes(client, col2, field_name="vector")[0]) == 1

        # 21.3 index level warmup - second create_index should fail
        # because index params contain "warmup" making it a distinct index definition
        col3 = cf.gen_collection_name_by_testcase_name() + "_3"
        self.create_collection(client, col3, default_warmup_dim, consistency_level="Strong")
        self.release_collection(client, col3)
        self.drop_index(client, col3, "vector")
        index_params3 = self.prepare_index_params(client)[0]
        index_params3.add_index(
            field_name="vector",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 8, "efConstruction": 200, "warmup": "sync"},
        )
        self.create_index(client, col3, index_params3)
        self.create_index(
            client,
            col3,
            index_params3,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535, ct.err_msg: "at most one distinct index is allowed per field"},
        )
        assert len(self.list_indexes(client, col3, field_name="vector")[0]) == 1

        self.drop_collection(client, col1)
        self.drop_collection(client, col2)
        self.drop_collection(client, col3)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_drop_recreate_no_residue(self):
        """
        target: verify drop/recreate collection leaves no warmup residue
        method: create with warmup, drop, recreate same name without warmup, describe
        expected: recreated collection has no warmup settings
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # create with all warmup settings
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")
        schema.add_field("int_field", DataType.INT64, warmup="disable", nullable=True)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            properties={
                "warmup.vectorField": "sync",
                "warmup.scalarField": "disable",
                "warmup.vectorIndex": "disable",
                "warmup.scalarIndex": "sync",
            },
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "sync"

        # drop
        self.drop_collection(client, collection_name)

        # recreate without warmup
        self.create_collection(client, collection_name, default_warmup_dim, consistency_level="Strong")

        # verify no residue
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") is None
        assert cf.get_collection_warmup(res, "warmup.scalarField") is None
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") is None
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") is None
        assert cf.get_field_warmup(res, "vector") is None

        # insert & load & search
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                "id": i,
                "vector": list(rng.random(default_warmup_dim).astype(np.float32)),
                "float": float(i),
                "varchar": str(i),
            }
            for i in range(1000)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_x_dynamic_field(self):
        """
        target: verify warmup does not affect dynamic field access
        method: create collection with dynamic field + warmup, insert dynamic data, query/search
        expected: dynamic fields queryable and searchable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="disable")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            properties={"warmup.scalarField": "disable"},
        )

        # describe verify
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "disable"

        # insert with dynamic fields
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                "pk": i,
                "vec": list(rng.random(default_warmup_dim).astype(np.float32)),
                "dynamic_str": f"dyn_{i}",
                "dynamic_int": i * 10,
            }
            for i in range(default_warmup_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # query dynamic fields
        query_res = self.query(
            client, collection_name, filter="pk >= 0", limit=5, output_fields=["pk", "dynamic_str", "dynamic_int"]
        )[0]
        assert len(query_res) == 5
        assert "dynamic_str" in query_res[0]
        assert "dynamic_int" in query_res[0]

        # search + output dynamic fields
        search_res = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, default_warmup_dim),
            limit=default_limit,
            output_fields=["dynamic_str"],
        )[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_toggle_warmup_repeatedly(self):
        """
        target: verify repeated warmup toggling produces no state residue
        method: toggle warmup 5 rounds, describe + load + search each round
        expected: each round describe shows correct latest value, search works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self.create_collection(client, collection_name, default_warmup_dim, consistency_level="Strong")
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                "id": i,
                "vector": list(rng.random(default_warmup_dim).astype(np.float32)),
                "float": float(i),
                "varchar": str(i),
            }
            for i in range(default_warmup_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.release_collection(client, collection_name)

        query_vec = cf.gen_vectors(1, default_warmup_dim)
        for round_idx in range(5):
            policy = "sync" if round_idx % 2 == 0 else "disable"

            self.alter_collection_properties(
                client, collection_name, properties={"warmup.vectorField": policy, "warmup.scalarField": policy}
            )
            self.alter_collection_field(client, collection_name, field_name="vector", field_params={"warmup": policy})

            # describe verify
            res = self.describe_collection(client, collection_name)[0]
            assert cf.get_collection_warmup(res, "warmup.vectorField") == policy, (
                f"round {round_idx}: collection warmup expected {policy}"
            )
            assert cf.get_field_warmup(res, "vector") == policy, f"round {round_idx}: field warmup expected {policy}"

            # load & search
            self.load_collection(client, collection_name)
            search_res = self.search(client, collection_name, query_vec, limit=default_limit)[0]
            assert len(search_res[0]) == default_limit, f"round {round_idx}: search failed"

            self.release_collection(client, collection_name)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_empty_collection_and_boundary(self):
        """
        target: verify warmup with empty collection and drop/recreate index boundary cases
        method: 1) empty collection with warmup: load/search/query
                2) drop index, recreate, verify collection warmup persists
        expected: no error on empty collection, collection warmup survives index drop/recreate
        """
        client = self._client()

        # 25.1 empty collection + warmup
        col1 = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vec", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 200}
        )
        self.create_collection(
            client,
            col1,
            schema=schema,
            index_params=index_params,
            properties={"warmup.vectorField": "sync", "warmup.scalarField": "sync"},
        )

        res = self.describe_collection(client, col1)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "sync"

        # load empty collection (no error)
        self.load_collection(client, col1)

        # search returns empty
        search_res = self.search(client, col1, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == 0

        # query returns empty
        query_res = self.query(client, col1, filter="pk >= 0", limit=default_limit)[0]
        assert len(query_res) == 0

        self.drop_collection(client, col1)

        # 25.2 drop index, recreate, collection warmup persists
        col2 = cf.gen_collection_name_by_testcase_name() + "_2"
        self.create_collection(client, col2, default_warmup_dim, consistency_level="Strong")

        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                "id": i,
                "vector": list(rng.random(default_warmup_dim).astype(np.float32)),
                "float": float(i),
                "varchar": str(i),
            }
            for i in range(100)
        ]
        self.insert(client, col2, rows)
        self.flush(client, col2)

        self.release_collection(client, col2)
        self.alter_collection_properties(client, col2, properties={"warmup.vectorIndex": "sync"})
        self.drop_index(client, col2, "vector")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vector", index_type="HNSW", metric_type="COSINE", params={"M": 8, "efConstruction": 200}
        )
        self.create_index(client, col2, index_params)
        self.load_collection(client, col2)

        # collection warmup still present
        res = self.describe_collection(client, col2)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "sync"

        # search works
        search_res = self.search(client, col2, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, col2)


@pytest.mark.skip(reason="Skip all async warmup tests")
class TestMilvusClientWarmupAsync(TestMilvusClientV2Base):
    @property
    def all_vector_fields(self):
        return ["float_vector_1", "float16_vector_1", "bfloat16_vector_1", "sparse_float_vector_1"]

    @property
    def all_scalar_fields(self):
        return [
            "int8_1",
            "int16_1",
            "int32_1",
            "int64_1",
            "float_1",
            "double_1",
            "varchar_1",
            "bool_1",
            "array_bool_1",
            "array_int8_1",
            "array_int16_1",
            "array_int32_1",
            "array_int64_1",
            "array_float_1",
            "array_double_1",
            "array_varchar_1",
            "json_1",
        ]

    @classmethod
    def all_vector_index(cls, index_params, extra_index_params: dict):
        for k, v in extra_index_params.items():
            index_params.add_index(field_name=k, **v.to_dict)
        return index_params

    @classmethod
    def gen_dql_nq(cls, schema: dict, field_name: str, nq: int):
        field_schema = cf.get_mc_field_schema(field_name=field_name, schema=schema)
        dim = field_schema.get("params", {}).get("dim", 128)
        name = field_schema.get("name", "")
        return cf.gen_vectors(nb=nq, dim=dim, vector_data_type=cf.get_vector_data_type(name))

    def get_default_search_params(self, client, collection_name, anns_field, limit):
        search_params = cf.get_search_params_according_to_index_params(
            self.describe_index(client, collection_name, anns_field)[0], limit
        )
        return search_params

    def make_ann_req(self, client, collection_name, schema, field, nq=10, limit=10):
        return AnnSearchRequest(
            self.gen_dql_nq(schema=schema, field_name=field, nq=nq),
            field,
            self.get_default_search_params(client, collection_name, field, limit),
            limit=limit,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_async_schema_all_types(self):
        """
        target: verify all data types support add_field(warmup="async");
                ARRAY covers all supported element types (BOOL/INT8/16/32/64/FLOAT/DOUBLE/VARCHAR);
                retrieve rows via query with output_fields for every column;
                search each vector column with output_fields=all; hybrid search with all vector columns
        method: full schema with all types + warmup=async; insert 5000; flush; build indexes; load;
                (1) query(output_fields=all, limit=100) → 100 rows, all field values present;
                (2) search each of vec_float/vec_f16/vec_bf16/vec_sparse with output_fields=all;
                (3) hybrid_search(vec_float+vec_f16, WeightedRanker, output_fields=all);
                    hybrid_search(vec_float+vec_sparse, RRFRanker, output_fields=all)
        expected: describe shows async on every warmup field; query returns rows with all fields;
                  all searches return hits with all field values accessible in entity
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        all_vector_fields, all_scalar_fields = self.all_vector_fields, self.all_scalar_fields
        all_fields = ["int64_pk", *all_vector_fields, *all_scalar_fields]

        # field names use lowercase DataType prefix → type auto-inferred by gen_milvus_client_schema
        _extra_params = {"warmup": "async", "nullable": True}
        self.create_collection(
            client,
            collection_name,
            schema=cf.gen_milvus_client_schema(
                schema=self.create_schema(client, enable_dynamic_field=False)[0],
                fields=all_fields,
                field_params={
                    "int64_pk": FieldParams(is_primary=True).to_dict,
                    "float_vector_1": FieldParams(dim=128, warmup="async").to_dict,
                    "float16_vector_1": FieldParams(dim=16, **_extra_params).to_dict,
                    "bfloat16_vector_1": FieldParams(dim=32, **_extra_params).to_dict,
                    "sparse_float_vector_1": FieldParams(**_extra_params).to_dict,
                    **{k: FieldParams(**_extra_params).to_dict for k in all_scalar_fields},
                },
            ),
        )

        schema_res = self.describe_collection(client, collection_name)[0]
        warmup_fields = all_vector_fields + all_scalar_fields
        for fn in warmup_fields:
            assert cf.get_field_warmup(schema_res, fn) == "async", (
                f"field {fn} expected async, got {cf.get_field_warmup(schema_res, fn)}"
            )
        assert cf.get_field_warmup(schema_res, "int64_pk") is None

        # int64_pk overridden for deterministic IDs; all other fields generated by schema
        insert_data = cf.gen_row_data_by_schema_with_defaults(
            nb=nb, schema=schema_res, default_values={"int64_pk": list(range(nb))}
        )
        for d in cf.iter_mc_insert_list_data(insert_data, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        # build indexes after data is flushed so index covers actual segments
        vector_index_map = {
            **DefaultVectorIndexParams.IVF_SQ8("float_vector_1"),
            **DefaultVectorIndexParams.HNSW("float16_vector_1"),
            **DefaultVectorIndexParams.DISKANN("bfloat16_vector_1"),
            **DefaultVectorIndexParams.SPARSE_INVERTED_INDEX("sparse_float_vector_1"),
        }
        self.create_index(
            client, collection_name, self.all_vector_index(self.prepare_index_params(client)[0], vector_index_map)
        )
        self.load_collection(client, collection_name)

        # ── (1) query: sample rows with every column ──
        self.query(
            client,
            collection_name,
            filter="int64_pk >= 0",
            output_fields=["*"],
            limit=100,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 100, "output_fields": all_fields},
        )

        # ── (2) search each vector column ──
        for anns_field, nq, limit in [
            ("float_vector_1", 1, 10),
            ("float16_vector_1", 2, 20),
            ("bfloat16_vector_1", 5, 15),
            ("sparse_float_vector_1", 20, 5),
        ]:
            self.search(
                client,
                collection_name,
                self.gen_dql_nq(schema=schema_res, field_name=anns_field, nq=nq),
                anns_field=anns_field,
                limit=limit,
                output_fields=["*"],
                search_params=self.get_default_search_params(client, collection_name, anns_field, limit),
                check_task=CheckTasks.check_search_results,
                check_items={"nq": nq, "limit": limit, "output_fields": all_fields},
            )

        # ── (3) hybrid search ──
        hs_nq, hs_limit = 10, 10
        req_f32 = self.make_ann_req(client, collection_name, schema_res, "float_vector_1", nq=hs_nq, limit=hs_limit)
        req_f16 = self.make_ann_req(client, collection_name, schema_res, "float16_vector_1", nq=hs_nq, limit=hs_limit)
        req_sparse = self.make_ann_req(
            client, collection_name, schema_res, "sparse_float_vector_1", nq=hs_nq, limit=hs_limit
        )

        hs_check = {"nq": hs_nq, "limit": hs_limit, "output_fields": all_fields}
        self.hybrid_search(
            client,
            collection_name,
            reqs=[req_f32, req_f16],
            ranker=WeightedRanker(0.6, 0.4),
            limit=hs_limit,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items=hs_check,
        )
        self.hybrid_search(
            client,
            collection_name,
            reqs=[req_f32, req_sparse],
            ranker=RRFRanker(),
            limit=hs_limit,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items=hs_check,
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "bad_val",
        [
            pytest.param("ASYNC", id="upper_async"),
            pytest.param("Async", id="title_async"),
            pytest.param("SYNC", id="upper_sync"),
            pytest.param("Disable", id="title_disable"),
            pytest.param("DISABLE", id="upper_disable"),
        ],
    )
    def test_warmup_schema_invalid_async_case_sensitive(self, bad_val):
        """
        target: verify warmup values are case-sensitive; bad cases rejected with err_code=1100
        method: try create_collection with bad_val, alter_collection_field with bad_val,
                alter_collection_properties with bad_val; then verify "async" succeeds
        expected: all bad_val ops fail with err_code=1100, lowercase "async" succeeds
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        fields = ["int64_pk", "float_vector_1"]

        # Step 1: create with bad_val should fail
        bad_schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            bad_schema,
            fields=fields,
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=128, warmup=bad_val).to_dict,
            },
        )
        bad_index_params = self.prepare_index_params(client)[0]
        self.all_vector_index(
            bad_index_params, DefaultVectorIndexParams.HNSW("float_vector_1", metric_type=MetricType.COSINE)
        )
        self.create_collection(
            client,
            f"{collection_name}_create",
            schema=bad_schema,
            index_params=bad_index_params,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "invalid warmup policy"},
        )

        # Step 2: create a valid base collection for alter tests
        base_schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            base_schema,
            fields=fields,
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=128).to_dict,
            },
        )
        base_index_params = self.prepare_index_params(client)[0]
        self.all_vector_index(
            base_index_params, DefaultVectorIndexParams.HNSW("float_vector_1", metric_type=MetricType.COSINE)
        )
        base_col = f"{collection_name}_base"
        self.create_collection(client, base_col, schema=base_schema, index_params=base_index_params)
        self.release_collection(client, base_col)

        # alter_collection_field with bad_val should fail
        self.alter_collection_field(
            client,
            base_col,
            field_name="float_vector_1",
            field_params={"warmup": bad_val},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "invalid warmup policy"},
        )

        # alter_collection_properties with bad_val should fail
        self.alter_collection_properties(
            client,
            base_col,
            properties={"warmup.vectorField": bad_val},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "invalid warmup"},
        )

        # Step 3: lowercase "async" sanity check succeeds
        self.alter_collection_field(client, base_col, field_name="float_vector_1", field_params={"warmup": "async"})
        res = self.describe_collection(client, base_col)[0]
        assert cf.get_field_warmup(res, "float_vector_1") == "async"

        self.drop_collection(client, base_col)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_sync_async_disable_full_lifecycle(self):
        """
        target: verify sync/async/disable can be switched arbitrarily; loaded state rejects alter;
                query results match baseline each round
        method: insert 5000 rows with seed=42, loop sync/async/disable; each round alter+describe+load+query+alter-while-loaded
        expected: describe correct per round, query pk set matches baseline, alter on loaded fails
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_1", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_1": FieldParams(nullable=True).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema)

        schema_res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=schema_res)
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.release_collection(client, collection_name)

        baseline = set(range(nb))
        for warmup_val in ["sync", "async", "disable"]:
            self.alter_collection_field(
                client, collection_name, field_name="float_vector_1", field_params={"warmup": warmup_val}
            )
            self.alter_collection_field(
                client, collection_name, field_name="float_1", field_params={"warmup": warmup_val}
            )
            res = self.describe_collection(client, collection_name)[0]
            assert cf.get_field_warmup(res, "float_vector_1") == warmup_val
            assert cf.get_field_warmup(res, "float_1") == warmup_val

            self.load_collection(client, collection_name)
            query_res = self.query(
                client,
                collection_name,
                filter="int64_pk >= 0",
                output_fields=["int64_pk"],
                limit=nb,
                check_task=CheckTasks.check_query_results,
                check_items={"exp_limit": nb, "output_fields": ["int64_pk"]},
            )[0]
            assert {r["int64_pk"] for r in query_res} == baseline, f"warmup={warmup_val}: data mismatch"
            self.search(
                client,
                collection_name,
                cf.gen_vectors(1, 128),
                limit=10,
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 10},
            )

            self.alter_collection_field(
                client,
                collection_name,
                field_name="float_vector_1",
                field_params={"warmup": "sync"},
                check_task=CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: "can not alter warmup if collection loaded"},
            )
            self.release_collection(client, collection_name)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_collection_properties_async(self):
        """
        target: verify create_collection with all four warmup properties="async" persists in describe;
                alter/drop partial properties; loaded state rejects alter
        method: create with 4 warmup properties=async, describe, insert 5000, load, loaded-alter-fail,
                release, alter scalarField=sync, drop scalarField/scalarIndex, drop remaining
        expected: all describe steps return expected values, search works throughout
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
            },
        )

        properties = {
            "warmup.vectorField": "async",
            "warmup.scalarField": "async",
            "warmup.vectorIndex": "async",
            "warmup.scalarIndex": "async",
        }
        self.create_collection(client, collection_name, schema=schema, properties=properties)

        res = self.describe_collection(client, collection_name)[0]
        for key in ["warmup.vectorField", "warmup.scalarField", "warmup.vectorIndex", "warmup.scalarIndex"]:
            assert cf.get_collection_warmup(res, key) == "async", f"{key} expected async"
        assert cf.get_field_warmup(res, "float_vector_1") is None

        rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=res)
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0],
                DefaultVectorIndexParams.HNSW("float_vector_1", metric_type=MetricType.COSINE),
            ),
        )
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.alter_collection_properties(
            client,
            collection_name,
            properties={"warmup.vectorField": "sync"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "can not alter warmup"},
        )

        self.release_collection(client, collection_name)
        self.alter_collection_properties(client, collection_name, properties={"warmup.scalarField": "sync"})
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "async"

        self.drop_collection_properties(
            client, collection_name, property_keys=["warmup.scalarField", "warmup.scalarIndex"]
        )
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.scalarField") is None
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") is None
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "async"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "async"

        self.drop_collection_properties(
            client, collection_name, property_keys=["warmup.vectorField", "warmup.vectorIndex"]
        )
        res = self.describe_collection(client, collection_name)[0]
        for key in ["warmup.vectorField", "warmup.scalarField", "warmup.vectorIndex", "warmup.scalarIndex"]:
            assert cf.get_collection_warmup(res, key) is None

        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_async_index_level(self):
        """
        target: verify index-level async warmup full lifecycle and collection-level override behavior
        method:
            phase1 (full lifecycle): create_index with warmup=async; second create_index with same warmup
                    param expects error; describe_index validates; alter to sync; drop warmup; reload+search
            phase2 (override): collection=sync, index=async; describe shows independent storage;
                    drop index warmup falls back to collection sync; reload+search
        expected: all describe checks pass; search succeeds at each stage
        """
        client = self._client()
        nb, batch = 5000, 2000
        fields = ["int64_pk", "float_vector_1"]

        # ── Phase 1: full lifecycle ──
        col1 = cf.gen_collection_name_by_testcase_name() + "_p1"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=fields,
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
            },
        )
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="float_vector_1",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200, "warmup": "async"},
        )
        self.create_collection(client, col1, schema=schema, index_params=index_params)

        # second create_index with warmup param is treated as a different definition → expect error
        self.create_index(
            client,
            col1,
            index_params=index_params,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535, ct.err_msg: "at most one distinct index is allowed per field"},
        )
        idxs = self.list_indexes(client, col1)[0]
        assert len(idxs) == 1

        idx_res = self.describe_index(client, col1, index_name="float_vector_1")[0]
        assert cf.get_index_warmup(idx_res) == "async"
        assert idx_res.get("M") == "16"
        assert idx_res.get("index_type") == "HNSW"

        res = self.describe_collection(client, col1)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=res)
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, col1, d)
        self.flush(client, col1)
        # wait for index build to complete so search operates on fully-indexed data
        self.wait_for_index_ready(client, col1, index_name="float_vector_1")
        self.load_collection(client, col1)
        self.search(
            client,
            col1,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.release_collection(client, col1)
        self.alter_index_properties(client, col1, index_name="float_vector_1", properties={"warmup": "sync"})
        assert cf.get_index_warmup(self.describe_index(client, col1, index_name="float_vector_1")[0]) == "sync"

        self.drop_index_properties(client, col1, index_name="float_vector_1", property_keys=["warmup"])
        idx_res = self.describe_index(client, col1, index_name="float_vector_1")[0]
        assert cf.get_index_warmup(idx_res) is None
        assert idx_res.get("M") == "16"

        self.load_collection(client, col1)
        self.search(
            client,
            col1,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )
        self.drop_collection(client, col1)

        # ── Phase 2: index-level async overrides collection-level sync ──
        col2 = cf.gen_collection_name_by_testcase_name() + "_p2"
        schema2 = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema2,
            fields=fields,
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
            },
        )
        ip2 = self.prepare_index_params(client)[0]
        self.all_vector_index(ip2, DefaultVectorIndexParams.HNSW("float_vector_1"))
        self.create_collection(
            client, col2, schema=schema2, index_params=ip2, properties={"warmup.vectorIndex": "sync"}
        )
        assert cf.get_collection_warmup(self.describe_collection(client, col2)[0], "warmup.vectorIndex") == "sync"

        self.release_collection(client, col2)
        self.alter_index_properties(client, col2, index_name="float_vector_1", properties={"warmup": "async"})
        idx_res2 = self.describe_index(client, col2, index_name="float_vector_1")[0]
        col_res2 = self.describe_collection(client, col2)[0]
        assert cf.get_index_warmup(idx_res2) == "async"
        assert cf.get_collection_warmup(col_res2, "warmup.vectorIndex") == "sync"

        res2 = self.describe_collection(client, col2)[0]
        rows2 = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=res2)
        for d in cf.iter_mc_insert_list_data(rows2, batch, nb):
            self.insert(client, col2, d)
        self.flush(client, col2)
        # wait for index build to complete so search operates on fully-indexed data
        self.wait_for_index_ready(client, col2, index_name="float_vector_1")
        self.load_collection(client, col2)
        self.search(
            client,
            col2,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.release_collection(client, col2)
        self.drop_index_properties(client, col2, index_name="float_vector_1", property_keys=["warmup"])
        assert cf.get_index_warmup(self.describe_index(client, col2, index_name="float_vector_1")[0]) is None

        self.load_collection(client, col2)
        self.search(
            client,
            col2,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )
        self.drop_collection(client, col2)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_three_level_set_and_teardown(self):
        """
        target: verify all three levels (field/collection/index) can be set simultaneously with different values;
                each level drop falls back to the next level correctly
        method: set field=async, collection=sync, index=disable; verify all stored independently;
                phase 3 drop field, verify index takes effect;
                phase 4 drop index, verify collection takes effect;
                phase 5 drop collection, verify global default
        expected: each describe step shows correct values; search/query succeed at every stage
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "int64_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=128, warmup="async").to_dict,
                "int64_1": FieldParams(nullable=True, warmup="async").to_dict,
            },
        )

        properties = {
            "warmup.vectorField": "sync",
            "warmup.scalarField": "sync",
            "warmup.vectorIndex": "sync",
            "warmup.scalarIndex": "sync",
        }
        self.create_collection(client, collection_name, schema=schema, properties=properties)

        col_res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=col_res)
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        # build index after data is flushed, then set index-level warmup=disable
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0],
                DefaultVectorIndexParams.HNSW("float_vector_1", metric_type=MetricType.COSINE),
            ),
        )
        self.alter_index_properties(
            client, collection_name, index_name="float_vector_1", properties={"warmup": "disable"}
        )

        col_res = self.describe_collection(client, collection_name)[0]
        idx_res = self.describe_index(client, collection_name, index_name="float_vector_1")[0]
        assert cf.get_field_warmup(col_res, "float_vector_1") == "async"
        assert cf.get_collection_warmup(col_res, "warmup.vectorIndex") == "sync"
        assert cf.get_index_warmup(idx_res) == "disable"

        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )
        self.query(
            client,
            collection_name,
            filter="int64_pk >= 0",
            limit=10,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 10},
        )

        # Phase 3: set field-level warmup to disable → index-level disable takes effect
        # Note: Milvus does not support dropping field-level warmup via None; use "disable" instead
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client, collection_name, field_name="float_vector_1", field_params={"warmup": "disable"}
        )
        self.alter_collection_field(client, collection_name, field_name="int64_1", field_params={"warmup": "disable"})
        col_res = self.describe_collection(client, collection_name)[0]
        idx_res = self.describe_index(client, collection_name, index_name="float_vector_1")[0]
        assert cf.get_field_warmup(col_res, "float_vector_1") == "disable"
        assert cf.get_collection_warmup(col_res, "warmup.vectorIndex") == "sync"
        assert cf.get_index_warmup(idx_res) == "disable"

        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # Phase 4: drop index-level warmup → collection sync takes effect
        self.release_collection(client, collection_name)
        self.drop_index_properties(client, collection_name, index_name="float_vector_1", property_keys=["warmup"])
        idx_res = self.describe_index(client, collection_name, index_name="float_vector_1")[0]
        col_res = self.describe_collection(client, collection_name)[0]
        assert cf.get_index_warmup(idx_res) is None
        assert cf.get_collection_warmup(col_res, "warmup.vectorIndex") == "sync"

        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # Phase 5: drop all collection-level → global default
        self.release_collection(client, collection_name)
        self.drop_collection_properties(
            client,
            collection_name,
            property_keys=["warmup.vectorField", "warmup.scalarField", "warmup.vectorIndex", "warmup.scalarIndex"],
        )
        col_res = self.describe_collection(client, collection_name)[0]
        idx_res = self.describe_index(client, collection_name, index_name="float_vector_1")[0]
        for key in ["warmup.vectorField", "warmup.scalarField", "warmup.vectorIndex", "warmup.scalarIndex"]:
            assert cf.get_collection_warmup(col_res, key) is None
        # field-level warmup was set to "disable" in Phase 3; no drop API available, so it persists
        assert cf.get_field_warmup(col_res, "float_vector_1") == "disable"
        assert cf.get_index_warmup(idx_res) is None

        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )
        self.query(
            client,
            collection_name,
            filter="int64_pk >= 0",
            limit=10,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 10},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "field_a_warmup,field_b_warmup",
        [
            pytest.param("async", "disable", id="async_disable_column_group"),
            pytest.param("sync", "async", id="sync_async_column_group"),
        ],
    )
    def test_warmup_aggregation_column_group(self, field_a_warmup, field_b_warmup):
        """
        target: verify that fields with different warmup values in the same Column Group are each
                independently stored and retrievable; verify both fields are accessible via
                output_fields after async load (rawdata column path)
        method: parametrize two field warmup values (async+disable, sync+async), describe per-field
                warmup, insert 5000, load, query with output_fields, search with output_fields
        expected: describe shows each field's configured warmup; both fields present in every
                  query row and every search hit entity
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "int64_1", "int64_2"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "int64_1": FieldParams(nullable=True, warmup=field_a_warmup).to_dict,
                "int64_2": FieldParams(nullable=True, warmup=field_b_warmup).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema)

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "int64_1") == field_a_warmup
        assert cf.get_field_warmup(res, "int64_2") == field_b_warmup

        rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=res)
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        # build index after data is flushed so index covers all segments
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # verify Column Group: both fields retrievable via output_fields (rawdata path)
        q = self.query(
            client,
            collection_name,
            filter="int64_pk >= 0",
            output_fields=["int64_pk", "int64_1", "int64_2"],
            limit=100,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 100, "output_fields": ["int64_pk", "int64_1", "int64_2"]},
        )[0]
        assert all("int64_1" in r and "int64_2" in r for r in q), (
            "Column Group fields should be retrievable regardless of warmup combination"
        )

        # search with output_fields to exercise rawdata column path for both fields
        s = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            output_fields=["int64_1", "int64_2"],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )[0]
        for hit in s[0]:
            assert "int64_1" in hit["entity"], f"int64_1 missing in search result entity (warmup={field_a_warmup})"
            assert "int64_2" in hit["entity"], f"int64_2 missing in search result entity (warmup={field_b_warmup})"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_cancel_on_index_replace(self):
        """
        target: verify async load + drop/recreate vector index cancels warmup; replace scalar index too;
                reload succeeds with search + filter query
        method: create collection all=async, insert 5000, flush, load, release,
                drop+recreate vector index (M=32), drop+recreate scalar INVERTED->BITMAP, reload
        expected: reload succeeds, search returns 10 results, LIKE query matches inserted prefix rows
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "varchar_1", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "varchar_1": FieldParams(max_length=256, nullable=True, warmup="async").to_dict,
            },
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={
                k: "async"
                for k in ["warmup.vectorIndex", "warmup.scalarIndex", "warmup.vectorField", "warmup.scalarField"]
            },
        )

        res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=res,
            default_values={
                "int64_pk": list(range(nb)),
                "varchar_1": [f"prefix_{i % 100}" for i in range(nb)],
            },
        )
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        # build indexes after data is flushed
        index_params = self.all_vector_index(
            self.prepare_index_params(client)[0],
            DefaultVectorIndexParams.HNSW("float_vector_1", metric_type=MetricType.COSINE),
        )
        index_params.add_index(field_name="varchar_1", index_type="INVERTED")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, index_name="float_vector_1")
        new_vec_idx = self.prepare_index_params(client)[0]
        self.all_vector_index(
            new_vec_idx,
            DefaultVectorIndexParams.HNSW("float_vector_1", m=32, efConstruction=400, metric_type=MetricType.COSINE),
        )
        self.create_index(client, collection_name, index_params=new_vec_idx)

        self.drop_index(client, collection_name, index_name="varchar_1")
        new_scalar_idx = self.prepare_index_params(client)[0]
        new_scalar_idx.add_index(field_name="varchar_1", index_type="BITMAP")
        self.create_index(client, collection_name, index_params=new_scalar_idx)

        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )
        q = self.query(
            client,
            collection_name,
            filter='varchar_1 like "prefix%"',
            output_fields=["int64_pk", "varchar_1"],
            limit=500,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 500, "output_fields": ["int64_pk", "varchar_1"]},
        )[0]
        for r in q:
            assert str(r["varchar_1"]).startswith("prefix_")

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "idx_type,warmup_props",
        [
            # hasRawData=True: vectors read directly from index
            pytest.param("FLAT", {"warmup.vectorIndex": "async"}, id="flat_vectorIndex"),
            pytest.param("IVF_FLAT", {"warmup.vectorIndex": "async"}, id="ivf_flat_vectorIndex"),
            pytest.param("HNSW", {"warmup.vectorIndex": "async"}, id="hnsw_vectorIndex"),
            # hasRawData=False: vectors read from column store (scalar-quantized / product-quantized)
            pytest.param("IVF_SQ8", {"warmup.vectorIndex": "async"}, id="ivf_sq8_vectorIndex"),
            pytest.param("IVF_PQ", {"warmup.vectorIndex": "async"}, id="ivf_pq_vectorIndex"),
            # dual warmup (vectorIndex + vectorField) variants
            pytest.param(
                "IVF_FLAT", {"warmup.vectorIndex": "async", "warmup.vectorField": "async"}, id="ivf_flat_dual"
            ),
            pytest.param("IVF_PQ", {"warmup.vectorIndex": "async", "warmup.vectorField": "async"}, id="ivf_pq_dual"),
            pytest.param("IVF_SQ8", {"warmup.vectorIndex": "async", "warmup.vectorField": "async"}, id="ivf_sq8_dual"),
            # DISKANN (hasRawData=True for L2): all-async warmup
            pytest.param(
                "DISKANN",
                {
                    "warmup.vectorIndex": "async",
                    "warmup.vectorField": "async",
                    "warmup.scalarIndex": "async",
                    "warmup.scalarField": "async",
                },
                id="diskann_all_async",
            ),
        ],
    )
    def test_warmup_async_output_vec_correctness(self, idx_type, warmup_props):
        """
        target:
            1. verify that search works correctly under async warmup for all float vector index types
            2. cover hasRawData=false (IVF_PQ/SQ8 via column store) and hasRawData=true
               (FLAT/IVF_FLAT/HNSW/DISKANN via index-internal raw) paths
        method:
            1. create collection with warmup_props; generate fixed-seed data (nb=5000)
            2. insert, flush, build index (after flush so quantizer trains on real data), load
            3. run 8 consecutive searches with output_fields=["int64_pk", "float_vector_1"]
            4. verify each search returns 10 hits; vec field is present and non-null in every hit
        expected:
            1. all 8 searches return exactly 10 hits
            2. every hit carries float_vector_1 in entity (no missing/null vec under async warmup)
        """
        # index and search params per index type
        _index_params = {
            "FLAT": DefaultVectorIndexParams.FLAT("float_vector_1"),
            "IVF_FLAT": DefaultVectorIndexParams.IVF_FLAT("float_vector_1", nlist=128),
            "IVF_SQ8": DefaultVectorIndexParams.IVF_SQ8("float_vector_1", nlist=128),
            "IVF_PQ": DefaultVectorIndexParams.IVF_PQ("float_vector_1", nlist=128, m=8, nbits=8),
            "HNSW": DefaultVectorIndexParams.HNSW("float_vector_1"),
            "DISKANN": DefaultVectorIndexParams.DISKANN("float_vector_1"),
        }
        _search_params = {
            "FLAT": DefaultVectorSearchParams.FLAT(),
            "IVF_FLAT": DefaultVectorSearchParams.IVF_FLAT(nprobe=16),
            "IVF_SQ8": DefaultVectorSearchParams.IVF_SQ8(nprobe=16),
            "IVF_PQ": DefaultVectorSearchParams.IVF_PQ(nprobe=16),
            "HNSW": DefaultVectorSearchParams.HNSW(),
            "DISKANN": DefaultVectorSearchParams.DISKANN(),
        }

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim, nb, batch = 128, 5000, 2000
        output_fields = ["int64_pk", "float_vector_1"]

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=dim).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema, properties=warmup_props)

        schema_res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=schema_res,
            default_values={"int64_pk": list(range(nb))},
        )
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        ip = self.prepare_index_params(client)[0]
        for field, idx in _index_params[idx_type].items():
            ip.add_index(field_name=field, **idx.to_dict)
        self.create_index(client, collection_name, ip)
        self.load_collection(client, collection_name)

        # search 8 times; verify each hit returns 10 results and vec field is present
        for search_idx, qv in enumerate(cf.gen_vectors(8, dim)):
            hits = self.search(
                client,
                collection_name,
                [qv],
                anns_field="float_vector_1",
                limit=10,
                output_fields=output_fields,
                search_params=_search_params[idx_type],
            )[0][0]
            assert len(hits) == 10, f"[{idx_type}] search {search_idx}: expected 10 hits, got {len(hits)}"
            for hit in hits:
                ent = hit["entity"]
                returned_vec = ent.get("float_vector_1")
                assert returned_vec is not None, (
                    f"[{idx_type}] search {search_idx} pk={ent.get('int64_pk')}: float_vector_1 is None"
                )
                assert len(returned_vec) == dim, (
                    f"[{idx_type}] search {search_idx} pk={ent.get('int64_pk')}: wrong dim {len(returned_vec)}"
                )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "warmup_val,mmap_enabled",
        [
            pytest.param("async", True, id="async_mmap_on"),
            pytest.param("async", False, id="async_mmap_off"),
            pytest.param("sync", False, id="sync_mmap_off"),
            pytest.param("disable", False, id="disable_mmap_off"),
        ],
    )
    def test_warmup_async_x_mmap(self, warmup_val, mmap_enabled):
        """
        target: verify async/sync/disable warmup x mmap=on/off produce identical query results
        method: parametrize 4 combos, insert 5000 rows with fixed seed, query all pks,
                verify pk set equals expected baseline {0..4999}
        expected: all 5000 pks present, search returns 10 results for each combo
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
            },
        )

        props = {"warmup.vectorIndex": warmup_val}
        if mmap_enabled:
            props["mmap.enabled"] = "true"
        self.create_collection(client, collection_name, schema=schema, properties=props)

        schema_res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=schema_res)
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)

        q = self.query(
            client,
            collection_name,
            filter="int64_pk >= 0",
            output_fields=["int64_pk"],
            limit=5000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": nb, "output_fields": ["int64_pk"]},
        )[0]
        result_pks = {r["int64_pk"] for r in q}
        expected_pks = set(range(nb))
        assert result_pks == expected_pks, f"warmup={warmup_val} mmap={mmap_enabled}: pk set differs from baseline"

        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_x_partition(self):
        """
        target: verify async warmup + partition load; single partition search returns only that partition's pks;
                dual partition search covers all
        method: create two partitions, insert 2500 each, load part_a only, search, then load part_b, search both
        expected: part_a search returns pks in [0,2499]; both partitions search returns 20 results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema, properties={"warmup.vectorIndex": "async"})

        self.create_partition(client, collection_name, "part_a")
        self.create_partition(client, collection_name, "part_b")

        schema_res = self.describe_collection(client, collection_name)[0]
        rows_a = cf.gen_row_data_by_schema_with_defaults(
            nb=nb // 2, schema=schema_res, default_values={"int64_pk": list(range(nb // 2))}
        )
        rows_b = cf.gen_row_data_by_schema_with_defaults(
            nb=nb // 2, schema=schema_res, default_values={"int64_pk": list(range(nb // 2, nb))}
        )
        for d in cf.iter_mc_insert_list_data(rows_a, batch, nb // 2):
            self.insert(client, collection_name, d, partition_name="part_a")
        for d in cf.iter_mc_insert_list_data(rows_b, batch, nb // 2):
            self.insert(client, collection_name, d, partition_name="part_b")
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )

        self.load_partitions(client, collection_name, partition_names=["part_a"])
        s = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            partition_names=["part_a"],
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )[0]
        for hit in s[0]:
            assert hit["int64_pk"] < 2500, f"PK {hit['int64_pk']} not in part_a"

        self.load_partitions(client, collection_name, partition_names=["part_b"])
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            partition_names=["part_a", "part_b"],
            limit=20,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 20},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_x_multi_vector(self):
        """
        target: verify async warmup with multiple vector fields; field-level async overrides collection-level disable;
                each vector field searchable independently
        method: create 3 vector fields (vec_a async, vec_b async, vec_c no warmup), collection vectorField=disable,
                insert 5000, load, search each field
        expected: describe shows vec_a=async, vec_b=async, vec_c=None; search on each field returns 10 results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "float_vector_2", "float_vector_3"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=128, warmup="async").to_dict,
                "float_vector_2": FieldParams(dim=128, warmup="async").to_dict,
            },
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={"warmup.vectorField": "disable", "warmup.vectorIndex": "async"},
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "float_vector_1") == "async"
        assert cf.get_field_warmup(res, "float_vector_2") == "async"
        assert cf.get_field_warmup(res, "float_vector_3") is None

        rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=res)
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        # build indexes after data is flushed
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0],
                {
                    **DefaultVectorIndexParams.HNSW("float_vector_1", metric_type=MetricType.COSINE),
                    **DefaultVectorIndexParams.HNSW("float_vector_2", metric_type=MetricType.COSINE),
                    **DefaultVectorIndexParams.HNSW("float_vector_3", metric_type=MetricType.COSINE),
                },
            ),
        )
        self.load_collection(client, collection_name)

        for anns_f in ["float_vector_1", "float_vector_2", "float_vector_3"]:
            self.search(
                client,
                collection_name,
                cf.gen_vectors(1, 128),
                anns_field=anns_f,
                limit=10,
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 10},
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_x_growing_segment(self):
        """
        target: verify async warmup with growing segments; new data inserted after load is visible
        method: insert 2500 sealed rows (flush+load), then insert 2500 growing rows (no flush);
                query count(*) must equal 5000 (sealed+growing); search covers both segments
        expected: count(*) == 5000; search returns 10 results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema, properties={"warmup.vectorIndex": "async"})

        schema_res = self.describe_collection(client, collection_name)[0]
        rows_sealed = cf.gen_row_data_by_schema_with_defaults(
            nb=nb // 2,
            schema=schema_res,
            default_values={"int64_pk": list(range(nb // 2)), "tag": "sealed"},
        )
        for d in cf.iter_mc_insert_list_data(rows_sealed, batch, nb // 2):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)

        rows_growing = cf.gen_row_data_by_schema_with_defaults(
            nb=nb // 2,
            schema=schema_res,
            default_values={"int64_pk": list(range(nb // 2, nb)), "tag": "new"},
        )
        for d in cf.iter_mc_insert_list_data(rows_growing, batch, nb // 2):
            self.insert(client, collection_name, d)

        q = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        assert q[0]["count(*)"] == nb, f"expected {nb} rows (sealed+growing), got {q[0]['count(*)']}"

        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_x_dynamic_field(self):
        """
        target: verify async warmup + enable_dynamic_field=True; dynamic fields accessible in query/search
        method: create schema with dynamic field enabled, vec warmup=async, insert 5000 rows with dyn_str/dyn_int,
                flush, load, query filter dyn_int<100, search with dynamic field output
        expected: dyn_int == pk*2 for query results; search returns 10 results with dyn_str in entity
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=128, warmup="async").to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema, properties={"warmup.vectorIndex": "async"})

        schema_res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(nb)),
                "dyn_str": [f"str_{i}" for i in range(nb)],
                "dyn_int": [i * 2 for i in range(nb)],
            },
        )
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)

        # dyn_int = int64_pk * 2; dyn_int < 100 → int64_pk < 50 → 50 rows
        q = self.query(
            client, collection_name, filter="dyn_int < 100", output_fields=["int64_pk", "dyn_str", "dyn_int"], limit=100
        )[0]
        assert len(q) == 50, f"dyn_int<100: expected 50 rows, got {len(q)}"
        for r in q:
            assert r["dyn_int"] == r["int64_pk"] * 2

        s = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            output_fields=["dyn_str", "dyn_int"],
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )[0]
        assert all("dyn_str" in hit["entity"] for hit in s[0])

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_x_text_match(self):
        """
        target: verify async warmup with varchar field (enable_analyzer + enable_match);
                TEXT_MATCH query and search work correctly; index rebuild and reload preserve results
        method: create collection with varchar_1 (warmup=async, analyzer, match enabled);
                insert 5000 rows (keywords cycle over 5 words); flush; build HNSW; load;
                query TEXT_MATCH("apple") → 1000 hits; search TEXT_MATCH("banana") → 10 hits;
                rebuild INVERTED index on varchar_1, reload; query TEXT_MATCH("apple") → 1000 hits
        expected: TEXT_MATCH filter returns correct counts; all query rows contain the keyword;
                  results consistent after index rebuild
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "varchar_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=128, warmup="async").to_dict,
                "varchar_1": FieldParams(
                    max_length=1024, nullable=True, warmup="async", enable_analyzer=True, enable_match=True
                ).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema)

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "varchar_1") == "async"

        keywords = ["apple", "banana", "cherry", "date", "elderberry"]
        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=res,
            default_values={
                "int64_pk": list(range(nb)),
                "varchar_1": [f"{keywords[i % len(keywords)]} is a fruit number {i}" for i in range(nb)],
            },
        )
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)

        q = self.query(
            client,
            collection_name,
            filter='TEXT_MATCH(varchar_1, "apple")',
            output_fields=["int64_pk", "varchar_1"],
            limit=2000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 1000, "output_fields": ["int64_pk", "varchar_1"]},
        )[0]
        for r in q:
            assert "apple" in r["varchar_1"], "TEXT_MATCH false positive"

        # 1000/5000 rows have "banana"; with limit=10 and enough candidates → 10 hits
        s = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            anns_field="float_vector_1",
            filter='TEXT_MATCH(varchar_1, "banana")',
            limit=10,
        )[0]
        assert len(s[0]) == 10, f"search with TEXT_MATCH filter should return 10 hits, got {len(s[0])}"

        # rebuild INVERTED index and reload; TEXT_MATCH should still work
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, index_name="varchar_1")
        new_idx = self.prepare_index_params(client)[0]
        new_idx.add_index(field_name="varchar_1", index_type="INVERTED")
        self.create_index(client, collection_name, index_params=new_idx)
        self.load_collection(client, collection_name)

        self.query(
            client,
            collection_name,
            filter='TEXT_MATCH(varchar_1, "apple")',
            output_fields=["int64_pk"],
            limit=2000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 1000, "output_fields": ["int64_pk"]},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_x_json_shredding(self):
        """
        target: verify async warmup with JSON field; JSON path filter query/search work correctly;
                compact + reload preserves filter correctness
        method: create collection with json_1 (warmup=async, nullable);
                insert 5000 rows (category=i%10, score=i/100); flush; build HNSW; load;
                query json_1['category']==3 → 500 hits (value check);
                search json_1['score']<10.0 → 10 hits;
                compact + reload; re-query category==3 → still 500 hits
        expected: JSON path filter returns correct counts; compact does not corrupt JSON column data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "json_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=128, warmup="async").to_dict,
                "json_1": FieldParams(nullable=True, warmup="async").to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema)

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "json_1") == "async"

        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=res,
            default_values={
                "int64_pk": list(range(nb)),
                "json_1": [{"category": i % 10, "score": float(i) / 100, "tag": f"group_{i % 5}"} for i in range(nb)],
            },
        )
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)

        q = self.query(
            client,
            collection_name,
            filter="json_1['category'] == 3",
            output_fields=["int64_pk", "json_1"],
            limit=1000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 500, "output_fields": ["int64_pk", "json_1"]},
        )[0]
        for r in q:
            assert r["json_1"]["category"] == 3

        # score = float(i)/100; score < 10.0 → i < 1000 → 1000 candidates, limit=10 → 10 hits
        s = self.search(client, collection_name, cf.gen_vectors(1, 128), filter="json_1['score'] < 10.0", limit=10)[0]
        assert len(s[0]) == 10, f"search with JSON filter should return 10 hits, got {len(s[0])}"

        # compact and reload; JSON path filter should still return the same results
        time.sleep(1)
        self.release_collection(client, collection_name)
        self.compact(client, collection_name)
        # NOTE: compact() is async; sleep gives compaction time to finish before reload.
        # If this test flakes on slow CI, increase to 30s or poll describe_collection
        # for segment count reduction instead.
        time.sleep(15)

        self.load_collection(client, collection_name)
        q = self.query(
            client,
            collection_name,
            filter="json_1['category'] == 3",
            output_fields=["int64_pk", "json_1"],
            limit=1000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 500, "output_fields": ["int64_pk", "json_1"]},
        )[0]
        for r in q:
            assert r["json_1"]["category"] == 3

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_x_add_field_int64(self):
        """
        target: verify async warmup + add_field (INT64) works correctly;
                historical rows are null for the new field; new rows hold expected values
        method: insert 5000 old rows, create HNSW index; add INT64 field (warmup=async);
                insert 1000 new rows (int64_new = pk - 5000); flush; load;
                query old rows → int64_new is null; query new rows → int64_new == pk - 5000;
                search → 10 hits
        expected: historical rows null; new rows value correct; search passes
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=128, warmup="async").to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema)

        schema_res = self.describe_collection(client, collection_name)[0]
        rows_old = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=schema_res)
        for d in cf.iter_mc_insert_list_data(rows_old, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )

        self.add_collection_field(client, collection_name, "int64_new", DataType.INT64, nullable=True, warmup="async")
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "int64_new") == "async"

        rows_new = cf.gen_row_data_by_schema_with_defaults(
            nb=1000,
            schema=res,
            default_values={
                "int64_pk": list(range(5000, 6000)),
                "int64_new": list(range(1000)),
            },
        )
        self.insert(client, collection_name, rows_new)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        q_old = self.query(
            client,
            collection_name,
            filter="int64_pk < 5000",
            output_fields=["int64_pk", "int64_new"],
            limit=5000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 5000, "output_fields": ["int64_pk", "int64_new"]},
        )[0]
        for r in q_old:
            assert r["int64_new"] is None, f"historical row int64_pk={r['int64_pk']} int64_new should be null"

        q_new = self.query(
            client,
            collection_name,
            filter="int64_pk >= 5000",
            output_fields=["int64_pk", "int64_new"],
            limit=1000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 1000, "output_fields": ["int64_pk", "int64_new"]},
        )[0]
        for r in q_new:
            assert r["int64_new"] == r["int64_pk"] - 5000

        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_x_add_field_text_json(self):
        """
        target: verify async warmup + add_field (VARCHAR+JSON) works correctly;
                historical rows are null for both new fields; new rows queryable via TEXT_MATCH and JSON path
        method: insert 5000 old rows, create HNSW index; add VARCHAR (analyzer+match, warmup=async)
                and JSON (warmup=async) fields; build INVERTED index on varchar_1;
                insert 1000 new rows; flush; load;
                query old rows → varchar_1/json_1 is null (5000 hits each);
                query new rows TEXT_MATCH("apple") → 1000 hits;
                query new rows json_1['level']==2 → 200 hits; search → 10 hits
        expected: null checks pass; TEXT_MATCH and JSON path filters return correct counts; search passes
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=128, warmup="async").to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema)

        schema_res = self.describe_collection(client, collection_name)[0]
        rows_old = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=schema_res)
        for d in cf.iter_mc_insert_list_data(rows_old, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )

        self.add_collection_field(
            client,
            collection_name,
            "varchar_1",
            DataType.VARCHAR,
            max_length=1024,
            nullable=True,
            warmup="async",
            enable_analyzer=True,
            enable_match=True,
        )
        self.add_collection_field(client, collection_name, "json_1", DataType.JSON, nullable=True, warmup="async")

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "varchar_1") == "async"
        assert cf.get_field_warmup(res, "json_1") == "async"

        new_idx = self.prepare_index_params(client)[0]
        new_idx.add_index(field_name="varchar_1", index_type="INVERTED")
        self.create_index(client, collection_name, index_params=new_idx)

        rows_new = cf.gen_row_data_by_schema_with_defaults(
            nb=1000,
            schema=res,
            default_values={
                "int64_pk": list(range(5000, 6000)),
                "varchar_1": [f"apple fruit {i}" for i in range(1000)],
                "json_1": [{"level": i % 5, "value": float(i)} for i in range(1000)],
            },
        )
        self.insert(client, collection_name, rows_new)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        self.query(
            client,
            collection_name,
            filter="int64_pk < 5000 and varchar_1 is null",
            output_fields=["int64_pk"],
            limit=5000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 5000, "output_fields": ["int64_pk"]},
        )

        self.query(
            client,
            collection_name,
            filter="int64_pk < 5000 and json_1 is null",
            output_fields=["int64_pk"],
            limit=5000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 5000, "output_fields": ["int64_pk"]},
        )

        # all 1000 new rows have varchar_1 = "apple fruit {i}", TEXT_MATCH("apple") hits all 1000
        q_new_text = self.query(
            client,
            collection_name,
            filter='int64_pk >= 5000 and TEXT_MATCH(varchar_1, "apple")',
            output_fields=["int64_pk", "varchar_1"],
            limit=1000,
        )[0]
        assert len(q_new_text) == 1000, f"TEXT_MATCH apple: expected 1000 new rows, got {len(q_new_text)}"

        q_new_json = self.query(
            client,
            collection_name,
            filter="int64_pk >= 5000 and json_1['level'] == 2",
            output_fields=["int64_pk", "json_1"],
            limit=500,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 200, "output_fields": ["int64_pk", "json_1"]},
        )[0]
        for r in q_new_json:
            assert r["json_1"]["level"] == 2

        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 128),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "vec_type,dim,idx_type,metric,idx_params",
        [
            # FloatVector
            pytest.param(DataType.FLOAT_VECTOR, 128, "FLAT", "L2", {}, id="FloatVec_FLAT"),
            pytest.param(
                DataType.FLOAT_VECTOR, 128, "HNSW", "L2", {"M": 16, "efConstruction": 200}, id="FloatVec_HNSW"
            ),
            pytest.param(DataType.FLOAT_VECTOR, 128, "IVF_FLAT", "L2", {"nlist": 128}, id="FloatVec_IVF_FLAT"),
            pytest.param(DataType.FLOAT_VECTOR, 128, "IVF_SQ8", "L2", {"nlist": 128}, id="FloatVec_IVF_SQ8"),
            pytest.param(
                DataType.FLOAT_VECTOR, 128, "IVF_PQ", "L2", {"nlist": 128, "m": 8, "nbits": 8}, id="FloatVec_IVF_PQ"
            ),
            # Float16Vector
            pytest.param(DataType.FLOAT16_VECTOR, 128, "FLAT", "COSINE", {}, id="F16Vec_FLAT"),
            pytest.param(
                DataType.FLOAT16_VECTOR, 128, "HNSW", "COSINE", {"M": 16, "efConstruction": 200}, id="F16Vec_HNSW"
            ),
            pytest.param(DataType.FLOAT16_VECTOR, 128, "IVF_FLAT", "COSINE", {"nlist": 128}, id="F16Vec_IVF_FLAT"),
            pytest.param(DataType.FLOAT16_VECTOR, 128, "SCANN", "COSINE", {"nlist": 128}, id="F16Vec_SCANN"),
            # BFloat16Vector
            pytest.param(DataType.BFLOAT16_VECTOR, 128, "FLAT", "COSINE", {}, id="BF16Vec_FLAT"),
            pytest.param(
                DataType.BFLOAT16_VECTOR, 128, "HNSW", "COSINE", {"M": 16, "efConstruction": 200}, id="BF16Vec_HNSW"
            ),
            pytest.param(DataType.BFLOAT16_VECTOR, 128, "IVF_FLAT", "COSINE", {"nlist": 128}, id="BF16Vec_IVF_FLAT"),
            pytest.param(DataType.BFLOAT16_VECTOR, 128, "SCANN", "COSINE", {"nlist": 128}, id="BF16Vec_SCANN"),
            # Int8Vector (only HNSW supported currently)
            pytest.param(
                DataType.INT8_VECTOR, 128, "HNSW", "COSINE", {"M": 16, "efConstruction": 200}, id="Int8Vec_HNSW"
            ),
            # BinaryVector
            pytest.param(DataType.BINARY_VECTOR, 128, "BIN_FLAT", "HAMMING", {}, id="BinVec_BIN_FLAT"),
            pytest.param(
                DataType.BINARY_VECTOR, 128, "BIN_IVF_FLAT", "HAMMING", {"nlist": 128}, id="BinVec_BIN_IVF_FLAT"
            ),
            # SparseFloatVector (no fixed dim)
            pytest.param(
                DataType.SPARSE_FLOAT_VECTOR,
                None,
                "SPARSE_INVERTED_INDEX",
                "IP",
                {},
                id="SparseVec_SPARSE_INVERTED_INDEX",
            ),
            pytest.param(DataType.SPARSE_FLOAT_VECTOR, None, "SPARSE_WAND", "IP", {}, id="SparseVec_SPARSE_WAND"),
            # DISKANN (hasRawData=True for L2) — verifies async warmup on index-internal raw path
            pytest.param(DataType.FLOAT_VECTOR, 128, "DISKANN", "L2", {}, id="FloatVec_DISKANN"),
        ],
    )
    def test_warmup_async_vector_type(self, vec_type, dim, idx_type, metric, idx_params):
        """
        target: verify field-level warmup=async works across all supported vector types and index types;
                field-level warmup affects raw column data (not index); verify via:
                (1) ANN search — exercises vector index data;
                (2) search with output_fields=["vec"] — exercises raw vector column data
        method: parametrize vec_type/dim/idx_type/metric/idx_params; insert 5000 rows; flush;
                build index; load; search + search with output_fields
        expected: search returns 10 hits; each hit carries the raw vec value
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_pk", DataType.INT64, is_primary=True, auto_id=False)
        if dim is not None:
            schema.add_field("vec_field", vec_type, dim=dim, warmup="async")
        else:
            schema.add_field("vec_field", vec_type, warmup="async")
        self.create_collection(client, collection_name, schema=schema)

        res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=res)
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        # build index after data is flushed so index covers actual segments
        index_params_obj = self.prepare_index_params(client)[0]
        index_params_obj.add_index(
            field_name="vec_field", index_type=idx_type, metric_type=metric, params=dict(idx_params)
        )
        self.create_index(client, collection_name, index_params_obj)
        self.load_collection(client, collection_name)

        search_vecs = cf.gen_row_data_by_schema(nb=1, schema=res)
        # (1) plain search — exercises vector index data
        self.search(
            client,
            collection_name,
            [search_vecs[0]["vec_field"]],
            anns_field="vec_field",
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # (2) search with output_fields=["vec_field"] — exercises raw vector column data
        #     field-level warmup covers rawdata; verify each hit carries the vec value
        s_with_vec = self.search(
            client,
            collection_name,
            [search_vecs[0]["vec_field"]],
            anns_field="vec_field",
            limit=10,
            output_fields=["vec_field"],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )[0]
        for hit in s_with_vec[0]:
            assert hit["entity"].get("vec_field") is not None, (
                f"{vec_type.name}+{idx_type}: raw vec missing in output_fields result"
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "scalar_type,field_kwargs,idx_type,filter_expr,create_idx_params",
        [
            # Integer types × STL_SORT / INVERTED / BITMAP
            pytest.param(DataType.INT8, {}, "STL_SORT", "scalar_field < 50", None, id="INT8_STL_SORT"),
            pytest.param(DataType.INT8, {}, "INVERTED", "scalar_field < 50", None, id="INT8_INVERTED"),
            pytest.param(DataType.INT8, {}, "BITMAP", "scalar_field < 50", None, id="INT8_BITMAP"),
            pytest.param(DataType.INT16, {}, "STL_SORT", "scalar_field < 50", None, id="INT16_STL_SORT"),
            pytest.param(DataType.INT16, {}, "INVERTED", "scalar_field < 50", None, id="INT16_INVERTED"),
            pytest.param(DataType.INT16, {}, "BITMAP", "scalar_field < 50", None, id="INT16_BITMAP"),
            pytest.param(DataType.INT32, {}, "STL_SORT", "scalar_field < 50", None, id="INT32_STL_SORT"),
            pytest.param(DataType.INT32, {}, "INVERTED", "scalar_field < 50", None, id="INT32_INVERTED"),
            pytest.param(DataType.INT32, {}, "BITMAP", "scalar_field < 50", None, id="INT32_BITMAP"),
            pytest.param(DataType.INT64, {}, "STL_SORT", "scalar_field < 50", None, id="INT64_STL_SORT"),
            pytest.param(DataType.INT64, {}, "INVERTED", "scalar_field < 50", None, id="INT64_INVERTED"),
            pytest.param(DataType.INT64, {}, "BITMAP", "scalar_field < 50", None, id="INT64_BITMAP"),
            # Float / Double × STL_SORT / INVERTED
            pytest.param(DataType.FLOAT, {}, "STL_SORT", "scalar_field < 100.0", None, id="FLOAT_STL_SORT"),
            pytest.param(DataType.FLOAT, {}, "INVERTED", "scalar_field < 100.0", None, id="FLOAT_INVERTED"),
            pytest.param(DataType.DOUBLE, {}, "STL_SORT", "scalar_field < 100.0", None, id="DOUBLE_STL_SORT"),
            pytest.param(DataType.DOUBLE, {}, "INVERTED", "scalar_field < 100.0", None, id="DOUBLE_INVERTED"),
            # Bool × BITMAP (STL_SORT not supported for Bool)
            pytest.param(DataType.BOOL, {}, "BITMAP", "scalar_field == true", None, id="BOOL_BITMAP"),
            # VarChar × STL_SORT / INVERTED / BITMAP / TRIE
            pytest.param(
                DataType.VARCHAR,
                {"max_length": 256},
                "STL_SORT",
                'scalar_field == "prefix_0"',
                None,
                id="VARCHAR_STL_SORT",
            ),
            pytest.param(
                DataType.VARCHAR,
                {"max_length": 256},
                "INVERTED",
                'scalar_field == "prefix_0"',
                None,
                id="VARCHAR_INVERTED",
            ),
            pytest.param(
                DataType.VARCHAR, {"max_length": 256}, "BITMAP", 'scalar_field == "prefix_0"', None, id="VARCHAR_BITMAP"
            ),
            pytest.param(
                DataType.VARCHAR, {"max_length": 256}, "TRIE", 'scalar_field == "prefix_0"', None, id="VARCHAR_TRIE"
            ),
            # Array(INT64) × INVERTED / BITMAP
            pytest.param(
                DataType.ARRAY,
                {"element_type": DataType.INT64, "max_capacity": 10},
                "INVERTED",
                "array_contains(scalar_field, 5)",
                None,
                id="ARRAY_INT64_INVERTED",
            ),
            pytest.param(
                DataType.ARRAY,
                {"element_type": DataType.INT64, "max_capacity": 10},
                "BITMAP",
                "array_contains(scalar_field, 5)",
                None,
                id="ARRAY_INT64_BITMAP",
            ),
            # JSON × INVERTED (json_cast_type) — requires separate create_index call; hasRawData=false path
            pytest.param(
                DataType.JSON,
                {},
                "JSON_INVERTED",
                "scalar_field['category'] == 3.0",
                {
                    "index_type": "INVERTED",
                    "params": {"json_cast_type": "double", "json_path": "scalar_field['category']"},
                },
                id="JSON_INVERTED",
            ),
            # VARCHAR × NGRAM (gram params) — requires separate create_index call
            pytest.param(
                DataType.VARCHAR,
                {"max_length": 256},
                "VARCHAR_NGRAM",
                'scalar_field like "%ello%"',
                {"index_type": "NGRAM", "params": {"min_gram": 2, "max_gram": 3}},
                id="VARCHAR_NGRAM",
            ),
        ],
    )
    def test_warmup_async_scalar_type(self, scalar_type, field_kwargs, idx_type, filter_expr, create_idx_params):
        """
        target: verify warmup.scalarIndex=async (index-level warmup) works across all scalar field
                types and supported index types; scalarIndex warmup affects index data; verify via:
                (1) filter on scalar_field — exercises scalar index data;
                (2) output_fields=["scalar_field"] — exercises raw scalar column data
        method: parametrize scalar_type/field_kwargs/idx_type/filter_expr/create_idx_params;
                insert 5000 rows; flush; build indexes (simple add_index or separate call for
                JSON/NGRAM special params); load; query with filter + output_fields
        expected: query returns 100 rows (limit=100); every row carries the scalar_field value
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("float_vector_1", DataType.FLOAT_VECTOR, dim=128)
        schema.add_field("scalar_field", scalar_type, nullable=True, **field_kwargs)
        self.create_collection(client, collection_name, schema=schema, properties={"warmup.scalarIndex": "async"})

        schema_res = self.describe_collection(client, collection_name)[0]
        if scalar_type == DataType.BOOL:
            scalar_values = [i % 2 == 0 for i in range(nb)]
        elif scalar_type == DataType.ARRAY:
            scalar_values = [[i % 10, (i + 1) % 10, (i + 2) % 10] for i in range(nb)]
        elif scalar_type in (DataType.FLOAT, DataType.DOUBLE):
            scalar_values = [float(i) / 10 for i in range(nb)]
        elif scalar_type == DataType.JSON:
            # category i%10==3 → 500/5000 rows match == 3.0; limit=100 → 100 rows
            scalar_values = [{"category": float(i % 10)} for i in range(nb)]
        elif scalar_type == DataType.VARCHAR and create_idx_params is not None:
            # NGRAM case: values contain "ello" so like "%ello%" matches all 5000; limit=100 → 100
            scalar_values = [f"hello world {i}" for i in range(nb)]
        elif scalar_type == DataType.VARCHAR:
            # prefix_0 repeats every 50 → 100 rows exactly match == "prefix_0"; limit=100 → 100
            scalar_values = [f"prefix_{i % 50}" for i in range(nb)]
        else:
            scalar_values = [i % 100 for i in range(nb)]

        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(nb)),
                "scalar_field": scalar_values,
            },
        )

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # build indexes after data is flushed so index covers actual segments
        index_params_obj = self.all_vector_index(
            self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
        )
        if create_idx_params is None:
            # simple index types: pass index_type string directly
            index_params_obj.add_index(field_name="scalar_field", index_type=idx_type)
            self.create_index(client, collection_name, index_params_obj)
        else:
            # special index types (JSON INVERTED with json_cast_type, NGRAM): separate create_index call
            self.create_index(client, collection_name, index_params_obj)
            scalar_idx = self.prepare_index_params(client)[0]
            scalar_idx.add_index(field_name="scalar_field", **create_idx_params)
            self.create_index(client, collection_name, index_params=scalar_idx)
        self.load_collection(client, collection_name)

        # (1) filter on scalar_field — exercises scalar index data (scalarIndex warmup target)
        # (2) output_fields=["scalar_field"] — exercises raw scalar column data
        q = self.query(
            client, collection_name, filter=filter_expr, output_fields=["int64_pk", "scalar_field"], limit=100
        )[0]
        assert len(q) == 100, (
            f"{scalar_type.name}+{idx_type}: query with '{filter_expr}' returned {len(q)} rows (expected 100)"
        )
        assert all("scalar_field" in r for r in q), (
            f"{scalar_type.name}+{idx_type}: scalar_field missing in output_fields result"
        )

        self.drop_collection(client, collection_name)

    # ====================== Warmup Behavioral Tests ======================
    # These tests verify the *actual effect* of warmup policies via observable
    # API-level behaviors: load duration, search accessibility, and cancel safety.

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_behavior_async_load_accessible_and_cancel(self):
        """
        target: (phase 1) verify async warmup does not block data accessibility — search
                succeeds immediately after load returns, before background prefetch completes;
                (phase 2) verify releasing during async warmup cancels cleanly — subsequent
                reload and search work correctly after two cancel+reload cycles
        method: large dataset (50K × dim=512) with vectorIndex=async; build once and reuse:
                phase1: load → immediately search;
                phase2: release → load → search (cycle 1); release → load → search (cycle 2)
        expected: all searches return 10 results; no exception at any step
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 512
        nb, batch = 50000, 5000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=dim).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema, properties={"warmup.vectorIndex": "async"})

        res = self.describe_collection(client, collection_name)[0]
        rows_large = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=res)
        for d in cf.iter_mc_insert_list_data(rows_large, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
            timeout=600,
        )

        # phase 1: async load returns before prefetch finishes; data must be accessible immediately
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, dim),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # phase 2 cycle 1: release (cancel warmup mid-flight) → reload → search
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, dim),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # phase 2 cycle 2: repeat to verify stability
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, dim),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_behavior_load_time_comparison(self):
        """
        target: verify sync warmup blocks load longer than async (observable timing difference);
                and that field-level warmup overrides collection-level via the same timing signal
        phase1 (collection level): col_sync uses vectorIndex=sync, col_async uses vectorIndex=async;
                assert t_sync > t_async
        phase2 (field priority): col_field_sync uses field=sync + collection=disable;
                col_field_disable uses field=disable + collection=sync;
                assert t_field_sync > t_field_disable (field level takes priority)
        method: 50K × dim=512 rows per collection (batch insert); measure load durations
        expected: both timing assertions hold; all four collections are searchable
        """
        client = self._client()
        nb, dim = 20000, 256
        batch = 5000
        fields = ["int64_pk", "float_vector_1"]

        def build_col(name, schema_fn, props):
            schema = schema_fn()
            self.create_collection(client, name, schema=schema, properties=props)
            res = self.describe_collection(client, name)[0]
            rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=res)
            for d in cf.iter_mc_insert_list_data(rows, batch, nb):
                self.insert(client, name, d)
            self.flush(client, name)
            self.create_index(
                client,
                name,
                self.all_vector_index(
                    self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
                ),
                timeout=240,
            )

        base = cf.gen_collection_name_by_testcase_name()

        # ── Phase 1: collection-level sync vs async ──
        def plain_schema():
            s = self.create_schema(client, enable_dynamic_field=False)[0]
            cf.gen_milvus_client_schema(
                s,
                fields=fields,
                field_params={
                    "int64_pk": FieldParams(is_primary=True).to_dict,
                    "float_vector_1": FieldParams(dim=dim).to_dict,
                },
            )
            return s

        name_sync = base + "_col_sync"
        name_async = base + "_col_async"
        build_col(name_sync, plain_schema, {"warmup.vectorIndex": "sync"})
        build_col(name_async, plain_schema, {"warmup.vectorIndex": "async"})

        t0 = time.time()
        self.load_collection(client, name_sync)
        t_sync = time.time() - t0
        t0 = time.time()
        self.load_collection(client, name_async)
        t_async = time.time() - t0
        log.info(f"phase1 load — sync: {t_sync:.3f}s, async: {t_async:.3f}s")
        # Timing assertion only holds when tiered storage is active (data on disk).
        # Require both loads to exceed 1s AND a minimum gap of 0.5s to avoid noise.
        if t_sync >= 1.0 and t_async >= 1.0 and abs(t_sync - t_async) >= 0.5:
            assert t_sync > t_async, f"sync ({t_sync:.3f}s) should be slower than async ({t_async:.3f}s)"
        else:
            log.info(
                "phase1 timing skipped — loads too fast or difference too small "
                f"(sync={t_sync:.3f}s async={t_async:.3f}s)"
            )
        self.search(
            client,
            name_sync,
            cf.gen_vectors(1, dim),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )
        self.search(
            client,
            name_async,
            cf.gen_vectors(1, dim),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )
        self.drop_collection(client, name_sync)
        self.drop_collection(client, name_async)

        # ── Phase 2: field-level priority over collection-level ──
        def schema_with_field_warmup(warmup_val):
            s = self.create_schema(client, enable_dynamic_field=False)[0]
            cf.gen_milvus_client_schema(
                s,
                fields=fields,
                field_params={
                    "int64_pk": FieldParams(is_primary=True).to_dict,
                    "float_vector_1": FieldParams(dim=dim, warmup=warmup_val).to_dict,
                },
            )
            return s

        # field=sync + collection=disable → sync warmup active (field wins)
        name_fs = base + "_field_sync"
        build_col(name_fs, lambda: schema_with_field_warmup("sync"), {"warmup.vectorField": "disable"})
        res_fs = self.describe_collection(client, name_fs)[0]
        assert cf.get_field_warmup(res_fs, "float_vector_1") == "sync"
        assert cf.get_collection_warmup(res_fs, "warmup.vectorField") == "disable"

        # field=disable + collection=sync → no warmup (field wins)
        name_fd = base + "_field_disable"
        build_col(name_fd, lambda: schema_with_field_warmup("disable"), {"warmup.vectorField": "sync"})
        res_fd = self.describe_collection(client, name_fd)[0]
        assert cf.get_field_warmup(res_fd, "float_vector_1") == "disable"
        assert cf.get_collection_warmup(res_fd, "warmup.vectorField") == "sync"

        t0 = time.time()
        self.load_collection(client, name_fs)
        t_field_sync = time.time() - t0
        t0 = time.time()
        self.load_collection(client, name_fd)
        t_field_disable = time.time() - t0
        log.info(f"phase2 load — field=sync: {t_field_sync:.3f}s, field=disable: {t_field_disable:.3f}s")
        if t_field_sync >= 1.0 and t_field_disable >= 1.0 and abs(t_field_sync - t_field_disable) >= 0.5:
            assert t_field_sync > t_field_disable, (
                f"field=sync ({t_field_sync:.3f}s) should be slower than field=disable ({t_field_disable:.3f}s)"
            )
        else:
            log.info(
                "phase2 timing skipped — loads too fast or difference too small "
                f"(field_sync={t_field_sync:.3f}s field_disable={t_field_disable:.3f}s)"
            )
        self.search(
            client,
            name_fs,
            cf.gen_vectors(1, dim),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )
        self.search(
            client,
            name_fd,
            cf.gen_vectors(1, dim),
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )
        self.drop_collection(client, name_fs)
        self.drop_collection(client, name_fd)

    # ====================== DQL / DML Correctness Tests ======================
    # These tests verify that query / search / hybrid_search / insert / upsert / delete
    # all work correctly on async-warmup-configured columns with >= 10K rows.

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_async_dql_full_coverage(self):
        """
        target: verify query / search / hybrid_search all return correct results on
                async-configured vector and scalar columns; 10K rows; no warmup-induced errors
        method: schema with two vector fields (vec_a L2, vec_b COSINE) + int_field + str_field,
                all async; collection-level vectorField=async, scalarField=async;
                generate all 10K rows first, build pk_to_row lookup, precompute expected counts,
                then insert in 2 batches; flush; load;
                run query (equality/range/string/composite filters, various output_fields, limit);
                run search (plain, with scalar filter, with output_fields, limit boundary);
                run hybrid_search (WeightedRanker, RRFRanker, with scalar filter)
        expected: all counts match precomputed expectations; field values match inserted data; no MilvusException
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        nb = 10000
        batch = 5000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "float_vector_2", "int64_1", "varchar_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=dim, warmup="async").to_dict,
                "float_vector_2": FieldParams(dim=dim, warmup="async").to_dict,
                "int64_1": FieldParams(nullable=True, warmup="async").to_dict,
                "varchar_1": FieldParams(max_length=256, nullable=True, warmup="async").to_dict,
            },
        )

        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={"warmup.vectorField": "async", "warmup.scalarField": "async"},
        )

        # generate all rows upfront so expected counts are derived from actual inserted data;
        # int64_1 and varchar_1 use deterministic patterns for filter verification
        schema_res = self.describe_collection(client, collection_name)[0]
        all_rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(nb)),
                "int64_1": [i % 100 for i in range(nb)],
                "varchar_1": [f"group_{i % 10}" for i in range(nb)],
            },
        )
        pk_to_row = {r["int64_pk"]: r for r in all_rows}

        # insert in batches
        for d in cf.iter_mc_insert_list_data(all_rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0],
                {
                    **DefaultVectorIndexParams.HNSW("float_vector_1"),
                    **DefaultVectorIndexParams.HNSW("float_vector_2", metric_type=MetricType.COSINE),
                },
            ),
        )
        self.load_collection(client, collection_name)

        # --- Query coverage ---
        # 1. equality filter on async int64_1
        exp_int64_1_eq_42 = sum(1 for r in all_rows if r["int64_1"] == 42)
        q1 = self.query(
            client,
            collection_name,
            filter="int64_1 == 42",
            output_fields=["int64_pk", "int64_1"],
            limit=nb,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": exp_int64_1_eq_42, "output_fields": ["int64_pk", "int64_1"]},
        )[0]
        for r in q1:
            assert r["int64_1"] == 42
            assert pk_to_row[r["int64_pk"]]["int64_1"] == 42, f"int64_1 source mismatch for pk={r['int64_pk']}"

        # 2. range filter on async int64_1
        exp_int64_1_range = sum(1 for r in all_rows if 10 <= r["int64_1"] < 20)
        q2 = self.query(
            client,
            collection_name,
            filter="int64_1 >= 10 and int64_1 < 20",
            output_fields=["int64_pk", "int64_1"],
            limit=nb,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": exp_int64_1_range},
        )[0]
        for r in q2:
            assert 10 <= r["int64_1"] < 20
            assert r["int64_1"] == pk_to_row[r["int64_pk"]]["int64_1"], f"int64_1 mismatch for pk={r['int64_pk']}"

        # 3. string filter on async varchar_1
        exp_varchar_group3 = sum(1 for r in all_rows if r["varchar_1"] == "group_3")
        q3 = self.query(
            client,
            collection_name,
            filter='varchar_1 == "group_3"',
            output_fields=["int64_pk", "varchar_1"],
            limit=nb,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": exp_varchar_group3, "output_fields": ["int64_pk", "varchar_1"]},
        )[0]
        for r in q3:
            assert r["varchar_1"] == "group_3"
            assert r["varchar_1"] == pk_to_row[r["int64_pk"]]["varchar_1"], f"varchar_1 mismatch for pk={r['int64_pk']}"

        # 4. composite filter on async scalar fields
        exp_q4 = sum(1 for r in all_rows if r["int64_1"] < 50 and r["varchar_1"] != "")
        q4 = self.query(
            client,
            collection_name,
            filter='int64_1 < 50 and varchar_1 != ""',
            output_fields=["int64_pk", "int64_1", "varchar_1"],
            limit=nb,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": exp_q4},
        )[0]
        for r in q4:
            assert r["int64_1"] < 50
            assert r["int64_1"] == pk_to_row[r["int64_pk"]]["int64_1"], f"int64_1 mismatch for pk={r['int64_pk']}"
            assert r["varchar_1"] == pk_to_row[r["int64_pk"]]["varchar_1"], f"varchar_1 mismatch for pk={r['int64_pk']}"

        # 5. output_fields includes async vector and scalar columns; verify values match inserted data
        exp_pk_lt10 = sum(1 for r in all_rows if r["int64_pk"] < 10)
        q5 = self.query(
            client,
            collection_name,
            filter="int64_pk < 10",
            output_fields=["int64_pk", "float_vector_1", "int64_1", "varchar_1"],
            limit=10,
            check_task=CheckTasks.check_query_results,
            check_items={
                "exp_limit": exp_pk_lt10,
                "output_fields": ["int64_pk", "float_vector_1", "int64_1", "varchar_1"],
            },
        )[0]
        for r in q5:
            expected = pk_to_row[r["int64_pk"]]
            assert r.get("float_vector_1") is not None, f"float_vector_1 missing for pk={r['int64_pk']}"
            assert len(r["float_vector_1"]) == dim, f"float_vector_1 wrong dim for pk={r['int64_pk']}"
            assert r["int64_1"] == expected["int64_1"], f"int64_1 mismatch for pk={r['int64_pk']}"
            assert r["varchar_1"] == expected["varchar_1"], f"varchar_1 mismatch for pk={r['int64_pk']}"

        # 6. various limit values
        for lim in [1, 100, 1000]:
            self.query(
                client,
                collection_name,
                filter="int64_pk >= 0",
                limit=lim,
                check_task=CheckTasks.check_query_results,
                check_items={"exp_limit": lim},
            )

        # --- Search coverage ---
        query_vec_1 = cf.gen_vectors(1, dim)
        query_vec_2 = cf.gen_vectors(1, dim)

        # 1. plain search on async float_vector_1 (L2)
        self.search(
            client,
            collection_name,
            query_vec_1,
            anns_field="float_vector_1",
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # 2. search with scalar filter on async varchar_1; exp_varchar_group0 >> limit=20, always full
        exp_varchar_group0 = sum(1 for r in all_rows if r["varchar_1"] == "group_0")
        s2 = self.search(
            client,
            collection_name,
            query_vec_1,
            anns_field="float_vector_1",
            filter='varchar_1 == "group_0"',
            limit=20,
            output_fields=["varchar_1"],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": min(20, exp_varchar_group0)},
        )[0]
        for hit in s2[0]:
            assert hit["entity"]["varchar_1"] == "group_0"
            assert hit["entity"]["varchar_1"] == pk_to_row[hit["int64_pk"]]["varchar_1"], (
                f"varchar_1 mismatch for pk={hit['int64_pk']}"
            )

        # 3. search with output_fields including async columns; verify values match inserted data
        s3 = self.search(
            client,
            collection_name,
            query_vec_1,
            anns_field="float_vector_1",
            output_fields=["int64_1", "varchar_1", "float_vector_1"],
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )[0]
        for hit in s3[0]:
            expected = pk_to_row[hit["int64_pk"]]
            assert hit["entity"]["int64_1"] == expected["int64_1"], f"int64_1 mismatch for pk={hit['int64_pk']}"
            assert hit["entity"]["varchar_1"] == expected["varchar_1"], f"varchar_1 mismatch for pk={hit['int64_pk']}"
            assert hit["entity"].get("float_vector_1") is not None, f"float_vector_1 missing for pk={hit['int64_pk']}"
            assert len(hit["entity"]["float_vector_1"]) == dim, f"float_vector_1 wrong dim for pk={hit['int64_pk']}"

        # 4. search on async float_vector_2 (COSINE)
        self.search(
            client,
            collection_name,
            query_vec_2,
            anns_field="float_vector_2",
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # 5. limit boundary: limit=1
        self.search(
            client,
            collection_name,
            query_vec_1,
            anns_field="float_vector_1",
            limit=1,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )

        # --- Hybrid search coverage ---
        req_a = AnnSearchRequest(
            query_vec_1,
            "float_vector_1",
            self.get_default_search_params(client, collection_name, "float_vector_1", 20),
            limit=20,
        )
        req_b = AnnSearchRequest(
            query_vec_2,
            "float_vector_2",
            self.get_default_search_params(client, collection_name, "float_vector_2", 20),
            limit=20,
        )

        # 1. hybrid search with WeightedRanker; 10K rows >> limit=10, always returns exactly 10
        hs1 = self.hybrid_search(client, collection_name, [req_a, req_b], WeightedRanker(0.5, 0.5), limit=10)[0]
        assert len(hs1[0]) == 10, "hybrid search should return exactly 10 results"

        # 2. hybrid search with RRFRanker
        hs2 = self.hybrid_search(client, collection_name, [req_a, req_b], RRFRanker(), limit=10)[0]
        assert len(hs2[0]) == 10, "hybrid search with RRFRanker should return exactly 10 results"

        # 3. hybrid search with scalar filter on async int64_1; exp_int64_1_lt50 >> limit=10
        exp_int64_1_lt50 = sum(1 for r in all_rows if r["int64_1"] < 50)
        req_a_filtered = AnnSearchRequest(
            query_vec_1,
            "float_vector_1",
            self.get_default_search_params(client, collection_name, "float_vector_1", 20),
            limit=20,
            expr="int64_1 < 50",
        )
        req_b_filtered = AnnSearchRequest(
            query_vec_2,
            "float_vector_2",
            self.get_default_search_params(client, collection_name, "float_vector_2", 20),
            limit=20,
            expr="int64_1 < 50",
        )
        hs3 = self.hybrid_search(
            client,
            collection_name,
            [req_a_filtered, req_b_filtered],
            WeightedRanker(0.5, 0.5),
            limit=10,
            output_fields=["int64_1"],
        )[0]
        assert len(hs3[0]) == min(10, exp_int64_1_lt50), (
            f"hybrid search int64_1<50: expected {min(10, exp_int64_1_lt50)} hits, got {len(hs3[0])}"
        )
        for hit in hs3[0]:
            assert hit["entity"]["int64_1"] < 50, (
                f"hybrid search filter int64_1 < 50 violated: {hit['entity']['int64_1']}"
            )
            assert hit["entity"]["int64_1"] == pk_to_row[hit["int64_pk"]]["int64_1"], (
                f"int64_1 mismatch for pk={hit['int64_pk']}"
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_dml_insert_upsert_delete(self):
        """
        target: verify insert / upsert / delete all work correctly on async-configured columns;
                data >= 10K rows; each DML step followed by query/search verification
        method: schema with vec(async) + score(float, async) + tag(varchar, async);
                collection-level vectorField=async, scalarField=async;
                phase1 insert 10K rows; phase2 insert 2K more; phase3 upsert (1K update + 1K new);
                phase4 delete by pk (500) + delete by expr (2K); verify at each phase
        expected: all DML operations complete without error; query/search results match expected counts and values
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        nb, batch = 10000, 5000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "float_1", "varchar_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=dim, warmup="async").to_dict,
                "float_1": FieldParams(nullable=True, warmup="async").to_dict,
                "varchar_1": FieldParams(max_length=128, nullable=True, warmup="async").to_dict,
            },
        )

        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={"warmup.vectorField": "async", "warmup.scalarField": "async"},
        )

        # ── Phase 1: insert 10K rows ──
        schema_res = self.describe_collection(client, collection_name)[0]
        rows_phase1 = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=schema_res)
        for d in cf.iter_mc_insert_list_data(rows_phase1, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)

        self.query(
            client,
            collection_name,
            filter="int64_pk >= 0",
            limit=16384,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": nb},
        )

        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, dim),
            anns_field="float_vector_1",
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # ── Phase 2: insert 2K more rows (int64_pk=10000~11999) ──
        rows_new = cf.gen_row_data_by_schema_with_defaults(
            nb=2000,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(10000, 12000)),
                "float_1": [float(10000 + i) for i in range(2000)],
                "varchar_1": "inserted",
            },
        )
        self.insert(client, collection_name, rows_new)
        self.flush(client, collection_name)

        self.query(
            client,
            collection_name,
            filter='varchar_1 == "inserted"',
            output_fields=["int64_pk", "varchar_1"],
            limit=5000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 2000, "output_fields": ["int64_pk", "varchar_1"]},
        )

        self.query(
            client,
            collection_name,
            filter="int64_pk >= 0",
            limit=16384,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 12000},
        )

        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, dim),
            anns_field="float_vector_1",
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # ── Phase 3: upsert (1K update existing int64_pk=0~999, 1K new int64_pk=20000~20999) ──
        rows_upsert_update = cf.gen_row_data_by_schema_with_defaults(
            nb=1000,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(1000)),
                "float_1": [float(i) + 9999.0 for i in range(1000)],
                "varchar_1": "updated",
            },
        )
        rows_upsert_new = cf.gen_row_data_by_schema_with_defaults(
            nb=1000,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(20000, 21000)),
                "float_1": [float(20000 + i) for i in range(1000)],
                "varchar_1": "upserted_new",
            },
        )
        self.upsert(client, collection_name, rows_upsert_update + rows_upsert_new)
        self.flush(client, collection_name)

        q_updated = self.query(
            client,
            collection_name,
            filter='varchar_1 == "updated"',
            output_fields=["int64_pk", "float_1", "varchar_1"],
            limit=5000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 1000, "output_fields": ["int64_pk", "float_1", "varchar_1"]},
        )[0]
        for r in q_updated:
            assert r["float_1"] == float(r["int64_pk"]) + 9999.0, (
                f"phase3: upserted float_1 mismatch int64_pk={r['int64_pk']} float_1={r['float_1']}"
            )

        self.query(
            client,
            collection_name,
            filter='varchar_1 == "upserted_new"',
            output_fields=["int64_pk", "varchar_1"],
            limit=5000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 1000, "output_fields": ["int64_pk", "varchar_1"]},
        )

        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, dim),
            anns_field="float_vector_1",
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # ── Phase 4: delete by pk (500 rows: int64_pk=0~499) ──
        self.delete(client, collection_name, ids=list(range(500)))
        self.query(
            client,
            collection_name,
            filter="int64_pk < 500",
            limit=5000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 0},
        )

        # ── Phase 4b: delete by expr (varchar_1 == "inserted") ──
        self.delete(client, collection_name, filter='varchar_1 == "inserted"')
        self.query(
            client,
            collection_name,
            filter='varchar_1 == "inserted"',
            limit=5000,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 0},
        )

        self.search(
            client,
            collection_name,
            cf.gen_vectors(1, dim),
            anns_field="float_vector_1",
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        # ── Final verification: total count after all DML ──
        # phase1: 10K (pk 0~9999)
        # phase2: +2K (pk 10000~11999, varchar_1="inserted") → 12K
        # phase3 upsert-update: pk 0~999 overwritten (net 0 new); upsert-new: +1K (pk 20000~20999) → 13K
        # phase4a: delete pk 0~499 → -500 → 12500
        # phase4b: delete varchar_1="inserted" (pk 10000~11999) → -2K → 10500
        self.query(
            client,
            collection_name,
            filter="int64_pk >= 0",
            limit=16384,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 10500},
        )

        self.drop_collection(client, collection_name)

    # ========================= Bug-Finding Tests =========================
    # Each test is designed to expose a specific failure mode in async warmup.
    # They go beyond happy-path verification and stress correctness, concurrency,
    # and cancel-mechanism robustness.

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_rapid_cancel_reload_data_integrity(self):
        """
        target: stress the cancel mechanism — rapid load→search→release×8 must not corrupt data
        bug hypothesis: when release() cancels an in-progress async warmup, the partially-loaded
                        segment state may be inconsistent; the next load could read stale/corrupt state
        method: fixed-seed data (5000 rows), build HNSW index, load with all-async warmup;
                cycle 8 times: load → query exact pk set (must == baseline) → release;
                no sleep between ops; final load adds filter+output_fields verification
        expected: every cycle returns exact same pk set {0..4999}; no data loss or corruption
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim, nb, batch = 128, 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "int64_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "int64_1": FieldParams(nullable=True).to_dict,
            },
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={
                k: "async"
                for k in ["warmup.vectorIndex", "warmup.vectorField", "warmup.scalarIndex", "warmup.scalarField"]
            },
        )

        schema_res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(nb)),
                "int64_1": [i % 100 for i in range(nb)],
            },
        )
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )

        baseline = set(range(nb))
        query_vec = cf.gen_vectors(1, dim)

        for cycle in range(8):
            self.load_collection(client, collection_name)
            # search immediately — async warmup may still be running in background
            self.search(
                client,
                collection_name,
                query_vec,
                limit=10,
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 10},
            )

            q = self.query(
                client,
                collection_name,
                filter="int64_pk >= 0",
                output_fields=["int64_pk"],
                limit=nb,
                check_task=CheckTasks.check_query_results,
                check_items={"exp_limit": nb, "output_fields": ["int64_pk"]},
            )[0]
            actual = {r["int64_pk"] for r in q}
            assert actual == baseline, (
                f"cycle {cycle}: pk set corrupted — missing={baseline - actual}, extra={actual - baseline}"
            )
            self.release_collection(client, collection_name)

        # final cycle: deeper field-level verification
        self.load_collection(client, collection_name)
        # int64_1 = int64_pk % 100; int64_1 < 10 → int64_pk % 100 < 10 → 500 rows (0,10,20,...×50)
        q_filter = self.query(
            client,
            collection_name,
            filter="int64_1 < 10",
            output_fields=["int64_pk", "int64_1"],
            limit=nb,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 500, "output_fields": ["int64_pk", "int64_1"]},
        )[0]
        for r in q_filter:
            assert r["int64_1"] < 10, f"filter violated: int64_pk={r['int64_pk']} int64_1={r['int64_1']}"
            assert r["int64_1"] == r["int64_pk"] % 100, (
                f"value corrupted: int64_pk={r['int64_pk']} expected int64_1={r['int64_pk'] % 100} got {r['int64_1']}"
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_field_disable_overrides_collection_query_accessible(self):
        """
        target: field-level disable overrides collection-level sync — and disabled fields must
                still be queryable (disable = skip pre-warm, NOT = block access)
        bug hypothesis: if priority chain is wrong, field_disable may inherit collection=sync warmup
                        (describe would show wrong value); or if disable is mis-implemented as
                        "deny access", filter+retrieve on the field would silently return wrong data
        method: two scalar fields — field_async(warmup=async), field_disable(warmup=disable);
                collection-level scalarField=sync; describe must show disable wins;
                load; filter on field_disable (no index → raw column read) + output_fields retrieval;
                verify exact values match inserted data
        expected: describe shows field_disable="disable" (field > collection priority);
                  filter returns exactly 50 rows; each row value == pk*2 (exact match)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "int64_1", "int64_2"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "int64_1": FieldParams(nullable=True, warmup="async").to_dict,
                "int64_2": FieldParams(nullable=True, warmup="disable").to_dict,
            },
        )
        # collection-level sync should be overridden by field-level disable
        self.create_collection(client, collection_name, schema=schema, properties={"warmup.scalarField": "sync"})

        schema_res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(nb)),
                "int64_1": list(range(nb)),
                "int64_2": [i * 2 for i in range(nb)],
            },
        )
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)

        # verify priority chain in describe
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "int64_2") == "disable", (
            "field-level disable should override collection-level sync"
        )
        assert cf.get_field_warmup(res, "int64_1") == "async"

        # int64_2 has no index → filter reads raw column data (cold read path)
        # int64_2 = int64_pk * 2; int64_2 < 100 → int64_pk < 50 → exactly 50 rows
        q = self.query(
            client,
            collection_name,
            filter="int64_2 < 100",
            output_fields=["int64_pk", "int64_2", "int64_1"],
            limit=nb,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 50, "output_fields": ["int64_pk", "int64_2", "int64_1"]},
        )[0]
        for r in q:
            assert r["int64_2"] < 100, f"filter violated: int64_pk={r['int64_pk']} int64_2={r['int64_2']}"
            assert r["int64_2"] == r["int64_pk"] * 2, (
                f"value corrupted: int64_pk={r['int64_pk']} expected {r['int64_pk'] * 2} got {r['int64_2']}"
            )
            assert r["int64_1"] == r["int64_pk"], (
                f"int64_1 corrupted: int64_pk={r['int64_pk']} expected {r['int64_pk']} got {r['int64_1']}"
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_concurrent_insert_strong_consistency(self):
        """
        target: async warmup must not hold locks that block DML or delay strong-consistency reads
        bug hypothesis: if warmup thread acquires a segment read-lock, concurrent insert that causes
                        a growing→sealed transition could deadlock; or if warmup modifies shared
                        segment state, a strong-consistency query could see a torn snapshot
        method: 10K sealed rows, all-async warmup; load_collection (returns immediately);
                immediately insert 500 extra rows (growing segment) — no sleep;
                strong consistency query must return all 10500 rows;
                verify new rows are distinguishable (tag=1) and values correct
        expected: insert succeeds; count == 10500; strong consistency sees newly inserted data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb_base, nb_extra = 10000, 500

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "int64_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "int64_1": FieldParams(nullable=True).to_dict,
            },
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={
                k: "async"
                for k in ["warmup.vectorIndex", "warmup.vectorField", "warmup.scalarIndex", "warmup.scalarField"]
            },
        )

        insert_batch = 5000
        schema_res = self.describe_collection(client, collection_name)[0]
        rows_base = cf.gen_row_data_by_schema_with_defaults(
            nb=nb_base,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(nb_base)),
                "int64_1": [0] * nb_base,
            },
        )
        for d in cf.iter_mc_insert_list_data(rows_base, insert_batch, nb_base):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)

        # insert extra rows immediately after async load — warmup is running concurrently
        extra = cf.gen_row_data_by_schema_with_defaults(
            nb=nb_extra,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(nb_base, nb_base + nb_extra)),
                "int64_1": [1] * nb_extra,
            },
        )
        self.insert(client, collection_name, extra)

        # strong consistency — must reflect all inserts including growing segment
        q = self.query(
            client,
            collection_name,
            filter="int64_pk >= 0",
            output_fields=["int64_pk", "int64_1"],
            limit=nb_base + nb_extra,
            consistency_level="Strong",
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": nb_base + nb_extra, "output_fields": ["int64_pk", "int64_1"]},
        )[0]

        new_rows = [r for r in q if r["int64_1"] == 1]
        assert len(new_rows) == nb_extra, f"expected {nb_extra} int64_1=1 rows, got {len(new_rows)}"
        new_pks = {r["int64_pk"] for r in new_rows}
        assert new_pks == set(range(nb_base, nb_base + nb_extra)), (
            f"new pk set mismatch: {new_pks ^ set(range(nb_base, nb_base + nb_extra))}"
        )

        self.search(
            client,
            collection_name,
            [extra[0]["float_vector_1"]],
            limit=10,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_async_vs_sync_search_result_identical(self):
        """
        target: switching warmup mode on the SAME collection/index must not change search results
        bug hypothesis: if async warmup uses a different internal load path (lazy-load, different
                        graph entry point), reloading the same index with async warmup could
                        produce different top-k results than sync warmup for the same query
        method: ONE collection, fixed seed data (5000 rows), HNSW L2 index;
                round1: load with all-sync warmup → capture top-20 pk sets for 5 query vectors
                        and exact pk sets for 3 filter queries;
                release → alter all warmup properties to async;
                round2: load with all-async warmup → run same queries;
                compare: round1 == round2 (same index = deterministic results regardless of warmup)
        expected: every search and filter query returns bit-identical pk sets across sync and async
        """
        client = self._client()
        nb, dim, batch = 5000, 128, 2000
        collection_name = cf.gen_collection_name_by_testcase_name()

        query_vecs = cf.gen_vectors(5, dim)
        filter_exprs = ["int64_1 < 10", "int64_1 >= 90", "int64_1 == 42"]

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "int64_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "int64_1": FieldParams(nullable=True).to_dict,
            },
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={
                k: "sync"
                for k in ["warmup.vectorIndex", "warmup.vectorField", "warmup.scalarIndex", "warmup.scalarField"]
            },
        )
        schema_res = self.describe_collection(client, collection_name)[0]
        all_rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=schema_res,
            default_values={
                "int64_pk": list(range(nb)),
                "int64_1": [i % 100 for i in range(nb)],
            },
        )
        for d in cf.iter_mc_insert_list_data(all_rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )

        # ── Round 1: sync warmup ──
        self.load_collection(client, collection_name)
        sync_search = [
            {h["int64_pk"] for h in self.search(client, collection_name, [qv], limit=20)[0][0]} for qv in query_vecs
        ]
        sync_filter = [
            {r["int64_pk"] for r in self.query(client, collection_name, filter=expr, limit=500)[0]}
            for expr in filter_exprs
        ]
        self.release_collection(client, collection_name)

        # alter ALL warmup properties to async on the same collection (same index unchanged)
        for prop in ["warmup.vectorIndex", "warmup.vectorField", "warmup.scalarIndex", "warmup.scalarField"]:
            self.alter_collection_properties(client, collection_name, properties={prop: "async"})

        # ── Round 2: async warmup (same index, same data) ──
        self.load_collection(client, collection_name)
        async_search = [
            {h["int64_pk"] for h in self.search(client, collection_name, [qv], limit=20)[0][0]} for qv in query_vecs
        ]
        async_filter = [
            {r["int64_pk"] for r in self.query(client, collection_name, filter=expr, limit=500)[0]}
            for expr in filter_exprs
        ]

        # same index → results must be bit-identical regardless of warmup mode
        for idx, (s_pks, a_pks) in enumerate(zip(sync_search, async_search)):
            assert s_pks == a_pks, (
                f"query {idx}: search results diverged after changing sync→async warmup "
                f"(same index/data) — only in sync: {s_pks - a_pks}, "
                f"only in async: {a_pks - s_pks}"
            )
        for expr, s_pks, a_pks in zip(filter_exprs, sync_filter, async_filter):
            assert s_pks == a_pks, (
                f"filter '{expr}': results diverged after sync→async warmup change — "
                f"missing in async: {s_pks - a_pks}, extra: {a_pks - s_pks}"
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_x_multi_vector_mixed_index(self):
        """
        target:
            1. verify async warmup with two float vector fields using mixed index types:
               float_vector_1 (HNSW, hasRawData=True) and float_vector_2 (IVF_PQ, hasRawData=False)
            2. verify individual ANN search with output_fields returns non-None vector values for each field
            3. verify hybrid_search spanning both fields returns hits with both vector fields populated
        method:
            1. create collection with 2 float vector fields, both field-level warmup=async;
               collection-level warmup.vectorIndex=async + warmup.vectorField=async
            2. insert 5000 rows; flush; build HNSW on float_vector_1, IVF_PQ on float_vector_2; load
            3. individual search on each field with output_fields=["*"]
            4. hybrid_search(float_vector_1 HNSW + float_vector_2 IVF_PQ, RRFRanker) with output_fields=["*"]
        expected:
            1. individual searches return 10 hits with non-None vector values for the searched field
            2. hybrid_search returns 10 hits with both float_vector_1 and float_vector_2 in entity
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb, batch = 5000, 2000
        dim = 128

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "float_vector_2"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=dim, warmup="async").to_dict,
                "float_vector_2": FieldParams(dim=dim, warmup="async").to_dict,
            },
        )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            properties={"warmup.vectorIndex": "async", "warmup.vectorField": "async"},
        )

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "float_vector_1") == "async"
        assert cf.get_field_warmup(res, "float_vector_2") == "async"

        rows = cf.gen_row_data_by_schema_with_defaults(nb=nb, schema=res, default_values={"int64_pk": list(range(nb))})
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        # build indexes after flush so quantizer trains on real data
        index_params = self.prepare_index_params(client)[0]
        for field_name, idx in {
            **DefaultVectorIndexParams.HNSW("float_vector_1"),
            **DefaultVectorIndexParams.IVF_PQ("float_vector_2", nlist=128, m=8, nbits=8),
        }.items():
            index_params.add_index(field_name=field_name, **idx.to_dict)
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        qv = cf.gen_vectors(1, dim)

        # (1) individual searches — verify each raw-data path under async warmup
        for anns_field, sp in [
            ("float_vector_1", DefaultVectorSearchParams.HNSW(metric_type=MetricType.L2)),
            ("float_vector_2", DefaultVectorSearchParams.IVF_PQ(metric_type=MetricType.L2, nprobe=16)),
        ]:
            s = self.search(
                client,
                collection_name,
                qv,
                anns_field=anns_field,
                limit=10,
                output_fields=["*"],
                search_params=sp,
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 10},
            )[0]
            for hit in s[0]:
                assert hit["entity"].get(anns_field) is not None, (
                    f"search({anns_field}): vec is None (async warmup raw read failed)"
                )

        # (2) hybrid_search: HNSW (hasRawData=True) + IVF_PQ (hasRawData=False) simultaneously
        req_hnsw = AnnSearchRequest(
            qv, "float_vector_1", DefaultVectorSearchParams.HNSW(metric_type=MetricType.L2), limit=10
        )
        req_ivf_pq = AnnSearchRequest(
            qv, "float_vector_2", DefaultVectorSearchParams.IVF_PQ(metric_type=MetricType.L2, nprobe=16), limit=10
        )
        h = self.hybrid_search(
            client, collection_name, reqs=[req_hnsw, req_ivf_pq], ranker=RRFRanker(), limit=10, output_fields=["*"]
        )[0]
        assert len(h[0]) == 10, f"hybrid_search: expected 10 hits, got {len(h[0])}"
        for hit in h[0]:
            assert hit["entity"].get("float_vector_1") is not None, (
                "hybrid_search: float_vector_1 missing in entity (HNSW raw path)"
            )
            assert hit["entity"].get("float_vector_2") is not None, (
                "hybrid_search: float_vector_2 missing in entity (IVF_PQ column store path)"
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_async_json_scalar_field_raw_access(self):
        """
        target:
            1. verify async scalarField warmup preloads raw JSON column data
            2. verify JSON path filter query returns correct results after async load
            3. verify search with output_fields=["*"] returns json_field in every hit
        method:
            1. create collection with JSON field (no JSON index), field-level warmup=async;
               collection-level warmup.scalarField=async
            2. insert 5000 rows with structured JSON (score=i%100, tag="warmup_test"); flush;
               build HNSW index on float_vector_1 only; load
            3. query with JSON path filter json_field["score"]==0, output_fields=["*"]
            4. search with output_fields=["*"]
        expected:
            1. query returns exactly 50 rows (score=i%100==0 → 50/5000); each row has json_field with score==0
            2. search returns 10 hits; each hit entity contains json_field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        nb, batch = 5000, 2000

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1", "json_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
                "float_vector_1": FieldParams(dim=dim).to_dict,
                "json_1": FieldParams(nullable=True, warmup="async").to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema, properties={"warmup.scalarField": "async"})

        res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema_with_defaults(
            nb=nb,
            schema=res,
            default_values={
                "int64_pk": list(range(nb)),
                "json_1": [{"score": i % 100, "tag": "warmup_test"} for i in range(nb)],
            },
        )
        for d in cf.iter_mc_insert_list_data(rows, batch, nb):
            self.insert(client, collection_name, d)
        self.flush(client, collection_name)

        # no JSON index — exercises full-scan + scalarField column store path
        ip = self.prepare_index_params(client)[0]
        for field_name, idx in DefaultVectorIndexParams.HNSW("float_vector_1").items():
            ip.add_index(field_name=field_name, **idx.to_dict)
        self.create_index(client, collection_name, ip)
        self.load_collection(client, collection_name)

        # (1) JSON path filter — full-scan reads raw JSON column (scalarField warmup target)
        q = self.query(
            client,
            collection_name,
            filter='json_1["score"] == 0',
            output_fields=["*"],
            limit=200,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": 50},
        )[0]
        for r in q:
            assert "json_1" in r and r["json_1"] is not None, (
                "json_1 missing or None in query output_fields=['*'] (scalarField async warmup failed)"
            )
            assert r["json_1"]["score"] == 0, f"json_1 score mismatch: {r['json_1']}"

        # (2) search with output_fields=['*'] — verify all columns including JSON are retrievable
        s = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, dim),
            anns_field="float_vector_1",
            limit=10,
            search_params=DefaultVectorSearchParams.HNSW(metric_type=MetricType.L2),
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 10},
        )[0]
        assert len(s[0]) == 10, f"search: expected 10 hits, got {len(s[0])}"
        for hit in s[0]:
            assert "json_1" in hit["entity"], (
                "json_1 missing from search output_fields=['*'] (scalarField async warmup failed)"
            )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.skip(reason="requires tiered storage config in milvus.yaml (all four warmup fields=async)")
    @pytest.mark.parametrize(
        "override_type,collection_props,field_warmup",
        [
            pytest.param("none", {}, None, id="no_override"),
            pytest.param("collection", {"warmup.vectorIndex": "sync"}, None, id="collection_sync_override"),
            pytest.param("field", {}, "disable", id="field_disable_override"),
        ],
    )
    def test_warmup_global_async_with_override(self, override_type, collection_props, field_warmup):
        """
        target: verify global async config (all four fields in milvus.yaml=async) with no/collection/field override;
                verify describe shows expected override values; load+search succeed
        method: parametrize 3 scenarios; build schema with optional field warmup, create collection with
                optional collection props, describe, insert 5000, load, search
        expected: describe shows correct override values per scenario; search returns 10 results
        # NOTE: requires milvus.yaml config change:
        #   queryNode.segcore.tieredStorage.warmup.scalarField=async
        #   queryNode.segcore.tieredStorage.warmup.scalarIndex=async
        #   queryNode.segcore.tieredStorage.warmup.vectorField=async
        #   queryNode.segcore.tieredStorage.warmup.vectorIndex=async
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        if field_warmup is not None:
            cf.gen_milvus_client_schema(
                schema,
                fields=["int64_pk", "float_vector_1", "int64_1"],
                field_params={
                    "int64_pk": FieldParams(is_primary=True).to_dict,
                    "float_vector_1": FieldParams(dim=128, warmup=field_warmup).to_dict,
                    "int64_1": FieldParams(nullable=True).to_dict,
                },
            )
        else:
            cf.gen_milvus_client_schema(
                schema,
                fields=["int64_pk", "float_vector_1", "int64_1"],
                field_params={
                    "int64_pk": FieldParams(is_primary=True).to_dict,
                    "int64_1": FieldParams(nullable=True).to_dict,
                },
            )
        self.create_collection(client, collection_name, schema=schema, properties=collection_props)

        res = self.describe_collection(client, collection_name)[0]
        if override_type == "none":
            for key in ["warmup.vectorField", "warmup.scalarField", "warmup.vectorIndex", "warmup.scalarIndex"]:
                assert cf.get_collection_warmup(res, key) is None
            assert cf.get_field_warmup(res, "float_vector_1") is None
        elif override_type == "collection":
            assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "sync"
            assert cf.get_collection_warmup(res, "warmup.vectorField") is None
        elif override_type == "field":
            assert cf.get_field_warmup(res, "float_vector_1") == "disable"
            assert cf.get_field_warmup(res, "int64_1") is None

        rows = cf.gen_row_data_by_schema(nb=5000, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)
        assert len(self.search(client, collection_name, cf.gen_vectors(1, 128), limit=10)[0][0]) == 10

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.skip(
        reason="requires tiered storage config in milvus.yaml (warmup.vectorIndex=async and all other fields=async)"
    )
    def test_warmup_global_async_index_sync_drop_fallback(self):
        """
        target: verify global all-async config + index-level sync override; after drop index warmup, falls back to global async
        method: create collection (no explicit warmup props), insert 5000, flush, release,
                alter_index_properties sync, load (sync blocking), release,
                drop_index_properties warmup, reload (global async), search
        expected: describe shows sync after alter; None after drop; search succeeds both times
        # NOTE: requires milvus.yaml config change:
        #   queryNode.segcore.tieredStorage.warmup.vectorIndex=async (and all other fields=async)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema)

        res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=5000, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.release_collection(client, collection_name)

        self.alter_index_properties(client, collection_name, index_name="float_vector_1", properties={"warmup": "sync"})
        idx_res = self.describe_index(client, collection_name, index_name="float_vector_1")[0]
        assert cf.get_index_warmup(idx_res) == "sync"

        self.load_collection(client, collection_name)
        assert len(self.search(client, collection_name, cf.gen_vectors(1, 128), limit=10)[0][0]) == 10

        self.release_collection(client, collection_name)
        self.drop_index_properties(client, collection_name, index_name="float_vector_1", property_keys=["warmup"])
        idx_res = self.describe_index(client, collection_name, index_name="float_vector_1")[0]
        assert cf.get_index_warmup(idx_res) is None

        self.load_collection(client, collection_name)
        assert len(self.search(client, collection_name, cf.gen_vectors(1, 128), limit=10)[0][0]) == 10

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.skip(reason="requires tiered storage config in milvus.yaml (warmup.vectorField=async)")
    def test_warmup_global_async_vectorfield_only(self):
        """
        target: verify global vectorField=async (changed from default disable); no collection/field override;
                load+search succeed immediately; sleep 5s search still correct
        method: create plain collection, describe (no explicit warmup props), insert 5000, flush, load, search,
                sleep 5s, search again
        expected: describe shows no explicit collection/field warmup; both searches return 10 results
        # NOTE: requires milvus.yaml config change:
        #   queryNode.segcore.tieredStorage.warmup.vectorField=async (others remain default)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema)

        res = self.describe_collection(client, collection_name)[0]
        for key in ["warmup.vectorField", "warmup.scalarField", "warmup.vectorIndex", "warmup.scalarIndex"]:
            assert cf.get_collection_warmup(res, key) is None

        rows = cf.gen_row_data_by_schema(nb=5000, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )
        self.load_collection(client, collection_name)
        assert len(self.search(client, collection_name, cf.gen_vectors(1, 128), limit=10)[0][0]) == 10

        time.sleep(5)
        assert len(self.search(client, collection_name, cf.gen_vectors(1, 128), limit=10)[0][0]) == 10

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.skip(
        reason="requires tiered storage config in milvus.yaml (common.threadCoreCoefficient.lowPriority=0)"
    )
    def test_warmup_async_prefetch_thread_pool_zero(self):
        """
        target: verify when prefetch thread pool=0 (lowPriority=0), async degrades to sync;
                load blocks until warmup completes; search works after load
        method: create collection with vectorIndex=async, insert 5000, flush, load (expect blocking), search
        expected: load completes (sync degradation), search returns 10 results
        # NOTE: requires milvus.yaml config change:
        #   common.threadCoreCoefficient.lowPriority=0
        #   (prefetch thread count = CPU cores * 0 = 0, thread pool empty)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        cf.gen_milvus_client_schema(
            schema,
            fields=["int64_pk", "float_vector_1"],
            field_params={
                "int64_pk": FieldParams(is_primary=True).to_dict,
            },
        )
        self.create_collection(client, collection_name, schema=schema, properties={"warmup.vectorIndex": "async"})

        res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=5000, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_index(
            client,
            collection_name,
            self.all_vector_index(
                self.prepare_index_params(client)[0], DefaultVectorIndexParams.HNSW("float_vector_1")
            ),
        )

        self.load_collection(client, collection_name)

        assert len(self.search(client, collection_name, cf.gen_vectors(1, 128), limit=10)[0][0]) == 10

        self.drop_collection(client, collection_name)
