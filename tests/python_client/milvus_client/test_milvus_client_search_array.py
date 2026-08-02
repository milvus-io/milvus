import pytest
from pymilvus import DataType
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base

default_dim = ct.default_dim
default_nb = 3000
default_limit = 10
default_nq = 1

PK_FIELD = "id"
VEC_FIELD = "emb"
INT64_ARRAY = "int64_array"
VARCHAR_ARRAY = "varchar_array"
FLOAT_ARRAY = "float_array"
BOOL_ARRAY = "bool_array"
ARRAY_FIELDS = [INT64_ARRAY, VARCHAR_ARRAY, FLOAT_ARRAY, BOOL_ARRAY]

CHECK_ITEMS = {
    "nq": default_nq,
    "limit": default_limit,
    "metric": "L2",
    "enable_milvus_client_api": True,
    "pk_name": PK_FIELD,
}


def _build_array_schema(wrapper, client, nullable=False):
    """Build schema: int64 PK, float_vector(128), 4 typed array fields."""
    schema = wrapper.create_schema(client)[0]
    schema.add_field(PK_FIELD, DataType.INT64, is_primary=True)
    schema.add_field(VEC_FIELD, DataType.FLOAT_VECTOR, dim=default_dim)
    schema.add_field(INT64_ARRAY, DataType.ARRAY, element_type=DataType.INT64, max_capacity=100, nullable=nullable)
    schema.add_field(
        VARCHAR_ARRAY,
        DataType.ARRAY,
        element_type=DataType.VARCHAR,
        max_capacity=100,
        max_length=128,
        nullable=nullable,
    )
    schema.add_field(FLOAT_ARRAY, DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=100, nullable=nullable)
    schema.add_field(BOOL_ARRAY, DataType.ARRAY, element_type=DataType.BOOL, max_capacity=100, nullable=nullable)
    return schema


def _gen_deterministic_data(nb, schema, null_ratio=0.0):
    """Generate deterministic array data. int64_array = [i%50, (i+1)%50, (i+2)%50]."""
    data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
    for i in range(nb):
        data[i][PK_FIELD] = i
        if null_ratio > 0 and i < int(nb * null_ratio):
            for f in ARRAY_FIELDS:
                data[i][f] = None
        else:
            data[i][INT64_ARRAY] = [i % 50, (i + 1) % 50, (i + 2) % 50]
            data[i][VARCHAR_ARRAY] = [f"s_{i % 30}", f"s_{(i + 10) % 30}"]
            data[i][FLOAT_ARRAY] = [float(i % 100), float((i * 3) % 100)]
            data[i][BOOL_ARRAY] = [i % 2 == 0, i % 3 == 0]
    return data


@pytest.mark.xdist_group("TestSearchArrayShared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchArrayShared(TestMilvusClientV2Base):
    """Shared collection for array search tests.
    Schema: int64(PK), float_vector(128), int64_array, varchar_array, float_array, bool_array
    Data: 3000 rows, deterministic arrays, INVERTED index on arrays, FLAT/L2 on vector
    """

    shared_alias = "TestSearchArrayShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchArrayShared" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = _build_array_schema(self, client)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)
        data = _gen_deterministic_data(default_nb, schema)
        self.__class__.shared_data = data
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=VEC_FIELD, metric_type="L2", index_type="FLAT")
        for f in ARRAY_FIELDS:
            idx.add_index(field_name=f, index_type="INVERTED")
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_array_contains(self):
        """
        target: verify array_contains filter returns only matching rows
        method: search with array_contains(int64_array, 5), validate returned IDs
        expected: every hit has 5 in its int64_array
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
        target_val = 5
        expr = f"array_contains({INT64_ARRAY}, {target_val})"
        expected_ids = {i for i in range(default_nb) if target_val in [i % 50, (i + 1) % 50, (i + 2) % 50]}
        res, _ = self.search(
            client,
            self.collection_name,
            data=vectors,
            anns_field=VEC_FIELD,
            limit=default_limit,
            filter=expr,
            output_fields=[INT64_ARRAY],
            check_task=CheckTasks.check_search_results,
            check_items=CHECK_ITEMS,
        )
        for hit in res[0]:
            assert hit.id in expected_ids
            assert target_val in hit.entity[INT64_ARRAY]

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_array_contains_all(self):
        """
        target: verify array_contains_all returns rows containing all specified values
        method: search with array_contains_all(int64_array, [0, 1])
        expected: every hit's int64_array contains both 0 and 1
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
        target_vals = [0, 1]
        expr = f"array_contains_all({INT64_ARRAY}, {target_vals})"
        res, _ = self.search(
            client,
            self.collection_name,
            data=vectors,
            anns_field=VEC_FIELD,
            limit=default_limit,
            filter=expr,
            output_fields=[INT64_ARRAY],
            check_task=CheckTasks.check_search_results,
            check_items=CHECK_ITEMS,
        )
        for hit in res[0]:
            arr = hit.entity[INT64_ARRAY]
            for v in target_vals:
                assert v in arr, f"ID {hit.id}: {arr} missing {v}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_array_contains_any(self):
        """
        target: verify array_contains_any returns rows containing at least one value
        method: search with array_contains_any(int64_array, [49, 48])
        expected: every hit's int64_array contains 49 or 48
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
        target_vals = [49, 48]
        expr = f"array_contains_any({INT64_ARRAY}, {target_vals})"
        res, _ = self.search(
            client,
            self.collection_name,
            data=vectors,
            anns_field=VEC_FIELD,
            limit=default_limit,
            filter=expr,
            output_fields=[INT64_ARRAY],
            check_task=CheckTasks.check_search_results,
            check_items=CHECK_ITEMS,
        )
        for hit in res[0]:
            arr = hit.entity[INT64_ARRAY]
            assert any(v in arr for v in target_vals), f"ID {hit.id}: {arr} has none of {target_vals}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_array_length(self):
        """
        target: verify array_length filter returns rows with correct array size
        method: search with array_length(int64_array) == 3
        expected: all returned rows have int64_array of length 3
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
        expr = f"array_length({INT64_ARRAY}) == 3"
        res, _ = self.search(
            client,
            self.collection_name,
            data=vectors,
            anns_field=VEC_FIELD,
            limit=default_limit,
            filter=expr,
            output_fields=[INT64_ARRAY],
            check_task=CheckTasks.check_search_results,
            check_items=CHECK_ITEMS,
        )
        for hit in res[0]:
            assert len(hit.entity[INT64_ARRAY]) == 3

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_array_access(self):
        """
        target: verify array index access filter works correctly
        method: search with int64_array[0] == 10
        expected: every hit has int64_array[0] == 10, matching rows where i % 50 == 10
        """
        client = self._client(alias=self.shared_alias)
        vectors = cf.gen_vectors(default_nq, default_dim)
        target_val = 10
        expr = f"{INT64_ARRAY}[0] == {target_val}"
        expected_ids = {i for i in range(default_nb) if i % 50 == target_val}
        res, _ = self.search(
            client,
            self.collection_name,
            data=vectors,
            anns_field=VEC_FIELD,
            limit=default_limit,
            filter=expr,
            output_fields=[INT64_ARRAY],
            check_task=CheckTasks.check_search_results,
            check_items=CHECK_ITEMS,
        )
        for hit in res[0]:
            assert hit.id in expected_ids
            assert hit.entity[INT64_ARRAY][0] == target_val


class TestSearchArrayIndependent(TestMilvusClientV2Base):
    """Independent tests for array search edge cases.
    Each test creates its own collection with specific configurations.
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_array_without_index(self):
        """
        target: verify array filter works via brute-force scan without INVERTED index
        method: create collection with array fields, NO inverted index, search with filter
        expected: search returns correct filtered results using brute-force scan
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = _build_array_schema(self, client)
        self.create_collection(client, collection_name, schema=schema)
        data = _gen_deterministic_data(default_nb, schema)
        self.insert(client, collection_name, data=data)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=VEC_FIELD, metric_type="L2", index_type="FLAT")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        vectors = cf.gen_vectors(default_nq, default_dim)
        target_val = 5
        expr = f"array_contains({INT64_ARRAY}, {target_val})"
        res, _ = self.search(
            client,
            collection_name,
            data=vectors,
            anns_field=VEC_FIELD,
            limit=default_limit,
            filter=expr,
            output_fields=[INT64_ARRAY],
            check_task=CheckTasks.check_search_results,
            check_items=CHECK_ITEMS,
        )
        for hit in res[0]:
            assert target_val in hit.entity[INT64_ARRAY]

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_array_nullable(self):
        """
        target: verify array search handles nullable array fields correctly
        method: insert 3000 rows with first 20% having None arrays, search with filter
        expected: only non-null rows matching the filter are returned
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = _build_array_schema(self, client, nullable=True)
        self.create_collection(client, collection_name, schema=schema)
        null_ratio = 0.2
        null_count = int(default_nb * null_ratio)
        data = _gen_deterministic_data(default_nb, schema, null_ratio=null_ratio)
        self.insert(client, collection_name, data=data)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=VEC_FIELD, metric_type="L2", index_type="FLAT")
        for f in ARRAY_FIELDS:
            idx.add_index(field_name=f, index_type="INVERTED")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        vectors = cf.gen_vectors(default_nq, default_dim)
        target_val = 5
        expr = f"array_contains({INT64_ARRAY}, {target_val})"
        res, _ = self.search(
            client,
            collection_name,
            data=vectors,
            anns_field=VEC_FIELD,
            limit=default_limit,
            filter=expr,
            output_fields=[INT64_ARRAY],
            check_task=CheckTasks.check_search_results,
            check_items=CHECK_ITEMS,
        )
        for hit in res[0]:
            assert hit.id >= null_count, f"ID {hit.id} is in the null range [0, {null_count})"
            assert hit.entity[INT64_ARRAY] is not None
            assert target_val in hit.entity[INT64_ARRAY]
