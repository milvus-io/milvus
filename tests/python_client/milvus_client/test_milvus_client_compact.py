# ruff: noqa: E712,E731,F401,F403,F405,F541,F841,I001,UP031,UP032,W291,W292,W293
# fmt: off
import pytest
import time
from collections import Counter

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from common.constants import *
from pymilvus import DataType
from pymilvus import AnnSearchRequest
from pymilvus import WeightedRanker


prefix = "client_compact"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name


class TestMilvusClientCompactInvalid(TestMilvusClientV2Base):
    """ Test case of compact interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2588")
    @pytest.mark.parametrize("name", [1, "12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_compact_invalid_collection_name_string(self, name):
        """
        target: test compact with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {name}. the first character of a collection name "
                             f"must be an underscore or letter: invalid parameter"}
        self.compact(client, name,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2587")
    @pytest.mark.parametrize("name", [1])
    def test_milvus_client_compact_invalid_collection_name_non_string(self, name):
        """
        target: test compact with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {name}. the first character of a collection name "
                             f"must be an underscore or letter: invalid parameter"}
        self.compact(client, name,
                     check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_clustering", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_compact_invalid_is_clustering(self, invalid_clustering):
        """
        target: test compact with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"is_clustering value {invalid_clustering} is illegal"}
        self.compact(client, collection_name, is_clustering=invalid_clustering,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_job_id", ["12-s"])
    def test_milvus_client_get_compact_state_invalid_job_id(self, invalid_job_id):
        """
        target: test compact with invalid collection name
        method: create connection, collection, insert and hybrid search with invalid collection name
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1,
                 ct.err_msg: f"compaction_id value {invalid_job_id} is illegal"}
        self.get_compaction_state(client, invalid_job_id,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_get_compaction_state_unknown_id(self):
        """
        target: reject an unknown compaction ID
        method: query compaction state with an unallocated int64 ID
        expected: the unknown ID remains undefined
        """
        client = self._client()
        compaction_id = (1 << 63) - 1

        state = self.get_compaction_state(client, compaction_id)[0]

        assert state == "UndefiedState"


_json_path_index_params = [
    ("INVERTED", "BOOL"),
    ("INVERTED", "DOUBLE"),
    ("INVERTED", "VARCHAR"),
    ("INVERTED", "JSON"),
    ("STL_SORT", "DOUBLE"),
    ("STL_SORT", "VARCHAR"),
    ("BITMAP", "BOOL"),
    ("BITMAP", "VARCHAR"),
]


class TestMilvusClientCompactValid(TestMilvusClientV2Base):
    """ Test case of hybrid search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def is_clustering(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=_json_path_index_params, ids=[f"{t[0]}_{t[1]}" for t in _json_path_index_params])
    def json_index_params(self, request):
        yield request.param

    @pytest.fixture(scope="function")
    def supported_varchar_scalar_index(self, json_index_params):
        yield json_index_params[0]

    @pytest.fixture(scope="function")
    def supported_json_cast_type(self, json_index_params):
        yield json_index_params[1]

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_compact_mix_preserves_topology_and_data(self):
        """
        target: verify ordinary MixCompaction rewrites multiple sealed segments without losing data
        method: create three flushed segments, compact, then inspect plans, active segments, and all primary keys
        expected: selected sources are replaced, unselected sources remain active, and all primary keys remain queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        batch_size = default_nb
        num_batches = 3
        total_rows = batch_size * num_batches
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            default_vector_field_name,
            index_type="FLAT",
            metric_type="COSINE",
        )
        self.create_collection(
            client,
            collection_name,
            default_dim,
            index_params=index_params,
            consistency_level="Strong",
            properties={"collection.autocompaction.enabled": "false"},
        )

        rng = np.random.default_rng()
        for batch in range(num_batches):
            vectors = rng.random((batch_size, default_dim), dtype=np.float32)
            rows = [
                {
                    default_primary_key_field_name: batch * batch_size + index,
                    default_vector_field_name: vectors[index].tolist(),
                }
                for index in range(batch_size)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

        segments_before = self.wait_for_compaction_eligible_segments(
            client,
            collection_name,
            minimum_segment_count=num_batches,
        )
        raw_source_ids = [segment.segment_id for segment in segments_before]
        source_ids = set(raw_source_ids)
        assert len(raw_source_ids) == len(source_ids), f"Duplicate persistent source IDs: {segments_before}"
        assert len(source_ids) >= num_batches, (
            f"Expected at least {num_batches} sealed inputs, got {len(source_ids)}: {segments_before}"
        )

        compact_id = self.compact(client, collection_name)[0]
        self.wait_for_compaction_ready(client, compact_id, timeout=180)

        plans = client.get_compaction_plans(compact_id).plans
        assert plans, f"MixCompaction {compact_id} completed without a compaction plan"
        merge_plans = [plan for plan in plans if len(plan.sources) >= 2]
        assert merge_plans, f"MixCompaction must include a multi-source merge plan: plans={plans}"
        assert all(len(plan.sources) == len(set(plan.sources)) for plan in plans), (
            f"A MixCompaction plan must not consume one source more than once: plans={plans}"
        )
        planned_source_counts = Counter(segment_id for plan in plans for segment_id in plan.sources)
        assert all(count == 1 for count in planned_source_counts.values()), (
            f"A source must occur in exactly one MixCompaction plan: counts={planned_source_counts}, plans={plans}"
        )
        planned_source_ids = set(planned_source_counts)
        raw_target_ids = [plan.target for plan in plans]
        target_ids = set(raw_target_ids)
        assert len(raw_target_ids) == len(target_ids), f"MixCompaction targets must be globally unique: plans={plans}"
        assert len(planned_source_ids) >= 2, (
            f"MixCompaction must merge multiple sources: sources={planned_source_ids}, plans={plans}"
        )
        assert planned_source_ids.issubset(source_ids), (
            f"Compaction plans contain unknown sources: before={source_ids}, planned={planned_source_ids}"
        )
        assert target_ids.isdisjoint(source_ids), (
            f"Compaction targets must be new segments: sources={source_ids}, targets={target_ids}"
        )

        segments_after = self.list_persistent_segments(client, collection_name)[0]
        raw_active_segment_ids = [segment.segment_id for segment in segments_after]
        active_segment_ids = set(raw_active_segment_ids)
        assert len(raw_active_segment_ids) == len(active_segment_ids), (
            f"Persistent segment listing must not contain duplicate active IDs: segments={segments_after}"
        )
        expected_active_ids = (source_ids - planned_source_ids) | target_ids
        assert active_segment_ids == expected_active_ids, (
            f"Active topology mismatch: active={active_segment_ids}, expected={expected_active_ids}, "
            f"planned_sources={planned_source_ids}, targets={target_ids}"
        )
        assert sum(segment.num_rows for segment in segments_after) == total_rows

        query_res = self.query(
            client,
            collection_name,
            filter=f"{default_primary_key_field_name} >= 0",
            output_fields=[default_primary_key_field_name],
            consistency_level="Strong",
        )[0]
        query_ids = [row[default_primary_key_field_name] for row in query_res]
        assert len(query_ids) == total_rows
        assert len(set(query_ids)) == total_rows
        assert set(query_ids) == set(range(total_rows))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("add_field", [True, False])
    def test_milvus_client_compact_normal(self, is_clustering, add_field):
        """
        target: test hybrid search with default normal case (2 vector fields)
        method: create connection, collection, insert and hybrid search
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_vector_field_name+"new", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64,
                         is_partition_key=True, is_clustering_key=is_clustering)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(default_vector_field_name+"new", metric_type="L2")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_vector_field_name+"new": list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i)} for i in range(10*default_nb)]
        self.insert(client, collection_name, rows)
        if add_field and not is_clustering:
            self.add_collection_field(client, collection_name, field_name="field_new", data_type=DataType.INT64,
                                      nullable=True, is_clustering_key=True)
            rows_new = [
                {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_vector_field_name+"new": list(rng.random((1, default_dim))[0]),
                 default_string_field_name: str(i)} for i in range(10*default_nb, 11*default_nb)]
            self.insert(client, collection_name, rows_new)
        self.flush(client, collection_name)
        # 3. compact
        compact_id = self.compact(client, collection_name, is_clustering=is_clustering)[0]
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id, is_clustering=is_clustering)[0]
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(1, f"Compact after index cost more than {cost}s")

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_compact_empty_collection(self, is_clustering):
        """
        target: test compact to empty collection
        method: create connection, collection, compact
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64,
                         is_partition_key=True, is_clustering_key=is_clustering)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. compact. Plain manual compaction must return the explicit no-op
        # handle; querying a hard-coded -1 alone would not verify this path.
        compact_id = self.compact(client, collection_name, is_clustering=is_clustering)[0]
        if not is_clustering:
            assert compact_id == -1
            assert len(client.get_compaction_plans(compact_id).plans) == 0
            assert self.get_compaction_state(client, compact_id)[0] == "Completed"
            assert self.wait_for_compaction_ready(client, compact_id, timeout=30)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_compact_json_path_index(self, is_clustering, supported_varchar_scalar_index,
                                                   supported_json_cast_type):
        """
        target: test hybrid search with default normal case (2 vector fields)
        method: create connection, collection, insert and hybrid search
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        json_field_name = "my_json"
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_vector_field_name+"new", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64,
                         is_partition_key=True, is_clustering_key=is_clustering)
        schema.add_field(json_field_name, DataType.JSON)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        index_params.add_index(default_vector_field_name+"new", metric_type="L2")
        index_params.add_index(field_name=json_field_name, index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type, "json_path": f"{json_field_name}['a']['b']"})
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a']"})
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}"})
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]['b']"})
        index_params.add_index(field_name=json_field_name,
                               index_type=supported_varchar_scalar_index,
                               params={"json_cast_type": supported_json_cast_type,
                                       "json_path": f"{json_field_name}['a'][0]"})

        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_vector_field_name+"new": list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i),
             json_field_name: {'a': {"b": i}}} for i in range(10*default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. compact
        compact_id = self.compact(client, collection_name, is_clustering=is_clustering)[0]
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id, is_clustering=is_clustering)[0]
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(1, f"Compact after index cost more than {cost}s")

        self.drop_collection(client, collection_name)
