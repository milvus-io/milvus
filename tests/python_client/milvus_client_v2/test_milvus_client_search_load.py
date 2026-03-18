import pytest
from pymilvus import DataType
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base

default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
field_name = ct.default_float_vec_field_name

# Sentinel values for expected search results
NOT_LOADED = "not_loaded"
NOT_FOUND = "not_found"
# Special sentinel: search on [p1, p2] with a specific error code/msg
NOT_LOADED_999 = "not_loaded_999"
NOT_FOUND_999 = "not_found_999"


class TestSearchLoadIndependent(TestMilvusClientV2Base):
    """ Test case of search combining load and other functions """

    def _create_collection_with_partitions_and_data(self, client, nb=200, partition_num=1,
                                                     is_index=False, dim=default_dim):
        """
        Helper: create a collection with default schema, partition_num extra partitions,
        insert data split evenly across all partitions (including _default), optionally create
        index and load.
        Returns (collection_name, partition_names_list, data_per_partition_count).
        partition_names_list[0] = "_default", partition_names_list[1] = "search_partition_0", etc.
        """
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Create extra partitions
        partition_names = ["_default"]
        for i in range(partition_num):
            p_name = f"search_partition_{i}"
            self.create_partition(client, collection_name, partition_name=p_name)
            partition_names.append(p_name)

        total_partitions = len(partition_names)
        per_partition = nb // total_partitions

        # Insert data evenly across partitions
        if nb > 0:
            start = 0
            for p_name in partition_names:
                data = cf.gen_default_rows_data(nb=per_partition, dim=dim, start=start, with_json=True)
                self.insert(client, collection_name, data=data, partition_name=p_name)
                start += per_partition
            self.flush(client, collection_name)

        if is_index:
            idx = self.prepare_index_params(client)[0]
            idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                          index_type="FLAT", params={})
            self.create_index(client, collection_name, index_params=idx)
            self.load_collection(client, collection_name)

        return collection_name, partition_names, per_partition

    def _create_index_flat(self, client, collection_name):
        """Helper: create a FLAT index on the default float vector field."""
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx)

    def _do_search(self, client, collection_name, partition_names, limit, expected):
        """Execute a single search and verify against expected result."""
        if expected == NOT_LOADED:
            self.search(client, collection_name, data=cf.gen_vectors(1, default_dim),
                        anns_field=field_name, search_params=default_search_params, limit=limit,
                        partition_names=partition_names,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                     "enable_milvus_client_api": True})
        elif expected == NOT_FOUND:
            self.search(client, collection_name, data=cf.gen_vectors(1, default_dim),
                        anns_field=field_name, search_params=default_search_params, limit=limit,
                        partition_names=partition_names,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 1, ct.err_msg: 'not found',
                                     "enable_milvus_client_api": True})
        elif expected == NOT_LOADED_999:
            self.search(client, collection_name, data=cf.gen_vectors(1, default_dim),
                        anns_field=field_name, search_params=default_search_params, limit=limit,
                        partition_names=partition_names,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 999,
                                     ct.err_msg: 'failed to search: collection not loaded',
                                     "enable_milvus_client_api": True})
        elif expected == NOT_FOUND_999:
            # partition not found with error code 999, msg includes partition name
            p2_name = partition_names[-1] if partition_names else ""
            self.search(client, collection_name, data=cf.gen_vectors(1, default_dim),
                        anns_field=field_name, search_params=default_search_params, limit=limit,
                        partition_names=partition_names,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 999,
                                     ct.err_msg: f'partition name {p2_name} not found',
                                     "enable_milvus_client_api": True})
        elif isinstance(expected, str) and expected == "not_loaded_65535":
            self.search(client, collection_name, data=cf.gen_vectors(1, default_dim),
                        anns_field=field_name, search_params=default_search_params, limit=limit,
                        partition_names=partition_names,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 65535,
                                     ct.err_msg: "collection not loaded",
                                     "enable_milvus_client_api": True})
        else:
            # expected is an integer: the expected result count
            self.search(client, collection_name, data=cf.gen_vectors(1, default_dim),
                        anns_field=field_name, search_params=default_search_params, limit=limit,
                        partition_names=partition_names,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": 1, "limit": expected, "enable_milvus_client_api": True,
                                     "metric": "COSINE", "pk_name": ct.default_int64_field_name})

    def _run_search_assertions(self, client, collection_name, p1_name, p2_name, limit,
                               expected_collection, expected_p1, expected_p2,
                               search_collection_partitions=None):
        """
        Run search on collection (no partition filter), p1, and p2 and verify expected results.
        search_collection_partitions: if set, override partition_names for the collection-level search.
        """
        # Search on collection
        coll_partitions = search_collection_partitions
        self._do_search(client, collection_name, coll_partitions, limit, expected_collection)
        # Search on p1
        self._do_search(client, collection_name, [p1_name], limit, expected_p1)
        # Search on p2
        self._do_search(client, collection_name, [p2_name], limit, expected_p2)

    # ==================================================================================
    # Group 1: Delete-based tests (nb=200, partition_num=1, delete IDs 50-150)
    # Each test: create collection, create index, execute ops, then search on collection/p1/p2
    # ==================================================================================

    # Parameters: (test_name, ops_sequence, expected_collection, expected_p1, expected_p2,
    #              search_collection_partitions_override)
    # ops_sequence is a list of operation tuples: (op_name, *args)
    # search_collection_partitions_override: None means no partition filter on collection search,
    #   "both" means [p1, p2], "p1p2" means [p1, p2]

    _DELETE_IDS = list(range(50, 150))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("ops,expected_coll,expected_p1,expected_p2,coll_search_parts", [
        # test_delete_load_collection_release_collection
        pytest.param(
            [("delete",), ("load_collection",), ("release_collection",), ("load_p2",)],
            50, NOT_LOADED, 50, None,
            id="delete_load_collection_release_collection_load_p2"),
        # test_load_collection_delete_release_partition
        pytest.param(
            [("load_collection",), ("delete",), ("release_p1",)],
            NOT_LOADED, NOT_LOADED, 50, "both",
            id="load_collection_delete_release_partition"),
        # test_load_partition_delete_release_collection (uses query, handled separately below)
        # test_load_collection_release_partition_delete
        pytest.param(
            [("load_collection",), ("release_p1",), ("delete",)],
            50, NOT_LOADED, 50, None,
            id="load_collection_release_partition_delete"),
    ])
    def test_search_after_delete_ops_l1(self, ops, expected_coll, expected_p1, expected_p2,
                                        coll_search_parts):
        """Parametrized L1 tests: delete + load/release operation sequences."""
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        delete_ids = self._DELETE_IDS
        for op in ops:
            self._execute_op(client, collection_name, p1_name, p2_name, delete_ids, op)
        parts = [p1_name, p2_name] if coll_search_parts == "both" else None
        self._run_search_assertions(client, collection_name, p1_name, p2_name, 200,
                                    expected_coll, expected_p1, expected_p2,
                                    search_collection_partitions=parts)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ops,expected_coll,expected_p1,expected_p2,coll_search_parts", [
        # test_delete_load_collection_release_partition
        pytest.param(
            [("delete",), ("load_collection",), ("release_p1",)],
            50, NOT_LOADED, 50, None,
            id="delete_load_collection_release_partition"),
        # test_delete_load_partition_release_collection
        pytest.param(
            [("delete",), ("load_p1",), ("release_collection",)],
            NOT_LOADED, NOT_LOADED, NOT_LOADED, None,
            id="delete_load_partition_release_collection"),
        # test_delete_release_collection_load_partition
        pytest.param(
            [("delete",), ("load_p1",), ("release_collection",), ("load_p2",)],
            50, NOT_LOADED, 50, None,
            id="delete_release_collection_load_partition"),
        # test_delete_load_partition_drop_partition
        pytest.param(
            [("delete",), ("load_p2",), ("release_p2",), ("drop_p2",)],
            NOT_LOADED, NOT_LOADED, NOT_FOUND, None,
            id="delete_load_partition_drop_partition"),
        # test_load_partition_delete_drop_partition
        pytest.param(
            [("load_p1",), ("delete",), ("drop_p2",)],
            50, 50, NOT_FOUND, None,
            id="load_partition_delete_drop_partition"),
        # test_load_partition_release_collection_delete
        pytest.param(
            [("load_p1",), ("release_collection",), ("delete",), ("load_collection",)],
            100, 50, 50, "both",
            id="load_partition_release_collection_delete"),
    ])
    def test_search_after_delete_ops_l2(self, ops, expected_coll, expected_p1, expected_p2,
                                        coll_search_parts):
        """Parametrized L2 tests: delete + load/release operation sequences."""
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        delete_ids = self._DELETE_IDS
        for op in ops:
            self._execute_op(client, collection_name, p1_name, p2_name, delete_ids, op)
        parts = [p1_name, p2_name] if coll_search_parts == "both" else None
        self._run_search_assertions(client, collection_name, p1_name, p2_name, 200,
                                    expected_coll, expected_p1, expected_p2,
                                    search_collection_partitions=parts)

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_delete_release_collection(self):
        """
        target: test load partition, delete, release collection, reload partition, then query/search
        method: Uses query (count) on collection and p1, search on p2
        expected: count=50 on collection and p1, not_loaded on p2
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        delete_ids = self._DELETE_IDS
        self.delete(client, collection_name, filter=f"{ct.default_int64_field_name} in {delete_ids}")
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        # query on collection and p1
        self.query(client, collection_name, filter='',
                   output_fields=[ct.default_count_output],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": [{ct.default_count_output: 50}],
                                "enable_milvus_client_api": True})
        self.query(client, collection_name, filter='',
                   output_fields=[ct.default_count_output],
                   partition_names=[p1_name],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": [{ct.default_count_output: 50}],
                                "enable_milvus_client_api": True})
        self.search(client, collection_name, data=cf.gen_vectors(1, default_dim),
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_drop_partition_delete(self):
        """
        target: test load partition drop partition delete (custom schema, no data inserted)
        method: create collection with 2 custom partitions, load p2, release+drop p2, search
        expected: p1+p2 search returns partition not found, p1 returns not loaded, p2 returns not found
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        p1_name = cf.gen_unique_str("par1")
        self.create_partition(client, collection_name, partition_name=p1_name)
        p2_name = cf.gen_unique_str("par2")
        self.create_partition(client, collection_name, partition_name=p2_name)
        self._create_index_flat(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        self.drop_partition(client, collection_name, partition_name=p2_name)
        # search on [p1, p2]
        self._do_search(client, collection_name, [p1_name, p2_name], 10, NOT_FOUND_999)
        # search on p1
        self._do_search(client, collection_name, [p1_name], 10, NOT_LOADED_999)
        # search on p2
        self._do_search(client, collection_name, [p2_name], 10, NOT_FOUND_999)

    # ==================================================================================
    # Group 2: Compact-based tests (nb=0, multi-batch insert, compact)
    # Data: 200 rows in p1 (2 batches of 100), 100 rows in p2
    # ==================================================================================

    def _setup_compact_test(self, client):
        """Setup for compact tests: create collection, insert multi-batch, flush."""
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=0, partition_num=1, is_index=True)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self.release_collection(client, collection_name)
        data1 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=0, with_json=True)
        self.insert(client, collection_name, data=data1, partition_name=p1_name)
        data2 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=100, with_json=True)
        self.insert(client, collection_name, data=data2, partition_name=p1_name)
        data3 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=200, with_json=True)
        self.insert(client, collection_name, data=data3, partition_name=p2_name)
        self.flush(client, collection_name)
        return collection_name, p1_name, p2_name

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("ops,expected_coll,expected_p1,expected_p2,coll_search_parts", [
        # test_load_collection_release_partition_compact
        pytest.param(
            [("load_collection",), ("release_p1",), ("compact",)],
            100, NOT_LOADED, 100, None,
            id="load_collection_release_partition_compact"),
    ])
    def test_search_after_compact_ops_l1(self, ops, expected_coll, expected_p1, expected_p2,
                                          coll_search_parts):
        """Parametrized L1 tests: compact + load/release operation sequences."""
        client = self._client()
        collection_name, p1_name, p2_name = self._setup_compact_test(client)
        for op in ops:
            self._execute_compact_op(client, collection_name, p1_name, p2_name, op)
        parts = [p1_name, p2_name] if coll_search_parts == "both" else None
        self._run_search_assertions(client, collection_name, p1_name, p2_name, 300,
                                    expected_coll, expected_p1, expected_p2,
                                    search_collection_partitions=parts)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ops,expected_coll,expected_p1,expected_p2,coll_search_parts", [
        # test_compact_load_collection_release_partition
        pytest.param(
            [("compact",), ("load_collection",), ("release_p1",)],
            100, NOT_LOADED, 100, None,
            id="compact_load_collection_release_partition"),
        # test_compact_load_collection_release_collection
        pytest.param(
            [("compact",), ("load_collection",), ("release_collection",), ("load_p1",)],
            NOT_LOADED, 200, NOT_LOADED, "both",
            id="compact_load_collection_release_collection_load_p1"),
        # test_compact_load_partition_release_collection
        pytest.param(
            [("compact",), ("load_p2",), ("release_collection",), ("load_p1",), ("load_p2",)],
            300, 200, 100, None,
            id="compact_load_partition_release_collection_load_both"),
        # test_load_collection_compact_drop_partition
        pytest.param(
            [("load_collection",), ("compact",), ("release_p2",), ("drop_p2",)],
            200, 200, NOT_FOUND, None,
            id="load_collection_compact_drop_partition"),
        # test_load_partition_compact_release_collection
        pytest.param(
            [("load_p2",), ("compact",), ("release_collection",), ("release_p2",)],
            NOT_LOADED, NOT_LOADED, NOT_LOADED, None,
            id="load_partition_compact_release_collection"),
    ])
    def test_search_after_compact_ops_l2(self, ops, expected_coll, expected_p1, expected_p2,
                                          coll_search_parts):
        """Parametrized L2 tests: compact + load/release operation sequences."""
        client = self._client()
        collection_name, p1_name, p2_name = self._setup_compact_test(client)
        for op in ops:
            self._execute_compact_op(client, collection_name, p1_name, p2_name, op)
        parts = [p1_name, p2_name] if coll_search_parts == "both" else None
        self._run_search_assertions(client, collection_name, p1_name, p2_name, 300,
                                    expected_coll, expected_p1, expected_p2,
                                    search_collection_partitions=parts)

    # ==================================================================================
    # Group 3: Flush-based tests (nb=200, partition_num=1, flush + load/release)
    # Each partition has 100 rows (200 / 2 partitions)
    # ==================================================================================

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("ops,expected_coll,expected_p1,expected_p2,coll_search_parts", [
        # test_load_partition_release_collection_flush
        pytest.param(
            [("flush",), ("load_p2",), ("release_collection",), ("flush",)],
            NOT_LOADED, NOT_LOADED, NOT_LOADED, None,
            id="load_partition_release_collection_flush"),
        # test_load_partition_drop_partition_flush
        pytest.param(
            [("flush",), ("load_p2",), ("release_p2",), ("drop_p2",), ("flush",)],
            NOT_LOADED, NOT_LOADED, NOT_FOUND, None,
            id="load_partition_drop_partition_flush"),
    ])
    def test_search_after_flush_ops_l1(self, ops, expected_coll, expected_p1, expected_p2,
                                        coll_search_parts):
        """Parametrized L1 tests: flush + load/release operation sequences."""
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        for op in ops:
            self._execute_op(client, collection_name, p1_name, p2_name, None, op)
        parts = [p1_name, p2_name] if coll_search_parts == "both" else None
        self._run_search_assertions(client, collection_name, p1_name, p2_name, 200,
                                    expected_coll, expected_p1, expected_p2,
                                    search_collection_partitions=parts)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ops,expected_coll,expected_p1,expected_p2,coll_search_parts", [
        # test_flush_load_collection_release_partition
        pytest.param(
            [("flush",), ("load_collection",), ("release_p1",)],
            100, NOT_LOADED, 100, None,
            id="flush_load_collection_release_partition"),
        # test_flush_load_collection_release_collection
        pytest.param(
            [("flush",), ("load_collection",), ("release_collection",), ("load_p2",)],
            100, NOT_LOADED, 100, None,
            id="flush_load_collection_release_collection_load_p2"),
        # test_flush_load_partition_release_collection
        pytest.param(
            [("flush",), ("load_p2",), ("release_collection",)],
            NOT_LOADED, NOT_LOADED, NOT_LOADED, None,
            id="flush_load_partition_release_collection"),
        # test_flush_load_partition_drop_partition
        pytest.param(
            [("flush",), ("load_p2",), ("release_p2",), ("drop_p2",)],
            NOT_FOUND, NOT_LOADED, NOT_FOUND, "both",
            id="flush_load_partition_drop_partition"),
        # test_flush_load_collection_drop_partition
        pytest.param(
            [("flush",), ("load_collection",), ("release_p2",), ("drop_p2",)],
            100, 100, NOT_FOUND, None,
            id="flush_load_collection_drop_partition"),
        # test_load_collection_release_partition_flush
        pytest.param(
            [("load_collection",), ("release_p2",), ("flush",)],
            100, 100, NOT_LOADED, None,
            id="load_collection_release_partition_flush"),
        # test_load_collection_release_collection_flush
        pytest.param(
            [("load_collection",), ("release_collection",), ("load_p2",), ("flush",)],
            NOT_LOADED, NOT_LOADED, 100, "both",
            id="load_collection_release_collection_flush_load_p2"),
        # test_load_partition_flush_release_collection
        pytest.param(
            [("load_p2",), ("flush",), ("release_collection",)],
            NOT_LOADED, NOT_LOADED, NOT_LOADED, "both",
            id="load_partition_flush_release_collection"),
        # test_load_collection_flush_drop_partition
        pytest.param(
            [("load_p1",), ("flush",), ("drop_p2",)],
            100, 100, NOT_FOUND, None,
            id="load_collection_flush_drop_partition"),
    ])
    def test_search_after_flush_ops_l2(self, ops, expected_coll, expected_p1, expected_p2,
                                        coll_search_parts):
        """Parametrized L2 tests: flush + load/release operation sequences."""
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        for op in ops:
            self._execute_op(client, collection_name, p1_name, p2_name, None, op)
        parts = [p1_name, p2_name] if coll_search_parts == "both" else None
        self._run_search_assertions(client, collection_name, p1_name, p2_name, 200,
                                    expected_coll, expected_p1, expected_p2,
                                    search_collection_partitions=parts)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_flush_release_partition(self):
        """
        target: test load collection, flush, search (200 results), release partition, search again
        method: load collection first, flush, verify 200 results, release p2, verify reduced results
        expected: first search returns 200, after release p2 returns 100 on collection, p1=100, p2=not_loaded
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        self.load_collection(client, collection_name)
        self.flush(client, collection_name)
        # first search: all loaded, 200 results
        self._do_search(client, collection_name, None, 200, 200)
        # release p2
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        # search after release
        self._do_search(client, collection_name, None, 200, 100)
        self._do_search(client, collection_name, [p1_name], 200, 100)
        self._do_search(client, collection_name, [p2_name], 200, NOT_LOADED)

    # ==================================================================================
    # Group 4: Special / miscellaneous tests
    # ==================================================================================

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_release_collection_multi_times(self):
        """
        target: test load and release multiple times
        method: release and load p2 five times, then search
        expected: collection=100 (p2 only), p1=not_loaded, p2=100
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        for _ in range(5):
            self.release_collection(client, collection_name)
            self.load_partitions(client, collection_name, partition_names=[p2_name])
        self._run_search_assertions(client, collection_name, p1_name, p2_name, 200,
                                    100, NOT_LOADED, 100)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_all_partitions(self):
        """
        target: test load and release all partitions
        method: load collection and release all partitions one by one
        expected: collection search returns error code 65535, collection not loaded
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p1_name])
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        self._do_search(client, collection_name, None, 200, "not_loaded_65535")

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_load_collection_create_partition(self):
        """
        target: test load collection and create partition and search
        method: load collection, create a new partition, search
        expected: search returns 200 results
        """
        client = self._client()
        collection_name, _, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        self._create_index_flat(client, collection_name)
        self.load_collection(client, collection_name)
        self.create_partition(client, collection_name, partition_name=cf.gen_unique_str("partition3"))
        self._do_search(client, collection_name, None, 200, 200)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_load_partition_create_partition(self):
        """
        target: test load partition and create partition and search
        method: load p1, create a new partition, search
        expected: search returns 100 results (p1 only)
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name = partition_names[0]
        self._create_index_flat(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        self.create_partition(client, collection_name, partition_name=cf.gen_unique_str("partition3"))
        self._do_search(client, collection_name, None, 200, 100)

    # ==================================================================================
    # Operation executors
    # ==================================================================================

    def _execute_op(self, client, collection_name, p1_name, p2_name, delete_ids, op):
        """Execute a single operation tuple for delete/flush-based tests."""
        op_name = op[0]
        if op_name == "delete":
            self.delete(client, collection_name,
                        filter=f"{ct.default_int64_field_name} in {delete_ids}")
        elif op_name == "load_collection":
            self.load_collection(client, collection_name)
        elif op_name == "release_collection":
            self.release_collection(client, collection_name)
        elif op_name == "release_p1":
            self.release_partitions(client, collection_name, partition_names=[p1_name])
        elif op_name == "release_p2":
            self.release_partitions(client, collection_name, partition_names=[p2_name])
        elif op_name == "load_p1":
            self.load_partitions(client, collection_name, partition_names=[p1_name])
        elif op_name == "load_p2":
            self.load_partitions(client, collection_name, partition_names=[p2_name])
        elif op_name == "drop_p2":
            self.drop_partition(client, collection_name, partition_name=p2_name)
        elif op_name == "flush":
            self.flush(client, collection_name)
        elif op_name == "compact":
            self.compact(client, collection_name)
        else:
            raise ValueError(f"Unknown operation: {op_name}")

    def _execute_compact_op(self, client, collection_name, p1_name, p2_name, op):
        """Execute a single operation tuple for compact-based tests."""
        # Reuses the same logic
        self._execute_op(client, collection_name, p1_name, p2_name, None, op)
