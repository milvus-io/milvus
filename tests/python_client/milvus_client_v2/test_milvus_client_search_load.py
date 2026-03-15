import random
import pytest
from pymilvus import DataType
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base

prefix = "search_collection"
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
half_nb = ct.default_nb // 2
field_name = ct.default_float_vec_field_name


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
        collection_name = cf.gen_unique_str(prefix)
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_load_collection_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load the collection
                5. release one partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        # load && release
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p1_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_load_collection_release_collection(self):
        """
        target: test delete load collection release collection
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load the collection
                5. release the collection
                6. load one partition
                7. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        # load && release
        self.load_collection(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_load_partition_release_collection(self):
        """
        target: test delete load partition release collection
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        self.release_collection(client, collection_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_release_collection_load_partition(self):
        """
        target: test delete load collection release collection
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load one partition
                5. release the collection
                6. load the other partition
                7. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_load_partition_drop_partition(self):
        """
        target: test delete load partition drop partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        self.drop_partition(client, collection_name, partition_name=p2_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not found',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_delete_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. delete half data in each partition
                5. release one partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load
        self.load_collection(client, collection_name)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        # release
        self.release_partitions(client, collection_name, partition_names=[p1_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name, p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_delete_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. release the collection and load one partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        # release
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        # search on collection, partition1, partition2
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
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_delete_drop_partition(self):
        """
        target: test load partition delete drop partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. drop the non-loaded partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        # release
        self.drop_partition(client, collection_name, partition_name=p2_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not found',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_release_partition_delete(self):
        """
        target: test load collection release partition delete
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. release one partition
                5. delete half data in each partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load && release
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p1_name])
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_release_collection_delete(self):
        """
        target: test load partition release collection delete
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. release the collection
                5. delete half data in each partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        self.release_collection(client, collection_name)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        self.delete(client, collection_name, filter=f"int64 in {delete_ids}")
        self.load_collection(client, collection_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name, p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 50, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_drop_partition_delete(self):
        """
        target: test load partition drop partition delete
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. release and drop the partition
                5. delete half data in each partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # Create schema
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
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        self.drop_partition(client, collection_name, partition_name=p2_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=10,
                    partition_names=[p1_name, p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 999, ct.err_msg: f'partition name {p2_name} not found',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=10,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 999, ct.err_msg: 'failed to search: collection not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=10,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 999, ct.err_msg: f'partition name {p2_name} not found',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_load_collection_release_partition(self):
        """
        target: test compact load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. compact
                4. load the collection
                5. release one partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=0, partition_num=1, is_index=True)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self.release_collection(client, collection_name)
        # insert data
        data1 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=0, with_json=True)
        self.insert(client, collection_name, data=data1, partition_name=p1_name)
        data2 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=100, with_json=True)
        self.insert(client, collection_name, data=data2, partition_name=p1_name)
        data3 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=200, with_json=True)
        self.insert(client, collection_name, data=data3, partition_name=p2_name)
        self.flush(client, collection_name)
        # compact
        self.compact(client, collection_name)
        # load && release
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p1_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_load_collection_release_collection(self):
        """
        target: test compact load collection release collection
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. compact
                4. load the collection
                5. release the collection
                6. load one partition
                7. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=0, partition_num=1, is_index=True)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self.release_collection(client, collection_name)
        # insert data
        data1 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=0, with_json=True)
        self.insert(client, collection_name, data=data1, partition_name=p1_name)
        data2 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=100, with_json=True)
        self.insert(client, collection_name, data=data2, partition_name=p1_name)
        data3 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=200, with_json=True)
        self.insert(client, collection_name, data=data3, partition_name=p2_name)
        self.flush(client, collection_name)
        # compact
        self.compact(client, collection_name)
        # load && release
        self.load_collection(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p1_name, p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p1_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 200, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_load_partition_release_collection(self):
        """
        target: test compact load partition release collection
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. compact
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=0, partition_num=1, is_index=True)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self.release_collection(client, collection_name)
        # insert data
        data1 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=0, with_json=True)
        self.insert(client, collection_name, data=data1, partition_name=p1_name)
        data2 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=100, with_json=True)
        self.insert(client, collection_name, data=data2, partition_name=p1_name)
        data3 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=200, with_json=True)
        self.insert(client, collection_name, data=data3, partition_name=p2_name)
        self.flush(client, collection_name)
        # compact
        self.compact(client, collection_name)
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 300, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p1_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 200, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_compact_drop_partition(self):
        """
        target: test load collection compact drop partition
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. load the collection
                4. compact
                5. release one partition and drop
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=0, partition_num=1, is_index=True)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self.release_collection(client, collection_name)
        # insert data
        data1 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=0, with_json=True)
        self.insert(client, collection_name, data=data1, partition_name=p1_name)
        data2 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=100, with_json=True)
        self.insert(client, collection_name, data=data2, partition_name=p1_name)
        data3 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=200, with_json=True)
        self.insert(client, collection_name, data=data3, partition_name=p2_name)
        self.flush(client, collection_name)
        # load
        self.load_collection(client, collection_name)
        # compact
        self.compact(client, collection_name)
        # release
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        self.drop_partition(client, collection_name, partition_name=p2_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 200, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p1_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 200, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not found',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_compact_release_collection(self):
        """
        target: test load partition compact release collection
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. load one partition
                4. compact
                5. release the collection
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=0, partition_num=1, is_index=True)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self.release_collection(client, collection_name)
        # insert data
        data1 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=0, with_json=True)
        self.insert(client, collection_name, data=data1, partition_name=p1_name)
        data2 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=100, with_json=True)
        self.insert(client, collection_name, data=data2, partition_name=p1_name)
        data3 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=200, with_json=True)
        self.insert(client, collection_name, data=data3, partition_name=p2_name)
        self.flush(client, collection_name)
        # load
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        # compact
        self.compact(client, collection_name)
        # release
        self.release_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_release_partition_compact(self):
        """
        target: test load collection release partition compact
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. load the collection
                4. release one partition
                5. compact
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=0, partition_num=1, is_index=True)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self.release_collection(client, collection_name)
        # insert data
        data1 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=0, with_json=True)
        self.insert(client, collection_name, data=data1, partition_name=p1_name)
        data2 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=100, with_json=True)
        self.insert(client, collection_name, data=data2, partition_name=p1_name)
        data3 = cf.gen_default_rows_data(nb=100, dim=default_dim, start=200, with_json=True)
        self.insert(client, collection_name, data=data3, partition_name=p2_name)
        self.flush(client, collection_name)
        # load && release
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p1_name])
        # compact
        self.compact(client, collection_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=300,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_collection_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load the collection
                5. release one partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # flush
        self.flush(client, collection_name)
        # load && release
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p1_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_collection_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load the collection
                5. release the collection
                6. load one partition
                7. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # flush
        self.flush(client, collection_name)
        # load && release
        self.load_collection(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_partition_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # flush
        self.flush(client, collection_name)
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        self.release_collection(client, collection_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_partition_drop_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load one partition
                5. release and drop the partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # flush
        self.flush(client, collection_name)
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        self.drop_partition(client, collection_name, partition_name=p2_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name, p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not found',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not found',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_collection_drop_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load collection
                5. release and drop one partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # flush
        self.flush(client, collection_name)
        # load && release
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        self.drop_partition(client, collection_name, partition_name=p2_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not found',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_flush_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. flush
                5. search on the collection -> len(res)==200
                5. release one partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load
        self.load_collection(client, collection_name)
        # flush
        self.flush(client, collection_name)
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 200, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        # release
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        # search on collection - returns results from loaded partitions only
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_flush_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. flush
                5. release the collection
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        # flush
        self.flush(client, collection_name)
        # release
        self.release_collection(client, collection_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name, p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_flush_drop_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. flush
                5. drop the non-loaded partition
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        # flush
        self.flush(client, collection_name)
        # release
        self.drop_partition(client, collection_name, partition_name=p2_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not found',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_partition_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. release one partition
                5. flush
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load && release
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        # flush
        self.flush(client, collection_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_collection_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. release the collection
                5. load one partition
                6. flush
                7. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load && release
        self.load_collection(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        # flush
        self.flush(client, collection_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name, p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_release_collection_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the partition
                4. release the collection
                5. flush
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        self.release_collection(client, collection_name)
        # flush
        self.flush(client, collection_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_drop_partition_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. release and drop the partition
                5. flush
                6. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load && release
        self.load_partitions(client, collection_name, partition_names=[p2_name])
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        self.drop_partition(client, collection_name, partition_name=p2_name)
        # flush
        self.flush(client, collection_name)
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not found',
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_release_collection_multi_times(self):
        """
        target: test load and release multiple times
        method: 1. create a collection and 2 partitions
                2. load and release multiple times
                3. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load and release
        for _ in range(5):
            self.release_collection(client, collection_name)
            self.load_partitions(client, collection_name, partition_names=[p2_name])
        # search on collection, partition1, partition2
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p1_name],
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1, ct.err_msg: 'not loaded',
                                 "enable_milvus_client_api": True})
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    partition_names=[p2_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_all_partitions(self):
        """
        target: test load and release all partitions
        method: 1. create a collection and 2 partitions
                2. load collection and release all partitions
                3. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name, p2_name = partition_names[0], partition_names[1]
        self._create_index_flat(client, collection_name)
        # load and release
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_names=[p1_name])
        self.release_partitions(client, collection_name, partition_names=[p2_name])
        # search on collection
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 65535,
                                 ct.err_msg: "collection not loaded",
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_load_collection_create_partition(self):
        """
        target: test load collection and create partition and search
        method: 1. create a collection and 2 partitions
                2. load collection and create a partition
                3. search
        expected: No exception
        """
        client = self._client()
        collection_name, _, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        self._create_index_flat(client, collection_name)
        # load and create partition
        self.load_collection(client, collection_name)
        self.create_partition(client, collection_name, partition_name="_default3")
        # search on collection
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 200, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_load_partition_create_partition(self):
        """
        target: test load partition and create partition and search
        method: 1. create a collection and 2 partitions
                2. load partition and create a partition
                3. search
        expected: No exception
        """
        client = self._client()
        collection_name, partition_names, _ = \
            self._create_collection_with_partitions_and_data(client, nb=200, partition_num=1, is_index=False)
        p1_name = partition_names[0]
        self._create_index_flat(client, collection_name)
        # load and create partition
        self.load_partitions(client, collection_name, partition_names=[p1_name])
        self.create_partition(client, collection_name, partition_name="_default3")
        # search on collection
        self.search(client, collection_name, data=vectors[:1],
                    anns_field=field_name, search_params=default_search_params, limit=200,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": 1, "limit": 100, "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
