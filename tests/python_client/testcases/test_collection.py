import random

import numpy
import pandas as pd
import pytest

from pymilvus import DataType
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from utils.util_log import test_log as log

prefix = "collection"
exp_name = "name"
exp_schema = "schema"
exp_num = "num_entities"
exp_primary = "primary"
exp_shards_num = "shards_num"
default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()
default_shards_num = 1
uid_count = "collection_count"
tag = "collection_count_tag"
uid_stats = "get_collection_stats"
uid_create = "create_collection"
uid_describe = "describe_collection"
uid_drop = "drop_collection"
uid_has = "has_collection"
uid_list = "list_collections"
uid_load = "load_collection"
partition1 = 'partition1'
partition2 = 'partition2'
field_name = default_float_vec_field_name
default_single_query = {
    "data": gen_vectors(1, default_dim),
    "anns_field": default_float_vec_field_name,
    "param": {"metric_type": "L2", "params": {"nprobe": 10}},
    "limit": default_top_k,
}

default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
default_binary_index_params = {"index_type": "BIN_IVF_FLAT", "metric_type": "JACCARD", "params": {"nlist": 64}}
default_nq = ct.default_nq
default_search_exp = "int64 >= 0"
default_limit = ct.default_limit
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
max_vector_field_num = ct.max_vector_field_num
SPARSE_FLOAT_VECTOR_data_type = DataType.SPARSE_FLOAT_VECTOR


class TestCollectionParams(TestcaseBase):
    """ Test case of collection interface """

    @pytest.fixture(scope="function", params=cf.gen_all_type_fields())
    def get_unsupported_primary_field(self, request):
        if request.param.dtype == DataType.INT64 or request.param.dtype == DataType.VARCHAR:
            pytest.skip("int64 type is valid primary key")
        yield request.param

    @pytest.fixture(scope="function", params=ct.invalid_dims)
    def get_invalid_dim(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_invalid_schema_type(self):
        """
        target: test collection with an invalid schema type
        method: create collection with non-CollectionSchema type schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        field, _ = self.field_schema_wrap.init_field_schema(name="field_name", dtype=DataType.INT64, is_primary=True)
        error = {ct.err_code: 0, ct.err_msg: "Schema type must be schema.CollectionSchema"}
        self.collection_wrap.init_collection(c_name, schema=field,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_none_schema(self):
        """
        target: test collection with none schema
        method: create collection with none schema
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 999,
                 ct.err_msg: f"Collection '{c_name}' not exist, or you can pass in schema to create one."}
        self.collection_wrap.init_collection(c_name, schema=None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_multi_float_vectors(self):
        """
        target: test collection with multi float vectors
        method: create collection with two float-vec fields
        expected: Collection created successfully
        """
        # 1. connect
        self._connect()
        # 2. create collection with multiple vectors
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_float_vec_field(dim=default_dim), cf.gen_float_vec_field(name="tmp", dim=default_dim)]
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_mix_vectors(self):
        """
        target: test collection with mix vectors
        method: create with float and binary vec
        expected: Collection created successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_vec_field(), cf.gen_binary_vec_field()]
        schema = cf.gen_collection_schema(fields=fields, auto_id=True)
        self.collection_wrap.init_collection(c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})


class TestCollectionDataframe(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test construct_from_dataframe
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_construct_from_dataframe(self):
        """
        target: test collection with dataframe data
        method: create collection and insert with dataframe
        expected: collection num entities equal to nb
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.check_collection_property,
                                                      check_items={exp_name: c_name, exp_schema: default_schema})
        # flush
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_construct_from_binary_dataframe(self):
        """
        target: test binary collection with dataframe
        method: create binary collection with dataframe
        expected: collection num entities equal to nb
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df, _ = cf.gen_default_binary_dataframe_data(nb=ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.check_collection_property,
                                                      check_items={exp_name: c_name, exp_schema: default_binary_schema})
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_from_none_dataframe(self):
        """
        target: test create collection by empty dataframe
        method: invalid dataframe type create collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 999, ct.err_msg: "Data type must be pandas.DataFrame"}
        self.collection_wrap.construct_from_dataframe(c_name, None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_from_dataframe_only_column(self):
        """
        target: test collection with dataframe only columns
        method: dataframe only has columns
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = pd.DataFrame(columns=[ct.default_int64_field_name, ct.default_float_vec_field_name])
        error = {ct.err_code: 0, ct.err_msg: "Cannot infer schema from empty dataframe"}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_from_inconsistent_dataframe(self):
        """
        target: test collection with data inconsistent
        method: create and insert with inconsistent data
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        # one field different type df
        mix_data = [(1, 2., [0.1, 0.2]), (2, 3., 4)]
        df = pd.DataFrame(data=mix_data, columns=list("ABC"))
        error = {ct.err_code: 1,
                 ct.err_msg: "The Input data type is inconsistent with defined schema, "
                             "{C} field should be a FLOAT_VECTOR, but got a {<class 'list'>} instead."}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field='A', check_task=CheckTasks.err_res,
                                                      check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_from_non_dataframe(self):
        """
        target: test create collection by invalid dataframe
        method: non-dataframe type create collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "Data type must be pandas.DataFrame."}
        df = cf.gen_default_list_data(nb=10)
        self.collection_wrap.construct_from_dataframe(c_name, df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_from_data_type_dataframe(self):
        """
        target: test collection with invalid dataframe
        method: create with invalid dataframe
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = pd.DataFrame({"date": pd.date_range('20210101', periods=3), ct.default_int64_field_name: [1, 2, 3]})
        error = {ct.err_code: 0, ct.err_msg: "Cannot infer schema from empty dataframe."}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_from_invalid_field_name(self):
        """
        target: test collection with invalid field name
        method: create with invalid field name dataframe
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = pd.DataFrame({'%$#': cf.gen_vectors(3, 2), ct.default_int64_field_name: [1, 2, 3]})
        error = {ct.err_code: 1, ct.err_msg: "Invalid field name"}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_none_primary_field(self):
        """
        target: test collection with none primary field
        method: primary_field is none
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        error = {ct.err_code: 0, ct.err_msg: "Schema must have a primary key field."}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=None,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_not_existed_primary_field(self):
        """
        target: test collection with not existed primary field
        method: primary field not existed
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        error = {ct.err_code: 0, ct.err_msg: "Primary field must in dataframe."}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=c_name,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_with_none_auto_id(self):
        """
        target: test construct with non-int64 as primary field
        method: non-int64 as primary field
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        error = {ct.err_code: 0, ct.err_msg: "Param auto_id must be bool type"}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      auto_id=None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_true_insert(self):
        """
        target: test construct with true auto_id
        method: auto_id=True and insert values
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(nb=100)
        error = {ct.err_code: 0, ct.err_msg: "Auto_id is True, primary field should not have data."}
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      auto_id=True, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_true_no_insert(self):
        """
        target: test construct with true auto_id
        method: auto_id=True and not insert ids(primary fields all values are None)
        expected: verify num entities
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data()
        # df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        df[ct.default_int64_field_name] = None
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      auto_id=True)
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_none_value_auto_id_true(self):
        """
        target: test construct with none value, auto_id
        method: df primary field with none value, auto_id=true
        expected: todo
        """
        self._connect()
        nb = 100
        df = cf.gen_default_dataframe_data(nb)
        df.iloc[:, 0] = numpy.NaN
        res, _ = self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                               primary_field=ct.default_int64_field_name, auto_id=True)
        mutation_res = res[1]
        assert cf._check_primary_keys(mutation_res.primary_keys, 100)
        assert self.collection_wrap.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_false(self):
        """
        target: test construct with false auto_id
        method: auto_id=False, primary_field correct
        expected: verify auto_id
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      auto_id=False)
        assert not self.collection_wrap.schema.auto_id
        assert self.collection_wrap.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_construct_none_value_auto_id_false(self):
        """
        target: test construct with none value, auto_id
        method: df primary field with none value, auto_id=false
        expected: raise exception
        """
        self._connect()
        nb = 100
        df = cf.gen_default_dataframe_data(nb)
        df.iloc[:, 0] = numpy.NaN
        error = {ct.err_code: 0, ct.err_msg: "Primary key type must be DataType.INT64"}
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name, auto_id=False,
                                                      check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_false_same_values(self):
        """
        target: test construct with false auto_id and same value
        method: auto_id=False, primary field same values
        expected: verify num entities
        """
        self._connect()
        nb = 100
        df = cf.gen_default_dataframe_data(nb)
        df.iloc[1:, 0] = 1
        res, _ = self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                               primary_field=ct.default_int64_field_name, auto_id=False)
        collection_w = res[0]
        collection_w.flush()
        assert collection_w.num_entities == nb
        mutation_res = res[1]
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_auto_id_false_negative_values(self):
        """
        target: test construct with negative values
        method: auto_id=False, primary field values is negative
        expected: verify num entities
        """
        self._connect()
        nb = 100
        df = cf.gen_default_dataframe_data(nb)
        new_values = pd.Series(data=[i for i in range(0, -nb, -1)])
        df[ct.default_int64_field_name] = new_values
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name, auto_id=False)
        assert self.collection_wrap.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_construct_from_dataframe_dup_name(self):
        """
        target: test collection with dup name and insert dataframe
        method: create collection with dup name, none schema, dataframe
        expected: two collection object is correct
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, primary_field=ct.default_int64_field_name,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        df = cf.gen_default_dataframe_data(ct.default_nb)
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name,
                                                      check_task=CheckTasks.check_collection_property,
                                                      check_items={exp_name: c_name, exp_schema: default_schema})
        # flush
        assert collection_w.num_entities == ct.default_nb
        assert collection_w.num_entities == self.collection_wrap.num_entities


class TestLoadCollection(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `collection.load()` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_load_replica_change(self):
        """
        target: test load replica change
        method: 1.load with replica 1
                2.load with a new replica number
                3.release collection
                4.load with a new replica
                5.create index is a must because get_query_segment_info could
                  only return indexed and loaded segment
        expected: The second time successfully loaded with a new replica number
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.load(replica_number=1)
        for seg in self.utility_wrap.get_query_segment_info(collection_w.name)[0]:
            assert len(seg.nodeIds) == 1

        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]")
        loading_progress, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert loading_progress == {'loading_progress': '100%'}

        # verify load different replicas thrown an exception
        collection_w.load(replica_number=2)
        one_replica, _ = collection_w.get_replicas()
        assert len(one_replica.groups) == 2

        collection_w.release()
        collection_w.load(replica_number=2)
        # replicas is not yet reflected in loading progress
        loading_progress, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert loading_progress == {'loading_progress': '100%'}
        two_replicas, _ = collection_w.get_replicas()
        assert len(two_replicas.groups) == 2
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]", check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{'int64': 0}]})

        # verify loaded segments included 2 replicas and twice num entities
        seg_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
        num_entities = 0
        for seg in seg_info:
            assert len(seg.nodeIds) == 2
            num_entities += seg.num_rows
        assert num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_load_replica_multi(self):
        """
        target: test load with multiple replicas
        method: 1.create collection with one shard
                2.insert multiple segments
                3.load with multiple replicas
                4.query and search
        expected: Query and search successfully
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)
        tmp_nb = 1000
        replica_number = 2
        for i in range(replica_number):
            df = cf.gen_default_dataframe_data(nb=tmp_nb, start=i * tmp_nb)
            insert_res, _ = collection_w.insert(df)
            assert collection_w.num_entities == (i + 1) * tmp_nb

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load(replica_number=replica_number)
        replicas = collection_w.get_replicas()[0]
        assert len(replicas.groups) == replica_number

        for seg in self.utility_wrap.get_query_segment_info(collection_w.name)[0]:
            assert len(seg.nodeIds) == replica_number

        query_res, _ = collection_w.query(expr=f"{ct.default_int64_field_name} in [0, {tmp_nb}]")
        assert len(query_res) == 2
        search_res, _ = collection_w.search(vectors, default_search_field, default_search_params, default_limit)
        assert len(search_res[0]) == ct.default_limit

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_load_replica_partitions(self):
        """
        target: test load replica with partitions
        method: 1.Create collection and one partition
                2.Insert data into collection and partition
                3.Load multi replicas with partition
                4.Query
        expected: Verify query result
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df_1 = cf.gen_default_dataframe_data(nb=ct.default_nb)
        df_2 = cf.gen_default_dataframe_data(nb=ct.default_nb, start=ct.default_nb)

        collection_w.insert(df_1)
        partition_w = self.init_partition_wrap(collection_w, ct.default_tag)
        partition_w.insert(df_2)
        assert collection_w.num_entities == ct.default_nb * 2
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        collection_w.load([partition_w.name], replica_number=2)
        for seg in self.utility_wrap.get_query_segment_info(collection_w.name)[0]:
            assert len(seg.nodeIds) == 2
        # default tag query 0 empty
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]", partition_names=[ct.default_tag],
                           check_tasks=CheckTasks.check_query_empty)
        # default query 0 empty
        collection_w.query(expr=f"{ct.default_int64_field_name} in [2000]",
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': df_2.iloc[:1, :1].to_dict('records')})

        error = {ct.err_code: 65538, ct.err_msg: "partition not loaded"}
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]",
                           partition_names=[ct.default_partition_name, ct.default_tag],
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_replica_non_shard_leader(self):
        """
        target: test replica groups which one of QN is not shard leader
        method: 1.deploy cluster with 5 QNs
                2.create collection with 2 shards
                3.insert and flush
                4.load with 2 replica number
                5.insert growing data
                6.search and query
        expected: Verify search and query results
        """
        # create and insert entities
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=2)
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        # load with multi replica and insert growing data
        collection_w.load(replica_number=2)
        df_growing = cf.gen_default_dataframe_data(100, start=ct.default_nb)
        collection_w.insert(df_growing)

        replicas = collection_w.get_replicas()[0]
        # verify there are 2 groups (2 replicas)
        assert len(replicas.groups) == 2
        log.debug(replicas)
        all_group_nodes = []
        for group in replicas.groups:
            # verify each group have 3 shards
            assert len(group.shards) == 2
            all_group_nodes.extend(group.group_nodes)
        # verify all groups has 5 querynodes
        assert len(all_group_nodes) == 5

        # Verify 2 replicas segments loaded
        seg_info, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
        for seg in seg_info:
            assert len(seg.nodeIds) == 2

        # verify search successfully
        res, _ = collection_w.search(vectors, default_search_field, default_search_params, default_limit)
        assert len(res[0]) == ct.default_limit

        # verify query sealed and growing data successfully
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0, {ct.default_nb}]",
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{'int64': 0}, {'int64': 3000}]})

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_replica_multiple_shard_leader(self):
        """
        target: test replica groups which one of QN is shard leader of multiple shards
        method: 1.deploy cluster with 5 QNs
                2.create collection with 3 shards
                3.insert and flush
                4.load with 2 replica number
                5.insert growng data
                6.search and query
        expected: Verify search and query results
        """
        # craete and insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=3)
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        # load with multi replicas and insert growing data
        collection_w.load(replica_number=2)
        df_growing = cf.gen_default_dataframe_data(100, start=ct.default_nb)
        collection_w.insert(df_growing)

        # verify replica infos
        replicas, _ = collection_w.get_replicas()
        log.debug(replicas)
        assert len(replicas.groups) == 2
        all_group_nodes = []
        for group in replicas.groups:
            # verify each group have 3 shards
            assert len(group.shards) == 3
            all_group_nodes.extend(group.group_nodes)
        # verify all groups has 5 querynodes
        assert len(all_group_nodes) == 5

        # Verify 2 replicas segments loaded
        seg_info, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
        for seg in seg_info:
            assert len(seg.nodeIds) == 2

        # Verify search successfully
        res, _ = collection_w.search(vectors, default_search_field, default_search_params, default_limit)
        assert len(res[0]) == ct.default_limit

        # Verify query sealed and growing entities successfully
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0, {ct.default_nb}]",
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{'int64': 0}, {'int64': 3000}]})

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_replica_sq_count_balance(self):
        """
        target: test load with multi replicas, and sq request load balance cross replicas
        method: 1.Deploy milvus with multi querynodes
                2.Insert entities and load with replicas
                3.Do query req many times
                4.Verify the querynode sq_req_count metrics
        expected: Infer whether the query request is load balanced.
        """
        from utils.util_k8s import get_metrics_querynode_sq_req_count
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=5000)
        mutation_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == 5000
        total_sq_count = 20
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        collection_w.load(replica_number=3)
        for i in range(total_sq_count):
            ids = [random.randint(0, 100) for _ in range(5)]
            collection_w.query(f"{ct.default_int64_field_name} in {ids}")

        replicas, _ = collection_w.get_replicas()
        log.debug(replicas)
        sq_req_count = get_metrics_querynode_sq_req_count()
        for group in replicas.groups:
            group_nodes = group.group_nodes
            group_sq_req_count = 0
            for node in group_nodes:
                group_sq_req_count += sq_req_count[node]
            log.debug(f"Group nodes {group_nodes} with total sq_req_count {group_sq_req_count}")

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_replicas_not_loaded(self):
        """
        target: test get replicas of not loaded collection
        method: not loaded collection and get replicas
        expected: raise an exception
        """
        # create, insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb

        res, _ = collection_w.get_replicas()
        assert len(res.groups) == 0

    @pytest.mark.tags(CaseLabel.L3)
    def test_count_multi_replicas(self):
        """
        target: test count multi replicas
        method: 1. load data with multi replicas
                2. count
        expected: verify count
        """
        # create -> insert -> flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        collection_w.flush()

        # index -> load replicas
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load(replica_number=2)

        # count
        collection_w.query(expr=f'{ct.default_int64_field_name} >= 0', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{"count(*)": ct.default_nb}]})


class TestReleaseAdvanced(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_partition_during_searching(self):
        """
        target: test release partition during searching
        method: insert entities into partition, flush and load partition, release partition during searching
        expected: raise exception
        """
        self._connect()
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 10, partition_num, is_index=False)[0]
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        par = collection_w.partitions
        par_name = par[partition_num].name
        par[partition_num].load()
        limit = 10
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name])
        par[partition_num].release()
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 65535,
                                         "err_msg": "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_indexed_collection_during_searching(self):
        """
        target: test release indexed collection during searching
        method: insert entities into partition, flush and load partition, release collection during searching
        expected: raise exception
        """
        self._connect()
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 10, partition_num, is_index=False)[0]
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        par = collection_w.partitions
        par_name = par[partition_num].name
        par[partition_num].load()
        limit = 10
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name], _async=True)
        collection_w.release()
        error = {ct.err_code: 65535, ct.err_msg: "collection not loaded"}
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name],
                            check_task=CheckTasks.err_res,
                            check_items=error)


class TestLoadPartition(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `load_collection` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_binary_index(self, request):
        log.info(request.param)
        if request.param["index_type"] in ct.binary_supported_index_types:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize('binary_index', gen_binary_index())
    @pytest.mark.parametrize('metric_type', ct.binary_metrics)
    def test_load_partition_after_index_binary(self, binary_index, metric_type):
        """
        target: test load binary_collection, after index created
        method: insert and create index, load binary_collection with correct params
        expected: no error raised
        """
        self._connect()
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, ct.default_nb, partition_num,
                                                    is_binary=True, is_index=False)[0]

        # for metric_type in ct.binary_metrics:
        binary_index["metric_type"] = metric_type
        if binary_index["index_type"] == "BIN_IVF_FLAT" and metric_type in ct.structure_metrics:
            error = {ct.err_code: 65535,
                     ct.err_msg: f"metric type {metric_type} not found or not supported, supported: [HAMMING JACCARD]"}
            collection_w.create_index(ct.default_binary_vec_field_name, binary_index,
                                      check_task=CheckTasks.err_res, check_items=error)
            collection_w.create_index(ct.default_binary_vec_field_name, ct.default_bin_flat_index)
        else:
            collection_w.create_index(ct.default_binary_vec_field_name, binary_index)
        par = collection_w.partitions
        par[partition_num].load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_dis_connect(self):
        """
        target: test load partition, without connection
        method: load partition with correct params, with a disconnected instance
        expected: load raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0}
                                               )
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load()
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 999, ct.err_msg: 'should create connection first.'}
        partition_w.load(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_partition_dis_connect(self):
        """
        target: test release collection, without connection
        method: release collection with correct params, with a disconnected instance
        expected: release raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0}
                                               )
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load()
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 1, ct.err_msg: 'should create connection first.'}
        partition_w.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_not_existed(self):
        """
        target: test load partition for invalid scenario
        method: load not existed partition
        expected: raise exception and report the error
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0})
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.drop()
        error = {ct.err_code: 200, ct.err_msg: 'partition not found[partition=%s]' % partition_name}
        partition_w.load(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_partition_not_load(self):
        """
        target: test release partition without load
        method: release partition without load
        expected: release success
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0})
        partition_w.release()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_release_after_drop(self):
        """
        target: test load and release partition after drop
        method: drop partition and then load and release it
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0})
        partition_w.drop()
        collection_w.create_index(ct.default_float_vec_field_name)
        error = {ct.err_code: 200, ct.err_msg: 'partition not found[partition=%s]' % partition_name}
        partition_w.load(check_task=CheckTasks.err_res, check_items=error)
        partition_w.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_release_partition_after_drop(self):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0})
        partition_w.drop()
        error = {ct.err_code: 200, ct.err_msg: 'partition not found[partition=%s]' % partition_name}
        partition_w.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_release_after_collection_drop(self):
        """
        target: test release collection after drop
        method: insert and flush, then release collection after load and drop
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        name = collection_w.name
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               description=description,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name, "description": description,
                                                            "is_empty": True, "num_entities": 0})
        collection_w.drop()
        error = {ct.err_code: 0, ct.err_msg: "collection not found"}
        partition_w.load(check_task=CheckTasks.err_res, check_items=error)
        partition_w.release(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_loaded_partition(self):
        """
        target: test load partition after load partition
        method: 1. load partition
                2. load the partition again
                3. query on the non-loaded partition
                4. load collection
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.load()
        error = {ct.err_code: 65538, ct.err_msg: 'partition not loaded'}
        collection_w.query(default_term_expr, partition_names=[partition2],
                           check_task=CheckTasks.err_res, check_items=error)
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_unloaded_partition(self):
        """
        target: test load partition after load an unloaded partition
        method: 1. load partition
                2. load another partition
                3. query on the collection
                4. load collection
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.load()
        collection_w.query(default_term_expr)
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_one_partition(self):
        """
        target: test load partition after load partition
        method: 1. load partition
                2. load collection
                3. query on the partitions
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        collection_w.load()
        collection_w.query(default_term_expr, partition_names=[partition1, partition2])

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_partitions_release_collection(self):
        """
        target: test release collection after load partitions
        method: 1. load partition
                2. release collection
                3. query on the partition
                4. load partitions
                5. query on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        collection_w.release()
        error = {ct.err_code: 65535, ct.err_msg: "collection not loaded"}
        collection_w.query(default_term_expr, partition_names=[partition1],
                           check_task=CheckTasks.err_res, check_items=error)
        partition_w1.load()
        partition_w2.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_collection(self):
        """
        target: test load collection after load partitions
        method: 1. load partition
                2. release collection
                3. load collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        collection_w.release()
        collection_w.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_load_release_partition(self):
        """
        target: test load partitions after load and release partition
        method: 1. load partition
                2. release partition
                3. query on the partition
                4. load partitions(include released partition and non-released partition)
                5. query on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        error = {ct.err_code: 65535,
                 ct.err_msg: 'collection not loaded'}
        collection_w.query(default_term_expr, partition_names=[partition1],
                           check_task=CheckTasks.err_res, check_items=error)
        partition_w1.load()
        partition_w2.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_release_partition(self):
        """
        target: test load collection after load and release partition
        method: 1. load partition
                2. release partition
                3. load collection
                4. search on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        collection_w.load()
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, partition_names=[partition1, partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_load_partition_release_partitions(self):
        """
        target: test load partitions after load partition and release partitions
        method: 1. load partition
                2. release partitions
                3. load partitions
                4. query on the partitions
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w2.release()
        partition_w1.load()
        partition_w2.load()
        collection_w.query(default_term_expr, partition_names=[partition1, partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_partition_release_partitions(self):
        """
        target: test load collection after load partition and release partitions
        method: 1. load partition
                2. release partitions
                3. query on the partitions
                4. load collection
                5. query on the partitions
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w2.release()
        error = {ct.err_code: 65535, ct.err_msg: 'collection not loaded'}
        collection_w.query(default_term_expr, partition_names=[partition1, partition2],
                           check_task=CheckTasks.err_res, check_items=error)
        collection_w.load()
        collection_w.query(default_term_expr, partition_names=[partition1, partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_after_load_drop_partition(self):
        """
        target: test load partition after load and drop partition
        method: 1. load partition
                2. drop the loaded partition
                3. load the left partition
                4. query on the partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w1.drop()
        partition_w2.load()
        collection_w.query(default_term_expr, partition_names=[partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_after_load_drop_partition(self):
        """
        target: test load collection after load and drop partition
        method: 1. load partition
                2. drop the loaded partition
                3. query on the partition
                4. drop another partition
                5. load collection
                6. query on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w1.drop()
        error = {ct.err_code: 65535, ct.err_msg: f'partition name {partition1} not found'}
        collection_w.query(default_term_expr, partition_names=[partition1, partition2],
                           check_task=CheckTasks.err_res, check_items=error)
        partition_w2.drop()
        collection_w.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_load_partition_after_load_drop_partition(self):
        """
        target: test release load partition after load and drop partition
        method: 1. load partition
                2. drop the loaded partition
                3. release another partition
                4. load the partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w1.drop()
        partition_w2.release()
        partition_w2.load()
        collection_w.query(default_term_expr, partition_names=[partition2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_load_collection_after_load_drop_partition(self):
        """
        target: test release load partition after load and drop partition
        method: 1. load partition
                2. drop the loaded partition
                3. release another partition
                4. load collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w1.release()
        partition_w1.drop()
        partition_w2.release()
        collection_w.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_another_partition_after_load_drop_partition(self):
        """
        target: test load another collection after load and drop one partition
        method: 1. load partition
                2. drop the unloaded partition
                3. load the partition again
                4. query on the partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.drop()
        partition_w1.load()
        collection_w.query(default_term_expr, partition_names=[partition1])

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_load_partition_after_load_partition_drop_another(self):
        """
        target: test release load partition after load and drop partition
        method: 1. load partition
                2. drop the unloaded partition
                3. release the loaded partition
                4. query on the released partition
                5. reload the partition
                6. query on the partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.drop()
        partition_w1.release()
        error = {ct.err_code: 65535,
                 ct.err_msg: 'collection not loaded'}
        collection_w.query(default_term_expr, partition_names=[partition1],
                           check_task=CheckTasks.err_res, check_items=error)
        partition_w1.load()
        collection_w.query(default_term_expr, partition_names=[partition1])

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_load_collection_after_load_partition_drop_another(self):
        """
        target: test release load partition after load and drop partition
        method: 1. load partition
                2. drop the unloaded partition
                3. release the loaded partition
                4. load collection
                5. query on the collection
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.drop()
        partition_w1.release()
        collection_w.load()
        collection_w.query(default_term_expr)

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_unloaded_partition(self):
        """
        target: test load collection after load and drop partition
        method: 1. load partition
                2. release the other partition
                3. query on the first partition
        expected: no exception
        """
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        collection_w.create_index(default_search_field)
        partition_w1 = self.init_partition_wrap(collection_w, partition1)
        partition_w2 = self.init_partition_wrap(collection_w, partition2)
        partition_w1.load()
        partition_w2.release()
        collection_w.query(default_term_expr, partition_names=[partition1])


class TestCollectionString(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test about string
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_string_field_is_primary(self):
        """
        target: test create collection with string field
        method: 1. create collection with string field and vector field
                2. set string fields is_primary=True
        expected: Create collection successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_string_pk_default_collection_schema()
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_with_muti_string_fields(self):
        """
        target: test create collection with muti string fields
        method: 1. create collection with primary string field and not primary string field
                2. string fields is_primary=True
        expected: Create collection successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field()
        vec_field = cf.gen_float_vec_field()
        string_field_1 = cf.gen_string_field(is_primary=True)
        string_field_2 = cf.gen_string_field(name=c_name)
        schema = cf.gen_collection_schema(fields=[int_field, string_field_1, string_field_2, vec_field])
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_only_string_field(self):
        """
        target: test create collection with one string field
        method: create collection with only string field
        expected: Raise exception
        """
        self._connect()
        string_field = cf.gen_string_field(is_primary=True)
        schema = cf.gen_collection_schema([string_field])
        error = {ct.err_code: 0, ct.err_msg: "No vector field is found"}
        self.collection_wrap.init_collection(name=cf.gen_unique_str(prefix), schema=schema,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_string_field_with_exceed_max_len(self):
        """
        target: test create collection with string field
        method: 1. create collection with string field
                2. String field max_length exceeds maximum
        expected: Raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        max_length = 65535 + 1
        string_field = cf.gen_string_field(max_length=max_length)
        schema = cf.gen_collection_schema([int_field, string_field, vec_field])
        error = {ct.err_code: 65535, ct.err_msg: f"the maximum length specified for the field({ct.default_string_field_name}) should be in (0, 65535]"}
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_invalid_string_field_dtype(self):
        """
        target: test create collection with string field
        method: create collection with string field, the string field datatype is invaild
        expected: Raise exception
        """
        self._connect()
        string_field = self.field_schema_wrap.init_field_schema(name="string", dtype=DataType.STRING)[0]
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[int_field, string_field, vec_field])
        error = {ct.err_code: 0, ct.err_msg: "string data type not supported yet, please use VarChar type instead"}
        self.collection_wrap.init_collection(name=cf.gen_unique_str(prefix), schema=schema,
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_string_field_is_primary_and_auto_id(self):
        """
        target: test create collection with string field
        method: create collection with string field, the string field primary and auto id are true
        expected: Create collection successfully
        """
        self._connect()
        int_field = cf.gen_int64_field()
        vec_field = cf.gen_float_vec_field()
        string_field = cf.gen_string_field(is_primary=True, auto_id=True)
        fields = [int_field, string_field, vec_field]
        schema = self.collection_schema_wrap.init_collection_schema(fields=fields)[0]
        self.init_collection_wrap(schema=schema, check_task=CheckTasks.check_collection_property,
                                  check_items={"schema": schema, "primary": ct.default_string_field_name})


class TestCollectionJSON(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test about json
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_collection_json_field_as_primary_key(self, auto_id):
        """
        target: test create collection with JSON field as primary key
        method: 1. create collection with one JSON field, and vector field
                2. set json field is_primary=true
                3. set auto_id as true
        expected: Raise exception (not supported)
        """
        self._connect()
        int_field = cf.gen_int64_field()
        vec_field = cf.gen_float_vec_field()
        string_field = cf.gen_string_field()
        # 1. create json field as primary key through field schema api
        error = {ct.err_code: 1, ct.err_msg: "Primary key type must be DataType.INT64 or DataType.VARCHAR"}
        json_field = cf.gen_json_field(is_primary=True, auto_id=auto_id)
        fields = [int_field, string_field, json_field, vec_field]
        self.collection_schema_wrap.init_collection_schema(fields=fields,
                                                           check_task=CheckTasks.err_res, check_items=error)
        # 2. create json field as primary key through collection schema api
        json_field = cf.gen_json_field()
        fields = [int_field, string_field, json_field, vec_field]
        self.collection_schema_wrap.init_collection_schema(fields=fields, primary_field=ct.default_json_field_name,
                                                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_float_field_name, ct.default_json_field_name])
    def test_collection_json_field_partition_key(self, primary_field):
        """
        target: test create collection with multiple JSON fields
        method: 1. create collection with multiple JSON fields, primary key field and vector field
                2. set json field is_primary=false
        expected: Raise exception
        """
        self._connect()
        cf.gen_unique_str(prefix)
        error = {ct.err_code: 1, ct.err_msg: "Primary key type must be DataType.INT64 or DataType.VARCHAR"}
        cf.gen_json_default_collection_schema(primary_field=primary_field, is_partition_key=True,
                                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_collection_json_field_supported_primary_key(self, primary_field):
        """
        target: test create collection with one JSON field
        method: 1. create collection with one JSON field, primary key field and vector field
                2. set json field is_primary=false
        expected: Create collection successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_json_default_collection_schema(primary_field=primary_field)
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_collection_multiple_json_fields_supported_primary_key(self, primary_field):
        """
        target: test create collection with multiple JSON fields
        method: 1. create collection with multiple JSON fields, primary key field and vector field
                2. set json field is_primary=false
        expected: Create collection successfully
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_multiple_json_default_collection_schema(primary_field=primary_field)
        self.collection_wrap.init_collection(name=c_name, schema=schema,
                                             check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})


class TestCollectionARRAY(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test about array
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_array_field_element_type_not_exist(self):
        """
        target: test create collection with ARRAY field without element type
        method: create collection with one array field without element type
        expected: Raise exception
        """
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        array_field = cf.gen_array_field(element_type=None)
        array_schema = cf.gen_collection_schema([int_field, vec_field, array_field])
        self.init_collection_wrap(schema=array_schema, check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 65535, ct.err_msg: "element data type None is not valid"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("element_type", [1001, 'a', [], (), {1}, DataType.BINARY_VECTOR,
                                              DataType.FLOAT_VECTOR, DataType.JSON, DataType.ARRAY])
    def test_collection_array_field_element_type_invalid(self, element_type):
        """
        target: Create a field with invalid element_type
        method: Create a field with invalid element_type
                1. Type not in DataType: 1, 'a', ...
                2. Type in DataType: binary_vector, float_vector, json_field, array_field
        expected: Raise exception
        """
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        array_field = cf.gen_array_field(element_type=element_type)
        array_schema = cf.gen_collection_schema([int_field, vec_field, array_field])
        error = {ct.err_code: 999, ct.err_msg: f"element type {element_type} is not supported"}
        if element_type in ['a', {1}]:
            error = {ct.err_code: 999, ct.err_msg: "Unexpected error"}
        if element_type in [[], ()]:
            error = {ct.err_code: 65535, ct.err_msg: "element data type None is not valid"}
        if element_type in [DataType.BINARY_VECTOR, DataType.FLOAT_VECTOR, DataType.JSON, DataType.ARRAY]:
            data_type = element_type.name
            if element_type == DataType.BINARY_VECTOR:
                data_type = "BinaryVector"
            if element_type == DataType.FLOAT_VECTOR:
                data_type = "FloatVector"
            if element_type == DataType.ARRAY:
                data_type = "Array"
            error = {ct.err_code: 999, ct.err_msg: f"element type {data_type} is not supported"}
        self.init_collection_wrap(schema=array_schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("https://github.com/milvus-io/pymilvus/issues/2041")
    def test_collection_array_field_no_capacity(self):
        """
        target: Create a field without giving max_capacity
        method: Create a field without giving max_capacity
        expected: Raise exception
        """
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        array_field = cf.gen_array_field(max_capacity=None)
        array_schema = cf.gen_collection_schema([int_field, vec_field, array_field])
        self.init_collection_wrap(schema=array_schema, check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 65535,
                                               ct.err_msg: "the value of max_capacity must be an integer"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("https://github.com/milvus-io/pymilvus/issues/2041")
    @pytest.mark.parametrize("max_capacity", [[], 'a', (), -1, 4097])
    def test_collection_array_field_invalid_capacity(self, max_capacity):
        """
        target: Create a field with invalid max_capacity
        method: Create a field with invalid max_capacity
                1. Type invalid: [], 'a', ()
                2. Value invalid: <0, >max_capacity(4096)
        expected: Raise exception
        """
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        array_field = cf.gen_array_field(max_capacity=max_capacity)
        array_schema = cf.gen_collection_schema([int_field, vec_field, array_field])
        self.init_collection_wrap(schema=array_schema, check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 65535,
                                               ct.err_msg: "the maximum capacity specified for a "
                                                           "Array should be in (0, 4096]"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_string_array_without_max_length(self):
        """
        target: Create string array without giving max length
        method: Create string array without giving max length
        expected: Raise exception
        """
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        array_field = cf.gen_array_field(element_type=DataType.VARCHAR)
        array_schema = cf.gen_collection_schema([int_field, vec_field, array_field])
        self.init_collection_wrap(schema=array_schema, check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 65535,
                                               ct.err_msg: "type param(max_length) should be specified for the "
                                                           "field(int_array)"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("https://github.com/milvus-io/pymilvus/issues/2041")
    @pytest.mark.parametrize("max_length", [[], 'a', (), -1, 65536])
    def test_collection_string_array_max_length_invalid(self, max_length):
        """
        target: Create string array with invalid max length
        method: Create string array with invalid max length
                1. Type invalid: [], 'a', ()
                2. Value invalid: <0, >max_length(65535)
        expected: Raise exception
        """
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        array_field = cf.gen_array_field(element_type=DataType.VARCHAR, max_length=max_length)
        array_schema = cf.gen_collection_schema([int_field, vec_field, array_field])
        self.init_collection_wrap(schema=array_schema, check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 65535,
                                               ct.err_msg: "the maximum length specified for a VarChar "
                                                           "should be in (0, 65535]"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_array_field_all_datatype(self):
        """
        target: test create collection with ARRAY field all data type
        method: 1. Create field respectively: int8, int16, int32, int64, varchar, bool, float, double
                2. Insert data respectively: int8, int16, int32, int64, varchar, bool, float, double
        expected: Raise exception
        """
        # Create field respectively
        nb = ct.default_nb
        pk_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        int8_array = cf.gen_array_field(name="int8_array", element_type=DataType.INT8, max_capacity=nb)
        int16_array = cf.gen_array_field(name="int16_array", element_type=DataType.INT16, max_capacity=nb)
        int32_array = cf.gen_array_field(name="int32_array", element_type=DataType.INT32, max_capacity=nb)
        int64_array = cf.gen_array_field(name="int64_array", element_type=DataType.INT64, max_capacity=nb)
        bool_array = cf.gen_array_field(name="bool_array", element_type=DataType.BOOL, max_capacity=nb)
        float_array = cf.gen_array_field(name="float_array", element_type=DataType.FLOAT, max_capacity=nb)
        double_array = cf.gen_array_field(name="double_array", element_type=DataType.DOUBLE, max_capacity=nb)
        string_array = cf.gen_array_field(name="string_array", element_type=DataType.VARCHAR, max_capacity=nb,
                                          max_length=100)
        array_schema = cf.gen_collection_schema([pk_field, vec_field, int8_array, int16_array, int32_array,
                                                 int64_array, bool_array, float_array, double_array, string_array])
        collection_w = self.init_collection_wrap(schema=array_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_schema: array_schema})

        # check array in collection.describe()
        res = collection_w.describe()[0]
        log.info(res)
        fields = [
            {"field_id": 100, "name": "int64", "description": "", "type": 5, "params": {},
             "element_type": 0, "is_primary": True},
            {"field_id": 101, "name": "float_vector", "description": "", "type": 101,
             "params": {"dim": ct.default_dim}, "element_type": 0},
            {"field_id": 102, "name": "int8_array", "description": "", "type": 22,
             "params": {"max_capacity": 2000}, "element_type": 2},
            {"field_id": 103, "name": "int16_array", "description": "", "type": 22,
             "params": {"max_capacity": 2000}, "element_type": 3},
            {"field_id": 104, "name": "int32_array", "description": "", "type": 22,
             "params": {"max_capacity": 2000}, "element_type": 4},
            {"field_id": 105, "name": "int64_array", "description": "", "type": 22,
             "params": {"max_capacity": 2000}, "element_type": 5},
            {"field_id": 106, "name": "bool_array", "description": "", "type": 22,
             "params": {"max_capacity": 2000}, "element_type": 1},
            {"field_id": 107, "name": "float_array", "description": "", "type": 22,
             "params": {"max_capacity": 2000}, "element_type": 10},
            {"field_id": 108, "name": "double_array", "description": "", "type": 22,
             "params": {"max_capacity": 2000}, "element_type": 11},
            {"field_id": 109, "name": "string_array", "description": "", "type": 22,
             "params": {"max_length": 100, "max_capacity": 2000}, "element_type": 21}
        ]
        # assert res["fields"] == fields

        # Insert data respectively
        nb = 10
        pk_values = [i for i in range(nb)]
        float_vec = cf.gen_vectors(nb, ct.default_dim)
        int8_values = [[numpy.int8(j) for j in range(nb)] for i in range(nb)]
        int16_values = [[numpy.int16(j) for j in range(nb)] for i in range(nb)]
        int32_values = [[numpy.int32(j) for j in range(nb)] for i in range(nb)]
        int64_values = [[numpy.int64(j) for j in range(nb)] for i in range(nb)]
        bool_values = [[numpy.bool_(j) for j in range(nb)] for i in range(nb)]
        float_values = [[numpy.float32(j) for j in range(nb)] for i in range(nb)]
        double_values = [[numpy.double(j) for j in range(nb)] for i in range(nb)]
        string_values = [[str(j) for j in range(nb)] for i in range(nb)]
        data = [pk_values, float_vec, int8_values, int16_values, int32_values, int64_values,
                bool_values, float_values, double_values, string_values]
        collection_w.insert(data)

        # check insert successfully
        collection_w.flush()
        collection_w.num_entities == nb


class TestCollectionMultipleVectorValid(TestcaseBase):
    """
    ******************************************************************
    #  The followings are valid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_key", [cf.gen_int64_field(is_primary=True), cf.gen_string_field(is_primary=True)])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("shards_num", [1, 3])
    def test_create_collection_multiple_vectors_all_supported_field_type(self, primary_key, auto_id, shards_num):
        """
        target: test create collection with multiple vector fields
        method: create collection with multiple vector fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_fields = []
        vector_limit_num = max_vector_field_num - 2
        # add multiple vector fields
        for i in range(vector_limit_num):
            vector_field_name = cf.gen_unique_str("field_name")
            field = cf.gen_float_vec_field(name=vector_field_name)
            int_fields.append(field)
        # add other vector fields to maximum fields num
        int_fields.append(cf.gen_int8_field())
        int_fields.append(cf.gen_int16_field())
        int_fields.append(cf.gen_int32_field())
        int_fields.append(cf.gen_float_field())
        int_fields.append(cf.gen_double_field())
        int_fields.append(cf.gen_string_field(cf.gen_unique_str("vchar_field_name")))
        int_fields.append(cf.gen_json_field())
        int_fields.append(cf.gen_bool_field())
        int_fields.append(cf.gen_array_field())
        int_fields.append(cf.gen_binary_vec_field())
        int_fields.append(primary_key)
        schema = cf.gen_collection_schema(fields=int_fields, auto_id=auto_id, shards_num=shards_num)
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_key", [ct.default_int64_field_name, ct.default_string_field_name])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_create_collection_multiple_vectors_different_dim(self, primary_key, auto_id, enable_dynamic_field):
        """
        target: test create collection with multiple vector fields (different dim)
        method: create collection with multiple vector fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        another_dim = ct.min_dim
        schema = cf.gen_default_collection_schema(primary_field=primary_key, auto_id=auto_id, dim=ct.max_dim,
                                                  enable_dynamic_field=enable_dynamic_field,
                                                  multiple_dim_array=[another_dim])
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_key", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_create_collection_multiple_vectors_maximum_dim(self, primary_key):
        """
        target: test create collection with multiple vector fields
        method: create collection with multiple vector fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(primary_field=primary_key, dim=ct.max_dim,
                                                  multiple_dim_array=[ct.max_dim])
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_key", [cf.gen_int64_field(is_primary=True), cf.gen_string_field(is_primary=True)])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("par_key_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_create_collection_multiple_vectors_partition_key(self, primary_key, auto_id, par_key_field):
        """
        target: test create collection with multiple vector fields
        method: create collection with multiple vector fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_fields = []
        vector_limit_num = max_vector_field_num - 2
        # add multiple vector fields
        for i in range(vector_limit_num):
            vector_field_name = cf.gen_unique_str("field_name")
            field = cf.gen_float_vec_field(name=vector_field_name)
            int_fields.append(field)
        # add other vector fields to maximum fields num
        int_fields.append(cf.gen_int8_field())
        int_fields.append(cf.gen_int16_field())
        int_fields.append(cf.gen_int32_field())
        int_fields.append(cf.gen_int64_field(cf.gen_unique_str("int_field_name"),
                                             is_partition_key=(par_key_field == ct.default_int64_field_name)))
        int_fields.append(cf.gen_float_field())
        int_fields.append(cf.gen_double_field())
        int_fields.append(cf.gen_string_field(cf.gen_unique_str("vchar_field_name"),
                                              is_partition_key=(par_key_field == ct.default_string_field_name)))
        int_fields.append(cf.gen_json_field())
        int_fields.append(cf.gen_bool_field())
        int_fields.append(cf.gen_array_field())
        int_fields.append(cf.gen_binary_vec_field())
        int_fields.append(primary_key)
        schema = cf.gen_collection_schema(fields=int_fields, auto_id=auto_id)
        collection_w = \
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.check_collection_property,
                                             check_items={exp_name: c_name, exp_schema: schema})[0]
        assert len(collection_w.partitions) == ct.default_partition_num


class TestCollectionMultipleVectorInvalid(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=ct.invalid_dims)
    def get_invalid_dim(self, request):
        yield request.param

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_key", [cf.gen_int64_field(is_primary=True), cf.gen_string_field(is_primary=True)])
    def test_create_collection_multiple_vectors_same_vector_field_name(self, primary_key):
        """
        target: test create collection with multiple vector fields
        method: create collection with multiple vector fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_fields = []
        vector_limit_num = max_vector_field_num - 2
        # add multiple vector fields
        for i in range(vector_limit_num):
            field = cf.gen_float_vec_field()
            int_fields.append(field)
        # add other vector fields to maximum fields num
        int_fields.append(cf.gen_int8_field())
        int_fields.append(cf.gen_int16_field())
        int_fields.append(cf.gen_int32_field())
        int_fields.append(cf.gen_float_field())
        int_fields.append(cf.gen_double_field())
        int_fields.append(cf.gen_string_field(cf.gen_unique_str("vchar_field_name")))
        int_fields.append(cf.gen_json_field())
        int_fields.append(cf.gen_bool_field())
        int_fields.append(cf.gen_array_field())
        int_fields.append(cf.gen_binary_vec_field())
        int_fields.append(primary_key)
        schema = cf.gen_collection_schema(fields=int_fields)
        error = {ct.err_code: 65535, ct.err_msg: "duplicated field name"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_vector_name", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_create_collection_multiple_vectors_invalid_part_vector_field_name(self, invalid_vector_name):
        """
        target: test create collection with multiple vector fields
        method: create collection with multiple vector fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_fields = []
        # add multiple vector fields
        vector_field_1 = cf.gen_float_vec_field(name=invalid_vector_name)
        int_fields.append(vector_field_1)
        vector_field_2 = cf.gen_float_vec_field(name="valid_field_name")
        int_fields.append(vector_field_2)
        # add other vector fields to maximum fields num
        int_fields.append(cf.gen_int64_field(is_primary=True))
        schema = cf.gen_collection_schema(fields=int_fields)
        error = {ct.err_code: 1701, ct.err_msg: "Invalid field name: %s" % invalid_vector_name}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_vector_name", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_create_collection_multiple_vectors_invalid_all_vector_field_name(self, invalid_vector_name):
        """
        target: test create collection with multiple vector fields
        method: create collection with multiple vector fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_fields = []
        # add multiple vector fields
        vector_field_1 = cf.gen_float_vec_field(name=invalid_vector_name)
        int_fields.append(vector_field_1)
        vector_field_2 = cf.gen_float_vec_field(name=invalid_vector_name + " ")
        int_fields.append(vector_field_2)
        # add other vector fields to maximum fields num
        int_fields.append(cf.gen_int64_field(is_primary=True))
        schema = cf.gen_collection_schema(fields=int_fields)
        error = {ct.err_code: 1701, ct.err_msg: "Invalid field name: %s" % invalid_vector_name}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip("issue #37543")
    def test_create_collection_multiple_vectors_invalid_dim(self, get_invalid_dim):
        """
        target: test create collection with multiple vector fields
        method: create collection with multiple vector fields
        expected: no exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        int_fields = []
        # add multiple vector fields
        vector_field_1 = cf.gen_float_vec_field(dim=get_invalid_dim)
        int_fields.append(vector_field_1)
        vector_field_2 = cf.gen_float_vec_field(name="float_vec_field")
        int_fields.append(vector_field_2)
        # add other vector fields to maximum fields num
        int_fields.append(cf.gen_int64_field(is_primary=True))
        schema = cf.gen_collection_schema(fields=int_fields)
        error = {ct.err_code: 65535, ct.err_msg: "invalid dimension"}
        self.collection_wrap.init_collection(c_name, schema=schema, check_task=CheckTasks.err_res, check_items=error)


class TestCollectionMmap(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_describe_collection_mmap(self):
        """
        target: enable or disable mmap in the collection
        method: enable or disable mmap in the collection
        expected: description information contains mmap
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=default_schema)
        collection_w.set_properties({'mmap.enabled': True})
        pro = collection_w.describe().get("properties")
        assert "mmap.enabled" in pro.keys()
        assert pro["mmap.enabled"] == 'True'
        collection_w.set_properties({'mmap.enabled': False})
        pro = collection_w.describe().get("properties")
        assert pro["mmap.enabled"] == 'False'
        collection_w.set_properties({'mmap.enabled': True})
        pro = collection_w.describe().get("properties")
        assert pro["mmap.enabled"] == 'True'

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_mmap_collection(self):
        """
        target: after loading, enable mmap for the collection
        method: 1. data preparation and create index
        2. load collection
        3. enable mmap on collection
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name, schema=default_schema)
        collection_w.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index,
                                  index_name=ct.default_index_name)
        collection_w.set_properties({'mmap.enabled': True})
        pro = collection_w.describe()[0].get("properties")
        assert pro["mmap.enabled"] == 'True'
        collection_w.load()
        collection_w.set_properties({'mmap.enabled': True},
                                    check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 104,
                                    ct.err_msg: f"collection already loaded"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_mmap_collection(self):
        """
        target: set mmap on collection
        method: 1. set mmap on collection
        2. drop collection
        3. describe collection
        expected: description information contains mmap
        """
        self._connect()
        c_name = "coll_rand"
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=default_schema)
        collection_w.set_properties({'mmap.enabled': True})
        collection_w.drop()
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=default_schema)
        pro = collection_w.describe().get("properties")
        assert "mmap.enabled" not in pro.keys()

    @pytest.mark.tags(CaseLabel.L2)
    def test_multiple_collections_enable_mmap(self):
        """
        target: enabling mmap for multiple collections in a single instance
        method: enabling mmap for multiple collections in a single instance
        expected: the collection description message for mmap is normal
        """
        self._connect()
        c_name = "coll_1"
        c_name2 = "coll_2"
        c_name3 = "coll_3"
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=default_schema)
        collection_w2, _ = self.collection_wrap.init_collection(c_name2, schema=default_schema)
        collection_w3, _ = self.collection_wrap.init_collection(c_name3, schema=default_schema)
        collection_w.set_properties({'mmap.enabled': True})
        collection_w2.set_properties({'mmap.enabled': True})
        pro = collection_w.describe().get("properties")
        pro2 = collection_w2.describe().get("properties")
        assert pro["mmap.enabled"] == 'True'
        assert pro2["mmap.enabled"] == 'True'
        collection_w3.set_properties({'mmap.enabled': True})
        pro3 = collection_w3.describe().get("properties")
        assert pro3["mmap.enabled"] == 'True'

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_collection_mmap(self):
        """
        target: after flush, collection enables mmap
        method: after flush, collection enables mmap
        expected: the collection description message for mmap is normal
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        collection_w, _ = self.collection_wrap.init_collection(c_name, schema=default_schema)
        collection_w.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index,
                                  index_name=ct.default_index_name)
        collection_w.alter_index(ct.default_index_name, {'mmap.enabled': False})
        collection_w.flush()
        collection_w.set_properties({'mmap.enabled': True})
        pro = collection_w.describe().get("properties")
        assert pro["mmap.enabled"] == 'True'
        collection_w.alter_index(ct.default_index_name, {'mmap.enabled': True})
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_enable_mmap_after_drop_collection(self):
        """
        target: enable mmap after deleting a collection
        method: enable mmap after deleting a collection
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, True, is_binary=True, is_index=False)[0]
        collection_w.drop()
        collection_w.set_properties({'mmap.enabled': True}, check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 100,
                                              ct.err_msg: f"collection not found"})


    