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
