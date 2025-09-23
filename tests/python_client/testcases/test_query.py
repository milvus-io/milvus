import utils.util_pymilvus as ut
from utils.util_log import test_log as log
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from common.text_generator import KoreanTextGenerator, ICUTextGenerator
from common.code_mapping import ConnectionErrorMessage as cem
from base.client_base import TestcaseBase
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_EVENTUALLY
from pymilvus import (
    FieldSchema,
    CollectionSchema,
    DataType,
)
import threading
from pymilvus import DefaultConfig
import time
import pytest
import random
import numpy as np
import pandas as pd
from collections import Counter
from faker import Faker

Faker.seed(19530)

fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")
fake_de = Faker("de_DE")
fake_jp = Faker("ja_JP")
fake_ko = Faker("ko_KR")



# patch faker to generate text with specific distribution
cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)

prefix = "query"
exp_res = "exp_res"
count = "count(*)"
default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'
default_mix_expr = "int64 >= 0 && varchar >= \"0\""
default_expr = f'{ct.default_int64_field_name} >= 0'
default_invalid_expr = "varchar >= 0"
default_string_term_expr = f'{ct.default_string_field_name} in [\"0\", \"1\"]'
default_index_params = ct.default_index
binary_index_params = ct.default_binary_index

default_entities = ut.gen_entities(ut.default_nb, is_normal=True)
default_pos = 5
json_field = ct.default_json_field_name
default_int_field_name = ct.default_int64_field_name
default_float_field_name = "float"
default_string_field_name = "varchar"


class TestQueryParams(TestcaseBase):
    """
    test Query interface
    query(collection_name, expr, output_fields=None, partition_names=None, timeout=None)
    """

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def random_primary_key(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_invalid(self):
        """
        target: test query with invalid term expression
        method: query with invalid term expr
        expected: raise exception
        """
        collection_w, entities = self.init_collection_general(prefix, insert_data=True, nb=10)[0:2]
        term_expr = f'{default_int_field_name} in {entities[:default_pos]}'
        error = {ct.err_code: 999, ct.err_msg: "cannot parse expression: int64 in"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

        # check missing the template variable
        expr = "int64 in {value_0}"
        expr_params = {"value_1": [0, 1]}
        error = {ct.err_code: 999, ct.err_msg: "the value of expression template variable name {value_0} is not found"}
        collection_w.query(expr=expr, expr_params=expr_params,
                           check_task=CheckTasks.err_res, check_items=error)

        # check the template variable type dismatch
        expr = "int64 in {value_0}"
        expr_params = {"value_0": 1}
        error = {ct.err_code: 999, ct.err_msg: "the value of term expression template variable {value_0} is not array"}
        collection_w.query(expr=expr, expr_params=expr_params,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query(self, enable_dynamic_field):
        """
        target: test query
        method: query with term expr
        expected: verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             enable_dynamic_field=enable_dynamic_field)[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values = []
            for vector in vectors[0]:
                vector = vector[ct.default_int64_field_name]
                int_values.append(vector)
            res = [{ct.default_int64_field_name: int_values[i]} for i in range(pos)]
        else:
            int_values = vectors[0][ct.default_int64_field_name].values.tolist()
            res = vectors[0].iloc[0:pos, :1].to_dict('records')

        term_expr = f'{ct.default_int64_field_name} in {int_values[:pos]}'
        collection_w.query(term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_auto_id_collection(self):
        """
        target: test query with auto_id=True collection
        method: test query with auto id
        expected: query result is correct
        """
        self._connect()
        df = cf.gen_default_dataframe_data()
        df[ct.default_int64_field_name] = None
        insert_res, _, = self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                                       primary_field=ct.default_int64_field_name,
                                                                       auto_id=True)
        assert self.collection_wrap.num_entities == ct.default_nb
        ids = insert_res[1].primary_keys
        pos = 5
        res = df.iloc[:pos, :1].to_dict('records')
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()

        # query with all primary keys
        term_expr_1 = f'{ct.default_int64_field_name} in {ids[:pos]}'
        for i in range(5):
            res[i][ct.default_int64_field_name] = ids[i]
        self.collection_wrap.query(term_expr_1,
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: res, "pk_name": self.collection_wrap.primary_field.name})

        # query with part primary keys
        term_expr_2 = f'{ct.default_int64_field_name} in {[ids[0], 0]}'
        self.collection_wrap.query(term_expr_2, check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: res[:1], "pk_name": self.collection_wrap.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_non_string_expr(self):
        """
        target: test query with non-string expr
        method: query with non-string expr, eg 1, [] ..
        expected: raise exception
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        exprs = [1, 2., [], {}, ()]
        error = {ct.err_code: 0, ct.err_msg: "The type of expr must be string"}
        for expr in exprs:
            collection_w.query(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="repeat with test_query, waiting for other expr")
    def test_query_expr_term(self):
        """
        target: test query with TermExpr
        method: query with TermExpr
        expected: query result is correct
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        res = vectors[0].iloc[:2, :1].to_dict('records')
        collection_w.query(default_term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.fixture(scope="function", params=[0, 10, 100])
    def offset(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("not stable")
    def test_query_during_upsert(self):
        """
        target: test query during upsert
        method: 1. create a collection and query
                2. query during upsert
                3. compare two query results
        expected: the two query results is the same
        """
        upsert_nb = 1000
        expr = f"int64 >= 0 && int64 <= {upsert_nb}"
        collection_w = self.init_collection_general(prefix, True)[0]
        res1 = collection_w.query(expr, output_fields=[default_float_field_name])[0]

        def do_upsert():
            data = cf.gen_default_data_for_upsert(upsert_nb)[0]
            collection_w.upsert(data=data)

        t = threading.Thread(target=do_upsert, args=())
        t.start()
        res2 = collection_w.query(expr, output_fields=[default_float_field_name])[0]
        t.join()
        assert [res1[i][default_float_field_name] for i in range(upsert_nb)] == \
               [res2[i][default_float_field_name] for i in range(upsert_nb)]


class TestQueryOperation(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test query interface operations
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_all_term_array(self):
        """
        target: test query with all array term expr
        method: query with all array value
        expected: verify query result
        """

        # init a collection and insert data
        collection_w, vectors, binary_raw_vectors = \
            self.init_collection_general(prefix, insert_data=True)[0:3]

        # data preparation
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        term_expr = f'{ct.default_int64_field_name} in {int_values}'
        check_vec = vectors[0].iloc[:, [0]][0:len(int_values)].to_dict('records')

        # query all array value
        collection_w.query(term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: check_vec, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_expr_half_term_array(self):
        """
        target: test query with half array term expr
        method: query with half array value
        expected: verify query result
        """

        half = ct.default_nb // 2
        collection_w, partition_w, df_partition, df_default = \
            self.insert_entities_into_two_partitions_in_half(half)

        int_values = df_default[ct.default_int64_field_name].values.tolist()
        term_expr = f'{ct.default_int64_field_name} in {int_values}'
        res, _ = collection_w.query(term_expr)
        assert len(res) == len(int_values)

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_expr_repeated_term_array(self):
        """
        target: test query with repeated term array on primary field with unique value
        method: query with repeated array value
        expected: return hit entities, no repeated
        """
        collection_w, vectors, binary_raw_vectors = self.init_collection_general(prefix, insert_data=True)[0:3]
        int_values = [0, 0, 0, 0]
        term_expr = f'{ct.default_int64_field_name} in {int_values}'
        res, _ = collection_w.query(term_expr)
        assert len(res) == 1
        assert res[0][ct.default_int64_field_name] == int_values[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_dup_ids_dup_term_array(self):
        """
        target: test query on duplicate primary keys with dup term array
        method: 1.create collection and insert dup primary keys
                2.query with dup term array
        expected: todo
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=100)
        df[ct.default_int64_field_name] = 0
        mutation_res, _ = collection_w.insert(df)
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].tolist()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        term_expr = f'{ct.default_int64_field_name} in {[0, 0, 0]}'
        res = df.iloc[:, :2].to_dict('records')
        collection_w.query(term_expr, output_fields=["*"], check_items=CheckTasks.check_query_results,
                           check_task={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_multi_logical_exprs(self):
        """
        target: test the scenario which search with many logical expressions
        method: 1. create collection
                3. search with the expr that like: int64 == 0 || int64 == 1 ........
        expected: run successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        multi_exprs = " || ".join(f'{default_int_field_name} == {i}' for i in range(60))

        collection_w.load()
        vectors_s = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        limit = 1000
        _, check_res = collection_w.search(vectors_s[:ct.default_nq], ct.default_float_vec_field_name,
                                           ct.default_search_params, limit, multi_exprs)
        assert (check_res == True)


class TestQueryString(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test query with string
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_expr_with_binary(self):
        """
        target: test query string expr with binary
        method: query string expr with binary
        expected: verify query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             is_binary=True, is_index=False)[0:2]
        collection_w.create_index(ct.default_binary_vec_field_name, binary_index_params)
        collection_w.load()
        assert collection_w.has_index()[0]
        res, _ = collection_w.query(default_string_term_expr, output_fields=[ct.default_binary_vec_field_name])
        assert len(res) == 2

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 24637")
    def test_query_after_insert_multi_threading(self):
        """
        target: test data consistency after multi threading insert
        method: multi threads insert, and query, compare queried data with original
        expected: verify data consistency
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        thread_num = 4
        threads = []
        primary_keys = []
        df_list = []

        # prepare original data for parallel insert
        for i in range(thread_num):
            df = cf.gen_default_dataframe_data(ct.default_nb, start=i * ct.default_nb)
            df_list.append(df)
            primary_key = df[ct.default_int64_field_name].values.tolist()
            primary_keys.append(primary_key)

        def insert(thread_i):
            log.debug(f'In thread-{thread_i}')
            mutation_res, _ = collection_w.insert(df_list[thread_i])
            assert mutation_res.insert_count == ct.default_nb
            assert mutation_res.primary_keys == primary_keys[thread_i]

        for i in range(thread_num):
            x = threading.Thread(target=insert, args=(i,))
            threads.append(x)
            x.start()
        for t in threads:
            t.join()
        assert collection_w.num_entities == ct.default_nb * thread_num

        # Check data consistency after parallel insert
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        df_dict_list = []
        for df in df_list:
            df_dict_list += df.to_dict('records')
        output_fields = ["*"]
        expression = "int64 >= 0"
        collection_w.query(expression, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: df_dict_list,
                                        "pk_name": collection_w.primary_field.name,
                                        "with_vec": True})


class TestQueryCount(TestcaseBase):
    """
    test query count(*)
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_compact_merge(self):
        """
        target: test count after compact merge segments
        method: 1. init 2 segments with same channel
                2. compact
                3. count
        expected: verify count
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)

        # init two segments
        tmp_nb = 100
        segment_num = 2
        for i in range(segment_num):
            df = cf.gen_default_dataframe_data(nb=tmp_nb, start=i * tmp_nb)
            collection_w.insert(df)
            collection_w.flush()

        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        # recreate index wait for compactTo indexed
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)

        collection_w.load()
        segment_info, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
        assert len(segment_info) == 1

        # count after compact
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: tmp_nb * segment_num}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_compact_delete(self):
        """
        target: test count after delete-compact
        method: 1. init segments
                2. delete half ids and compact
                3. count
        expected: verify count
        """
        # create -> index -> insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)

        # delete half entities, flush
        half_expr = f'{ct.default_int64_field_name} in {[i for i in range(ct.default_nb // 2)]}'
        collection_w.delete(half_expr)
        assert collection_w.num_entities == ct.default_nb

        # compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()

        # load and count
        collection_w.load()
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb // 2}],
                                        "pk_name": collection_w.primary_field.name}
                           )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ct.all_index_types[10:12])
    def test_counts_expression_sparse_vectors(self, index):
        """
        target: test count with expr
        method: count with expr
        expected: verify count
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w = self.init_collection_wrap(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data()
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)
        collection_w.load()
        collection_w.query(expr=default_expr, output_fields=[count],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb}],
                                        "pk_name": collection_w.primary_field.name})
        expr = "int64 > 50 && int64 < 100 && float < 75"
        collection_w.query(expr=expr, output_fields=[count],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: 24}],
                                        "pk_name": collection_w.primary_field.name})
        batch_size = 100
        collection_w.query_iterator(batch_size=batch_size, expr=default_expr,
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": ct.default_nb,
                                                 "batch_size": batch_size,
                                                 "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.repeat(3)
    @pytest.mark.skip(reason="issue #36538")
    def test_count_query_search_after_release_partition_load(self):
        """
        target: test query count(*) after release collection and load partition
        method: 1. create a collection and 2 partitions with nullable and default value fields
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. release the collection and load one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(prefix, True, 200, partition_num=1, is_index=True)[0]
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 200}],
                                        "pk_name": collection_w.primary_field.name})
        collection_w.release()
        partition_w1, partition_w2 = collection_w.partitions
        # load
        partition_w1.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        collection_w.release()
        # partition_w1.load()
        collection_w.load(partition_names=[partition_w1.name])
        # search on collection, partition1, partition2
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}],
                                        "pk_name": collection_w.primary_field.name})
        partition_w1.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}],
                                        "pk_name": collection_w.primary_field.name})
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        collection_w.search(vectors[:1], ct.default_float_vec_field_name, ct.default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})


class TestQueryNoneAndDefaultData(TestcaseBase):
    """
    test Query interface with none and default data
    query(collection_name, expr, output_fields=None, partition_names=None, timeout=None)
    """

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["STL_SORT", "INVERTED"])
    def numeric_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["TRIE", "INVERTED", "BITMAP"])
    def varchar_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[0, 0.5, 1])
    def null_data_percent(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_by_normal_with_none_data(self, enable_dynamic_field, null_data_percent):
        """
        target: test query with none data
        method: query with term expr with nullable fields, insert data including none
        expected: verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             enable_dynamic_field=enable_dynamic_field,
                                                             nullable_fields={
                                                                 default_float_field_name: null_data_percent})[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            int_values = vectors[0][ct.default_int64_field_name].values.tolist()
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f'{ct.default_int64_field_name} in {int_values[:pos]}'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_by_expr_none_with_none_data(self, enable_dynamic_field, null_data_percent):
        """
        target: test query by none expr with nullable fields, insert data including none
        method: query by expr None after inserting data including none
        expected: verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             enable_dynamic_field=enable_dynamic_field,
                                                             nullable_fields={
                                                                 default_float_field_name: null_data_percent})[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f''
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           limit=pos, check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_by_nullable_field_with_none_data(self):
        """
        target: test query with nullable fields expr, insert data including none into nullable Fields
        method: query by nullable field expr after inserting data including none
        expected: verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, enable_dynamic_field=True,
                                                             nullable_fields={default_float_field_name: 0.5})[0:2]
        pos = 5
        int_values, float_values = [], []
        for vector in vectors[0]:
            int_values.append(vector[ct.default_int64_field_name])
            float_values.append(vector[default_float_field_name])
        res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
               range(pos)]

        term_expr = f'{default_float_field_name} < {pos}'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_after_none_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index,
                                                      null_data_percent):
        """
        target: test query after different index on scalar fields
        method: query after different index on nullable fields
        expected: verify query result
        """
        # 1. initialize with data
        nullable_fields = {ct.default_int32_field_name: null_data_percent,
                           ct.default_int16_field_name: null_data_percent,
                           ct.default_int8_field_name: null_data_percent,
                           ct.default_bool_field_name: null_data_percent,
                           ct.default_float_field_name: null_data_percent,
                           ct.default_double_field_name: null_data_percent,
                           ct.default_string_field_name: null_data_percent}
        # 2. create collection, insert default_nb
        collection_w, vectors = self.init_collection_general(prefix, True, 1000, is_all_data_type=True, is_index=False,
                                                             nullable_fields=nullable_fields)[0:2]
        # 3. create index on vector field and load
        index = "HNSW"
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 4. create index on scalar field with None data
        scalar_index_params = {"index_type": varchar_scalar_index, "params": {}}
        collection_w.create_index(ct.default_string_field_name, scalar_index_params)
        # 5. create index on scalar field with default data
        scalar_index_params = {"index_type": numeric_scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int32_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int16_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int8_field_name, scalar_index_params)
        if numeric_scalar_index != "STL_SORT":
            collection_w.create_index(ct.default_bool_field_name, scalar_index_params)
        collection_w.create_index(ct.default_float_field_name, scalar_index_params)
        collection_w.load()
        pos = 5
        int64_values, float_values = [], []
        scalar_fields = vectors[0]
        for i in range(pos):
            int64_values.append(scalar_fields[0][i])
            float_values.append(scalar_fields[5][i])
        res = [{ct.default_int64_field_name: int64_values[i], default_float_field_name: float_values[i]} for i in
               range(pos)]

        term_expr = f'0 <= {ct.default_int64_field_name} < {pos}'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_default_value_with_insert(self, enable_dynamic_field):
        """
        target: test query normal case with default value set
        method: create connection, collection with default value set, insert and query
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic_field,
                                                             default_value_fields={
                                                                 ct.default_float_field_name: np.float32(10.0)})[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            int_values = vectors[0][ct.default_int64_field_name].values.tolist()
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f'{ct.default_int64_field_name} in {int_values[:pos]}'
        # 2. query
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_default_value_without_insert(self, enable_dynamic_field):
        """
        target: test query normal case with default value set
        method: create connection, collection with default value set, no insert and query
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, False, enable_dynamic_field=enable_dynamic_field,
                                                             default_value_fields={
                                                                 ct.default_float_field_name: np.float32(10.0)})[0:2]

        term_expr = f'{ct.default_int64_field_name} > 0'
        # 2. query
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [], "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_after_default_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index):
        """
        target: test query after different index on default value data
        method: test query after different index on default value and corresponding search params
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        default_value_fields = {ct.default_int32_field_name: np.int32(1),
                                ct.default_int16_field_name: np.int32(2),
                                ct.default_int8_field_name: np.int32(3),
                                ct.default_bool_field_name: True,
                                ct.default_float_field_name: np.float32(10.0),
                                ct.default_double_field_name: 10.0,
                                ct.default_string_field_name: "1"}
        collection_w, vectors = self.init_collection_general(prefix, True, 1000, partition_num=1, is_all_data_type=True,
                                                             is_index=False, default_value_fields=default_value_fields)[
                                0:2]
        # 2. create index on vector field and load
        index = "HNSW"
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field with None data
        scalar_index_params = {"index_type": varchar_scalar_index, "params": {}}
        collection_w.create_index(ct.default_string_field_name, scalar_index_params)
        # 4. create index on scalar field with default data
        scalar_index_params = {"index_type": numeric_scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int32_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int16_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int8_field_name, scalar_index_params)
        if numeric_scalar_index != "STL_SORT":
            collection_w.create_index(ct.default_bool_field_name, scalar_index_params)
        collection_w.create_index(ct.default_float_field_name, scalar_index_params)
        collection_w.load()
        pos = 5
        int64_values, float_values = [], []
        scalar_fields = vectors[0]
        for i in range(pos):
            int64_values.append(scalar_fields[0][i])
            float_values.append(scalar_fields[5][i])
        res = [{ct.default_int64_field_name: int64_values[i], default_float_field_name: float_values[i]} for i in
               range(pos)]

        term_expr = f'0 <= {ct.default_int64_field_name} < {pos}'
        # 5. query
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #36003")
    def test_query_both_default_value_non_data(self, enable_dynamic_field):
        """
        target: test query normal case with default value set
        method: create connection, collection with default value set, insert and query
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic_field,
                                                             nullable_fields={ct.default_float_field_name: 1},
                                                             default_value_fields={
                                                                 ct.default_float_field_name: np.float32(10.0)})[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f'{ct.default_float_field_name} in [10.0]'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           limit=pos, check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.tags(CaseLabel.GPU)
    def test_query_after_different_index_with_params_none_default_data(self, varchar_scalar_index, numeric_scalar_index,
                                                                       null_data_percent):
        """
        target: test query after different index
        method: test query after different index on none default data
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, True, 1000, partition_num=1,
                                                             is_all_data_type=True, is_index=False,
                                                             nullable_fields={
                                                                 ct.default_string_field_name: null_data_percent},
                                                             default_value_fields={
                                                                 ct.default_float_field_name: np.float32(10.0)})[0:2]
        # 2. create index on vector field and load
        index = "HNSW"
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field with None data
        scalar_index_params = {"index_type": varchar_scalar_index, "params": {}}
        collection_w.create_index(ct.default_string_field_name, scalar_index_params)
        # 4. create index on scalar field with default data
        scalar_index_params = {"index_type": numeric_scalar_index, "params": {}}
        collection_w.create_index(ct.default_float_field_name, scalar_index_params)
        collection_w.load()
        pos = 5
        int64_values, float_values = [], []
        scalar_fields = vectors[0]
        for i in range(pos):
            int64_values.append(scalar_fields[0][i])
            float_values.append(scalar_fields[5][i])
        res = [{ct.default_int64_field_name: int64_values[i], default_float_field_name: float_values[i]} for i in
               range(pos)]

        term_expr = f'{ct.default_int64_field_name} in {int64_values[:pos]}'
        # 5. query
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_iterator_with_none_data(self, null_data_percent):
        """
        target: test query iterator normal with none data
        method: 1. query iterator
                2. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        batch_size = 100
        collection_w = self.init_collection_general(prefix, True, is_index=False,
                                                    nullable_fields={ct.default_string_field_name: null_data_percent})[
            0]
        collection_w.create_index(ct.default_float_vec_field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search iterator
        expr = "int64 >= 0"
        collection_w.query_iterator(batch_size, expr=expr,
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": ct.default_nb,
                                                 "pk_name": collection_w.primary_field.name,
                                                 "batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #36123")
    def test_query_normal_none_data_partition_key(self, enable_dynamic_field, null_data_percent):
        """
        target: test query normal case with none data inserted
        method: create connection, collection with nullable fields, insert data including none, and query
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic_field,
                                                             nullable_fields={
                                                                 ct.default_float_field_name: null_data_percent},
                                                             is_partition_key=ct.default_float_field_name)[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            int_values = vectors[0][ct.default_int64_field_name].values.tolist()
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f'{ct.default_int64_field_name} in {int_values[:pos]}'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #36538")
    def test_query_none_count(self, null_data_percent):
        """
        target: test query count(*) with None and default data
        method: 1. create a collection and 2 partitions with nullable and default value fields
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. release the collection and load one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(prefix, True, 200, partition_num=1, is_index=True,
                                                    nullable_fields={ct.default_float_field_name: null_data_percent},
                                                    default_value_fields={ct.default_string_field_name: "data"})[0]
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 200}],
                                        "pk_name": collection_w.primary_field.name})
        collection_w.release()
        partition_w1, partition_w2 = collection_w.partitions
        # load
        partition_w1.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        collection_w.release()
        # partition_w1.load()
        collection_w.load(partition_names=[partition_w1.name])
        # search on collection, partition1, partition2
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}],
                                        "pk_name": collection_w.primary_field.name})
        partition_w1.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}],
                                        "pk_name": collection_w.primary_field.name})
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        collection_w.search(vectors[:1], ct.default_float_vec_field_name, ct.default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})


class TestQueryTextMatch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test query text match
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("tokenizer", ["standard"])
    def test_query_text_match_en_normal(
            self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            assert len(res) > 0
            log.info(f"res len {len(res)}")
            for r in res:
                assert token in r[field]

            # verify inverted index
            if enable_inverted_index:
                if field == "word":
                    expr = f"{field} == '{token}'"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    log.info(f"res len {len(res)}")
                    for r in res:
                        assert r[field] == token
        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            for r in res:
                assert any([token in r[field] for token in top_10_tokens])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("lang_type", ["chinese"])
    def test_query_text_match_zh_normal(
            self, lang_type, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "type": lang_type,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if lang_type == "chinese":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        # query with blank space and punctuation marks
        for field in text_fields:
            expr = f"text_match({field}, ' ') or text_match({field}, ',') or text_match({field}, '.')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) == 0

        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            assert len(res) > 0
            log.info(f"res len {len(res)}")
            for r in res:
                assert token in r[field]

            # verify inverted index
            if enable_inverted_index:
                if field == "word":
                    expr = f"{field} == '{token}'"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    log.info(f"res len {len(res)}")
                    for r in res:
                        assert r[field] == token
        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            for r in res:
                assert any(
                    [token in r[field] for token in top_10_tokens]), f"top 10 tokens {top_10_tokens} not in {r[field]}"

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("tokenizer", ["icu"])
    def test_query_text_match_with_icu_tokenizer(
            self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match with icu tokenizer
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = ICUTextGenerator()
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents_with_analyzer_params(df[field].tolist(), analyzer_params)
        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            assert len(res) > 0
            log.info(f"res len {len(res)}")
            for r in res:
                assert token in r[field]

            # verify inverted index
            if enable_inverted_index:
                if field == "word":
                    expr = f"{field} == '{token}'"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    log.info(f"res len {len(res)}")
                    for r in res:
                        assert r[field] == token
        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            for r in res:
                assert any([token in r[field] for token in top_10_tokens])


    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("tokenizer", ["jieba", "standard"])
    def test_query_text_match_with_growing_segment(
            self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # generate growing segment
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        time.sleep(3)
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0

        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0

        # flush and then query again
        collection_w.flush()
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0


    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("lang_type", ["chinese"])
    def test_query_text_match_zh_en_mix(
            self, lang_type, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "type": lang_type,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if lang_type == "chinese":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower() + " " + fake_en.word().lower(),
                "sentence": fake.sentence().lower() + " " + fake_en.sentence().lower(),
                "paragraph": fake.paragraph().lower() + " " + fake_en.paragraph().lower(),
                "text": fake.text().lower() + " " + fake_en.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert token in r[field]

            # verify inverted index
            if enable_inverted_index:
                if field == "word":
                    expr = f"{field} == '{token}'"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    log.info(f"res len {len(res)}")
                    for r in res:
                        assert r[field] == token
        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert any(
                    [token in r[field] for token in top_10_tokens]), f"top 10 tokens {top_10_tokens} not in {r[field]}"

        # query single field for multi-word
        for field in text_fields:
            # match latest 10 most common  english words
            top_10_tokens = []
            for word, count in cf.get_top_english_tokens(wf_map[field], 10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert any(
                    [token in r[field] for token in top_10_tokens]), f"top 10 tokens {top_10_tokens} not in {r[field]}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_stop_words(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        stops_words = ["in", "of"]
        analyzer_params = {
            "tokenizer": "standard",
            "filter": [
                       {
                           "type": "stop",
                           "stop_words": stops_words,
                       }],
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence().lower() + " ".join(stops_words),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one word
        for field in text_fields:
            for token in stops_words:
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(res)}")
                assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_lowercase(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["lowercase"],
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one word
        for field in text_fields:
            tokens =[item[0] for item in wf_map[field].most_common(1)]
            for token in tokens:
                # search with Capital case
                token = token.capitalize()
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                capital_case_res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(capital_case_res)}")
                # search with lower case
                token = token.lower()
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                lower_case_res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(lower_case_res)}")

                # search with upper case
                token = token.upper()
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                upper_case_res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(upper_case_res)}")
                assert len(capital_case_res) == len(lower_case_res)  and len(capital_case_res) == len(upper_case_res)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_length_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
            "filter": [
                {
                    "type": "length",  # Specifies the filter type as length
                    "max": 10,  # Sets the maximum token length to 10 characters
                }
            ],
        }

        long_word = "a" * 11
        max_length_word = "a" * 10
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + long_word + " " + max_length_word,
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query sentence field with long word
        for field in text_fields:
            tokens =[long_word]
            for token in tokens:
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                assert len(res) == 0
        # query sentence field with max length word
        for field in text_fields:
            tokens =[max_length_word]
            for token in tokens:
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                assert len(res) == data_size


    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_stemmer_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
            "filter": [{
                "type": "stemmer",  # Specifies the filter type as stemmer
                "language": "english",  # Sets the language for stemming to English
            }]
        }
        word_pairs = {
            "play": ['play', 'plays', 'played', 'playing'],
            "book": ['book', 'books', 'booked', 'booking'],
            "study": ['study', 'studies', 'studied', 'studying'],
        }

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(word_pairs.keys()),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query sentence field with variant word
        for field in text_fields:
            for stem in word_pairs.keys():
                tokens = word_pairs[stem]
                for token in tokens:
                    expr = f"text_match({field}, '{token}')"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    pytest.assume(len(res) == data_size, f"stem {stem} token {token} not found in {res}")


    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_ascii_folding_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        from unidecode import unidecode
        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["asciifolding"],
        }

        origin_texts = [
            "Café Möller serves crème brûlée",
            "José works at Škoda in São Paulo",
            "The œuvre of Łukasz includes æsthetic pieces",
            "München's König Street has günstig prices",
            "El niño está jugando en el jardín",
            "Le système éducatif français"
        ]

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(origin_texts),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query sentence field with variant word
        for field in text_fields:
            for text in origin_texts:
                ascii_folding_text = unidecode(text)
                expr = f"""text_match({field}, "{ascii_folding_text}")"""
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                pytest.assume(len(res) == data_size, f"origin {text} ascii_folding text {ascii_folding_text} not found in {res}")

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_decompounder_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        word_list = ["dampf", "schiff", "fahrt", "brot", "backen", "automat"]
        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["lowercase",
                {
                "type": "decompounder",  # Specifies the filter type as decompounder
                "word_list": word_list,  # Sets the word list for decompounding
            }],
        }

        origin_texts = [
            "Die tägliche Dampfschifffahrt von Hamburg nach Oslo startet um sechs Uhr morgens.",
            "Unser altes Dampfschiff macht eine dreistündige Rundfahrt durch den Hafen.",
            "Der erfahrene Dampfschifffahrtskapitän kennt jede Route auf dem Fluss.",
            "Die internationale Dampfschifffahrtsgesellschaft erweitert ihre Flotte.",
            "Während der Dampfschifffahrt können Sie die Küstenlandschaft bewundern.",
            "Der neue Brotbackautomat produziert stündlich frische Brötchen.",
            "Im Maschinenraum des Dampfschiffs steht ein moderner Brotbackautomat.",
            "Die Brotbackautomatentechnologie wird ständig verbessert.",
            "Unser Brotbackautomat arbeitet mit traditionellen Rezepten.",
            "Der programmierbare Brotbackautomat bietet zwanzig verschiedene Programme.",
        ]

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(origin_texts),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        # query sentence field with word list
        for field in text_fields:
            match_text = " ".join(word_list)
            expr = f"text_match({field}, '{match_text}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            pytest.assume(len(res) == data_size, f"res len {len(res)}, data size {data_size}")

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_alphanumonly_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        common_non_ascii = [
            'é',  # common in words like café, résumé
            '©',  # copyright
            '™',  # trademark
            '®',  # registered trademark
            '°',  # degrees, e.g. 20°C
            '€',  # euro currency
            '£',  # pound sterling
            '±',  # plus-minus sign
            '→',  # right arrow
            '•'  # bullet point
        ]
        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["alphanumonly"],
        }

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(common_non_ascii),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        # query sentence field with word list
        for field in text_fields:
            match_text = " ".join(common_non_ascii)
            expr = f"text_match({field}, '{match_text}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            pytest.assume(len(res) == 0, f"res len {len(res)}, data size {data_size}")


    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_cncharonly_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        non_zh_char_word_list = ["hello", "milvus", "vector", "database", "19530"]

        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["cncharonly"],
        }

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(non_zh_char_word_list),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        # query sentence field with word list
        for field in text_fields:
            match_text = " ".join(non_zh_char_word_list)
            expr = f"text_match({field}, '{match_text}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            pytest.assume(len(res) == 0, f"res len {len(res)}, data size {data_size}")

    @pytest.mark.parametrize("dict_kind", ["ipadic", "ko-dic", "cc-cedict"])
    def test_query_text_match_with_Lindera_tokenizer(self, dict_kind):
        """
        target: test text match with lindera tokenizer
        method: 1. enable text match, use lindera tokenizer and insert data with varchar in different lang
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": {
            "type": "lindera",
            "dict_kind": dict_kind
            }
        }
        if dict_kind == "ipadic":
            fake = fake_jp
        elif dict_kind == "ko-dic":
            fake = KoreanTextGenerator()
        elif dict_kind == "cc-cedict":
            fake = fake_zh
        else:
            fake = fake_en
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        data = [
            {
                "id": i,
                "sentence": fake.sentence(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        # query sentence field with word list
        for field in text_fields:
            match_text = df["sentence"].iloc[0]
            expr = f"text_match({field}, '{match_text}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            assert len(res) > 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_with_combined_expression_for_single_field(self):
        """
        target: test query text match with combined expression for single field
        method: 1. enable text match, and insert data with varchar
                2. get the most common words and form the combined expression with and operator
                3. verify the result
        expected: query successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup and get the tf-idf, then base on it to crate expr and ground truth
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        df_new = cf.split_dataframes(df, fields=text_fields)
        log.info(f"df \n{df}")
        log.info(f"new df \n{df_new}")
        for field in text_fields:
            expr_list = []
            wf_counter = Counter(wf_map[field])
            pd_tmp_res_list = []
            for word, count in wf_counter.most_common(2):
                tmp = f"text_match({field}, '{word}')"
                log.info(f"tmp expr {tmp}")
                expr_list.append(tmp)
                tmp_res = cf.manual_check_text_match(df_new, word, field)
                log.info(f"manual check result for  {tmp} {len(tmp_res)}")
                pd_tmp_res_list.append(tmp_res)
            log.info(f"manual res {len(pd_tmp_res_list)}, {pd_tmp_res_list}")
            final_res = set(pd_tmp_res_list[0])
            for i in range(1, len(pd_tmp_res_list)):
                final_res = final_res.intersection(set(pd_tmp_res_list[i]))
            log.info(f"intersection res {len(final_res)}")
            log.info(f"final res {final_res}")
            and_expr = " and ".join(expr_list)
            log.info(f"expr: {and_expr}")
            res, _ = collection_w.query(expr=and_expr, output_fields=text_fields)
            log.info(f"res len {len(res)}, final res {len(final_res)}")
            assert len(res) == len(final_res)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_with_combined_expression_for_multi_field(self):
        """
        target: test query text match with combined expression for multi field
        method: 1. enable text match, and insert data with varchar
                2. create the combined expression with `and`, `or` and `not` operator for multi field
                3. verify the result
        expected: query successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup and get the tf-idf, then base on it to crate expr and ground truth
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        df_new = cf.split_dataframes(df, fields=text_fields)
        log.info(f"new df \n{df_new}")
        for i in range(2):
            query, text_match_expr, pandas_expr = (
                cf.generate_random_query_from_freq_dict(
                    wf_map, min_freq=3, max_terms=5, p_not=0.2
                )
            )
            log.info(f"expr: {text_match_expr}")
            res, _ = collection_w.query(expr=text_match_expr, output_fields=text_fields)
            onetime_res = res
            log.info(f"res len {len(res)}")
            step_by_step_results = []
            for expr in query:
                if isinstance(expr, dict):
                    if "not" in expr:
                        key = expr["not"]["field"]
                    else:
                        key = expr["field"]

                    tmp_expr = cf.generate_text_match_expr(expr)
                    res, _ = collection_w.query(
                        expr=tmp_expr, output_fields=text_fields
                    )
                    text_match_df = pd.DataFrame(res)
                    log.info(
                        f"text match res {len(text_match_df)}\n{text_match_df[key]}"
                    )
                    log.info(f"tmp expr {tmp_expr} {len(res)}")
                    tmp_idx = [r["id"] for r in res]
                    step_by_step_results.append(tmp_idx)
                    pandas_filter_res = cf.generate_pandas_text_match_result(
                        expr, df_new
                    )
                    tmp_pd_idx = pandas_filter_res["id"].tolist()
                    diff_id = set(tmp_pd_idx).union(set(tmp_idx)) - set(
                        tmp_pd_idx
                    ).intersection(set(tmp_idx))
                    log.info(f"diff between text match and manual check {diff_id}")
                    assert len(diff_id) == 0
                    for idx in diff_id:
                        log.info(df[df["id"] == idx][key].values)
                    log.info(
                        f"pandas_filter_res {len(pandas_filter_res)} \n {pandas_filter_res}"
                    )
                if isinstance(expr, str):
                    step_by_step_results.append(expr)
            final_res = cf.evaluate_expression(step_by_step_results)
            log.info(f"one time res {len(onetime_res)}, final res {len(final_res)}")
            if len(onetime_res) != len(final_res):
                log.info("res is not same")
                assert False

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_text_match_with_multi_lang(self):
        """
        target: test text match with multi-language text data
        method: 1. enable text match, and insert data with varchar in different language
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """

        # 1. initialize with data
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data_en = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2)
        ]
        fake = fake_de
        data_de = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2, data_size)
        ]
        data = data_en + data_de
        df = pd.DataFrame(data)
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup and get the tf-idf, then base on it to crate expr and ground truth
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        df_new = cf.split_dataframes(df, fields=text_fields)
        log.info(f"new df \n{df_new}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # query single field for one word
        for field in text_fields:
            token = wf_map[field].most_common()[-1][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert token in r[field]

        # query single field for multi-word
        for field in text_fields:
            # match top 3 most common words
            multi_words = []
            for word, count in wf_map[field].most_common(3):
                multi_words.append(word)
            string_of_multi_words = " ".join(multi_words)
            expr = f"text_match({field}, '{string_of_multi_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert any([token in r[field] for token in multi_words])

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_text_match_with_addition_inverted_index(self):
        """
        target: test text match with addition inverted index
        method: 1. enable text match, and insert data with varchar
                2. create inverted index
                3. get the most common words and query with text match
                4. query with inverted index and verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        # 1. initialize with data
        fake_en = Faker("en_US")
        analyzer_params = {
            "tokenizer": "standard",
        }
        dim = 128
        default_fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        default_schema = CollectionSchema(
            fields=default_fields, description="test collection"
        )

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=default_schema
        )
        data = []
        data_size = 10000
        for i in range(data_size):
            d = {
                "id": i,
                "word": fake_en.word().lower(),
                "sentence": fake_en.sentence().lower(),
                "paragraph": fake_en.paragraph().lower(),
                "text": fake_en.text().lower(),
                "emb": cf.gen_vectors(1, dim)[0],
            }
            data.append(d)
        batch_size = 5000
        for i in range(0, data_size, batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < data_size
                else data[i:data_size]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        df = pd.DataFrame(data)
        df_split = cf.split_dataframes(df, fields=["word", "sentence", "paragraph", "text"])
        log.info(f"dataframe\n{df}")
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language="en")
        # query single field for one word
        for field in text_fields:
            token = wf_map[field].most_common()[-1][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            pandas_res = df_split[df_split.apply(lambda row: token in row[field], axis=1)]
            log.info(f"res len {len(res)}, pandas res len {len(pandas_res)}")
            log.info(f"pandas res\n{pandas_res}")
            assert len(res) == len(pandas_res)
            log.info(f"res len {len(res)}")
            for r in res:
                assert token in r[field]
            if field == "word":
                assert len(res) == wf_map[field].most_common()[-1][1]
                expr = f"{field} == '{token}'"
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(res)}")
                assert len(res) == wf_map[field].most_common()[-1][1]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("combine_op", ["and", "or"])
    def test_query_text_match_with_non_varchar_fields_expr(self, combine_op):
        """
        target: test text match with non-varchar fields expr
        method: 1. enable text match for varchar field and add some non varchar fields
                2. insert data, create index and load
                3. query with text match expr and non-varchar fields expr
                4. verify the result
        expected: query result is correct
        """
        # 1. initialize with data
        fake_en = Faker("en_US")
        analyzer_params = {
            "tokenizer": "standard",
        }
        dim = 128
        default_fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="age",
                dtype=DataType.INT64,
            ),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        default_schema = CollectionSchema(
            fields=default_fields, description="test collection"
        )

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=default_schema
        )
        data = []
        data_size = 10000
        for i in range(data_size):
            d = {
                "id": i,
                "age": random.randint(1, 100),
                "word": fake_en.word().lower(),
                "sentence": fake_en.sentence().lower(),
                "paragraph": fake_en.paragraph().lower(),
                "text": fake_en.text().lower(),
                "emb": cf.gen_vectors(1, dim)[0],
            }
            data.append(d)
        batch_size = 5000
        for i in range(0, data_size, batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < data_size
                else data[i:data_size]
            )
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language="en")
        # query single field for one word
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            tm_expr = f"text_match({field}, '{token}')"
            int_expr = "age > 10"
            combined_expr = f"{tm_expr} {combine_op} {int_expr}"
            log.info(f"expr: {combined_expr}")
            res, _ = collection_w.query(expr=combined_expr, output_fields=["id", field, "age"])
            log.info(f"res len {len(res)}")
            for r in res:
                if combine_op == "and":
                    assert token in r[field] and r["age"] > 10
                if combine_op == "or":
                    assert token in r[field] or r["age"] > 10

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_text_match_with_some_empty_string(self):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar with some empty string
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        # 1. initialize with data
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data_en = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2)
        ]
        data_empty = [
            {
                "id": i,
                "word": "",
                "sentence": " ",
                "paragraph": "",
                "text": " ",
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2, data_size)
        ]
        data = data_en + data_empty
        df = pd.DataFrame(data)
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup and get the tf-idf, then base on it to crate expr and ground truth
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        df_new = cf.split_dataframes(df, fields=text_fields)
        log.info(f"new df \n{df_new}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # query single field for one word
        for field in text_fields:
            token = wf_map[field].most_common()[-1][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert token in r[field]
        # query single field for multi-word
        for field in text_fields:
            # match top 3 most common words
            multi_words = []
            for word, count in wf_map[field].most_common(3):
                multi_words.append(word)
            string_of_multi_words = " ".join(multi_words)
            expr = f"text_match({field}, '{string_of_multi_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert any([token in r[field] for token in multi_words])

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_text_match_with_nullable(self):
        """
        target: test text match with nullable
        method: 1. enable text match and nullable, and insert data with varchar with some None value
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        # 1. initialize with data
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
                nullable=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
                nullable=True,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
                nullable=True,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
                nullable=True,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data_null = [
            {
                "id": i,
                "word": None if random.random() < 0.9 else fake.word().lower(),
                "sentence": None if random.random() < 0.9 else fake.sentence().lower(),
                "paragraph": None if random.random() < 0.9 else fake.paragraph().lower(),
                "text": None if random.random() < 0.9 else fake.paragraph().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(0, data_size)
        ]
        data = data_null
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i:i + batch_size]
                if i + batch_size < len(df)
                else data[i:len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one word
        for field in text_fields:
            token = wf_map[field].most_common()[-1][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=text_fields)
            log.info(f"res len {len(res)}, \n{res}")
            assert len(res) > 0
            for r in res:
                assert token in r[field]
        # query single field for multi-word
        for field in text_fields:
            # match top 3 most common words
            multi_words = []
            for word, count in wf_map[field].most_common(3):
                multi_words.append(word)
            string_of_multi_words = " ".join(multi_words)
            expr = f"text_match({field}, '{string_of_multi_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=text_fields)
            log.info(f"res len {len(res)}, {res}")
            assert len(res) > 0
            for r in res:
                assert any([token in r[field] for token in multi_words])


class TestQueryCompareTwoColumn(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_query_compare_two_column_with_last_expr_skip(self):
        """
        target: test query compare two column with last expr skip
        method: 1. create collection and insert data
                2. query with two column compare expr with last expr help to skip compare
        expected: query successfully
        """
        fields = [
            FieldSchema(name="f1", dtype=DataType.INT64, is_primary=True, nullable=False),
            FieldSchema(name="f2", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="f3", dtype=DataType.INT64, is_primary=False, nullable=False),
            FieldSchema(name="f4", dtype=DataType.INT64, is_primary=False, nullable=False),
        ]
        schema = CollectionSchema(fields=fields)
        col_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(
            name=col_name,
            schema=schema,
        )
        collection_w.create_index("f3", {"index_type": "INVERTED"})
        collection_w.create_index("f2", {"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "IP"})
        for i in range(100):
            collection_w.insert({"f1": i, "f2": {10: 0.07638, 3: 0.3925}, "f3": 4, "f4": 2})
        collection_w.flush()
        collection_w.load()
        # f4 / 4 > 1 always false, so f3 < f4 will not be executed
        res = collection_w.query(expr="f4 / 4 > 1 and f3 < f4", output_fields=["count(*)"])[0]
        assert res[0]['count(*)'] == 0
        collection_w.drop()


class TestQueryTextMatchNegative(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_with_unsupported_tokenizer(self):
        """
        target: test query text match with unsupported tokenizer
        method: 1. enable text match, and use unsupported tokenizer
                2. create collection
        expected: create collection failed and return error
        """
        analyzer_params = {
            "tokenizer": "Unsupported",
        }
        dim = 128
        default_fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="title",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="overview",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="genres",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="producer",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="cast",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        default_schema = CollectionSchema(
            fields=default_fields, description="test collection"
        )
        error = {ct.err_code: 2000, ct.err_msg: "unsupported tokenizer"}
        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=default_schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )


class TestQueryFunction(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_query_function_calls(self):
        """
        target: test query data
        method: create collection and insert data
                query with mix call expr in string field and int field
        expected: query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             primary_field=ct.default_string_field_name)[0:2]
        res = vectors[0].iloc[:, 1:3].to_dict('records')
        output_fields = [default_float_field_name, default_string_field_name]
        for mixed_call_expr in [
            "not empty(varchar) && int64 >= 0",
            # function call is case-insensitive
            "not EmPty(varchar) && int64 >= 0",
            "not EMPTY(varchar) && int64 >= 0",
            "starts_with(varchar, varchar) && int64 >= 0",
        ]:
            collection_w.query(
                mixed_call_expr,
                output_fields=output_fields,
                check_task=CheckTasks.check_query_results,
                check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_invalid(self):
        """
        target: test query with invalid call expression
        method: query with invalid call expr
        expected: raise exception
        """
        collection_w, entities = self.init_collection_general(
            prefix, insert_data=True, nb=10)[0:2]
        test_cases = [
            (
                "A_FUNCTION_THAT_DOES_NOT_EXIST()".lower(),
                "function A_FUNCTION_THAT_DOES_NOT_EXIST() not found".lower(),
            ),
            # empty
            ("empty()", "function empty() not found"),
            (f"empty({default_int_field_name})", "function empty(int64_t) not found"),
            # starts_with
            (f"starts_with({default_int_field_name})", "function starts_with(int64_t) not found"),
            (f"starts_with({default_int_field_name}, {default_int_field_name})",
             "function starts_with(int64_t, int64_t) not found"),
        ]
        for call_expr, err_msg in test_cases:
            error = {ct.err_code: 65535, ct.err_msg: err_msg}
            collection_w.query(
                call_expr, check_task=CheckTasks.err_res, check_items=error
            )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue 36685")
    def test_query_text_match_with_unsupported_fields(self):
        """
        target: test enable text match with unsupported field
        method: 1. enable text match in unsupported field
                2. create collection
        expected: create collection failed and return error
        """
        analyzer_params = {
            "tokenizer": "standard",
        }
        dim = 128
        default_fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="title",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="overview",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="age",
                dtype=DataType.INT64,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        default_schema = CollectionSchema(
            fields=default_fields, description="test collection"
        )
        error = {ct.err_code: 2000, ct.err_msg: "field type is not supported"}
        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=default_schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )
