import random
import time
import pandas as pd
import numpy as np
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from common.common_type import CaseLabel, CheckTasks
from pymilvus.grpc_gen.common_pb2 import SegmentState

prefix = "delete"
half_nb = ct.default_nb // 2
tmp_nb = 3000
tmp_expr = f'{ct.default_int64_field_name} in {[0]}'
query_res_tmp_expr = [{f'{ct.default_int64_field_name}': 0}]
query_tmp_expr_str = [{f'{ct.default_string_field_name}': "0"}]
exp_res = "exp_res"
default_string_expr = "varchar in [ \"0\"]"
default_invalid_string_exp = "varchar >= 0"
index_name1 = cf.gen_unique_str("float")
index_name2 = cf.gen_unique_str("varchar")
default_search_params = ct.default_search_params


class TestDeleteParams(TestcaseBase):
    """
    Test case of delete interface
    def delete(expr, partition_name=None, timeout=None, **kwargs)
    return MutationResult
    Only the `in` operator is supported in the expr
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize('is_binary', [False, True])
    def test_delete_entities(self, is_binary):
        """
        target: test delete data from collection
        method: 1.create and insert nb with flush
                2.load collection
                3.delete half of nb
                4.query with deleted ids
        expected: Query result is empty
        """
        # init collection with default_nb default data
        collection_w, _, _, ids = self.init_collection_general(prefix, insert_data=True, is_binary=is_binary)[0:4]
        expr = f'{ct.default_int64_field_name} in {ids[:half_nb]}'

        # delete half of data
        del_res = collection_w.delete(expr)[0]
        assert del_res.delete_count == half_nb
        # This flush will not persist the deleted ids, just delay the time to ensure that queryNode consumes deleteMsg
        collection_w.num_entities

        # query with deleted ids
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize('is_binary', [False, True])
    def test_delete_entities_with_range(self, is_binary):
        """
        target: test delete data from collection
        method: 1.create and insert nb with flush
                2.load collection
                3.delete half of nb
                4.query with deleted ids
        expected: Query result is empty
        """
        # init collection with default_nb default data
        collection_w, _, _, ids = self.init_collection_general(prefix, insert_data=True, auto_id=True, is_binary=is_binary)[0:4]
        expr = f'{ct.default_int64_field_name} < {ids[half_nb]}'

        # delete half of data
        del_res = collection_w.delete(expr)[0]
        assert del_res.delete_count == half_nb
        # This flush will not persist the deleted ids, just delay the time to ensure that queryNode consumes deleteMsg
        collection_w.num_entities

        # query with deleted ids
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_without_connection(self):
        """
        target: test delete without connect
        method: delete after remove connection
        expected: raise exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]

        # remove connection and delete
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: "should create connect first"}
        collection_w.delete(expr=tmp_expr, check_task=CheckTasks.err_res, check_items=error)

    # Not Milvus Exception
    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_expr_none(self):
        """
        target: test delete with None expr
        method: delete with None expr
        expected: raise exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        error = {ct.err_code: 0, ct.err_msg: "expr cannot be None"}
        collection_w.delete(expr=None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr", [1, [], ()])
    def test_delete_expr_non_string(self, expr):
        """
        target: test delete with non-string expression
        method: delete with non-string expr
        expected: raise exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        error = {ct.err_code: 0, ct.err_msg: f"expr value {expr} is illegal"}
        collection_w.delete(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr", ["12-s", "中文"])
    def test_delete_invalid_expr_string(self, expr):
        """
        target: test delete with invalid string expr
        method: delete with invalid string
        expected: Raise exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        error = {ct.err_code: 1,
                 ct.err_msg: f"failed to create expr plan, expr = {expr}"}
        collection_w.delete(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_expr_empty_value(self):
        """
        target: test delete with empty array expr
        method: delete with expr: "id in []"
        expected: assert num entities
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        expr = f'{ct.default_int64_field_name} in {[]}'

        # delete empty entities
        collection_w.delete(expr)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_expr_single(self):
        """
        target: test delete with one value
        method: delete with expr: "id in [0]"
        expected: Describe num entities by one
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        expr = f'{ct.default_int64_field_name} in {[0]}'
        del_res, _ = collection_w.delete(expr)
        assert del_res.delete_count == 1
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_expr_all_values(self):
        """
        target: test delete with all values
        method: delete with expr: "id in [all]"
        expected: num entities unchanged and deleted data will not be queried
        """
        # init collection with default_nb default data
        collection_w, _, _, ids = self.init_collection_general(prefix, insert_data=True)[0:4]
        expr = f'{ct.default_int64_field_name} in {ids}'
        del_res, _ = collection_w.delete(expr)

        # assert results
        assert del_res.delete_count == ct.default_nb
        assert collection_w.num_entities == ct.default_nb

        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_expr_with_vector(self):
        """
        target: test delete with vector field
        method: delete with expr vector field
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True,
                                                    is_all_data_type=True, is_index=True)[0]
        expr = f"{ct.default_float_vec_field_name} in [[0.1]]"
        error = {ct.err_code: 1,
                 ct.err_msg: f"failed to create expr plan, expr = {expr}"}

        collection_w.delete(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_not_existed_values(self):
        """
        target: test delete not existed values
        method: delete data not in the collection
        expected: No exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]

        # No exception
        expr = f'{ct.default_int64_field_name} in {[tmp_nb]}'
        collection_w.delete(expr=expr)
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_results,
                           check_items={exp_res: query_res_tmp_expr})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_part_not_existed_values(self):
        """
        target: test delete part non-existed values
        method: delete ids which part not existed
        expected: delete existed id, ignore non-existed id
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        expr = f'{ct.default_int64_field_name} in {[0, tmp_nb]}'
        collection_w.delete(expr=expr)
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_expr_inconsistent_values(self):
        """
        target: test delete with inconsistent type values
        method: delete with non-int64 type values
        expected: raise exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        expr = f'{ct.default_int64_field_name} in {[0.0, 1.0]}'

        # Bad exception message
        error = {ct.err_code: 1, ct.err_msg: "failed to create expr plan,"}
        collection_w.delete(expr=expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_expr_mix_values(self):
        """
        target: test delete with mix type values
        method: delete with int64 and float values
        expected: raise exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        expr = f'{ct.default_int64_field_name} in {[0, 1.0]}'

        # Bad exception message
        error = {ct.err_code: 1, ct.err_msg: "failed to create expr plan"}
        collection_w.delete(expr=expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_delete_partition(self):
        """
        target: test delete from partition
        method: delete with partition names
        expected: verify partition entities are deleted
        """
        # init collection and partition
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)

        # load collection and insert data to partition
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        df = cf.gen_default_dataframe_data(tmp_nb)
        partition_w.insert(df)

        # delete ids from partition
        del_res, _ = collection_w.delete(tmp_expr, partition_name=partition_w.name)
        assert del_res.delete_count == 1

        # query with deleted id and query with existed id
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty, partition_names=[partition_w.name])
        res = df.iloc[1:2, :1].to_dict('records')
        collection_w.query(f'{ct.default_int64_field_name} in [1]',
                           check_task=CheckTasks.check_query_results, check_items={exp_res: res})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_default_partition(self):
        """
        target: test delete from default partition
        method: delete with partition name "_default"
        expected: assert delete successfully
        """
        # create, insert with flush, load collection
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        del_res, _ = collection_w.delete(tmp_expr, partition_name=ct.default_partition_name)
        assert del_res.delete_count == 1
        collection_w.num_entities
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.parametrize("partition_name", [1, [], {}, ()])
    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_non_string_partition_name(self, partition_name):
        """
        target: test delete with non-string partition name
        method: delete with non-string partition name
        expected: Raise exception
        """
        # create, insert with flush, load collection
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]

        error = {ct.err_code: 0,
                 ct.err_msg: f"partition_name value {partition_name} is illegal"}
        collection_w.delete(tmp_expr, partition_name=partition_name,
                            check_task=CheckTasks.err_res, check_items=error)


class TestDeleteOperation(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test delete interface operations
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_from_empty_collection(self):
        """
        target: test delete entities from an empty collection
        method: create a collection and delete entities
        expected: No exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.delete(tmp_expr)[0]
        # todo assert del_res.delete_count == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_entities_repeatedly(self):
        """
        target: test delete entities twice
        method: delete with same expr twice
        expected: No exception for second deletion
        """
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]

        # assert delete successfully and no exception
        collection_w.delete(expr=tmp_expr)
        collection_w.num_entities
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)
        collection_w.delete(expr=tmp_expr)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_after_index(self):
        """
        target: test delete after creating index
        method: 1.insert, flush, load
                2.create index and re-load
                3.delete entities
                4.search
        expected: assert index and deleted id not in search result
        """
        # create collection, insert tmp_nb, flush and load
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False)[0:2]

        # create index
        index_params = {"index_type": "IVF_SQ8",
                        "metric_type": "COSINE", "params": {"nlist": 64}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params)
        assert collection_w.has_index()[0]
        collection_w.release()
        collection_w.load()
        # delete entity
        collection_w.delete(tmp_expr)
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)
        assert collection_w.has_index()[0]

        # search with id 0 vectors
        search_res, _ = collection_w.search([vectors[0][ct.default_float_vec_field_name][0]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        assert 0 not in search_res[0].ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_and_index(self):
        """
        target: test delete and create index
        method: 1.insert
                2.delete half
                3.flush and create index
                4.search
        expected: Empty search result
        """
        # init collection and insert data without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)

        # delete half and flush
        expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys[:ct.default_nb // 2]}'
        del_res, _ = collection_w.delete(expr)
        assert collection_w.num_entities in [ct.default_nb, ct.default_nb // 2]

        # create index
        index_params = {"index_type": "IVF_SQ8",
                        "metric_type": "COSINE", "params": {"nlist": 64}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params)
        assert collection_w.has_index()[0]

        collection_w.load()
        search_res, _ = collection_w.search([df[ct.default_float_vec_field_name][0]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        log.debug(search_res[0].ids)
        # assert search results not contains deleted ids
        inter = set(insert_res.primary_keys[:ct.default_nb // 2]).intersection(set(search_res[0].ids))
        log.debug(inter)
        assert len(inter) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_query_ids_both_sealed_and_channel(self):
        """
        target: test query that delete ids from both channel and sealed
        method: 1.create and insert
                2.delete id 0 and flush
                3.load and query id 0
                4.insert new id and delete the id
                5.query id 0 and new id
        expected: Empty query result
        """
        # init collection and insert data without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # delete id 0 and flush
        del_res, _ = collection_w.delete(tmp_expr)
        assert del_res.delete_count == 1
        assert collection_w.num_entities == tmp_nb

        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

        # insert id tmp_nb and delete id 0 and tmp_nb
        df_new = cf.gen_default_dataframe_data(nb=1, start=tmp_nb)
        collection_w.insert(df_new)
        collection_w.delete(expr=f'{ct.default_int64_field_name} in {[tmp_nb]}')

        # query with id 0 and tmp_nb
        collection_w.query(expr=f'{ct.default_int64_field_name} in {[0, tmp_nb]}',
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_search(self):
        """
        target: test delete and search
        method: search entities after it was deleted
        expected: deleted entity is not in the search result
        """
        # init collection with nb default data
        collection_w, _, _, ids = self.init_collection_general(prefix, insert_data=True)[0:4]
        entity, _ = collection_w.query(tmp_expr, output_fields=["*"])
        search_res, _ = collection_w.search([entity[0][ct.default_float_vec_field_name]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        # assert search results contains entity
        assert 0 in search_res[0].ids

        expr = f'{ct.default_int64_field_name} in {ids[:ct.default_nb // 2]}'
        collection_w.delete(expr)
        search_res_2, _ = collection_w.search([entity[0][ct.default_float_vec_field_name]],
                                              ct.default_float_vec_field_name,
                                              ct.default_search_params, ct.default_limit)
        # assert search result is not equal to entity
        log.debug(f"Second search result ids: {search_res_2[0].ids}")
        inter = set(ids[:ct.default_nb // 2]
                    ).intersection(set(search_res_2[0].ids))
        # Using bounded staleness, we could still search the "deleted" entities,
        # since the search requests arrived query nodes earlier than query nodes consume the delete requests.
        assert len(inter) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_search_rename_collection(self):
        """
        target: test delete and search in the renamed collection
        method: search entities after it was deleted
        expected: deleted entity is not in the search result
        """
        # init collection with nb default data
        collection_w, _, _, ids = self.init_collection_general(prefix, insert_data=True)[0:4]
        entity, _ = collection_w.query(tmp_expr, output_fields=[ct.default_float_vec_field_name])
        search_res, _ = collection_w.search([entity[0][ct.default_float_vec_field_name]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        # assert search results contains entity
        assert 0 in search_res[0].ids
        # rename collection
        old_collection_name = collection_w.name
        new_collection_name = cf.gen_unique_str(prefix + "new")
        self.utility_wrap.rename_collection(old_collection_name, new_collection_name)
        collection_w = self.init_collection_wrap(name=new_collection_name)
        # delete entities
        expr = f'{ct.default_int64_field_name} in {ids[:ct.default_nb // 2]}'
        collection_w.delete(expr)
        search_res_2, _ = collection_w.search([entity[0][ct.default_float_vec_field_name]],
                                              ct.default_float_vec_field_name,
                                              ct.default_search_params, ct.default_limit)
        # assert search result is not equal to entity
        log.debug(f"Second search result ids: {search_res_2[0].ids}")
        inter = set(ids[:ct.default_nb // 2]
                    ).intersection(set(search_res_2[0].ids))
        # Using bounded staleness, we could still search the "deleted" entities,
        # since the search requests arrived query nodes earlier than query nodes consume the delete requests.
        assert len(inter) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_expr_repeated_values(self):
        """
        target: test delete with repeated values
        method: 1.insert data with unique primary keys
                2.delete with repeated values: 'id in [0, 0]'
        expected: delete one entity
        """
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        expr = f'{ct.default_int64_field_name} in {[0, 0, 0]}'
        del_res, _ = collection_w.delete(expr)
        assert del_res.delete_count == 3
        collection_w.num_entities
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_duplicate_primary_keys(self):
        """
        target: test delete from duplicate primary keys
        method: 1.insert data with dup ids
                2.delete with repeated or not values
        expected: currently only delete one entity, query get one entity
        todo delete all entities
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=tmp_nb)
        df[ct.default_int64_field_name] = 0
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb
        del_res, _ = collection_w.delete(tmp_expr)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # Just one query res and search res, because de-dup
        res, _ = collection_w.query(tmp_expr, output_fields=["*"])
        assert len(res) == 0

        search_res, _ = collection_w.search([df[ct.default_float_vec_field_name][1]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit,
                                            output_fields=[ct.default_int64_field_name, ct.default_float_field_name])
        assert len(search_res) == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_empty_partition(self):
        """
        target: test delete empty partition
        method: delete from an empty partition
        expected: No exception
        """
        # init collection and partition
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)

        collection_w.delete(tmp_expr, partition_name=partition_w.name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_not_existed_partition(self):
        """
        target: test delete from an not existed partition
        method: delete from an fake partition
        expected: raise exception
        """
        # init collection with tmp_nb data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]

        # raise exception
        error = {ct.err_code: 200,
                 ct.err_msg: f"Failed to get partition id: partition={ct.default_tag}: partition not found"}
        collection_w.delete(tmp_expr, partition_name=ct.default_tag,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_from_partition_with_another_ids(self):
        """
        target: delete another partition entities from partition
        method: 1.insert nb entities into two partitions in half
                2.delete entities from partition_1 with partition_2 values
                3.delete entities from partition_1 with partition_1 values
        expected: Entities in partition_1 will be deleted
        """
        half = tmp_nb // 2
        # create, insert, flush, load
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)

        # delete entities from another partition
        expr = f'{ct.default_int64_field_name} in {[0]}'
        collection_w.delete(expr, partition_name=ct.default_partition_name)
        collection_w.query(expr, check_task=CheckTasks.check_query_results, check_items={
                           exp_res: query_res_tmp_expr})

        # delete entities from own partition
        collection_w.delete(expr, partition_name=partition_w.name)
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_from_partitions_with_same_ids(self):
        """
        target: test delete same ids from two partitions with same data
        method: 1.insert same nb data into two partitions
                2.delete same ids from partition_1
        expected: The data only in partition_1 will be deleted
        """
        # init collection and partition
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)

        # insert same data into partition_w and default partition
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        partition_w.insert(df)

        # delete same id 0 from default_partition, and query on it get empty result
        collection_w.delete(tmp_expr, partition_name=ct.default_partition_name)
        assert collection_w.num_entities == tmp_nb * 2
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(tmp_expr, partition_names=[ct.default_partition_name],
                           check_task=CheckTasks.check_query_empty)

        # query on partition_w with id 0 and get an result
        collection_w.query(tmp_expr, partition_names=[partition_w.name],
                           check_task=CheckTasks.check_query_results, check_items={exp_res: query_res_tmp_expr})

    @pytest.mark.tags(CaseLabel.L0)
    def test_delete_auto_id_collection(self):
        """
        target: test delete from auto_id collection
        method: delete entities from auto_id=true collection
        expected: versify delete successfully
        """
        # init an auto_id collection and insert tmp_nb data
        collection_w, _, _, ids = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True, auto_id=True)[0:4]

        # delete with insert ids
        expr = f'{ct.default_int64_field_name} in {[ids[0]]}'
        res, _ = collection_w.delete(expr)

        # verify delete result
        assert res.delete_count == 1
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_query_without_loading(self):
        """
        target: test delete and query without loading
        method: 1.insert and flush data
                2.delete ids
                3.query without loading
        expected: Raise exception
        """
        # create collection, insert data without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb

        # delete
        res = collection_w.delete(tmp_expr)[0]
        assert res.delete_count == 1

        # query without loading and raise exception
        error = {ct.err_code: 65535, ct.err_msg: "collection not loaded"}
        collection_w.query(expr=tmp_expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_sealed_segment_without_flush(self):
        """
        target: test delete without flush
        method: 1.insert and flush data
                2.delete ids from collection and no flush
                3.load and query with id
        expected: No query result
        """
        # create collection, insert data without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb

        # delete
        del_res, _ = collection_w.delete(tmp_expr)
        assert del_res.delete_count == 1

        # load and query with id
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_growing_data_channel_delete(self):
        """
        target: test delete entities from growing segment, and channel deleteMsg
        method: 1.create collection
                2.load collection
                3.insert data and delete ids
                4.query deleted ids
        expected: No query result
        """
        # create collection
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        # load collection and the queryNode watch the insertChannel
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # insert data
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        # delete id 0
        del_res = collection_w.delete(tmp_expr)[0]
        assert del_res.delete_count == 1
        # query id 0
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_sealed_data_channel_delete(self):
        """
        target: test delete sealed data and get deleteMsg from insertChannel
        method: 1.create, insert and flush data
                2.load collection
                3.delete id without flush
                4.query deleted ids (queryNode get deleted ids from channel not persistence)
        expected: Delete successfully and no query result
        """
        # create collection and insert flush data
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb

        # load collection and queryNode subscribe channel
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # delete ids and query
        collection_w.delete(tmp_expr)
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_sealed_segment_with_flush(self):
        """
        target: test delete data from sealed segment and flush delta log
        method: 1.create and insert and flush data
                2.delete entities and flush (insert and flush)
                3.load collection (load data and delta log)
                4.query deleted ids
        expected: No query result
        """
        # create collection
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        # insert and flush data
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        # assert collection_w.num_entities == tmp_nb

        # delete id 0
        del_res = collection_w.delete(tmp_expr)[0]
        assert del_res.delete_count == 1

        # insert data and flush data and delete ids.
        # if no insert, datanode will not really flush delete ids
        collection_w.insert(cf.gen_default_dataframe_data(nb=1, start=tmp_nb))
        log.info(f'Collection num entities: {collection_w.num_entities}')

        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_sealed_segment_with_twice_flush(self):
        """
        target: test delete data from sealed segment and flush delta log
        method: 1.create and insert and flush data
                2.delete entities and flush (insert and flush)
                3.load collection (load data and delta log)
                4.query deleted ids
        expected: No query result
        """
        # create collection
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        # insert and flush data
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb

        # delete id 0 and flush
        del_res = collection_w.delete(tmp_expr)[0]
        assert del_res.delete_count == 1
        collection_w.insert(cf.gen_default_dataframe_data(nb=1, start=tmp_nb))
        log.info(collection_w.num_entities)
        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_sealed_data_sealed_delete(self):
        """
        target: test delete with sealed data and sealed delete request
        method: 1.create, insert
                2.delete and flush (will flush data and delete)
                3.load and query
        expected: Empty query result
        """
        # create collection
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        # insert without flush
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # delete id 0 and flush
        del_res = collection_w.delete(tmp_expr)[0]
        assert del_res.delete_count == 1
        assert collection_w.num_entities == tmp_nb

        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("to_query", [True, False])
    @pytest.mark.parametrize("to_flush", [True, False])
    def test_delete_insert_same_id_growing(self, to_query, to_flush):
        """
        target: test insert same id entity after delete from growing data
        method: 1.create and load
                2.insert entities and no flush
                3.delete id 0 entity
                4.insert new entity with same id
                5.query with the id
        expected: Verify that the query gets the newly inserted entity
        """
        # init collection and insert data without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # delete
        del_expr = f"{ct.default_int64_field_name} in [0, 1, 3, 5]"
        del_res, _ = collection_w.delete(del_expr)
        log.debug(f'to_query:{to_query}')
        if to_query:
            collection_w.query(del_expr, check_task=CheckTasks.check_query_empty)

        # insert entity with primary key 0, new scalar field data, new vector data
        df_new = cf.gen_default_dataframe_data(4, start=tmp_nb)
        df_new[ct.default_int64_field_name] = [0, 1, 3, 5]
        collection_w.insert(df_new)
        log.debug(f'to_flush:{to_flush}')
        if to_flush:
            log.debug(collection_w.num_entities)

        # query entity
        res = df_new.iloc[:, [0, 1, -1]].to_dict('records')
        collection_w.query(del_expr, output_fields=[ct.default_float_vec_field_name, ct.default_float_field_name],
                           check_task=CheckTasks.check_query_results, check_items={'exp_res': res, 'with_vec': True})
        search_res, _ = collection_w.search(data=[df_new[ct.default_float_vec_field_name][0]],
                                            anns_field=ct.default_float_vec_field_name,
                                            param=default_search_params, limit=1)
        assert search_res[0][0].id == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("to_query", [True, False])
    def test_delete_insert_same_id_sealed(self, to_query):
        """
        target: test insert same id entity after delete from sealed data
        method: 1.create and insert with flush
                2.load and query with the id
                3.delte the id entity
                4.insert new entity with the same id and flush
                5.query the id
        expected: Verify that the query gets the newly inserted entity
        """
        # init collection and insert data without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # insert
        nb = 1000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        log.debug(collection_w.num_entities)

        # load and query
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        res = df.iloc[:1, :1].to_dict('records')
        collection_w.search(data=[df[ct.default_float_vec_field_name][0]], anns_field=ct.default_float_vec_field_name,
                            param=default_search_params, limit=1)
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_results, check_items={'exp_res': res})

        # delete
        collection_w.delete(tmp_expr)
        if to_query:
            collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

        # re-insert same pk, new scalar field data, new vector data
        df_new = cf.gen_default_dataframe_data(nb=1, start=nb)
        df_new[ct.default_int64_field_name] = [0]
        collection_w.insert(df_new)
        log.debug(collection_w.num_entities)

        # re-query
        res = df_new.iloc[[0], [0, 1, -1]].to_dict('records')
        collection_w.query(tmp_expr, output_fields=[ct.default_float_vec_field_name, ct.default_float_field_name],
                           check_task=CheckTasks.check_query_results, check_items={'exp_res': res, 'with_vec': True})
        search_res, _ = collection_w.search(data=[df_new[ct.default_float_vec_field_name][0]],
                                            anns_field=ct.default_float_vec_field_name,
                                            param=default_search_params, limit=1)
        assert search_res[0][0].id == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_entity_loop(self):
        """
        target: test delete all entities one by one in a loop
        method: delete data one by one for a loop
        expected: No exception
        """
        # init an auto_id collection and insert tmp_nb data, flush and load
        collection_w, _, _, ids = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True, auto_id=True)[0:4]

        for del_id in ids:
            expr = f'{ct.default_int64_field_name} in {[del_id]}'
            res = collection_w.delete(expr)[0]
            assert res.delete_count == 1

        # query with all ids
        expr = f'{ct.default_int64_field_name} in {ids}'
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_flush_loop(self):
        """
        target: test delete and flush in a loop
        method: in a loop, delete batch and flush, until delete all entities
        expected: No exception
        """
        # init an auto_id collection and insert tmp_nb data
        collection_w, _, _, ids = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True, auto_id=True)[0:4]

        batch = 10
        for i in range(tmp_nb // batch):
            expr = f'{ct.default_int64_field_name} in {ids[i * batch: (i + 1) * batch]}'
            res, _ = collection_w.delete(expr)
            assert res.delete_count == batch

        # query with all ids
        expr = f'{ct.default_int64_field_name} in {ids}'
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("to_flush_data", [True, False])
    @pytest.mark.parametrize("to_flush_delete", [True, False])
    def test_delete_merge_same_id_channel_and_sealed(self, to_flush_data, to_flush_delete):
        """
        target: test merge same delete ids from channel and sealed
        method: 1.create, insert
                2.delete id and flush (data and deleted become sealed)
                3.load and query (verify delete successfully)
                4.insert entity with deleted id
                5.delete id
                6.query with id
        expected: Empty query result
        """
        # init collection and insert data without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # delete id 0 and flush
        del_res, _ = collection_w.delete(tmp_expr)
        assert del_res.delete_count == 1
        assert collection_w.num_entities == tmp_nb

        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

        # insert new entity with same id 0 and query
        df_new = cf.gen_default_dataframe_data(1)
        collection_w.insert(df_new)
        if to_flush_data:
            log.debug(collection_w.num_entities)
        collection_w.query(tmp_expr, output_fields=[ct.default_float_vec_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': df_new.iloc[[0], [0, 4]].to_dict('records'), 'with_vec': True})

        collection_w.delete(tmp_expr)
        if to_flush_delete:
            log.debug(collection_w.num_entities)
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_merge_ids_channel_and_sealed(self):
        """
        target: test merge deleted ids come from both channel and sealed
        method: 1.create, insert ids [0, tmp_nb) with shard_num=1
                2.delete id 0 and flush
                3.load and query with id 0
                4.delete id 1 (merge same segment deleted ids 0 and 1)
                5.query with id 0 and 1
        expected: Empty query result
        """
        # init collection and insert data without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # delete id 0 and flush
        del_res, _ = collection_w.delete(tmp_expr)
        assert del_res.delete_count == 1
        assert collection_w.num_entities == tmp_nb

        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

        # delete id 1 and query id 0 and 1
        collection_w.delete(expr=f'{ct.default_int64_field_name} in {[1]}')
        collection_w.query(expr=f'{ct.default_int64_field_name} in {[0, 1]}',
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_query_after_handoff(self):
        """
        target: test search after delete and handoff
        method: 1.create and load collection
                2.insert entities and delete id 0
                3.flush entities
                4.query deleted id after handoff completed
        expected: Delete successfully, query get empty result
        """
        # init collection and load
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        index_params = {"index_type": "IVF_SQ8",
                        "metric_type": "L2", "params": {"nlist": 64}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params)
        collection_w.load()

        # insert data and delete id 0
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        del_res, _ = collection_w.delete(tmp_expr)

        # flush
        assert collection_w.num_entities == tmp_nb

        # wait for the handoff to complete
        while True:
            time.sleep(0.5)
            segment_infos = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
            if len(segment_infos) > 0 and segment_infos[0].state == SegmentState.Sealed:
                break
        # query deleted id
        collection_w.query(tmp_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="TODO")
    def test_delete_multi_threading(self):
        """
        target: test delete multi threading
        method: delete multi threading
        expected: delete successfully
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_insert_multi(self):
        """
        target: test delete after multi insert
        method: 1.create
                2.insert multi times, no flush
                3.load
                3.delete even number
                4.search and query
        expected: Verify result
        """
        # create collection, insert multi times, each with tmp_nb entities
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        multi = 3
        for i in range(multi):
            start = i * tmp_nb
            df = cf.gen_default_dataframe_data(tmp_nb, start=start)
            collection_w.insert(df)

        # delete even numbers
        ids = [i for i in range(0, tmp_nb * multi, 2)]
        expr = f'{ct.default_int64_field_name} in {ids}'
        collection_w.delete(expr)

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)
        search_res, _ = collection_w.search(cf.gen_vectors(ct.default_nq, ct.default_dim),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        for res_id in search_res[0].ids:
            assert res_id not in ids

    @pytest.mark.tags(CaseLabel.L3)
    def test_delete_sealed_only(self):
        """
        target: test delete sealed-only
        method: 1.deploy sealed-only: two dmlChannel and three queryNodes
                2.create and insert with flush
                3.load
                4.delete all data
                5.query
        expected:
        """
        # init collection and insert data without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=2)
        # insert 3000 entities into 3 segments
        segment_num = 3
        segment_per_count = 2000
        ids = []
        for i in range(segment_num):
            df = cf.gen_default_dataframe_data(nb=segment_per_count, start=(i * segment_per_count))
            res, _ = collection_w.insert(df)
            assert collection_w.num_entities == (i + 1) * segment_per_count
            ids.extend(res.primary_keys)

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        expr = f'{ct.default_int64_field_name} in {ids}'
        collection_w.delete(expr)

        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("issue #26820")
    @pytest.mark.parametrize("consistency_level", ["Bounded", "Session", "Eventually"])
    def test_delete_flush_query_consistency_not_strong(self, consistency_level):
        """
        target: test delete, flush and query with Consistency level not strong
        method: 1.delete ids
                2.flush
                3.query with Consistency level not strong
        expected: query successfully
        """
        # init collection
        collection_w = self.init_collection_general(prefix, True)[0]

        # delete and flush
        delete_ids = [i for i in range(ct.default_nb // 2)]
        delete_expr = f"{ct.default_int64_field_name} in {delete_ids}"
        res = collection_w.delete(delete_expr)[0]
        assert res.delete_count == ct.default_nb // 2
        collection_w.flush()

        # query with Consistency level not strong
        collection_w.query(expr=delete_expr, consistency_level=consistency_level,
                           check_task=CheckTasks.check_query_empty)


class TestDeleteString(TestcaseBase):
    """
    Test case of delete interface with string 
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_entities_repeatedly_with_string(self):
        """
        target: test delete entities twice with string expr
        method: delete with same expr twice
        expected: No exception for second deletion
        """
        # init collection with nb default data
        collection_w = \
            self.init_collection_general(prefix, nb=tmp_nb, insert_data=True, primary_field=ct.default_string_field_name)[0]

        # assert delete successfully and no exception
        collection_w.delete(expr=default_string_expr)
        collection_w.num_entities
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)
        collection_w.delete(expr=default_string_expr)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_all_index_with_string(self):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is string field
                2.create string and float index ,delete entities, query 
                3.search
        expected: assert index and deleted id not in search result
        """
        # create collection, insert tmp_nb, flush and load
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                             primary_field=ct.default_string_field_name)[0:2]

        # create index
        index_params_one = {"index_type": "IVF_SQ8",
                            "metric_type": "COSINE", "params": {"nlist": 64}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params_one, index_name=index_name1)
        index_params_two = {}
        collection_w.create_index(ct.default_string_field_name, index_params=index_params_two, index_name=index_name2)
        assert collection_w.has_index(index_name=index_name2)

        collection_w.release()
        collection_w.load()
        # delete entity
        collection_w.delete(default_string_expr)
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)
        assert collection_w.has_index(index_name=index_name2)

        # search with id 0 vectors
        search_res, _ = collection_w.search([vectors[0][ct.default_float_vec_field_name][0]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        assert "0" not in search_res[0].ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_and_index_with_string(self):
        """
        target: test delete and create index
        method: 1.create, insert, string field is primary
                2.delete half
                3.flush and create index
                4.search
        expected: Empty search result
        """
        # init collection and insert data without flush
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)

        # delete half and flush
        expr = f'{ct.default_string_field_name} in {insert_res.primary_keys[:ct.default_nb // 2]}'
        expr = expr.replace("'", "\"")
        collection_w.delete(expr)
        collection_w.flush()

        # create index
        index_params = {"index_type": "IVF_SQ8",
                        "metric_type": "COSINE", "params": {"nlist": 64}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params)
        assert collection_w.has_index()[0]

        collection_w.load()
        res = collection_w.query(expr="", output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == ct.default_nb // 2
        search_res, _ = collection_w.search([df[ct.default_float_vec_field_name][0]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        log.debug(search_res[0].ids)
        # assert search results not contains deleted ids
        inter = set(insert_res.primary_keys[:ct.default_nb // 2]).intersection(set(search_res[0].ids))
        log.debug(inter)
        assert len(inter) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_query_ids_both_sealed_and_channel_with_string(self):
        """
        target: test query that delete ids from both channel and sealed
        method: 1.create and insert, string field is primary
                2.delete id 0 and flush
                3.load and query id 0
                4.insert new id and delete the id
                5.query id 0 and new id
        expected: Empty query result
        """
        # init collection and insert data without flush
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # delete id 0 and flush
        del_res, _ = collection_w.delete(default_string_expr)
        assert del_res.delete_count == 1
        assert collection_w.num_entities == tmp_nb

        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

        # insert id tmp_nb and delete id 0 and tmp_nb
        df_new = cf.gen_default_dataframe_data(nb=1, start=tmp_nb)
        collection_w.insert(df_new)
        collection_w.delete(expr=f'{ct.default_string_field_name} in ["tmp_nb"]')

        # query with id 0 and tmp_nb
        collection_w.query(expr=f'{ct.default_string_field_name} in ["0", "tmp_nb"]',
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_search_with_string(self):
        """
        target: test delete and search
        method: search entities after it was deleted, string field is primary
        expected: deleted entity is not in the search result
        """
        # init collection with nb default data
        collection_w, _, _, ids = self.init_collection_general(prefix, insert_data=True,
                                                               primary_field=ct.default_string_field_name)[0:4]
        entity, _ = collection_w.query(default_string_expr, output_fields=["*"])
        search_res, _ = collection_w.search([entity[0][ct.default_float_vec_field_name]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        # assert search results contains entity
        assert "0" in search_res[0].ids

        expr = f'{ct.default_string_field_name} in {ids[:ct.default_nb // 2]}'
        expr = expr.replace("'", "\"")
        collection_w.delete(expr)
        search_res_2, _ = collection_w.search([entity[0][ct.default_float_vec_field_name]],
                                              ct.default_float_vec_field_name,
                                              ct.default_search_params, ct.default_limit)
        # assert search result is not equal to entity
        log.debug(f"Second search result ids: {search_res_2[0].ids}")
        inter = set(ids[:ct.default_nb // 2]
                    ).intersection(set(search_res_2[0].ids))
        # Using bounded staleness, we could still search the "deleted" entities,
        # since the search requests arrived query nodes earlier than query nodes consume the delete requests.
        assert len(inter) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_expr_repeated_values_with_string(self):
        """
        target: test delete with repeated values
        method: 1.insert data with unique primary keys, string field is primary
                2.delete with repeated values: 'id in [0, 0]'
        expected: delete one entity
        """
        # init collection with nb default data
        collection_w = \
            self.init_collection_general(prefix, nb=tmp_nb, insert_data=True, primary_field=ct.default_string_field_name)[0]
        expr = f'{ct.default_string_field_name} in ["0", "0", "0"]'
        del_res, _ = collection_w.delete(expr)
        assert del_res.delete_count == 3
        collection_w.num_entities
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_duplicate_primary_keys_with_string(self):
        """
        target: test delete from duplicate primary keys
        method: 1.insert data with dup ids, string field is primary
                2.delete with repeated or not values
        expected: currently only delete one entity, query get one entity
        todo delete all entities
        """
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_default_dataframe_data(nb=tmp_nb)
        df[ct.default_string_field_name] = "0"
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb
        del_res, _ = collection_w.delete(default_string_expr)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # Just one query res and search res, because de-dup
        res, _ = collection_w.query(default_string_expr, output_fields=["*"])
        assert len(res) == 0

        search_res, _ = collection_w.search([df[ct.default_float_vec_field_name][1]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit,
                                            output_fields=[ct.default_int64_field_name, ct.default_float_field_name,
                                                           ct.default_string_field_name])
        assert len(search_res) == 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_from_partitions_with_same_ids_of_string(self):
        """
        target: test delete same ids from two partitions with same data
        method: 1.insert same nb data into two partitions
                2.delete same ids from partition_1
        expected: The data only in partition_1 will be deleted
        """
        # init collection and partition
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)

        # insert same data into partition_w and default partition
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        partition_w.insert(df)

        # delete same id 0 from default_partition, and query on it get empty result
        collection_w.delete(default_string_expr,
                            partition_name=ct.default_partition_name)
        assert collection_w.num_entities == tmp_nb * 2
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_string_expr, partition_names=[ct.default_partition_name],
                           check_task=CheckTasks.check_query_empty)

        # query on partition_w with id 0 and get an result
        collection_w.query(default_string_expr, partition_names=[partition_w.name],
                           check_task=CheckTasks.check_query_results, check_items={exp_res: query_tmp_expr_str})

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_sealed_segment_without_flush_with_string(self):
        """
        target: test delete without flush
        method: 1.insert and flush data
                2.delete ids from collection and no flush
                3.load and query with id
        expected: No query result
        """
        # create collection, insert data without flush
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb

        # delete
        del_res, _ = collection_w.delete(default_string_expr)
        assert del_res.delete_count == 1

        # load and query with id
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_growing_data_channel_delete_with_string(self):
        """
        target: test delete entities from growing segment, and channel deleteMsg
        method: 1.create collection, string field is primary
                2.load collection
                3.insert data and delete ids
                4.query deleted ids
        expected: No query result
        """
        # create collection
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        # load collection and the queryNode watch the insertChannel
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # insert data
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        # delete id 0
        del_res = collection_w.delete(default_string_expr)[0]
        assert del_res.delete_count == 1
        # query id 0
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_sealed_data_channel_delete_with_string(self):
        """
        target: test delete sealed data and get deleteMsg from insertChannel
        method: 1.create, insert and flush data, string field is primary
                2.load collection
                3.delete id without flush
                4.query deleted ids (queryNode get deleted ids from channel not persistence)
        expected: Delete successfully and no query result
        """
        # create collection and insert flush data
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb

        # load collection and queryNode subscribe channel
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # delete ids and query
        collection_w.delete(default_string_expr)
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_sealed_segment_with_flush_string(self):
        """
        target: test delete data from sealed segment and flush delta log
        method: 1.create and insert and flush data, string field is primary
                2.delete entities and flush (insert and flush)
                3.load collection (load data and delta log)
                4.query deleted ids
        expected: No query result
        """
        # create collection
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        # insert and flush data
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb

        # delete id 0 and flush
        del_res = collection_w.delete(default_string_expr)[0]
        assert del_res.delete_count == 1
        collection_w.insert(cf.gen_default_dataframe_data(nb=1, start=tmp_nb))
        log.info(collection_w.num_entities)
        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_sealed_data_sealed_delete_string(self):
        """
        target: test delete with sealed data and sealed delete request
        method: 1.create, insert, string field is primary
                2.delete and flush (will flush data and delete)
                3.load and query
        expected: Empty query result
        """
        # create collection
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        # insert without flush
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # delete id 0 and flush
        del_res = collection_w.delete(default_string_expr)[0]
        assert del_res.delete_count == 1
        assert collection_w.num_entities == tmp_nb

        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_entity_loop_with_string(self):
        """
        target: test delete all entities one by one in a loop
        method: delete data one by one for a loop
        expected: No exception
        """
        # init an auto_id collection and insert tmp_nb data, flush and load
        collection_w, _, _, ids = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True,
                                                               primary_field=ct.default_string_field_name)[0:4]

        for del_id in ids:
            expr = f'{ct.default_string_field_name} in {[del_id]}'
            expr = expr.replace("'", "\"")
            res = collection_w.delete(expr)[0]
            assert res.delete_count == 1

        # query with all ids
        expr = f'{ct.default_string_field_name} in {ids}'
        expr = expr.replace("'", "\"")
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_flush_loop_with_string(self):
        """
        target: test delete and flush in a loop
        method: in a loop, delete batch and flush, until delete all entities
        expected: No exception
        """
        # init an auto_id collection and insert tmp_nb data
        collection_w, _, _, ids = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True,
                                                               primary_field=ct.default_string_field_name)[0:4]

        batch = 10
        for i in range(tmp_nb // batch):
            expr = f'{ct.default_string_field_name} in {ids[i * batch: (i + 1) * batch]}'
            expr = expr.replace("'", "\"")
            res, _ = collection_w.delete(expr)
            assert res.delete_count == batch

        # query with all ids
        expr = f'{ct.default_string_field_name} in {ids}'
        expr = expr.replace("'", "\"")
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("to_flush_data", [True, False])
    @pytest.mark.parametrize("to_flush_delete", [True, False])
    def test_delete_merge_same_id_channel_and_sealed_string(self, to_flush_data, to_flush_delete):
        """
        target: test merge same delete ids from channel and sealed
        method: 1.create, insert, string field is primary
                2.delete id and flush (data and deleted become sealed)
                3.load and query (verify delete successfully)
                4.insert entity with deleted id
                5.delete id
                6.query with id
        expected: Empty query result
        """
        # init collection and insert data without flush
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema, shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # delete id 0 and flush
        del_res, _ = collection_w.delete(default_string_expr)
        assert del_res.delete_count == 1
        assert collection_w.num_entities == tmp_nb

        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

        # insert new entity with same id 0 and query
        df_new = cf.gen_default_dataframe_data(1)
        collection_w.insert(df_new)
        if to_flush_data:
            log.debug(collection_w.num_entities)
        collection_w.query(default_string_expr, output_fields=[ct.default_float_vec_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': df_new.iloc[[0], [2, 4]].to_dict('records'),
                                        'primary_field': ct.default_string_field_name, 'with_vec': True})

        collection_w.delete(default_string_expr)
        if to_flush_delete:
            log.debug(collection_w.num_entities)
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_merge_ids_channel_and_sealed_string(self):
        """
        target: test merge deleted ids come from both channel and sealed
        method: 1.create, insert ids [0, tmp_nb) with shard_num=1, string field is primary
                2.delete id 0 and flush
                3.load and query with id 0
                4.delete id 1 (merge same segment deleted ids 0 and 1)
                5.query with id 0 and 1
        expected: Empty query result
        """
        # init collection and insert data without flush
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema, shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # delete id 0 and flush
        del_res, _ = collection_w.delete(default_string_expr)
        assert del_res.delete_count == 1
        assert collection_w.num_entities == tmp_nb

        # load and query id 0
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

        # delete id 1 and query id 0 and 1
        collection_w.delete(expr=f'{ct.default_string_field_name} in ["1"]')
        collection_w.query(expr=f'{ct.default_string_field_name} in ["0", "1"]',
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_query_after_handoff_with_string(self):
        """
        target: test search after delete and handoff
        method: 1.create and load collection, string field is primary
                2.insert entities and delete id 0
                3.flush entities
                4.query deleted id after handoff completed
        expected: Delete successfully, query get empty result
        """
        # init collection and load
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema, shards_num=1)
        index_params = {"index_type": "IVF_SQ8",
                        "metric_type": "L2", "params": {"nlist": 64}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params)
        collection_w.load()

        # insert data and delete id 0
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        del_res, _ = collection_w.delete(default_string_expr)

        # flush
        assert collection_w.num_entities == tmp_nb

        # wait for the handoff to complete
        while True:
            time.sleep(0.5)
            segment_infos = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
            if len(segment_infos) > 0 and segment_infos[0].state == SegmentState.Sealed:
                break
        # query deleted id
        collection_w.query(default_string_expr,
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_insert_multi_with_string(self):
        """
        target: test delete after multi insert with string
        method: 1.create a collection with string field is primary
                2.insert multi times, no flush
                3.load
                3.delete even number
                4.search and query
        expected: Verify result
        """
        # create collection, insert multi times, each with tmp_nb entities
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), schema=schema)
        multi = 3
        for i in range(multi):
            start = i * tmp_nb
            df = cf.gen_default_dataframe_data(tmp_nb, start=start)
            collection_w.insert(df)

        # delete even numbers
        ids = [str(i) for i in range(0, tmp_nb * multi, 2)]
        expr = f'{ct.default_string_field_name} in {ids}'
        expr = expr.replace("'", "\"")
        collection_w.delete(expr)

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)
        search_res, _ = collection_w.search(cf.gen_vectors(ct.default_nq, ct.default_dim),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        for res_id in search_res[0].ids:
            assert res_id not in ids

    @pytest.mark.tags(CaseLabel.L0)
    def test_delete_invalid_expr(self):
        """
        target: test delete data with string expr
        method: 1.create collection, insert data and collection.load()
                2.collection delete with invalid expr 
                3.query expr
        expected: Raise exception
        """
        collection_w = \
            self.init_collection_general(prefix, nb=tmp_nb, insert_data=True, primary_field=ct.default_string_field_name)[0]
        collection_w.load()
        error = {ct.err_code: 0,
                 ct.err_msg: f"failed to create expr plan, expr = {default_invalid_string_exp}"}
        collection_w.delete(expr=default_invalid_string_exp,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("to_query", [True, False])
    # @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_delete_insert_same_id_sealed_string(self, to_query):
        """
        target: test insert same id entity after delete from sealed data
        method: 1.create and insert with flush, string is pk field
                2.load and query with the  id
                3.delete the id entity
                4.insert new entity with the same id and flush
                5.query the id
        expected: Verify that the query gets the newly inserted entity
        """
        # init collection and insert data without flush
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), schema=schema)

        # insert
        df = cf.gen_default_dataframe_data(1000)
        collection_w.insert(df)
        log.debug(collection_w.num_entities)

        # load and query
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        res = df.iloc[:1, 2:3].to_dict('records')
        collection_w.search(data=[df[ct.default_float_vec_field_name][0]], anns_field=ct.default_float_vec_field_name,
                            param=default_search_params, limit=1)
        collection_w.query(default_string_expr, check_task=CheckTasks.check_query_results, check_items={'exp_res': res})

        # delete
        collection_w.delete(default_string_expr)
        if to_query:
            collection_w.query(default_string_expr,
                               check_task=CheckTasks.check_query_empty)

        # re-insert
        df_new = cf.gen_default_dataframe_data(nb=1)
        collection_w.insert(df_new)
        log.debug(collection_w.num_entities)

        # re-query
        res = df_new.iloc[[0], [2, 4]].to_dict('records')
        log.info(res)
        collection_w.query(default_string_expr, output_fields=[ct.default_float_vec_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': res,
                                        'primary_field': ct.default_string_field_name,
                                        'with_vec': True})
        collection_w.search(data=[df_new[ct.default_float_vec_field_name][0]],
                            anns_field=ct.default_float_vec_field_name,
                            param=default_search_params, limit=1)

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_with_string_field_is_empty(self):
        """
        target: test delete with string field is empty
        method: 1.string field is PK, insert empty data
                2.delete ids from collection
                3.load and query with id
        expected: No query result
        """
        # create collection, insert data without flush
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

        nb = 3000
        df = cf.gen_default_list_data(nb)
        df[2] = [""for _ in range(nb)] 

        collection_w.insert(df)
        collection_w.flush()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        assert collection_w.num_entities == nb

        # delete
        string_expr = "varchar in [\"\", \"\"]"
        del_res, _ = collection_w.delete(string_expr)
        assert del_res.delete_count == 2

        # load and query with id
        collection_w.load()
        collection_w.query(string_expr, check_task=CheckTasks.check_query_empty)


class TestDeleteComplexExpr(TestcaseBase):
    """
    Test case of delete interface with complex expr
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("expression", cf.gen_normal_expressions()[1:])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_delete_normal_expressions(self, expression, enable_dynamic_field):
        """
        target: test delete entities using normal expression
        method: delete using normal expression
        expected: delete successfully
        """
        # init collection with nb default data
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic_field)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i, _id in enumerate(insert_ids):
            if enable_dynamic_field:
                int64 = _vectors[i][ct.default_int64_field_name]
                float = _vectors[i][ct.default_float_field_name]
            else:
                int64 = _vectors.int64[i]
                float = _vectors.float[i]
            if not expression or eval(expression):
                filter_ids.append(_id)

        # delete with expressions
        res = collection_w.delete(expression)[0]
        assert res.delete_count == len(filter_ids)

        # query to check
        collection_w.query(f"int64 in {filter_ids}", check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_array_field_expressions())
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_delete_array_expressions(self, expression, enable_dynamic_field):
        """
        target: test delete entities using normal expression
        method: delete using normal expression
        expected: delete successfully
        """
        # 1. create a collection
        nb = ct.default_nb
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema, enable_dynamic_field=enable_dynamic_field)

        # 2. insert data
        array_length = 100
        data = []
        for i in range(nb):
            arr = {ct.default_int64_field_name: i,
                   ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0],
                   ct.default_int32_array_field_name: [np.int32(i) for i in range(array_length)],
                   ct.default_float_array_field_name: [np.float32(i) for i in range(array_length)],
                   ct.default_string_array_field_name: [str(i) for i in range(array_length)]}
            data.append(arr)
        collection_w.insert(data)
        collection_w.flush()

        # 3. filter result with expression in collection
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i in range(nb):
            int32_array = data[i][ct.default_int32_array_field_name]
            float_array = data[i][ct.default_float_array_field_name]
            string_array = data[i][ct.default_string_array_field_name]
            if not expression or eval(expression):
                filter_ids.append(i)

        # 4. delete by array expression
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        collection_w.load()
        res = collection_w.delete(expression)[0]
        assert res.delete_count == len(filter_ids)

        # 5. query to check
        collection_w.query(expression, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field_name", ["varchar", "json_field['string']", "NewStr"])
    @pytest.mark.parametrize("like", ["like", "LIKE"])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_delete_string_expressions_like(self, field_name, like, enable_dynamic_field):
        """
        target: test delete expr like
        method: delete using expression like
        expected: delete successfully
        """
        if field_name == "NewStr" and enable_dynamic_field is False:
            pytest.skip("only support when enable_dynamic_filed == True")
        # init collection with nb default data
        nb = 1000
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, False, enable_dynamic_field=enable_dynamic_field)[0:4]

        # insert
        string_list = [cf.gen_str_by_length() for _ in range(nb)]
        if enable_dynamic_field:
            data = cf.gen_default_rows_data(nb)
            for i in range(nb):
                data[i][ct.default_json_field_name] = {"string": string_list[i]}
                data[i]['NewStr'] = string_list[i]
                data[i][ct.default_string_field_name] = string_list[i]
        else:
            data = cf.gen_default_dataframe_data(nb)
            data[ct.default_json_field_name] = [{"string": string_list[i]} for i in range(nb)]
            data[ct.default_string_field_name] = string_list
        collection_w.insert(data)
        collection_w.flush()
        collection_w.load()

        # delete with expressions
        deleted_str = [s for s in string_list if s.startswith('a')]
        expression = f"{field_name} {like} 'a%'"
        res = collection_w.delete(expression)[0]
        assert res.delete_count == len(deleted_str)

        # query to check
        collection_w.load()
        collection_w.query("int64 >= 0", output_fields=['count(*)'],
                           check_task=CheckTasks.check_query_results,
                           check_items={'count(*)': nb - len(deleted_str)})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_expr_empty_string(self):
        """
        target: test delete with expr empty
        method: delete with expr=""
        expected: raise exception
        """
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, True)[0]

        # delete
        error = {ct.err_code: 1, ct.err_msg: "expr cannot be empty"}
        collection_w.delete(expr="", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_complex_expr_before_load(self):
        """
        target: test delete before load
        method: delete with any complex expr
        expected: raise exception
        """
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, False)[0]

        # delete
        error = {ct.err_code: 1, ct.err_msg: "collection not loaded: unrecoverable error"}
        collection_w.delete(expr="int64 >= 0", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["json_contains", "JSON_CONTAINS"])
    @pytest.mark.parametrize("field_name", ["json_field['list']", "list"])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_delete_expr_json_contains_base(self, expr_prefix, field_name, enable_dynamic_field):
        """
        target: test delete expr using json_contains
        method: delete using expression using json_contains
        expected: delete successfully
        """
        if field_name == "list" and enable_dynamic_field is False:
            pytest.skip("only support when enable_dynamic_filed == True")
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, False, enable_dynamic_field=enable_dynamic_field)[0]

        # insert
        listMix = [[i, i + 2] for i in range(ct.default_nb)]  # only int
        if enable_dynamic_field:
            data = cf.gen_default_rows_data()
            for i in range(ct.default_nb):
                data[i][ct.default_json_field_name] = {"list": listMix[i]}
                data[i]['list'] = listMix[i]
        else:
            data = cf.gen_default_dataframe_data()
            data[ct.default_json_field_name] = [{"list": listMix[i]} for i in range(ct.default_nb)]
        collection_w.insert(data)
        collection_w.flush()
        collection_w.load()

        # delete with expressions
        delete_ids = random.randint(2, ct.default_nb - 2)
        expression = f"{expr_prefix}({field_name}, {delete_ids})"
        res = collection_w.delete(expression)[0]
        exp_ids = cf.assert_json_contains(expression, listMix)
        assert res.delete_count == len(exp_ids)

        # query to check
        collection_w.query(expression, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_all", "JSON_CONTAINS_ALL",
                                             "json_contains_any", "JSON_CONTAINS_ANY"])
    @pytest.mark.parametrize("field_name", ["json_field['list']", "list"])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_delete_expr_json_contains_all(self, expr_prefix, field_name, enable_dynamic_field):
        """
        target: test delete expr using json_contains
        method: delete using expression using json_contains
        expected: delete successfully
        """
        if field_name == "list" and enable_dynamic_field is False:
            pytest.skip("only support when enable_dynamic_filed == True")
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, False, enable_dynamic_field=enable_dynamic_field)[0]

        # insert
        listMix = [[i, i * 0.00001, bool(i % 2), [i, str(i)]] for i in range(ct.default_nb)]  # mix int, float, list, bool
        if enable_dynamic_field:
            data = cf.gen_default_rows_data()
            for i in range(ct.default_nb):
                data[i][ct.default_json_field_name] = {"list": listMix[i]}
                data[i]['list'] = listMix[i]
        else:
            data = cf.gen_default_dataframe_data()
            data[ct.default_json_field_name] = [{"list": listMix[i]} for i in range(ct.default_nb)]
        collection_w.insert(data)
        collection_w.flush()
        collection_w.load()

        # delete with expressions
        ids = random.randint(0, ct.default_nb)
        delete_ids = [bool(ids % 2), ids]
        expression = f"{expr_prefix}({field_name}, {delete_ids})"
        res = collection_w.delete(expression)[0]
        exp_ids = cf.assert_json_contains(expression, listMix)
        assert res.delete_count == len(exp_ids)

        # query to check
        collection_w.query(expression, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expressions",
                             cf.gen_field_compare_expressions(["int64_1", "json_field['int'][0]"],
                                                              ["int64_2", "json_field['int'][1]"]))
    def test_delete_expr_compare_two_variables(self, expressions):
        """
        target: test delete expr using 2 variables
        method: delete with expressions using compare 2 variables
        expected: delete successfully
        """
        # init collection with nb default data
        nb = 1000
        dim = 32
        fields = [cf.gen_int64_field("int64_1"), cf.gen_int64_field("int64_2"),
                  cf.gen_json_field("json_field"), cf.gen_float_vec_field("float_vector", dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, primary_field="int64_1")
        collection_w = self.init_collection_wrap(schema=schema)

        # insert
        int64_1_values = [i for i in range(nb)]
        int64_2_values = [random.randint(0, nb) for _ in range(nb)]
        vectors = cf.gen_vectors(nb, dim)
        json_values = [[i, int64_2_values[i]] for i in range(nb)]
        data = pd.DataFrame({
            "int64_1": int64_1_values,
            "int64_2": int64_2_values,
            "json_field": [{"int": json_values[i]} for i in range(nb)],
            "float_vector": vectors
        })
        collection_w.insert(data)
        collection_w.flush()
        collection_w.create_index("float_vector")
        collection_w.load()

        # delete with expressions
        error = {ct.err_code: 1, ct.err_msg: f"failed to create expr plan, expr = {expressions}"}
        collection_w.delete(expressions, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_json_field_expressions())
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_delete_expr_json_field(self, expression, enable_dynamic_field):
        """
        target: test delete entities using normal expression
        method: delete using normal expression
        expected: delete successfully
        """
        # init collection with nb default data
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic_field)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        json_field = {}
        for i, _id in enumerate(insert_ids):
            if enable_dynamic_field:
                json_field['number'] = _vectors[i][ct.default_json_field_name]['number']
                json_field['float'] = _vectors[i][ct.default_json_field_name]['float']
            else:
                json_field['number'] = _vectors[ct.default_json_field_name][i]['number']
                json_field['float'] = _vectors[ct.default_json_field_name][i]['float']
            if not expression or eval(expression):
                filter_ids.append(_id)

        # delete with expressions
        res = collection_w.delete(expression)[0]
        assert res.delete_count == len(filter_ids)

        # query to check
        collection_w.query(f"int64 in {filter_ids}", check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("normal_expression, json_expression", zip(cf.gen_normal_expressions()[1:4],
                                                                       cf.gen_json_field_expressions()[6:9]))
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_delete_expr_complex_mixed(self, normal_expression, json_expression, enable_dynamic_field):
        """
        target: test delete entities using normal expression
        method: delete using normal expression
        expected: delete successfully
        """
        # init collection with nb default data
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic_field)[0:4]

        # filter result with expression in collection
        expression = normal_expression + ' and ' + json_expression
        _vectors = _vectors[0]
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        json_field = {}
        for i, _id in enumerate(insert_ids):
            if enable_dynamic_field:
                json_field['number'] = _vectors[i][ct.default_json_field_name]['number']
                json_field['float'] = _vectors[i][ct.default_json_field_name]['float']
                int64 = _vectors[i][ct.default_int64_field_name]
                float = _vectors[i][ct.default_float_field_name]
            else:
                json_field['number'] = _vectors[ct.default_json_field_name][i]['number']
                json_field['float'] = _vectors[ct.default_json_field_name][i]['float']
                int64 = _vectors.int64[i]
                float = _vectors.float[i]
            if not expression or eval(expression):
                filter_ids.append(_id)

        # delete with expressions
        res = collection_w.delete(expression)[0]
        assert res.delete_count == len(filter_ids)

        # query to check
        collection_w.query(f"int64 in {filter_ids}", check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_normal_string_expressions(["varchar", "json_field['string']", "NewStr"]))
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    def test_delete_string_expressions_normal(self, expression, enable_dynamic_field):
        """
        target: test delete expr like
        method: delete using expression like
        expected: delete successfully
        """
        if "NewStr" in expression and enable_dynamic_field is False:
            pytest.skip("only support when enable_dynamic_filed == True")
        # init collection with nb default data
        nb = 1000
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, False, enable_dynamic_field=enable_dynamic_field)[0:4]

        # insert
        if enable_dynamic_field:
            data = cf.gen_default_rows_data(nb)
            for i in range(nb):
                data[i][ct.default_json_field_name] = {"string": str(i)}
                data[i]['NewStr'] = str(i)
        else:
            data = cf.gen_default_dataframe_data(nb)
            data[ct.default_json_field_name] = [{"string": str(i)} for i in range(nb)]
        collection_w.insert(data)
        collection_w.flush()
        collection_w.load()

        # calculate the result
        _vectors = data
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        json_field = {}
        for i in range(nb):
            if enable_dynamic_field:
                json_field['string'] = _vectors[i][ct.default_json_field_name]['string']
                varchar = _vectors[i][ct.default_string_field_name]
                NewStr = _vectors[i]['NewStr']
            else:
                json_field['string'] = _vectors[ct.default_json_field_name][i]['string']
                varchar = _vectors.varchar[i]
            if not expression or eval(expression):
                filter_ids.append(i)

        # delete with expressions
        res = collection_w.delete(expression)[0]
        assert res.delete_count == len(filter_ids)

        # query to check
        collection_w.load()
        collection_w.query("int64 >= 0", output_fields=['count(*)'],
                           check_task=CheckTasks.check_query_results,
                           check_items={'count(*)': nb - len(filter_ids)})

