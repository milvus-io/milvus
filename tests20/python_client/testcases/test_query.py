import logging

import pytest
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log

prefix = "query"
default_term_expr = f'{ct.default_int_field_name} in [0, 1]'


@pytest.mark.skip(reason="waiting for debug")
class TestQueryBase(TestcaseBase):
    """
    test Query interface
    query(collection_name, expr, output_fields=None, partition_names=None, timeout=None)
    """

    def test_query(self):
        """
        target: test query
        method: query with term expr
        expected: verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        int_values = vectors[0][cf.default_int64_field_name].values.tolist()
        pos = 5
        term_expr = f'{ct.default_int_field_name} in {int_values[:pos]}'
        res = collection_w.query(term_expr)
        logging.getLogger().debug(res)

    def test_query_empty_collection(self):
        """
        target: test query empty collection
        method: query on a empty collection
        expected: empty result
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.load_collection()
        res, _ = collection_w.query(default_term_expr)
        assert len(res) == 0

    def test_query_auto_id_collection(self):
        """
        target: test query on collection that primary field auto_id=True
        method: 1.create collection with auto_id=True 2.query on primary field
        expected: todo
        """
        pass

    def test_query_expr_none(self):
        """
        target: test query with none expr
        method: query with expr None
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        error = {ct.err_code: 1, ct.err_msg: "invalid expr"}
        collection_w.query(None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.parametrize("expr", [1, 2., [], {}, ()])
    def test_query_expr_non_string(self, expr):
        """
        target: test query with non-string expr
        method: query with non-string expr, eg 1, [] ..
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        error = {ct.err_code: 1, ct.err_msg: "expr must string type"}
        collection_w.query(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.parametrize("expr", ["12-s", "中文", "a", " "])
    def test_query_expr_invalid_string(self, expr):
        """
        target: test query with invalid expr
        method: query with invalid string expr
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        error = {ct.err_code: 1, ct.err_msg: "invalid expr"}
        collection_w.query(expr, check_task=CheckTasks.err_res, check_items=error)

    def test_query_expr_term(self):
        """
        target: test query with TermExpr
        method: query with TermExpr
        expected: query result is correct
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        res, _ = collection_w.query(default_term_expr)
        log.info(res)

    def test_query_expr_not_existed_field(self):
        """
        target: test query with not existed field
        method: query by term expr with fake field
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        term_expr = 'field in [1, 2]'
        error = {ct.err_code: 1, ct.err_msg: "field not existed"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

    def test_query_expr_unsupported_field(self):
        """
        target: test query on unsupported field
        method: query on float field
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        term_expr = f'{ct.default_float_field_name} in [1., 2.]'
        error = {ct.err_code: 1, ct.err_msg: "only supported on int field"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

    def test_query_expr_non_primary_field(self):
        """
        target: test query on non-primary field
        method: query on non-primary int field
        expected: raise exception
        """
        # field_name = "int2"
        # fields = ut.add_field_default(field_name=field_name)
        # c_name = ut.gen_unique_str()
        # connect.create_collection(c_name, fields)
        # entities = ut.add_field(field_name=field_name)
        # connect.insert(collection, entities)
        # connect.flush(c_name)
        # term_expr = f'{field_name} in [1, 2]'
        # msg = 'only supported on primary field'
        # with pytest.raises(Exception, match=msg):
        #     connect.query(collection, term_expr)

    @pytest.mark.parametrize("expr", [f'{ct.default_int_field_name} inn [1, 2]',
                                      f'{ct.default_int_field_name} not in [1, 2]',
                                      f'{ct.default_int_field_name} in not [1, 2]'])
    def test_query_expr_wrong_term_keyword(self, expr):
        """
        target: test query with wrong term expr keyword
        method: query with wrong keyword term expr
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        error = {ct.err_code: 1, ct.err_msg: "invalid expr"}
        collection_w.query(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.parametrize("expr", [f'{ct.default_int_field_name} in 1',
                                      f'{ct.default_int_field_name} in "in"',
                                      f'{ct.default_int_field_name} in (mn)'])
    def test_query_expr_non_array_term(self, expr):
        """
        target: test query with non-array term expr
        method: query with non-array term expr
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        error = {ct.err_code: 1, ct.err_msg: "invalid expr"}
        collection_w.query(expr, check_task=CheckTasks.err_res, check_items=error)

    def test_query_expr_empty_term_array(self):
        """
        target: test query with empty array term expr
        method: query with empty term expr
        expected: empty rsult
        """
        term_expr = f'{ct.default_int_field_name} in []'
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        res, _ = collection_w.query(term_expr)
        assert len(res) == 0

    def test_query_expr_inconstant_term_array(self):
        """
        target: test query with term expr that field and array are inconsistent
        method: query with int field and float values
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        int_values = [1., 2.]
        term_expr = f'{ct.default_int_field_name} in {int_values}'
        error = {ct.err_code: 1, ct.err_msg: "Invalid str"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

    def test_query_expr_mix_term_array(self):
        """
        target: test query with mix type value expr
        method: query with term expr that has int and float type value
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        int_values = [1., 2]
        term_expr = f'{ct.default_int_field_name} in {int_values}'
        error = {ct.err_code: 1, ct.err_msg: "Invalid str"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.parametrize("constant", [[1], (), {}, " "])
    def test_query_expr_non_constant_array_term(self, constant):
        """
        target: test query with non-constant array term expr
        method: query with non-constant array expr
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        term_expr = f'{ct.default_int_field_name} in [{constant}]'
        error = {ct.err_code: 1, ct.err_msg: "Invalid str"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

    def test_query_output_field_none(self):
        """
        target: test query with none output field
        method: query with output field=None
        expected: return all fields
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        res, _ = collection_w.query(default_term_expr, output_fields=None)
        fields = [ct.default_int_field_name, ct.default_float_field_name, ct.default_float_vec_field_name]
        assert res[0].keys() == fields

    def test_query_output_one_field(self):
        """
        target: test query with output one field
        method: query with output one field
        expected: return one field
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        res, _ = collection_w.query(default_term_expr, output_fields=[ct.default_int_field_name])
        assert res[0].keys() == [ct.default_int_field_name]

    def test_query_output_all_fields(self):
        """
        target: test query with none output field
        method: query with output field=None
        expected: return all fields
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        fields = [ct.default_int_field_name, ct.default_float_field_name, ct.default_float_vec_field_name]
        res, _ = collection_w.query(default_term_expr, output_fields=fields)
        assert res[0].keys() == fields

    def test_query_output_not_existed_field(self):
        """
        target: test query output not existed field
        method: query with not existed output field
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        error = {ct.err_code: 1, ct.err_msg: 'cannot find field'}
        collection_w.query(default_term_expr, output_fields=["int"], check_items=CheckTasks.err_res, check_task=error)

    def test_query_output_part_not_existed_field(self):
        """
        target: test query output part not existed field
        method: query with part not existed field
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        error = {ct.err_code: 1, ct.err_msg: 'cannot find field'}
        fields = [ct.default_int64_field_name, "int"]
        collection_w.query(default_term_expr, output_fields=fields, check_items=CheckTasks.err_res, check_task=error)

    def test_query_empty_output_fields(self):
        """
        target: test query with empty output fields
        method: query with empty output fields
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        error = {ct.err_code: 1, ct.err_msg: 'output fields is empty'}
        collection_w.query(default_term_expr, output_fields=[], check_items=CheckTasks.err_res, check_task=error)

    @pytest.mark.parametrize("fields", ct.get_invalid_string)
    def test_query_invalid_output_fields(self, fields):
        """
        target: test query with invalid output fields
        method: query with invalid field fields
        expected: raise exception
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        error = {ct.err_code: 1, ct.err_msg: 'invalid output fields'}
        collection_w.query(default_term_expr, output_fields=fields, check_items=CheckTasks.err_res, check_task=error)

    def test_query_partition(self):
        """
        target: test query on partition
        method: create a partition and query
        expected: verify query result
        """
        conn = self._connect()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        partition_w.insert(df)
        conn.flush([collection_w.name])
        partition_w.load()
        res, _ = collection_w.query(default_term_expr, partition_names=[partition_w.name])
        # todo res

    def test_query_partition_without_loading(self):
        """
        target: test query on partition without loading
        method: query on partition and no loading
        expected: raise exception
        """
        conn = self._connect()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        partition_w.insert(df)
        conn.flush([collection_w.name])
        error = {ct.err_code: 1, ct.err_msg: 'cannot find collection'}
        collection_w.query(default_term_expr, partition_names=[partition_w.name],
                           check_items=CheckTasks.err_res, check_task=error)

    def test_query_default_partition(self):
        """
        target: test query on default partition
        method: query on default partition
        expected: verify query result
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        res, _ = collection_w.query(default_term_expr, partition_names=[ct.default_partition_name])
        # todo res

    def test_query_empty_partition(self):
        """
        target: test query on empty partition
        method: query on a empty collection
        expected: empty query result
        """
        conn = self._connect()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        conn.flush([collection_w.name])
        partition_w.load()
        res, _ = collection_w.query(default_term_expr, partition_names=[partition_w.name])
        assert len(res) == 0

    def test_query_not_existed_partition(self):
        """
        target: test query on a not existed partition
        method: query on not existed partition
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        collection_w.load()
        partition_names = ct.gen_unique_str()
        collection_w.query(default_term_expr, partition_names=[partition_names])
        error = {ct.err_code: 1, ct.err_msg: 'cannot find partition'}
        collection_w.query(default_term_expr, partition_names=[partition_names],
                           check_items=CheckTasks.err_res, check_task=error)


@pytest.mark.skip(reason="waiting for debug")
class TestQueryOperation(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test query interface operations
    ******************************************************************
    """

    def test_query_without_connection(self):
        """
        target: test query without connection
        method: close connect and query
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 1, ct.err_msg: 'should create connect first'}
        collection_w.query(default_term_expr, check_task=CheckTasks.err_res, check_items=error)

    def test_query_without_loading(self):
        """
        target: test query without loading
        method: no loading before query
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data)
        conn, _ = self.connection_wrap.get_connection()
        conn.flush([c_name])
        assert collection_w.num_entities == ct.default_nb
        error = {ct.err_code: 1, ct.err_msg: "can not find collection"}
        collection_w.query(default_term_expr, check_task=CheckTasks.err_res, check_items=error)

    def test_query_expr_single_term_array(self):
        """
        target: test query with single array term expr
        method: query with single array value
        expected: query result is one entity
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        term_expr = f'{ct.default_int_field_name} in [0]'
        res, _ = collection_w.query(term_expr)
        assert len(res) == 1
        df = vectors[0]
        assert res[0][ct.default_int_field_name] == df[ct.default_int64_field_name].values.tolist()[0]
        assert res[1][ct.default_float_field_name] == df[ct.default_float_field_name].values.tolist()[0]
        assert res[2][ct.default_float_vec_field_name] == df[ct.default_float_vec_field_name].values.tolist()[0]

    def test_query_binary_expr_single_term_array(self):
        """
        target: test query with single array term expr
        method: query with single array value
        expected: query result is one entity
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True, is_binary=True)
        term_expr = f'{ct.default_int_field_name} in [0]'
        res, _ = collection_w.query(term_expr)
        assert len(res) == 1
        int_values = vectors[0][ct.default_int_field_name].values.tolist()
        float_values = vectors[0][ct.default_float_field_name].values.tolist()
        vec_values = vectors[0][ct.default_float_vec_field_name].values.tolist()
        assert res[0][ct.default_int_field_name] == int_values[0]
        assert res[1][ct.default_float_field_name] == float_values[0]
        assert res[2][ct.default_float_vec_field_name] == vec_values[0]

    def test_query_expr_all_term_array(self):
        """
        target: test query with all array term expr
        method: query with all array value
        expected: verify query result
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        int_values = vectors[0][ct.default_int_field_name].values.tolist()
        term_expr = f'{ct.default_int_field_name} in {int_values}'
        res, _ = collection_w.query(term_expr)
        assert len(res) == ct.default_nb
        for i in ct.default_nb:
            assert res[i][ct.default_int_field_name] == int_values[i]

    def test_query_expr_half_term_array(self):
        """
        target: test query with half array term expr
        method: query with half array value
        expected: verify query result
        """
        half = ct.default_nb // 2
        collection_w, partition_w, _, df_default = self.insert_entities_into_two_partitions_in_half(half)
        int_values = df_default[ct.default_int_field_name].values.tolist()
        float_values = df_default[ct.default_float_field_name].values.tolist()
        vec_values = df_default[ct.default_float_vec_field_name].values.tolist()
        term_expr = f'{ct.default_int_field_name} in {int_values}'
        res, _ = collection_w.query(term_expr)
        assert len(res) == half
        for i in half:
            assert res[i][ct.default_int_field_name] == int_values[i]
            assert res[i][ct.default_float_field_name] == float_values[i]
            assert res[i][ct.default_float_vec_field_name] == vec_values[i]

    def test_query_expr_repeated_term_array(self):
        """
        target: test query with repeated term array on primary field with unique value
        method: query with repeated array value
        expected: verify query result
        """
        collection_w, vectors, _, = self.init_collection_general(prefix, insert_data=True)
        int_values = [0, 0]
        term_expr = f'{ct.default_int_field_name} in {int_values}'
        res, _ = collection_w.query(term_expr)
        assert len(res) == 1
        assert res[0][ct.default_int_field_name] == int_values[0]

    def test_query_after_index(self, get_simple_index):
        """
        target: test query after creating index
        method: query after index
        expected: query result is correct
        """
        # entities, ids = init_data(connect, collection)
        # assert len(ids) == ut.default_nb
        # connect.create_index(collection, ut.default_float_vec_field_name, get_simple_index)
        # connect.load_collection(collection)
        # res = connect.query(collection, default_term_expr)
        # logging.getLogger().info(res)

    def test_query_after_search(self):
        """
        target: test query after search
        method: query after search
        expected: query result is correct
        """
        # entities, ids = init_data(connect, collection)
        # assert len(ids) == ut.default_nb
        # top_k = 10
        # nq = 2
        # query, _ = ut.gen_query_vectors(ut.default_float_vec_field_name, entities, top_k=top_k, nq=nq)
        # connect.load_collection(collection)
        # search_res = connect.search(collection, query)
        # assert len(search_res) == nq
        # assert len(search_res[0]) == top_k
        # query_res = connect.query(collection, default_term_expr)
        # logging.getLogger().info(query_res)

    def test_query_partition_repeatedly(self):
        """
        target: test query repeatedly on partition
        method: query on partition twice
        expected: verify query result
        """
        conn = self._connect()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        partition_w.insert(df)
        conn.flush([collection_w.name])
        partition_w.load()
        res_one, _ = collection_w.query(default_term_expr, partition_names=[partition_w.name])
        res_two, _ = collection_w.query(default_term_expr, partition_names=[partition_w.name])
        assert res_one == res_two

    def test_query_another_partition(self):
        """
        target: test query another partition
        method: 1. insert entities into two partitions
                2.query on one partition and query result empty
        expected: query result is empty
        """
        half = ct.default_nb // 2
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)
        term_expr = f'{ct.default_int_field_name} in [{half}]'
        # half entity in _default partition rather than partition_w
        res, _ = collection_w.query(term_expr, partition_names=[partition_w.name])
        assert len(res) == 0

    def test_query_multi_partitions_multi_results(self):
        """
        target: test query on multi partitions and get multi results
        method: 1.insert entities into two partitions
                2.query on two partitions and query multi result
        expected: query results from two partitions
        """
        half = ct.default_nb // 2
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)
        term_expr = f'{ct.default_int_field_name} in [{half - 1}, {half}]'
        # half entity in _default, half-1 entity in partition_w
        res, _ = collection_w.query(term_expr, partition_names=[ct.default_partition_name, partition_w.name])
        assert len(res) == 2

    def test_query_multi_partitions_single_result(self):
        """
        target: test query on multi partitions and get single result
        method: 1.insert into two partitions
                2.query on two partitions and query single result
        expected: query from two partitions and get single result
        """
        half = ct.default_nb // 2
        collection_w, partition_w = self.insert_entities_into_two_partitions_in_half(half)
        term_expr = f'{ct.default_int_field_name} in [{half}]'
        # half entity in _default
        res, _ = collection_w.query(term_expr, partition_names=[ct.default_partition_name, partition_w.name])
        assert len(res) == 1
        assert res[0][ct.default_int_field_name] == half

    def insert_entities_into_two_partitions_in_half(self, half):
        """
        insert default entities into two partitions(partition_w and _default) in half(int64 and float fields values)
        :param half: half of nb
        :return: collection wrap and partition wrap
        """
        conn = self._connect()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        # insert [0, half) into partition_w
        df_partition = cf.gen_default_dataframe_data(nb=half, start=0)
        partition_w.insert(df_partition)
        # insert [half, nb) into _default
        df_default = cf.gen_default_dataframe_data(nb=half, start=half)
        collection_w.insert(df_default)
        conn.flush([collection_w.name])
        collection_w.load()
        return collection_w, partition_w, df_partition, df_default
