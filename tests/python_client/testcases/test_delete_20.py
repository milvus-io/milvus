import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from common.common_type import CaseLabel, CheckTasks

prefix = "delete"
half_nb = ct.default_nb // 2
tmp_nb = 100
tmp_expr = f'{ct.default_int64_field_name} in {[0]}'


class TestDeleteParams(TestcaseBase):
    """
    Test case of delete interface
    def delete(expr, partition_name=None, timeout=None, **kwargs)
    return MutationResult
    Only the `in` operator is supported in the expr
    """

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize('is_binary', [False, True])
    def test_delete_entities(self, is_binary):
        """
        target: test delete data from collection
        method: 1.create and insert nb
                2. delete half of nb
        expected: assert num entities
        """
        # init collection with default_nb default data
        collection_w, _, _, ids = self.init_collection_general(prefix, insert_data=True, is_binary=is_binary)
        expr = f'{ct.default_int64_field_name} in {ids[0][:half_nb]}'

        # delete half of data
        collection_w.delete(expr)
        assert collection_w.num_entities == half_nb

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_expr_none(self):
        """
        target: test delete with None expr
        method: delete with None expr
        expected: todo
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        collection_w.delete(None)
        log.debug(collection_w.num_entities)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr", [1, "12-s", "中文", [], ()])
    @pytest.mark.skip(reason="Delete function is not implemented")
    def test_delete_expr_non_string(self, expr):
        """
        target: test delete with non-string expression
        method: delete with non-string expr
        expected: raise exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        error = {ct.err_code: 0, ct.err_msg: "..."}
        collection_w.delete(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L1)
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
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.skip(reason="Delete function is not implemented")
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
        collection_w.delete(expr)
        assert collection_w.num_entities == tmp_nb - 1

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_expr_all_values(self):
        """
        target: test delete with all values
        method: delete with expr: "id in [all]"
        expected: num entities becomes zero
        """
        # init collection with default_nb default data
        collection_w, _, _, ids = self.init_collection_general(prefix, insert_data=True)
        expr = f'{ct.default_int64_field_name} in {ids[0]}'
        collection_w.delete(expr)
        assert collection_w.num_entities == 0
        assert collection_w.is_empty

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_not_existed_values(self):
        """
        target: test delete not existed values
        method: delete data not in the collection
        expected: raise exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        expr = f'{ct.default_int64_field_name} in {[tmp_nb]}'

        # raise exception
        error = {ct.err_code: 0, ct.err_msg: "..."}
        collection_w.delete(expr=expr, check_task=CheckTasks.err_res, check_items=error)


@pytest.mark.skip(reason="Waiting for development")
class TestDeleteOperation(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test delete interface operations
    ******************************************************************
    """

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_empty_collection(self):
        """
        target: test delete entities from an empty collection
        method: create an collection and delete entities
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        # raise exception
        error = {ct.err_code: 0, ct.err_msg: "..."}
        collection_w.delete(expr=tmp_expr, check_task=CheckTasks.err_res, check_items=error)
        collection_w.delete(tmp_expr)

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_entities_repeatedly(self):
        """
        target: test delete entities twice
        method: delete with same expr twice
        expected: raise exception
        """
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        # assert delete successfully
        collection_w.delete(expr=tmp_expr)
        # raise exception
        error = {ct.err_code: 0, ct.err_msg: "..."}
        collection_w.delete(expr=tmp_expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_after_index(self):
        """
        target: test delete after creating index
        method: 1.create index 2.delete entities
        expected: assert index and num entities
        """
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]

        # create index
        index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params)
        assert collection_w.has_index()[0]
        # delete entity
        collection_w.delete(tmp_expr)
        assert collection_w.num_entities == tmp_nb - 1
        assert collection_w.has_index()

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_query(self):
        """
        target: test delete and query
        method: query entity after it was deleted
        expected: query result is empty
        """
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        # assert delete successfully
        collection_w.delete(expr=tmp_expr)
        res = collection_w.query(expr=tmp_expr)[0]
        assert len(res) == 0
