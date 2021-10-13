import pytest

from base.client_base import TestcaseBase
# from common import common_func as cf
from common import common_type as ct
# from utils.util_log import test_log as log
from common.common_type import CaseLabel

prefix = "delete"


class TestDeleteParams(TestcaseBase):
    """
    Test case of delete interface
    def delete(expr, partition_name=None, timeout=None, **kwargs)
    return MutationResult
    Only the `in` operator is supported in the expr
    """

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L0)
    def test_delete(self):
        """
        target: test delete data from collection
        method: 1.create and insert nb
                2. delete half of nb
        expected: assert num entities
        """
        # init collection with nb default data
        collection_w, data = self.init_collection_general(prefix, insert_data=True)[:2]
        assert collection_w.num_entities == ct.default_nb
        half = ct.default_nb // 2
        expr = f'{ct.default_int64_field_name} in {data[0][:half]}'
        # delete half of data
        collection_w.delete(expr)
        assert collection_w.num_entities == half

    @pytest.mark.skip(reason="Delete function is not implemented")
    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_empty(self):
        """
        target: test delete with empty array expr
        method: delete with expr: "id in []"
        expected: assert num entities
        """
        # init collection with nb default data
        collection_w = self.init_collection_general(prefix, insert_data=True)[0]
        expr = f'{ct.default_int64_field_name} in {[]}'
        # delete half of data
        collection_w.delete(expr)
        assert collection_w.num_entities == ct.default_nb


@pytest.mark.skip(reason="Waiting for development")
class TestDeleteOperation(TestcaseBase):
    pass
