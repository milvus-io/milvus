import pytest
from base.ClientRequest import ApiReq
from utils.util_log import my_log as log
from common.common_type import *
from common.common_func import *


class TestCollection(ApiReq):
    """ Test case of collection interface """

    @pytest.mark.tags(CaseLabel.L3)
    def test_case(self):
        schema = get_default_collection_schema()
        c_name = get_unique_str("collection")
        collection, _ = self.collection.collection_init(c_name, schema=schema, check_res='')
        log.info(type(collection))
        assert collection.is_empty
