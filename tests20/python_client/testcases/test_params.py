import pytest
from milvus import DataType

from common.common_type import *
from common.common_func import *
from base.client_request import ApiReq
from utils.util_log import test_log as log


class TestParams(ApiReq):
    def test_1(self):
        self.connection.configure(check_res='', default={"host": "192.168.1.240", "port": "19530"})
        res_ = self.connection.get_connection(alias='default')
        log.info("res : %s" % str(res_))
        log.info("self.connection : %s" % str(self.connection))
        log.info("self.collection : %s" % str(self.collection))
        log.info("self.partition : %s" % str(self.partition))
        log.info("self.index : %s" % str(self.index))
        log.info("self.utility : %s" % str(self.utility))


#     @pytest.mark.parametrize("collection_name", get_invalid_strs)
#     @pytest.mark.parametrize("fields", [get_binary_default_fields()])
#     @pytest.mark.parametrize("partition_tag, field_name, params, entities",
#                              [(default_tag, default_float_vec_field_name, gen_simple_index()[0], get_entities()[0])])
#     def test_collection_name_params_check(self, collection_name, fields, partition_tag, field_name, params, entities):
#
#         self.create_collection(collection_name, fields, check_res=cname_param_check)
#         self.get_collection_stats(collection_name, check_res=cname_param_check)
#         self.flush(collection_name, check_res=cname_param_check)
#
#         self.drop_collection(collection_name, check_res=cname_param_check)
#         self.has_collection(collection_name, check_res=cname_param_check)
#         self.describe_collection(collection_name, check_res=cname_param_check)
#         self.load_collection(collection_name, check_res=cname_param_check)
#         self.release_collection(collection_name, check_res=cname_param_check)
#
#         self.create_partition(collection_name, partition_tag, check_res=cname_param_check)
#         self.drop_partition(collection_name, partition_tag, check_res=cname_param_check)
#         self.has_partition(collection_name, partition_tag, check_res=cname_param_check)
#         self.load_partitions(collection_name, partition_tag, check_res=cname_param_check)
#         self.release_partitions(collection_name, partition_tag, check_res=cname_param_check)
#         self.list_partitions(collection_name, check_res=cname_param_check)
#
#         self.create_index(collection_name, field_name, params, check_res=cname_param_check)
#         self.drop_index(collection_name, field_name, check_res=cname_param_check)
#         self.describe_index(collection_name, field_name, check_res=cname_param_check)
#         self.search(collection_name, dsl=[1], check_res=cname_param_check)
#
#         # self.insert(collection_name, entities=[1])
#
#     @pytest.mark.parametrize("collection_name, fields", [(get_unique_str(), get_binary_default_fields())])
#     @pytest.mark.parametrize("partition_tag", get_invalid_strs)
#     def test_partition_tag_params_check(self, collection_name, fields, partition_tag):
#         self.create_collection(collection_name, fields)
#
#         self.create_partition(collection_name, partition_tag, check_res=pname_param_check)
#         self.drop_partition(collection_name, partition_tag, check_res=pname_param_check)
#         self.has_partition(collection_name, partition_tag, check_res=pname_param_check)
#         """No parameter check"""
#         # self.load_partitions(collection_name, partition_tag, check_res=None)
#         # self.release_partitions(collection_name, partition_tag, check_res=None)
#
#         self.drop_collection(collection_name)
