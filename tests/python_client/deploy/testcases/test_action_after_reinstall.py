import pytest
import random
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.milvus_sys import MilvusSys
from utils.util_pymilvus import *
from deploy.base import TestDeployBase
from deploy import common as dc
from deploy.common import gen_index_param, gen_search_param

default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
binary_field_name = default_binary_vec_field_name
default_search_exp = "int64 >= 0"
default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'


class TestActionBeforeReinstall(TestDeployBase):
    """ Test case of action before reinstall """

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." %
                 method.__name__)
        log.info("skip drop collection")

    @pytest.mark.skip()
    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("index_type", dc.all_index_types)  # , "BIN_FLAT"
    def test_task_1(self, index_type, data_size):
        """
        before reinstall: create collection and insert data, load and search
        after reinstall: get collection, search, create index, load, and search
        """
        name = "task_1_" + index_type
        insert_data = False
        is_binary = True if "BIN" in index_type else False
        is_flush = False
        # init collection
        collection_w = self.init_collection_general(insert_data=insert_data, is_binary=is_binary, nb=data_size,
                                                    is_flush=is_flush, name=name)[0]
        if is_binary:
            _, vectors_to_search = cf.gen_binary_vectors(
                default_nb, default_dim)
            default_search_field = ct.default_binary_vec_field_name
        else:
            vectors_to_search = cf.gen_vectors(default_nb, default_dim)
            default_search_field = ct.default_float_vec_field_name
        search_params = gen_search_param(index_type)[0]

        # search
        collection_w.search(vectors_to_search[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})
        # query
        output_fields = [ct.default_int64_field_name]
        collection_w.query(default_term_expr, output_fields=output_fields,
                           check_task=CheckTasks.check_query_not_empty)
        # create index
        default_index = gen_index_param(index_type)
        collection_w.create_index(default_search_field, default_index)
        # release and load after creating index
        collection_w.release()
        collection_w.load()
        # search
        collection_w.search(vectors_to_search[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})
        # query
        output_fields = [ct.default_int64_field_name]
        collection_w.query(default_term_expr, output_fields=output_fields,
                           check_task=CheckTasks.check_query_not_empty)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("index_type", dc.all_index_types)  # , "BIN_FLAT"
    def test_task_2(self, index_type, data_size):
        """
        before reinstall: create collection, insert data and create index,load and search
        after reinstall: get collection, search, insert data, create index, load, and search
        """
        name = "task_2_" + index_type
        is_binary = True if "BIN" in index_type else False
        # init collection
        collection_w = self.init_collection_general(insert_data=False, is_binary=is_binary, nb=data_size,
                                                    is_flush=False, name=name, active_trace=True)[0]
        vectors_to_search = cf.gen_vectors(default_nb, default_dim)
        default_search_field = ct.default_float_vec_field_name
        if is_binary:
            _, vectors_to_search = cf.gen_binary_vectors(
                default_nb, default_dim)
            default_search_field = ct.default_binary_vec_field_name

        search_params = gen_search_param(index_type)[0]
        output_fields = [ct.default_int64_field_name]
        # search
        collection_w.search(vectors_to_search[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})
        # query
        collection_w.query(default_term_expr, output_fields=output_fields,
                           check_task=CheckTasks.check_query_not_empty)
        # insert data
        self.init_collection_general(insert_data=True, is_binary=is_binary, nb=data_size,
                                     is_flush=False, name=name, active_trace=True)
        # create index
        default_index = gen_index_param(index_type)
        collection_w.create_index(default_search_field, default_index)
        # release and load after
        collection_w.release()
        collection_w.load()
        # search
        collection_w.search(vectors_to_search[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})
        # query
        collection_w.query(default_term_expr, output_fields=output_fields,
                           check_task=CheckTasks.check_query_not_empty)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("replica_number", [0,1,2])
    @pytest.mark.parametrize("is_compacted", [True, False])
    @pytest.mark.parametrize("is_deleted", [True, False])
    @pytest.mark.parametrize("is_string_indexed", [True, False])
    @pytest.mark.parametrize("is_vector_indexed", [True, False])  # , "BIN_FLAT"
    @pytest.mark.parametrize("segment_status", ["only_growing", "sealed", "all"])  # , "BIN_FLAT"
    # @pytest.mark.parametrize("is_empty", [True, False])  # , "BIN_FLAT" (keep one is enough)
    @pytest.mark.parametrize("index_type", random.sample(dc.all_index_types, 3))  # , "BIN_FLAT"
    def test_task_all(self, index_type, is_compacted,
                        segment_status, is_vector_indexed, is_string_indexed, replica_number, is_deleted, data_size):
        """
        before reinstall: create collection and insert data, load and search
        after reinstall: get collection, search, create index, load, and search
        """
        name = f"index_type_{index_type}_segment_status_{segment_status}_is_vector_indexed_{is_vector_indexed}_is_string_indexed_{is_string_indexed}_is_compacted_{is_compacted}_is_deleted_{is_deleted}_replica_number_{replica_number}_data_size_{data_size}"
        ms = MilvusSys()
        is_binary = True if "BIN" in index_type else False
        # insert with small size data without flush to get growing segment
        collection_w = self.init_collection_general(insert_data=True, is_binary=is_binary, nb=3000,
                                                    is_flush=False, name=name)[0]
        
        # load for growing segment
        if replica_number > 0:
            collection_w.load(replica_number=replica_number)

        delete_expr = f"{ct.default_int64_field_name} in [0,1,2,3,4,5,6,7,8,9]"
        # delete data for growing segment
        if is_deleted:
            collection_w.delete(expr=delete_expr)
        if segment_status == "only_growing":
            pytest.skip("already get growing segment, skip testcase")
        # insert with flush multiple times to generate multiple sealed segment
        for i in range(5):
            self.init_collection_general(insert_data=True, is_binary=is_binary, nb=data_size,
                                                is_flush=False, name=name)[0]
        if is_binary:
            default_index_field = ct.default_binary_vec_field_name
        else:
            default_index_field = ct.default_float_vec_field_name
        if is_vector_indexed:
            # create index
            default_index_param = gen_index_param(index_type)
            collection_w.create_index(default_index_field, default_index_param)
        if is_string_indexed:
            # create index
            default_string_index_params = {}
            collection_w.create_index(default_string_field_name, default_string_index_params)
        # delete data for sealed segment
        delete_expr = f"{ct.default_int64_field_name} in [10,11,12,13,14,15,16,17,18,19]"
        if is_deleted:
            collection_w.delete(expr=delete_expr)
        if is_compacted:
            collection_w.compact()
        # reload after flush and create index
        if replica_number > 0:
            collection_w.release()
            collection_w.load(replica_number=replica_number)
        


