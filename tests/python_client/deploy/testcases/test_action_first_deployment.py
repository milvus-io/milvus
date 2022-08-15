import pytest
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.milvus_sys import MilvusSys
from utils.util_pymilvus import *
from deploy.base import TestDeployBase
from deploy.common import gen_index_param, gen_search_param
from utils.util_log import test_log as log


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

prefix = "deploy_test"


class TestActionFirstDeployment(TestDeployBase):
    """ Test case of action before reinstall """

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." %
                 method.__name__)
        log.info("skip drop collection")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("index_type", ["HNSW","BIN_IVF_FLAT"])
    def test_task_all_empty(self,index_type):
        """
        before reinstall: create collection
        """
        name = ""
        for k,v in locals().items():
            if k in ["self", "name"]:
                continue
            name += f"_{k}_{v}"
        name = prefix + name + "_" + "empty"
        is_binary = False
        if "BIN" in name:
            is_binary = True
        self.init_collection_general(insert_data=False, is_binary=is_binary, name=name)[0]


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("replica_number", [0, 1, 2])
    @pytest.mark.parametrize("is_compacted", ["is_compacted", "not_compacted"])
    @pytest.mark.parametrize("is_deleted", ["is_deleted", "not_deleted"])
    @pytest.mark.parametrize("is_string_indexed", ["is_string_indexed", "not_string_indexed"])
    @pytest.mark.parametrize("is_vector_indexed", ["is_vector_indexed", "not_vector_indexed"])
    @pytest.mark.parametrize("segment_status", ["only_growing", "only_sealed", "all"])
    @pytest.mark.parametrize("index_type", ["HNSW", "BIN_IVF_FLAT"]) #"IVF_FLAT", "HNSW", "BIN_IVF_FLAT"
    def test_task_all(self, index_type, is_compacted,
                      segment_status, is_vector_indexed, is_string_indexed, replica_number, is_deleted, data_size):
        """
        before reinstall: create collection and insert data, load and search
        """
        name = ""
        for k,v in locals().items():
            if k in ["self", "name"]:
                continue
            name += f"_{k}_{v}"
        name = prefix + name
        log.info(f"collection name: {name}")
        self._connect()
        ms = MilvusSys()
        if len(ms.query_nodes) < replica_number:
            # this step is to make sure this testcase can run on standalone mode
            # or cluster mode which has only one querynode
            pytest.skip("skip test, not enough nodes")

        log.info(f"collection name: {name}, replica_number: {replica_number}, is_compacted: {is_compacted},"
                 f"is_deleted: {is_deleted}, is_vector_indexed: {is_vector_indexed}, is_string_indexed: {is_string_indexed},"
                 f"segment_status: {segment_status}, index_type: {index_type}")

        is_binary = True if "BIN" in index_type else False

        # params for search and query
        if is_binary:
            _, vectors_to_search = cf.gen_binary_vectors(
                default_nb, default_dim)
            default_search_field = ct.default_binary_vec_field_name
        else:
            vectors_to_search = cf.gen_vectors(default_nb, default_dim)
            default_search_field = ct.default_float_vec_field_name
        search_params = gen_search_param(index_type)[0]

        # init collection and insert with small size data without flush to get growing segment
        collection_w = self.init_collection_general(insert_data=True, is_binary=is_binary, nb=3000,
                                                    is_flush=False, is_index=True, name=name)[0]
        # load for growing segment
        if replica_number >= 1:
            try:
                collection_w.release()
            except Exception as e:
                log.error(
                    f"release collection failed: {e} maybe the collection is not loaded")
            collection_w.load(replica_number=replica_number)

        # delete data for growing segment
        delete_expr = f"{ct.default_int64_field_name} in {[i for i in range(0,10)]}"
        if is_deleted == "is_deleted":
            collection_w.delete(expr=delete_expr)

        # search and query for growing segment
        if replica_number >= 1:
            collection_w.search(vectors_to_search[:default_nq], default_search_field,
                                search_params, default_limit,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                            "limit": default_limit})
            output_fields = [ct.default_int64_field_name]
            collection_w.query(default_term_expr, output_fields=output_fields,
                            check_task=CheckTasks.check_query_not_empty)

        # skip subsequent operations when segment_status is set to only_growing
        if segment_status == "only_growing":
            pytest.skip(
                "already get growing segment, skip subsequent operations")
        # insert with flush multiple times to generate multiple sealed segment
        for i in range(5):
            self.init_collection_general(insert_data=True, is_binary=is_binary, nb=data_size,
                                         is_flush=False, is_index=True, name=name)
            # at this step, all segment are sealed
            collection_w.flush()
        # delete data for sealed segment and before index
        delete_expr = f"{ct.default_int64_field_name} in {[i for i in range(10,20)]}"
        if is_deleted == "is_deleted":
            collection_w.delete(expr=delete_expr)       
        # params for creating index
        if is_binary:
            default_index_field = ct.default_binary_vec_field_name
        else:
            default_index_field = ct.default_float_vec_field_name

        # create index for vector
        if is_vector_indexed == "is_vector_indexed":
            default_index_param = gen_index_param(index_type)
            collection_w.create_index(default_index_field, default_index_param)

        # create index for string
        if is_string_indexed == "is_string_indexed":
            default_string_index_params = {}
            default_string_index_name = "_default_string_idx"
            collection_w.create_index(
                default_string_field_name, default_string_index_params, index_name=default_string_index_name)

        # delete data for sealed segment and afer index
        delete_expr = f"{ct.default_int64_field_name} in {[i for i in range(20,30)]}"
        if is_deleted == "is_deleted":
            collection_w.delete(expr=delete_expr)
        if is_compacted == "is_compacted":
            collection_w.compact()
        # get growing segment before reload
        if segment_status == "all":
            self.init_collection_general(insert_data=True, is_binary=is_binary, nb=3000,
                                         is_flush=False, is_index=True, name=name)
        # reload after flush and creating index
        if replica_number > 0:
            collection_w.release()
            collection_w.load(replica_number=replica_number)

        # insert data to get growing segment after reload
        if segment_status == "all":
            self.init_collection_general(insert_data=True, is_binary=is_binary, nb=3000,
                                         is_flush=False, is_index=True, name=name)
        
        # search and query for sealed and growing segment
        if replica_number > 0:
            collection_w.search(vectors_to_search[:default_nq], default_search_field,
                                search_params, default_limit,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                            "limit": default_limit})
            output_fields = [ct.default_int64_field_name]
            collection_w.query(default_term_expr, output_fields=output_fields,
                            check_task=CheckTasks.check_query_not_empty)