import pytest
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.milvus_sys import MilvusSys
from utils.util_pymilvus import *
from deploy.base import TestDeployBase
from deploy.common import gen_index_param, gen_search_param, get_collections
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



class TestActionSecondDeployment(TestDeployBase):
    """ Test case of action before reinstall """

    @pytest.fixture(scope="function", params=get_collections())
    def all_collection_name(self, request):
        if request.param == [] or request.param == "":
            pytest.skip("The collection name is invalid")
        yield request.param

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." %
                 method.__name__)
        log.info("skip drop collection")

    @pytest.mark.tags(CaseLabel.L3)
    def test_check(self, all_collection_name, data_size):
        """
        before reinstall: create collection
        """
        self._connect()
        ms = MilvusSys()
        name = all_collection_name
        is_binary = False
        if "BIN" in name:
            is_binary = True
        collection_w = self.init_collection_general(
            insert_data=False, name=name, is_binary=is_binary, active_trace=True)[0]
        schema = collection_w.schema
        data_type = [field.dtype.name for field in schema.fields]
        field_name = [field.name for field in schema.fields]
        type_field_map = dict(zip(data_type,field_name))
        if is_binary:
            default_index_field = ct.default_binary_vec_field_name
            vector_index_type = "BIN_IVF_FLAT"
        else:
            default_index_field = ct.default_float_vec_field_name
            vector_index_type = "IVF_FLAT"       
        
        is_vector_indexed = False
        is_string_indexed = False
        indexed_fields = [index.field_name for index in collection_w.indexes]
        binary_vector_index_types = [index.params["index_type"] for index in collection_w.indexes if index.field_name == type_field_map.get("BINARY_VECTOR", "")]
        float_vector_index_types = [index.params["index_type"] for index in collection_w.indexes if index.field_name == type_field_map.get("FLOAT_VECTOR", "")]
        string_index_types = [index.params["index_type"] for index in collection_w.indexes if index.field_name == type_field_map.get("VARCHAR", "")]
        index_names = [index.index_name for index in collection_w.indexes] # used to drop index
        vector_index_types = binary_vector_index_types + float_vector_index_types
        if len(vector_index_types) > 0:
            is_vector_indexed = True
            vector_index_type = vector_index_types[0]

        if len(string_index_types) > 0:
            is_string_indexed = True
 
        try:
            replicas, _ = collection_w.get_replicas(enable_traceback=False)
            replicas_loaded = len(replicas.groups)
        except Exception as e:
            log.info("get replicas failed")
            replicas_loaded = 0
        # params for search and query
        if is_binary:
            _, vectors_to_search = cf.gen_binary_vectors(
                default_nb, default_dim)
            default_search_field = ct.default_binary_vec_field_name
        else:
            vectors_to_search = cf.gen_vectors(default_nb, default_dim)
            default_search_field = ct.default_float_vec_field_name
        search_params = gen_search_param(vector_index_type)[0]        
        
        # load if not loaded
        if replicas_loaded == 0:
            collection_w.load()
        
        # search and query
        if "empty" in name:
            # if the collection is empty, the search result should be empty, so no need to check            
            check_task = None
        else:
            check_task = CheckTasks.check_search_results
        
        collection_w.search(vectors_to_search[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            output_fields=[ct.default_int64_field_name],
                            check_task=check_task,
                            check_items={"nq": default_nq,
                                        "limit": default_limit})
        if "empty" in name:
            check_task = None
        else:
            check_task = CheckTasks.check_query_not_empty
        collection_w.query(default_term_expr, output_fields=[ct.default_int64_field_name],
                        check_task=check_task)

        # flush
        collection_w.num_entities

        # search and query
        if "empty" in name:
            check_task = None
        else:
            check_task = CheckTasks.check_search_results
        collection_w.search(vectors_to_search[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            output_fields=[ct.default_int64_field_name],
                            check_task=check_task,
                            check_items={"nq": default_nq,
                                        "limit": default_limit})
        if "empty" in name:
            check_task = None
        else:
            check_task = CheckTasks.check_query_not_empty
        collection_w.query(default_term_expr, output_fields=[ct.default_int64_field_name],
                        check_task=check_task)
        
        # insert data and flush
        for i in range(2):
            self.init_collection_general(insert_data=True, is_binary=is_binary, nb=data_size,
                                         is_flush=False, is_index=True, name=name)
        collection_w.num_entities
        
        # delete data
        delete_expr = f"{ct.default_int64_field_name} in [0,1,2,3,4,5,6,7,8,9]"
        collection_w.delete(expr=delete_expr)

        # search and query
        collection_w.search(vectors_to_search[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            output_fields=[ct.default_int64_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                        "limit": default_limit})
        collection_w.query(default_term_expr, output_fields=[ct.default_int64_field_name],
                        check_task=CheckTasks.check_query_not_empty)
        
        # drop index if exist
        if len(index_names) > 0:
            for index_name in index_names:
                collection_w.drop_index(index_name=index_name)
            # search and query after dropping index
            collection_w.search(vectors_to_search[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            output_fields=[ct.default_int64_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                        "limit": default_limit})
            collection_w.query(default_term_expr, output_fields=[ct.default_int64_field_name],
                            check_task=CheckTasks.check_query_not_empty)        

        # create index
        default_index_param = gen_index_param(vector_index_type)
        collection_w.create_index(default_index_field, default_index_param, index_name=cf.gen_unique_str())    
        collection_w.create_index(default_string_field_name, {}, index_name=cf.gen_unique_str())

        # search and query
        collection_w.search(vectors_to_search[:default_nq], default_search_field,
                        search_params, default_limit,
                        default_search_exp,
                        output_fields=[ct.default_int64_field_name],
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": default_nq,
                                    "limit": default_limit})
        collection_w.query(default_term_expr, output_fields=[ct.default_int64_field_name],
                        check_task=CheckTasks.check_query_not_empty)          

        # release and reload with changed replicas
        collection_w.release()
        replica_number = 1
        if replicas_loaded in [0,1] and len(ms.query_nodes)>=2 :
            replica_number = 2
        collection_w.load(replica_number=replica_number)

        # search and query
        collection_w.search(vectors_to_search[:default_nq], default_search_field,
                        search_params, default_limit,
                        default_search_exp,
                        output_fields=[ct.default_int64_field_name],
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": default_nq,
                                    "limit": default_limit})
        collection_w.query(default_term_expr, output_fields=[ct.default_int64_field_name],
                        check_task=CheckTasks.check_query_not_empty)