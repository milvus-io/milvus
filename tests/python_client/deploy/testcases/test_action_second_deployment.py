import pytest
import re
import time
import pymilvus
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.milvus_sys import MilvusSys
from utils.util_pymilvus import *
from deploy.base import TestDeployBase
from deploy.common import gen_index_param, gen_search_param, get_deploy_test_collections
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
binary_field_name = ct.default_binary_vec_field_name
default_search_exp = "int64 >= 0"
default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'

pymilvus_version = pymilvus.__version__


class TestActionSecondDeployment(TestDeployBase):
    """ Test case of action before reinstall """

    @pytest.fixture(scope="function", params=get_deploy_test_collections())
    def all_collection_name(self, request):
        if request.param == [] or request.param == "":
            pytest.skip("The collection name is invalid")
        yield request.param

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." %
                 method.__name__)
        log.info("show collection info")
        log.info(f"collection {self.collection_w.name} has entities: {self.collection_w.num_entities}")

        res, _ = self.utility_wrap.get_query_segment_info(self.collection_w.name)
        log.info(f"The segment info of collection {self.collection_w.name} is {res}")

        index_infos = [index.to_dict() for index in self.collection_w.indexes]
        log.info(f"collection {self.collection_w.name} index infos {index_infos}")
        log.info("skip drop collection")

    def create_index(self, collection_w, default_index_field, default_index_param):

        index_field_map = dict([(index.field_name, index.index_name) for index in collection_w.indexes])
        index_infos = [index.to_dict() for index in collection_w.indexes]
        log.info(f"index info: {index_infos}")
        # log.info(f"{default_index_field:} {default_index_param:}")
        if len(index_infos) > 0:
            log.info(
                f"current index param is {index_infos[0]['index_param']}, passed in param is {default_index_param}")
            log.info(
                f"current index name is {index_infos[0]['index_name']}, passed in param is {index_field_map.get(default_index_field)}")
        collection_w.create_index(default_index_field, default_index_param,
                                  index_name=index_field_map.get(default_index_field, gen_unique_str("test")))
        collection_w.create_index(default_string_field_name, {},
                                  index_name=index_field_map.get(default_string_field_name, gen_unique_str("test")))

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
        collection_w, _ = self.collection_wrap.init_collection(name=name)
        self.collection_w = collection_w
        schema = collection_w.schema
        data_type = [field.dtype for field in schema.fields]
        field_name = [field.name for field in schema.fields]
        type_field_map = dict(zip(data_type, field_name))
        if is_binary:
            default_index_field = ct.default_binary_vec_field_name
            vector_index_type = "BIN_IVF_FLAT"
        else:
            default_index_field = ct.default_float_vec_field_name
            vector_index_type = "IVF_FLAT"

        binary_vector_index_types = [index.params["index_type"] for index in collection_w.indexes if
                                     index.field_name == type_field_map.get(100, "")]
        float_vector_index_types = [index.params["index_type"] for index in collection_w.indexes if
                                    index.field_name == type_field_map.get(101, "")]
        index_field_map = dict([(index.field_name, index.index_name) for index in collection_w.indexes])
        index_names = [index.index_name for index in collection_w.indexes]  # used to drop index
        vector_index_types = binary_vector_index_types + float_vector_index_types
        if len(vector_index_types) > 0:
            vector_index_type = vector_index_types[0]
        try:
            t0 = time.time()
            self.utility_wrap.wait_for_loading_complete(name)
            log.info(f"wait for {name} loading complete cost {time.time() - t0}")
        except Exception as e:
            log.error(e)
        # get replicas loaded
        try:
            replicas = collection_w.get_replicas(enable_traceback=False)
            replicas_loaded = len(replicas.groups)
        except Exception as e:
            log.error(e)
            replicas_loaded = 0

        log.info(f"collection {name} has {replicas_loaded} replicas")
        actual_replicas = re.search(r'replica_number_(.*?)_', name).group(1)
        assert replicas_loaded == int(actual_replicas)
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
            # create index for vector if not exist before load
            is_vector_indexed = False
            index_infos = [index.to_dict() for index in collection_w.indexes]
            for index_info in index_infos:
                if "metric_type" in index_info.keys() or "metric_type" in index_info["index_param"]:
                    is_vector_indexed = True
                    break
            if is_vector_indexed is False:
                default_index_param = gen_index_param(vector_index_type)
                self.create_index(collection_w, default_index_field, default_index_param)
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
        if pymilvus_version >= "2.2.0":
            collection_w.flush()
        else:
            collection_w.collection.num_entities

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
            self.insert_data_general(insert_data=True, is_binary=is_binary, nb=data_size,
                                    is_flush=False, is_index=True, name=name,
                                     enable_dynamic_field=False, with_json=False)
        if pymilvus_version >= "2.2.0":
            collection_w.flush()
        else:
            collection_w.collection.num_entities

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
                collection_w.release()
                collection_w.drop_index(index_name=index_name)
            default_index_param = gen_index_param(vector_index_type)
            self.create_index(collection_w, default_index_field, default_index_param)

            collection_w.load()
            collection_w.search(vectors_to_search[:default_nq], default_search_field,
                                search_params, default_limit,
                                default_search_exp,
                                output_fields=[ct.default_int64_field_name],
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "limit": default_limit})
            collection_w.query(default_term_expr, output_fields=[ct.default_int64_field_name],
                               check_task=CheckTasks.check_query_not_empty)

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
        if replicas_loaded in [0, 1] and len(ms.query_nodes) >= 2:
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
