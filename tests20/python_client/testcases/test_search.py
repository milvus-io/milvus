import threading
import time
import pytest
import random
import numpy as np

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

uid = "search_collection"
default_schema = cf.gen_default_collection_schema()
default_collection_name = cf.gen_unique_str(uid)
default_search_field = ct.default_float_vec_field_name
default_search_exp = "int64 >= 0"
default_dim = ct.default_dim
default_nq = ct.default_nq
default_limit = ct.default_limit
default_search_params = ct.default_search_params
epsilon = ct.epsilon


class TestCollectionSearch(TestcaseBase):
    """ Test case of search interface """

    def init_data(self, insert_data=False, nb=3000, partition_num=0, multiple=False, is_binary=False):
        """
        target: initialize before search
        method: create connection and collection
        expected: return collection
        """
        log.info("Test case of search interface: initialize before test case")
        if not multiple:
            self.clear_env()
        else:
            global default_collection_name
            default_collection_name = cf.gen_unique_str(uid)
        # 1 create collection
        if not is_binary:
            default_schema = cf.gen_default_collection_schema()
        else:
            default_schema = cf.gen_default_binary_collection_schema()
        log.info("init_data: collection creation")
        collection_w = self.init_collection_wrap(name=default_collection_name,
                                                 schema=default_schema)
        # 2 add extra partition if specified (default is 1 partition named "_default")
        if partition_num > 0:
            log.info("init_data: creating partitions")
            for i in range(partition_num):
                partition_name = "search_partition_" + str(i)
                collection_w.create_partition(partition_name=partition_name,
                                              description="search partition")
            par = collection_w.partitions
            assert len(par) == (partition_num + 1)
            log.info("init_data: created partitions %s" % par)
        # 3 insert data if specified
        if insert_data:
            vectors = []
            raw_vectors = []
            log.info("init_data: inserting default data into collection %s (num_entities: %s)"
                     % (collection_w.name, nb))
            if partition_num > 0:
                num = partition_num + 1
                for i in range(num):
                    if not is_binary:
                        vectors_num = [[random.random() for _ in range(default_dim)]
                                       for _ in range(nb // num)]
                        vectors += vectors_num
                        collection_w.insert([[i for i in range(nb // num)],
                                             [np.float32(i) for i in range(nb // num)],
                                             vectors_num], par[i].name)
                    else:
                        default_binary_data, binary_raw_data = \
                            cf.gen_default_binary_dataframe_data(nb // num)
                        collection_w.insert(default_binary_data, par[i].name)
                        vectors += default_binary_data
                        raw_vectors += binary_raw_data
            else:
                if not is_binary:
                    vectors = [[random.random() for _ in range(default_dim)] for _ in range(nb)]
                    collection_w.insert([[i for i in range(nb)],
                                         [np.float32(i) for i in range(nb)],
                                         vectors])
                else:
                    default_binary_data, raw_vectors = cf.gen_default_binary_dataframe_data(nb)
                    collection_w.insert(default_binary_data)
                    vectors = default_binary_data
            collection_w.load()
            assert collection_w.is_empty == False
            assert collection_w.num_entities == nb
            log.info("init_data: inserted default data into collection %s (num_entities: %s)"
                     % (collection_w.name, collection_w.num_entities))
            if not is_binary:
                return collection_w, vectors
            else:
                return collection_w, vectors, raw_vectors
        else:
            assert collection_w.is_empty == True
            assert collection_w.num_entities == 0
            return collection_w

    def clear_data(self, collection_w):
        """
        target: clear env after test case
        method: drop collection
        expected: return collection
        """
        log.info("clear_data: Dropping collection %s" % collection_w.name)
        collection_w.drop()
        log.info("clear_data: Dropped collection %s" % collection_w.name)

    def clear_env(self):
        """
        target: clear the environment before test cases
        method: delete all the collections test cases created before
        expected: deleted successfully
        """
        log.info("Test case of search interface: clearing environment")
        # 1. connect
        self._connect()
        # 2. delete all the collections test search cases created before
        log.info("clear_env: clearing env")
        res_list, _ = self.utility_wrap.list_collections()
        count = 0
        for res in res_list:
            collection, _ = self.collection_wrap.init_collection(name=res)
            if "search_collection" in res:
                self.clear_data(collection)
                count = count + 1
        res_list_new, _ = self.utility_wrap.list_collections()
        for res in res_list_new:
            assert not ("search_collection" in res)
        log.info("clear_env: cleared env (deleted %s search collections)" % count)

    def search_results_check(self, search_res, nq=default_nq, limit=default_limit):
        """
        target: check the search results
        method: 1. check the query number
                2. check the limit(topK)
                3. check the distance
        expected: check the search is ok
        """
        log.info("search_results_check: checking the searching results")
        assert len(search_res) == nq
        for hits in search_res:
            assert len(hits) == limit
            assert len(hits.ids) == limit
        log.info("search_results_check: checked the searching results")

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L3)
    def test_search_no_connection(self):
        """
        target: test search without connection
        method: create and delete connection, then search
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_no_connection (searching without connection)")
        # 1. initialize with data
        collection_w, _ = self.init_data(True)
        # 2. search
        log.info("test_search_no_connection: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, default_limit,
                                     default_search_exp)
        self.search_results_check(res)
        log.info("test_search_no_connection: searched collection %s" % collection_w.name)
        # 3. remove connection
        log.info("test_search_no_connection: removing connection")
        self.connection_wrap.remove_connection(alias='default')
        log.info("test_search_no_connection: removed connection")
        # 4. search without connection
        log.info("test_search_no_connection: searching without connection")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1, "err_msg": "no connection"})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_no_collection(self):
        """
        target: test the scenario which search the non-exist collection
        method: 1. create collection
                2. drop collection
                3. search the dropped collection
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_no_collection (searching the non-exist collection)")
        # 1. initialize without data
        collection_w = self.init_data()
        # 2. Drop collection
        self.clear_data(collection_w)
        # 3. Search without collection
        log.info("test_search_no_collection: Searching without collection ")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors, default_search_field,
                            default_search_params, default_limit, "float_vector > 0",
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "collection %s doesn't exist!" % collection_w.name})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_with_empty_collection(self):
        """
        target: test search with empty connection
        method: search the empty collection
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_with_empty_collection")
        # 1 initialize without data
        collection_w = self.init_data()
        # 2 search collection without data
        log.info("test_search_with_empty_collection: Searching empty collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "collection hasn't been loaded or has been released"})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_with_empty_vectors(self):
        """
        target: test search with empty query vector
        method: search using empty query vector
        expected: search successfully with 0 results
        """
        log.info("Test case of search interface: test_search_with_empty_vectors")
        # 1 initialize without data
        collection_w, _ = self.init_data(True, 10)
        # 2 search collection without data
        log.info("test_search_with_empty_vectors: Searching collection %s using empty vector" % collection_w.name)
        res, _ = collection_w.search([], default_search_field, default_search_params,
                                     default_limit, default_search_exp)
        assert len(res) == 0
        log.info("test_search_with_empty_vectors: test PASS with %s results" % len(res))

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_param_missing(self):
        """
        target: test search with incomplete parameters
        method: search with incomplete parameters
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_missing")
        # 1 initialize without data
        collection_w = self.init_data()
        # 2 search collection with missing parameters
        log.info("test_search_param_missing: Searching collection %s with missing parameters" % collection_w.name)
        try:
            collection_w.search()
        except TypeError as e:
            assert "missing" and "'data', 'anns_field', 'param', 'limit', and 'expression'" in str(e)
            log.info("test_search_no_collection: test PASS with expected assertion: %s" % e)

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_param_invalid_dim(self):
        """
        target: test search with invalid parameter values
        method: search with invalid dim
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_dim")
        # 1. initialize with data
        collection_w, _ = self.init_data(True)
        # 2. search with invalid dim
        log.info("test_search_param_invalid_dim: searching with invalid dim")
        wrong_dim = 129
        vectors = [[random.random() for _ in range(wrong_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "UnexpectedError"})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_param_invalid_metric_type(self):
        """
        target: test search with invalid parameter values
        method: search with invalid metric type
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_metric_type")
        # 1. initialize with data
        collection_w, _ = self.init_data(True)
        # 2.2 search with invalid metric_type
        log.info("test_search_param_invalid_metric_type: searching with invalid metric_type")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        search_params = {"metric_type": "L10", "params": {"nprobe": 10}}
        collection_w.search(vectors[:default_nq], default_search_field, search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "metric type not found"})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_param_invalid_limit(self):
        """
        target: test search with invalid parameter values
        method: search with invalid limit: 0 and maximum
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_limit")
        # 1. initialize with data
        collection_w, _ = self.init_data(True)
        # 2 search with invalid limit (topK)
        log.info("test_search_param_invalid_limit: searching with invalid limit (topK)")
        limit = 0
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        log.info("test_search_param_invalid_limit: searching with invalid limit (topK) = %s" % limit)
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "division by zero"})
        limit = 16385
        log.info("test_search_param_invalid_limit: searching with invalid max limit (topK) = %s" % limit)
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "limit %d is too large" % limit})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_param_invalid_field(self):
        """
        target: test search with invalid parameter values
        method: search with invalid field
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_field")
        # 1. initialize with data
        collection_w, _ = self.init_data(True)
        # 2 search with invalid field
        log.info("test_search_param_invalid_field: searching with invalid field")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        log.info("test_search_param_invalid_field: searching with invalid field (empty)")
        collection_w.search(vectors[:default_nq], " ", default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "invalid expression"})
        log.info("test_search_param_invalid_field: searching with invalid field")
        invalid_search_field = "floatvector"
        collection_w.search(vectors[:default_nq], invalid_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "invalid search field"})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_param_invalid_expr(self):
        """
        target: test search with invalid parameter values
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_expr")
        # 1. initialize with data
        collection_w, _ = self.init_data(True)
        # 2 search with invalid expr
        log.info("test_search_param_invalid_expr: searching with invalid expression")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        log.info("test_search_param_invalid_expr: searching with invalid expr (empty)")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, " ",
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "invalid expression"})
        log.info("test_search_param_invalid_expr: searching with invalid expr")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, "int63 >= 0",
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "invalid expression"})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_index_partition_not_existed(self):
        """
        target: test search not existed partition
        method: search with not existed partition
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_index_partition_not_existed")
        # 1. initialize with data
        collection_w, _ = self.init_data(True)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 3. search the non exist partition
        partition_name = "search_non-exist"
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, [partition_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "PartitonName: %s not found" % partition_name})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_param_invalid_binary(self):
        """
        target: test search within binary data (invalid parameter)
        method: search with wrong metric type
        expected: raise exception and report the error
        """
        log.info("test_search_param_invalid_binary: test invalid paramter with binary data")
        # 1. initialize with binary data
        collection_w, _, _ = self.init_data(insert_data=True, is_binary=True)
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        # 3. search with exception
        _, binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        wrong_search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector", wrong_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "unsupported"})
    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L3)
    def test_search_before_after_delete(self):
        """
        target: test search function before and after deletion
        method: 1 search the collection
                2 delete a partition
                3 search the collection
        expected: the deleted entities should not be searched
        """
        log.info("test_search_before_after_delete: test search after deleting entities")
        # 1. initialize with data
        partition_num = 1
        limit = 1000
        collection_w, _ = self.init_data(True, 1000, partition_num)
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        log.info("test_search_before_after_delete: searching before deleting partitions")
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, limit, default_search_exp)
        self.search_results_check(res, limit=limit)
        log.info("test_search_before_after_delete: searched before deleting partitions")
        # 3. delete partitions
        log.info("test_search_before_after_delete: deleting a partition")
        par = collection_w.partitions
        deleted_entity_num = par[partition_num].num_entities
        collection_w.drop_partition(par[partition_num].name)
        log.info("test_search_before_after_delete: deleted a partition")
        collection_w.release()
        collection_w.load()
        # 4. search after delete partitions
        log.info("test_search_before_after_delete: searching after deleting partitions")
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, limit, default_search_exp)
        self.search_results_check(res, limit=(limit-deleted_entity_num))
        log.info("test_search_before_after_delete: searched after deleting partitions")

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_after_different_index(self):
        """
        target: test search with different index
        method: test search with different index
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_after_different_index")
        # 1. initialize with data
        collection_w, _ = self.init_data(insert_data=True, partition_num=2)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create different index
        default_index_list = ["IVF_FLAT", "IVF_PQ", "IVF_SQ8", "HNSW",
                              "RHNSW_FLAT", "RHNSW_PQ", "RHNSW_SQ", "ANNOY"]
        for index in default_index_list:
            log.info("test_search_after_different_index: Creating index-%s" % index)
            default_index = {"index_type": index, "params": {"nlist": 128}, "metric_type": "L2"}
            collection_w.create_index("float_vector", default_index)
            log.info("test_search_after_different_index: Created index-%s" % index)
            # 3. search
            log.info("test_search_after_different_index: Searching after creating index-%s" % index)
            res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                         default_search_params, default_limit, default_search_exp)
            self.search_results_check(res)
            log.info("test_search_after_different_index: Searched after creating %s" % index)

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_after_index_different_metric_type(self):
        """
        target: test search with different metric type
        method: test search with different metric type
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_after_index_different_metric_type")
        # 1. initialize with data
        collection_w, _ = self.init_data(insert_data=True, partition_num=2)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create different index
        default_index_list = ["IVF_FLAT", "IVF_PQ", "IVF_SQ8", "HNSW",
                              "RHNSW_FLAT", "RHNSW_PQ", "RHNSW_SQ", "ANNOY"]
        for index in default_index_list:
            log.info("test_search_after_different_index: Creating index-%s" % index)
            default_index = {"index_type": index, "params": {"nlist": 128}, "metric_type": "IP"}
            collection_w.create_index("float_vector", default_index)
            log.info("test_search_after_different_index: Created index-%s" % index)
            # 3. search
            log.info("test_search_after_different_index: Searching after creating index-%s" % index)
            res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                         default_search_params, default_limit, default_search_exp)
            self.search_results_check(res)
            log.info("test_search_after_different_index: Searched after creating %s" % index)

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_collection_multiple_times(self):
        """
        target: test search for multiple times
        method: search for multiple times
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_collection_multiple_times")
        # 1. initialize with data
        collection_w, _ = self.init_data(True)
        # 2. search for multiple times
        N = 5
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        for i in range(N):
            log.info("test_search_collection_multiple_times: searching round %d" % (i+1))
            res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                         default_search_params, default_limit, default_search_exp)
            log.info("test_search_collection_multiple_times: Searched results (nq: %s, limit: %d)"
                     % (len(res), default_limit))
            self.search_results_check(res)
        log.info("test_search_collection_multiple_times: test PASS")

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_index_one_partition(self):
        """
        target: test search from partition
        method: search from one partition
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_index_one_partition")
        # 1. initialize with data
        collection_w, _ = self.init_data(True, 1000, 1)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 3. search in one partition
        log.info("test_search_index_one_partition: searching (1000 entities) through one partition")
        par = collection_w.partitions
        log.info("test_search_index_one_partition: partitions: %s" % par)
        partition_name = par[1].name
        entity_num = par[1].num_entities
        limit = 1000
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, limit, default_search_exp,
                                     [partition_name])
        self.search_results_check(res, limit=entity_num)
        log.info("test_search_index_one_partition: searched %s entities through one partition" % entity_num)

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_index_partitions(self):
        """
        target: test search from partitions
        method: search from one partitions
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_index_partitions")
        # 1. initialize with data
        collection_w, _ = self.init_data(True, 1000, 1)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 3. search through partitions
        log.info("test_search_index_partitions: searching (1000 entities) through partitions")
        par = collection_w.partitions
        log.info("test_search_index_partitions: partitions: %s" % par)
        limit = 1000
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, limit, default_search_exp,
                                     [par[0].name, par[1].name])
        self.search_results_check(res, limit=limit)
        log.info("test_search_index_partitions: searched %s entities through partitions" % limit)

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_index_partition_empty(self):
        """
        target: test search the empty partition
        method: search from the empty partition
        expected: searched successfully with 0 results
        """
        log.info("Test case of search interface: test_search_index_partition_empty")
        # 1. initialize with data
        collection_w, _ = self.init_data(True)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create empty partition
        partition_name = "search_partition_empty"
        collection_w.create_partition(partition_name=partition_name, description="search partition empty")
        par = collection_w.partitions
        log.info("test_search_index_partition_empty: partitions: %s" % par)
        collection_w.load()
        # 3. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 4. search the empty partition
        log.info("test_search_index_partition_empty: searching %s entities through empty partition" % default_limit)
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     default_search_params, default_limit,
                                     default_search_exp, [partition_name])
        self.search_results_check(res, limit=0)
        log.info("test_search_index_partition_empty: searched 0 entity through empty partition")

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_binary_jaccard_flat_index(self):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with L2
        expected: the return distance equals to the computed value
        """
        log.info("Test case of search interface: test_search_binary_jaccard_flat_index")
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector = self.init_data(insert_data=True, is_binary=True)
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        distance_0 = cf.jaccard(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.jaccard(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        res, _ = collection_w.search(binary_vectors[:default_nq], "binary_vector",
                                     search_params, default_limit, "int64 >= 0")
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_binary_flat_with_L2(self):
        """
        target: search binary collection, and check the result: distance
        method: compare the return distance value with value computed with L2
        expected: the return distance equals to the computed value
        """
        log.info("Test case of search interface: test_search_binary_flat_with_L2")
        # 1. initialize with binary data
        collection_w, _, _ = self.init_data(insert_data=True, is_binary=True)
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("binary_vector", default_index)
        # 3. search and assert
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit, "int64 >= 0",
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "create index failed"})

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_binary_hamming_flat_index(self):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with L2
        expected: the return distance equals to the computed value
        """
        log.info("Test case of search interface: test_search_binary_hamming_flat_index")
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector = self.init_data(insert_data=True, is_binary=True)
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "HAMMING"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        distance_0 = cf.hamming(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.hamming(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "HAMMING", "params": {"nprobe": 10}}
        res, _ = collection_w.search(binary_vectors[:default_nq], "binary_vector",
                                     search_params, default_limit, "int64 >= 0")
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_binary_tanimoto_flat_index(self):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector = self.init_data(insert_data=True, is_binary=True)
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "TANIMOTO"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        distance_0 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "TANIMOTO", "params": {"nprobe": 10}}
        res, _ = collection_w.search(binary_vectors[:default_nq], "binary_vector",
                                     search_params, default_limit, "int64 >= 0")
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L3)
    def test_search_multi_collections(self):
        """
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        """
        log.info("Test case of search interface: test_search_multi_collections")
        self._connect()
        connection_num = 10
        for i in range(connection_num):
            # 1. initialize with data
            log.info("test_search_multi_collections: search round %d" % (i+1))
            collection_w, _ = self.init_data(insert_data=True, multiple=True)
            # 2. search
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
            log.info("test_search_multi_collections: searching %s entities (nq = %s) from collection %s" %
                     (default_limit, default_nq, collection_w.name))
            res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                         default_search_params, default_limit,
                                         default_search_exp)
            self.search_results_check(res)
        log.info("test_search_multi_collections: searched %s collections" % connection_num)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.timeout(300)
    def test_search_concurrent_multi_threads(self):
        """
        target: test concurrent search with multi-processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        log.info("Test case of search interface: test_search_concurrent_multi_threads")
        threads_num = 10
        threads = []
        # 1. initialize with data
        collection_w, _ = self.init_data(True)

        def search(collection_w):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
            res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                         default_search_params, default_limit, default_search_exp)
            assert len(res) == default_nq
            for hits in res:
                assert len(hits) == default_limit
                assert len(hits.ids) == default_limit

        # 2. search with multi-processes
        log.info("test_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()
        log.info("test_search_concurrent_multi_threads: searched with %s processes" % threads_num)
