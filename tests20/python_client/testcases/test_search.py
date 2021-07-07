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

prefix = "search_collection"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name


class TestCollectionSearch(TestcaseBase):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_no_connection(self):
        """
        target: test search without connection
        method: create and delete connection, then search
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_no_connection")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. remove connection
        log.info("test_search_no_connection: removing connection")
        self.connection_wrap.remove_connection(alias='default')
        log.info("test_search_no_connection: removed connection")
        # 3. search without connection
        log.info("test_search_no_connection: searching without connection")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1, "err_msg": "should create connect first"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_no_collection(self):
        """
        target: test the scenario which search the non-exist collection
        method: 1. create collection
                2. drop collection
                3. search the dropped collection
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_no_collection "
                 "(searching the non-exist collection)")
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. Drop collection
        collection_w.drop()
        # 3. Search without collection
        log.info("test_search_no_collection: Searching without collection ")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors, default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "collection %s doesn't exist!" % collection_w.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_release_collection(self):
        """
        target: test the scenario which search the released collection
        method: 1. create collection
                2. release collection
                3. search the released collection
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_release_collection")
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix, True, 10, 1)[0]
        # 2. release collection
        collection_w.release()
        # 3. Search the released collection
        log.info("test_search_release_collection: Searching without collection ")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors, default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "collection %s was not loaded "
                                                    "into memory" % collection_w.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_release_partition(self):
        """
        target: test the scenario which search the released collection
        method: 1. create collection
                2. release partition
                3. search with specifying the released partition
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_release_partition")
        # 1. initialize with data
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 1000, partition_num)[0]
        par = collection_w.partitions
        par_name = par[partition_num].name
        # 2. release partition
        conn = self.connection_wrap.get_connection()[0]
        conn.release_partitions(collection_w.name, [par_name])
        # 3. Search the released partition
        log.info("test_search_release_partition: Searching specifying the released partition")
        limit = 1000
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "partition has been released"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_empty_collection(self):
        """
        target: test search with empty connection
        method: search the empty collection
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_with_empty_collection")
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search collection without data before load
        log.info("test_search_with_empty_collection: Searching empty collection %s"
                 % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        err_msg = "collection" + collection_w.name + "was not loaded into memory"
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, timeout=1,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": err_msg})
        # 3. search collection without data after load
        collection_w.load()
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_missing(self):
        """
        target: test search with incomplete parameters
        method: search with incomplete parameters
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_missing")
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search collection with missing parameters
        log.info("test_search_param_missing: Searching collection %s "
                 "with missing parameters" % collection_w.name)
        try:
            collection_w.search()
        except TypeError as e:
            assert "missing 4 required positional arguments: 'data', " \
                   "'anns_field', 'param', and 'limit'" in str(e)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_invalid_dim(self):
        """
        target: test search with invalid parameter values
        method: search with invalid dim
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_dim")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search with invalid dim
        log.info("test_search_param_invalid_dim: searching with invalid dim")
        wrong_dim = 129
        vectors = [[random.random() for _ in range(wrong_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "The dimension of query entities "
                                                    "is different from schema"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_invalid_metric_type(self):
        """
        target: test search with invalid parameter values
        method: search with invalid metric type
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_metric_type")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search with invalid metric_type
        log.info("test_search_param_invalid_metric_type: searching with invalid metric_type")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        search_params = {"metric_type": "L10", "params": {"nprobe": 10}}
        collection_w.search(vectors[:default_nq], default_search_field, search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "metric type not found"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("limit", [0, 16385])
    def test_search_param_invalid_limit(self, limit):
        """
        target: test search with invalid parameter values
        method: search with invalid limit: 0 and maximum
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_limit")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search with invalid limit (topK)
        log.info("test_search_param_invalid_limit: searching with "
                 "invalid limit (topK) = %s" % limit)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        err_msg = "limit %d is too large!" % limit
        if limit == 0:
            err_msg = "`limit` value 0 is illegal"
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue 6259")
    @pytest.mark.parametrize("invalid_search_field",
                             ["", " ", "float-vector"])
    def test_search_param_invalid_field(self, invalid_search_field):
        """
        target: test search with invalid parameter values
        method: search with invalid field
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_field")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search with invalid field
        log.info("test_search_param_invalid_field: searching with "
                 "invalid field: %s" % invalid_search_field)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], invalid_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "invalid search field"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_search_expr",
                             [" ", "int63 >= 0"])
    def test_search_param_invalid_expr(self, invalid_search_expr):
        """
        target: test search with invalid parameter values
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_param_invalid_expr")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2 search with invalid expr
        log.info("test_search_param_invalid_expr: searching with "
                 "invalid expr: %s" % invalid_search_expr)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, invalid_search_expr,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "invalid expression %s"
                                                    % invalid_search_expr})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_partition_deleted(self):
        """
        target: test search deleted partition
        method: 1. search the collection
                2. delete a partition
                3. search the deleted partition
        expected: raise exception and report the error
        """
        log.info("test_search_partition_deleted: test search after partition deletion")
        # 1. initialize with data
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 1000, partition_num)[0]
        # 2. delete partitions
        log.info("test_search_partition_deleted: deleting a partition")
        par = collection_w.partitions
        deleted_par_name = par[partition_num].name
        collection_w.drop_partition(deleted_par_name)
        log.info("test_search_partition_deleted: deleted a partition")
        collection_w.load()
        # 3. search after delete partitions
        log.info("test_search_partition_deleted: searching deleted partition")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            [deleted_par_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "PartitonName: %s not found" % deleted_par_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_index_partition_not_existed(self):
        """
        target: test search not existed partition
        method: search with not existed partition
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_index_partition_not_existed")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_invalid_binary(self):
        """
        target: test search within binary data (invalid parameter)
        method: search with wrong metric type
        expected: raise exception and report the error
        """
        log.info("test_search_param_invalid_binary: test invalid paramter with binary data")
        # 1. initialize with binary data
        collection_w = self.init_collection_general(prefix, True, is_binary=True)[0]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        # 3. search with exception
        binary_vectors = cf.gen_binary_vectors(3000, default_dim)[1]
        wrong_search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector", wrong_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "unsupported"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_binary_flat_with_L2(self):
        """
        target: search binary collection using FlAT with L2
        method: search binary collection using FLAT with L2
        expected: raise exception and report error
        """
        log.info("Test case of search interface: test_search_binary_flat_with_L2")
        # 1. initialize with binary data
        collection_w = self.init_collection_general(prefix, True, is_binary=True)[0]
        # 2. search and assert
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit, "int64 >= 0",
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "Search failed"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_output_field_vector(self):
        """
        target: test search with vector as output field
        method: search with one vector output_field
        expected: raise exception and report the error
        """
        log.info("Test case of search interface: test_search_output_field_vector")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search
        log.info("test_search_output_field_vector: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=[default_search_field],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "Search doesn't support "
                                                    "vector field as output_fields"})

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_normal(self):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        log.info("Test case of search interface: test_search_normal")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True)
        # 2. search
        log.info("test_search_normal: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_empty_vectors(self):
        """
        target: test search with empty query vector
        method: search using empty query vector
        expected: search successfully with 0 results
        """
        log.info("Test case of search interface: test_search_with_empty_vectors")
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search collection without data
        log.info("test_search_with_empty_vectors: Searching collection %s "
                 "using empty vector" % collection_w.name)
        collection_w.search([], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 0})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_before_after_delete(self):
        """
        target: test search function before and after deletion
        method: 1. search the collection
                2. delete a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        log.info("test_search_before_after_delete: test search after deleting entities")
        # 1. initialize with data
        partition_num = 1
        nb = 1000
        limit = 1000
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb, partition_num)
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        log.info("test_search_before_after_delete: searching before deleting partitions")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit})
        # 3. delete partitions
        log.info("test_search_before_after_delete: deleting a partition")
        par = collection_w.partitions
        deleted_entity_num = par[partition_num].num_entities
        entity_num = nb - deleted_entity_num
        collection_w.drop_partition(par[partition_num].name)
        log.info("test_search_before_after_delete: deleted a partition")
        collection_w.load()
        # 4. search non-deleted part after delete partitions
        log.info("test_search_before_after_delete: searching after deleting partitions")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids[:entity_num],
                                         "limit": limit-deleted_entity_num})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_partition_after_release_one(self):
        """
        target: test search function before and after release
        method: 1. search the collection
                2. release a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        log.info("test_search_partition_after_release_one: test search after releasing entities")
        # 1. initialize with data
        nb = 1000
        limit = 1000
        partition_num = 1
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb, partition_num)
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        log.info("test_search_partition_after_release_one: searching before deleting partitions")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit})
        # 3. release one partition
        log.info("test_search_partition_after_release_one: releasing a partition")
        par = collection_w.partitions
        deleted_entity_num = par[partition_num].num_entities
        entity_num = nb - deleted_entity_num
        conn = self.connection_wrap.get_connection()[0]
        conn.release_partitions(collection_w.name, [par[partition_num].name])
        log.info("test_search_partition_after_release_one: released a partition")
        # 4. search collection after release one partition
        log.info("test_search_partition_after_release_one: searching after deleting partitions")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids[:entity_num],
                                         "limit": limit - deleted_entity_num})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_partition_after_release_all(self):
        """
        target: test search function before and after release
        method: 1. search the collection
                2. release a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        log.info("test_search_before_after_release: test search after releasing entities")
        # 1. initialize with data
        nb = 1000
        limit = 1000
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb, 1)
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        log.info("test_search_partition_after_release_all: searching before deleting partitions")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit})
        # 3. release all partitions
        log.info("test_search_partition_after_release_all: releasing a partition")
        par = collection_w.partitions
        conn = self.connection_wrap.get_connection()[0]
        conn.release_partitions(collection_w.name, [par[0].name, par[1].name])
        log.info("test_search_partition_after_release_all: released a partition")
        # 4. search collection after release all partitions
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_collection_after_release_load(self):
        """
        target: search the pre-released collection after load
        method: 1. create collection
                2. release collection
                3. load collection
                4. search the pre-released collection
        expected: search successfully
        """
        log.info("Test case of search interface: test_search_collection_after_release_load")
        # 1. initialize without data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, default_nb, 1)
        # 2. release collection
        collection_w.release()
        # 3. Search the pre-released collection after load
        collection_w.load()
        log.info("test_search_partition_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_partition_after_release_load(self):
        """
        target: search the pre-released collection after load
        method: 1. create collection
                2. release a partition
                3. load partition
                3. search the pre-released partition
        expected: search successfully
        """
        log.info("Test case of search interface: test_search_partition_after_release_load")
        # 1. initialize without data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, default_nb, 1)
        # 2. release collection
        log.info("test_search_partition_after_release_load: releasing a partition")
        par = collection_w.partitions
        conn = self.connection_wrap.get_connection()[0]
        conn.release_partitions(collection_w.name, [par[1].name])
        log.info("test_search_partition_after_release_load: released a partition")
        # 3. Search the collection after load
        limit = 1000
        collection_w.load()
        log.info("test_search_partition_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit})
        # 4. Search the pre-released partition after load
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            limit, default_search_exp,
                            [par[1].name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids[par[0].num_entities:],
                                         "limit": par[1].num_entities})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_load_flush_load(self):
        """
        target: test search when load before flush
        method: 1. search the collection
                2. insert data and load
                3. flush, and load
        expected: search success with limit(topK)
        """
        log.info("test_search_load_flush_load: search new data without another load")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. insert data
        insert_ids = cf.insert_data(collection_w, default_nb)[3]
        # 3. load data
        collection_w.load()
        # 4. flush and load
        collection_w.partitions
        collection_w.load()
        # 5. search for new data without load
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue 5858")
    def test_search_new_data(self):
        """
        target: test search new inserted data without load
        method: 1. search the collection
                2. insert new data
                3. search the collection without load again
        expected: new data should be searched
        """
        log.info("test_search_new_data: search new data without another load")
        # 1. initialize with data
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old)
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        log.info("test_search_new_data: searching for original data after load")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": nb_old})
        # 3. insert new data
        nb_new = 300
        insert_ids_new = cf.insert_data(collection_w, nb_new)[3]
        insert_ids.extend(insert_ids_new)
        # 4. search for new data without load
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": nb_old+nb_new})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_after_different_index(self, index, params):
        """
        target: test search with different index
        method: test search with different index
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_after_different_index")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, partition_num=1)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create different index
        log.info("test_search_after_different_index: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_after_different_index: Created index-%s" % index)
        # 3. search
        log.info("test_search_after_different_index: Searching after creating index-%s" % index)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_after_index_different_metric_type(self, index, params):
        """
        target: test search with different metric type
        method: test search with different metric type
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_after_index_different_metric_type")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, partition_num=1)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create different index
        log.info("test_search_after_different_index: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_after_different_index: Created index-%s" % index)
        # 3. search
        log.info("test_search_after_different_index: Searching after creating index-%s" % index)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_collection_multiple_times(self):
        """
        target: test search for multiple times
        method: search for multiple times
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_collection_multiple_times")
        # 1. initialize with data
        search_num = 5
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True)
        # 2. search for multiple times
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        for i in range(search_num):
            log.info("test_search_collection_multiple_times: searching round %d" % (i+1))
            collection_w.search(vectors[:default_nq], default_search_field,
                                default_search_params, default_limit, default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_index_one_partition(self):
        """
        target: test search from partition
        method: search from one partition
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_index_one_partition")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, partition_num=1)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 3. search in one partition
        log.info("test_search_index_one_partition: searching (1000 entities) through one partition")
        limit = 1000
        par = collection_w.partitions
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par[1].name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids[par[0].num_entities:],
                                         "limit": par[1].num_entities})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_index_partitions(self):
        """
        target: test search from partitions
        method: search from one partitions
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_index_partitions")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, partition_num=1)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 3. search through partitions
        log.info("test_search_index_partitions: searching (1000 entities) through partitions")
        par = collection_w.partitions
        log.info("test_search_index_partitions: partitions: %s" % par)
        limit = 1000
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par[0].name, par[1].name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_names",
                             [["(.*)"], ["search(.*)"]])
    def test_search_index_partitions_fuzzy(self, partition_names):
        """
        target: test search from partitions
        method: search from partitions with fuzzy
                partition name
        expected: searched successfully
        """
        log.info("Test case of search interface: test_search_index_partitions_fuzzy")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, partition_num=1)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 3. search through partitions
        log.info("test_search_index_partitions_fuzzy: searching through partitions")
        limit = 1000
        nb = default_nb
        par = collection_w.partitions
        if partition_names == ["search(.*)"]:
            insert_ids = insert_ids[par[0].num_entities:]
            limit = par[1].num_entities
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            partition_names,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partition_empty(self):
        """
        target: test search the empty partition
        method: search from the empty partition
        expected: searched successfully with 0 results
        """
        log.info("Test case of search interface: test_search_index_partition_empty")
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
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
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, [partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_jaccard_flat_index(self):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with L2
        expected: the return distance equals to the computed value
        """
        log.info("Test case of search interface: test_search_binary_jaccard_flat_index")
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, _ = \
            self.init_collection_general(prefix, True, 2, is_binary=True)
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        distance_0 = cf.jaccard(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.jaccard(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:default_nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0")[0]
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_hamming_flat_index(self):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with L2
        expected: the return distance equals to the computed value
        """
        log.info("Test case of search interface: test_search_binary_hamming_flat_index")
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, _ = \
            self.init_collection_general(prefix, True, 2, is_binary=True)
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "HAMMING"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        distance_0 = cf.hamming(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.hamming(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "HAMMING", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:default_nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0")[0]
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_tanimoto_flat_index(self):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, _ = \
            self.init_collection_general(prefix, True, 2, is_binary=True)
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "TANIMOTO"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        distance_0 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "TANIMOTO", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:default_nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0")[0]
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression, limit",
                             zip(cf.gen_normal_expressions(),
                                 [1000, 999, 898, 997, 2, 3]))
    def test_search_with_expression(self, expression, limit):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        log.info("Test case of search interface: test_search_with_expression")
        # 1. initialize with data
        nb = 1000
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb)
        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        # 3. search with different expressions
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, nb, expression,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_output_fields_empty(self):
        """
        target: test search with output fields
        method: search with empty output_field
        expected: search success
        """
        log.info("Test case of search interface: test_search_with_output_fields_empty")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True)
        # 2. search
        log.info("test_search_with_output_fields_empty: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  default_search_params, default_limit,
                                  default_search_exp, output_fields=[],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "ids": insert_ids,
                                               "limit": default_limit})[0]
        assert len(res[0][0].entity._row_data) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_output_fields_not_exist(self):
        """
        target: test search with output fields
        method: search with non-exist output_field
        expected: search success
        """
        log.info("Test case of search interface: test_search_with_output_fields_not_exist")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True)
        # 2. search
        log.info("test_search_with_output_fields_not_exist: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  default_search_params, default_limit,
                                  default_search_exp, output_fields=["int63"],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "ids": insert_ids,
                                               "limit": default_limit})[0]
        assert len(res[0][0].entity._row_data) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_output_field(self):
        """
        target: test search with output fields
        method: search with one output_field
        expected: search success
        """
        log.info("Test case of search interface: test_search_with_output_field")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True)
        # 2. search
        log.info("test_search_with_output_field: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  default_search_params, default_limit,
                                  default_search_exp, output_fields=[default_int64_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "ids": insert_ids,
                                               "limit": default_limit})[0]
        assert len(res[0][0].entity._row_data) != 0
        assert default_int64_field_name in res[0][0].entity._row_data

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields(self):
        """
        target: test search with output fields
        method: search with multiple output_field
        expected: search success
        """
        log.info("Test case of search interface: test_search_with_output_fields")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, is_all_data_type=True)
        # 2. search
        log.info("test_search_with_output_fields: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  default_search_params, default_limit,
                                  default_search_exp,
                                  output_fields=[default_int64_field_name,
                                                 default_float_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "ids": insert_ids,
                                               "limit": default_limit})[0]
        assert len(res[0][0].entity._row_data) != 0
        assert (default_int64_field_name and default_float_field_name) in res[0][0].entity._row_data

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_all_data_type(self):
        """
        target: test search using different supported data type
        method: search using different supported data type
        expected: search success
        """
        log.info("Test case of search interface: test_search_expression_all_data_type")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, is_all_data_type=True)
        # 2. search
        log.info("test_search_expression_all_data_type: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        search_exp = "int64 >= 0 && int32 >= 0 && int16 >= 0 " \
                     "&& int8 >= 0 && float >= 0 && double >= 0"
        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  default_search_params, default_limit, search_exp,
                                  output_fields=[default_int64_field_name,
                                                 default_float_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "ids": insert_ids,
                                               "limit": default_limit})[0]
        assert len(res[0][0].entity._row_data) != 0
        assert (default_int64_field_name and default_float_field_name) in res[0][0].entity._row_data

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_collections(self):
        """
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        """
        log.info("Test case of search interface: test_search_multi_collections")
        self._connect()
        collection_num = 10
        for i in range(collection_num):
            # 1. initialize with data
            log.info("test_search_multi_collections: search round %d" % (i + 1))
            collection_w, _, _, insert_ids = self.init_collection_general(prefix, True)
            # 2. search
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
            log.info("test_search_multi_collections: searching %s entities (nq = %s) from collection %s" %
                     (default_limit, default_nq, collection_w.name))
            collection_w.search(vectors[:default_nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
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
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True)

        def search(collection_w):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
            collection_w.search(vectors[:default_nq], default_search_field,
                                default_search_params, default_limit, default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit})

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



