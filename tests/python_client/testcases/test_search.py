import multiprocessing
import numbers

import pytest
from time import sleep

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from common.constants import *
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_EVENTUALLY

prefix = "search_collection"
search_num = 10
max_dim = ct.max_dim
epsilon = ct.epsilon
gracefulTime = ct.gracefulTime
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_string_exp =  "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]

uid = "test_search"
nq = 1
epsilon = 0.001
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
search_param = {"nprobe": 1}
entity = gen_entities(1, is_normal=True)
entities = gen_entities(default_nb, is_normal=True)
raw_vectors, binary_entities = gen_binary_entities(default_nb)
default_query, _ = gen_search_vectors_params(field_name, entities, default_top_k, nq)
index_name1 = cf.gen_unique_str("float")
index_name2 = cf.gen_unique_str("varhar")


class TestCollectionSearchInvalid(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=ct.get_invalid_vectors)
    def get_invalid_vectors(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_fields_type(self, request):
        if isinstance(request.param, str):
            pytest.skip("string is valid type for field")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_fields_value(self, request):
        if not isinstance(request.param, str):
            pytest.skip("field value only support string")
        if request.param == "":
            pytest.skip("empty field is valid")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_ints)
    def get_invalid_limit(self, request):
        if isinstance(request.param, int) and request.param >= 0:
            pytest.skip("positive int is valid type for limit")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_expr_type(self, request):
        if isinstance(request.param, str):
            pytest.skip("string is valid type for expr")
        if request.param is None:
            pytest.skip("None is valid for expr")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_expr_value(self, request):
        if not isinstance(request.param, str):
            pytest.skip("expression value only support string")
        if request.param == "":
            pytest.skip("empty field is valid")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_expr_bool_value(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_partition(self, request):
        if request.param == []:
            pytest.skip("empty is valid for partition")
        if request.param is None:
            pytest.skip("None is valid for partition")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_output_fields(self, request):
        if request.param == []:
            pytest.skip("empty is valid for output_fields")
        if request.param is None:
            pytest.skip("None is valid for output_fields")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_ints)
    def get_invalid_travel_timestamp(self, request):
        if request.param == 9999999999:
            pytest.skip("9999999999 is valid for travel timestamp")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_ints)
    def get_invalid_guarantee_timestamp(self, request):
        if request.param == 9999999999:
            pytest.skip("9999999999 is valid for guarantee_timestamp")
        yield request.param

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
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. remove connection
        log.info("test_search_no_connection: removing connection")
        self.connection_wrap.remove_connection(alias='default')
        log.info("test_search_no_connection: removed connection")
        # 3. search without connection
        log.info("test_search_no_connection: searching without connection")
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "should create connect first"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_no_collection(self):
        """
        target: test the scenario which search the non-exist collection
        method: 1. create collection
                2. drop collection
                3. search the dropped collection
        expected: raise exception and report the error
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. Drop collection
        collection_w.drop()
        # 3. Search without collection
        log.info("test_search_no_collection: Searching without collection ")
        collection_w.search(vectors, default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "collection %s doesn't exist!" % collection_w.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_missing(self):
        """
        target: test search with incomplete parameters
        method: search with incomplete parameters
        expected: raise exception and report the error
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search collection with missing parameters
        log.info("test_search_param_missing: Searching collection %s "
                 "with missing parameters" % collection_w.name)
        try:
            collection_w.search()
        except TypeError as e:
            assert "missing 4 required positional arguments: 'data', " \
                   "'anns_field', 'param', and 'limit'" in str(e)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_vectors(self, get_invalid_vectors):
        """
        target: test search with invalid parameter values
        method: search with invalid data
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid field
        invalid_vectors = get_invalid_vectors
        log.info("test_search_param_invalid_vectors: searching with "
                 "invalid vectors: {}".format(invalid_vectors))
        collection_w.search(invalid_vectors, default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "`search_data` value {} is illegal".format(invalid_vectors)})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_dim(self):
        """
        target: test search with invalid parameter values
        method: search with invalid dim
        expected: raise exception and report the error
        """
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_field_type(self, get_invalid_fields_type):
        """
        target: test search with invalid parameter type
        method: search with invalid field type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid field
        invalid_search_field = get_invalid_fields_type
        log.info("test_search_param_invalid_field_type: searching with "
                 "invalid field: %s" % invalid_search_field)
        collection_w.search(vectors[:default_nq], invalid_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "`anns_field` value {} is illegal".format(invalid_search_field)})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_field_value(self, get_invalid_fields_value):
        """
        target: test search with invalid parameter values
        method: search with invalid field value
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid field
        invalid_search_field = get_invalid_fields_value
        log.info("test_search_param_invalid_field_value: searching with "
                 "invalid field: %s" % invalid_search_field)
        collection_w.search(vectors[:default_nq], invalid_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "Field %s doesn't exist in schema"
                                                    % invalid_search_field})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_invalid_metric_type(self, get_invalid_metric_type):
        """
        target: test search with invalid parameter values
        method: search with invalid metric type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, 10)[0]
        # 2. search with invalid metric_type
        log.info("test_search_param_invalid_metric_type: searching with invalid metric_type")
        invalid_metric = get_invalid_metric_type
        search_params = {"metric_type": invalid_metric, "params": {"nprobe": 10}}
        collection_w.search(vectors[:default_nq], default_search_field, search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "metric type not found"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_invalid_params_type(self, index, params):
        """
        target: test search with invalid search params
        method: test search with invalid params type
        expected: raise exception and report the error
        """
        if index == "FLAT":
            pytest.skip("skip in FLAT index")
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 5000,
                                                                      is_index=True)[0:4]
        # 2. create index and load
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        invalid_search_params = cf.gen_invaild_search_params_type()
        message = "Search params check failed"
        for invalid_search_param in invalid_search_params:
            if index == invalid_search_param["index_type"]:
                search_params = {"metric_type": "L2", "params": invalid_search_param["search_params"]}
                collection_w.search(vectors[:default_nq], default_search_field,
                                    search_params, default_limit,
                                    default_search_exp,
                                    check_task=CheckTasks.err_res,
                                    check_items={"err_code": 1,
                                                 "err_msg": message})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_limit_type(self, get_invalid_limit):
        """
        target: test search with invalid limit type
        method: search with invalid limit type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid field
        invalid_limit = get_invalid_limit
        log.info("test_search_param_invalid_limit_type: searching with "
                 "invalid limit: %s" % invalid_limit)
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            invalid_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "`limit` value %s is illegal" % invalid_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [0, 16385])
    def test_search_param_invalid_limit_value(self, limit):
        """
        target: test search with invalid limit value
        method: search with invalid limit: 0 and maximum
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search with invalid limit (topK)
        log.info("test_search_param_invalid_limit_value: searching with "
                 "invalid limit (topK) = %s" % limit)
        err_msg = "limit %d is too large!" % limit
        if limit == 0:
            err_msg = "`limit` value 0 is illegal"
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_expr_type(self, get_invalid_expr_type):
        """
        target: test search with invalid parameter type
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2 search with invalid expr
        invalid_search_expr = get_invalid_expr_type
        log.info("test_search_param_invalid_expr_type: searching with "
                 "invalid expr: {}".format(invalid_search_expr))

        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, invalid_search_expr,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "The type of expr must be string ,"
                                                    "but {} is given".format(type(invalid_search_expr))})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_expr_value(self, get_invalid_expr_value):
        """
        target: test search with invalid parameter values
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2 search with invalid expr
        invalid_search_expr = get_invalid_expr_value
        log.info("test_search_param_invalid_expr_value: searching with "
                 "invalid expr: %s" % invalid_search_expr)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, invalid_search_expr,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "invalid expression %s"
                                                    % invalid_search_expr})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_expr_bool(self, get_invalid_expr_bool_value):
        """
        target: test search with invalid parameter values
        method: search with invalid bool search expressions
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, is_all_data_type=True)[0]
        # 2 search with invalid bool expr
        invalid_search_expr_bool = f"{default_bool_field_name} == {get_invalid_expr_bool_value}"
        log.info("test_search_param_invalid_expr_bool: searching with "
                 "invalid expr: %s" % invalid_search_expr_bool)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, invalid_search_expr_bool,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "failed to create query plan"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_partition_invalid_type(self, get_invalid_partition):
        """
        target: test search invalid partition
        method: search with invalid partition type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search the invalid partition
        partition_name = get_invalid_partition
        err_msg = "`partition_name_array` value {} is illegal".format(partition_name)
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, partition_name,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields_invalid_type(self, get_invalid_output_fields):
        """
        target: test search with output fields
        method: search with invalid output_field
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search
        log.info("test_search_with_output_fields_invalid_type: Searching collection %s" % collection_w.name)
        output_fields = get_invalid_output_fields
        err_msg = "`output_fields` value {} is illegal".format(output_fields)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_release_collection(self):
        """
        target: test the scenario which search the released collection
        method: 1. create collection
                2. release collection
                3. search the released collection
        expected: raise exception and report the error
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix, True, 10)[0]
        # 2. release collection
        collection_w.release()
        # 3. Search the released collection
        log.info("test_search_release_collection: Searching without collection ")
        collection_w.search(vectors, default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "collection %s was not loaded "
                                                    "into memory" % collection_w.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_release_partition(self):
        """
        target: test the scenario which search the released collection
        method: 1. create collection
                2. release partition
                3. search the released partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 10, partition_num, is_index=True)[0]
        par = collection_w.partitions
        par_name = par[partition_num].name
        par[partition_num].load()
        # 2. release partition
        par[partition_num].release()
        # 3. Search the released partition
        log.info("test_search_release_partition: Searching the released partition")
        limit = 10
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "partition has been released"})

    @pytest.mark.skip("enable this later using session/strong consistency")
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_empty_collection(self):
        """
        target: test search with empty connection
        method: 1. search the empty collection before load
                2. search the empty collection after load
                3. search collection with data inserted but not load again
        expected: 1. raise exception if not loaded
                  2. return topk=0  if loaded
                  3. return topk successfully
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. search collection without data before load
        log.info("test_search_with_empty_collection: Searching empty collection %s"
                 % collection_w.name)
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
        # 4. search with data inserted but not load again
        data = cf.gen_default_dataframe_data(nb=2000)
        insert_res = collection_w.insert(data)[0]
        # Using bounded staleness, maybe we cannot search the "inserted" requests,
        # since the search requests arrived query nodes earlier than query nodes consume the insert requests.
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            guarantee_timestamp=insert_res.timestamp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_res.primary_keys,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_empty_collection_with_partition(self):
        """
        target: test search with empty collection
        method: 1. collection an empty collection with partitions
                2. load
                3. search
        expected: return 0 result
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        par = collection_w.partitions
        # 2. search collection without data after load
        collection_w.load()
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})
        # 2. search a partition without data after load
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            [par[1].name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_partition_deleted(self):
        """
        target: test search deleted partition
        method: 1. create a collection with partitions
                2. delete a partition
                3. search the deleted partition
        expected: raise exception and report the error
        """
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
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            [deleted_par_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "PartitonName: %s not found" % deleted_par_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[1:9],
                                 ct.default_index_params[1:9]))
    def test_search_different_index_invalid_params(self, index, params):
        """
        target: test search with different index
        method: test search with different index
        expected: searched successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 5000,
                                                                      partition_num=1,
                                                                      is_index=True)[0:4]
        # 2. create different index
        if params.get("m"):
            if (default_dim % params["m"]) != 0:
                params["m"] = default_dim // 4
        log.info("test_search_different_index_invalid_params: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_different_index_invalid_params: Created index-%s" % index)
        collection_w.load()
        # 3. search
        log.info("test_search_different_index_invalid_params: Searching after creating index-%s" % index)
        search_params = cf.gen_invalid_search_param(index)
        collection_w.search(vectors, default_search_field,
                            search_params[0], default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "Search params check failed"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partition_not_existed(self):
        """
        target: test search not existed partition
        method: search with not existed partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 3. search the non exist partition
        partition_name = "search_non_exist"
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, [partition_name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "PartitonName: %s not found" % partition_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 15407")
    def test_search_param_invalid_binary(self):
        """
        target: test search within binary data (invalid parameter)
        method: search with wrong metric type
        expected: raise exception and report the error
        """
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 15407")
    def test_search_binary_flat_with_L2(self):
        """
        target: search binary collection using FlAT with L2
        method: search binary collection using FLAT with L2
        expected: raise exception and report error
        """
        # 1. initialize with binary data
        collection_w = self.init_collection_general(prefix, True, is_binary=True)[0]
        # 2. search and assert
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(2, default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit, "int64 >= 0",
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "Search failed"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields_not_exist(self):
        """
        target: test search with output fields
        method: search with non-exist output_field
        expected: raise exception
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True)[0:4]
        # 2. search
        log.info("test_search_with_output_fields_not_exist: Searching collection %s" % collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=["int63"],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: "Field int63 not exist"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("output_fields", [[default_search_field], ["%"]])
    def test_search_output_field_vector(self, output_fields):
        """
        target: test search with vector as output field
        method: search with one vector output_field or
                wildcard for vector
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search
        log.info("test_search_output_field_vector: Searching collection %s" % collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "Search doesn't support "
                                                    "vector field as output_fields"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", [["*%"], ["**"], ["*", "@"]])
    def test_search_output_field_invalid_wildcard(self, output_fields):
        """
        target: test search with invalid output wildcard
        method: search with invalid output_field wildcard
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search
        log.info("test_search_output_field_invalid_wildcard: Searching collection %s" % collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": f"Field {output_fields[-1]} not exist"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_travel_timestamp(self, get_invalid_travel_timestamp):
        """
        target: test search with invalid travel timestamp
        method: search with invalid travel timestamp
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, 10)[0]
        # 2. search with invalid travel timestamp
        log.info("test_search_param_invalid_travel_timestamp: searching with invalid travel timestamp")
        invalid_travel_time = get_invalid_travel_timestamp
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            travel_timestamp=invalid_travel_time,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "`travel_timestamp` value %s is illegal" % invalid_travel_time})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_guarantee_timestamp(self, get_invalid_guarantee_timestamp):
        """
        target: test search with invalid guarantee timestamp
        method: search with invalid guarantee timestamp
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, 10)[0]
        # 2. search with invalid travel timestamp
        log.info("test_search_param_invalid_guarantee_timestamp: searching with invalid guarantee timestamp")
        invalid_guarantee_time = get_invalid_guarantee_timestamp
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            default_limit, default_search_exp,
                            guarantee_timestamp=invalid_guarantee_time,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "`guarantee_timestamp` value %s is illegal"
                                                    % invalid_guarantee_time})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("round_decimal", [7, -2, 999, 1.0, None, [1], "string", {}])
    def test_search_invalid_round_decimal(self, round_decimal):
        """
        target: test search with invalid round decimal
        method: search with invalid round decimal
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, nb=10)[0]
        # 2. search
        log.info("test_search_invalid_round_decimal: Searching collection %s" % collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, round_decimal=round_decimal,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": f"`round_decimal` value {round_decimal} is illegal"})


class TestCollectionSearch(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function",
                    params=[default_nb, default_nb_medium])
    def nb(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[2, 500])
    def nq(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[8, 128])
    def dim(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["JACCARD", "HAMMING", "TANIMOTO"])
    def metrics(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def is_flush(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_normal(self, nq, dim, auto_id, is_flush):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: 1. search returned with 0 before travel timestamp
                  2. search successfully with limit(topK) after travel timestamp
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush)[0:5]
        # 2. search before insert time_stamp
        log.info("test_search_normal: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            travel_timestamp=time_stamp - 1,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0})
        # 3. search after insert time_stamp
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            travel_timestamp=time_stamp,
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_hit_vectors(self, nq, dim, auto_id):
        """
        target: test search with vectors in collections
        method: create connections,collection insert and search vectors in collections
        expected: search successfully with limit(topK) and can be hit at top 1 (min distance is 0)
        """
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim)[0:4]
        # get vectors that inserted into collection
        vectors = np.array(_vectors[0]).tolist()
        vectors = [vectors[i][-1] for i in range(nq)]
        log.info("test_search_with_hit_vectors: searching collection %s" % collection_w.name)
        search_res, _ = collection_w.search(vectors[:nq], default_search_field,
                                            default_search_params, default_limit,
                                            default_search_exp,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nq,
                                                         "ids": insert_ids,
                                                         "limit": default_limit})
        log.info("test_search_with_hit_vectors: checking the distance of top 1")
        for hits in search_res:
            # verify that top 1 hit is itself,so min distance is 0
            assert hits.distances[0] == 0.0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dup_times", [1, 2, 3])
    def test_search_with_dup_primary_key(self, dim, auto_id, _async, dup_times):
        """
        target: test search with duplicate primary key
        method: 1.insert same data twice
                2.search
        expected: search results are de-duplicated
        """
        # initialize with data
        nb = ct.default_nb
        nq = ct.default_nq
        collection_w, insert_data, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                                auto_id=auto_id,
                                                                                dim=dim)[0:4]
        # insert dup data multi times
        for i in range(dup_times):
            insert_res, _ = collection_w.insert(insert_data[0])
            insert_ids.extend(insert_res.primary_keys)
        # search
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:nq], default_search_field,
                                            default_search_params, default_limit,
                                            default_search_exp, _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nq,
                                                         "ids": insert_ids,
                                                         "limit": default_limit,
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()
        # assert that search results are de-duplicated
        for hits in search_res:
            ids = hits.ids
            assert sorted(list(set(ids))) == sorted(ids)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_empty_vectors(self, dim, auto_id, _async):
        """
        target: test search with empty query vector
        method: search using empty query vector
        expected: search successfully with 0 results
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix, True,
                                                    auto_id=auto_id, dim=dim)[0]
        # 2. search collection without data
        log.info("test_search_with_empty_vectors: Searching collection %s "
                 "using empty vector" % collection_w.name)
        collection_w.search([], default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 0,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_ndarray(self, dim, auto_id, _async):
        """
        target: test search with ndarray
        method: search using ndarray data
        expected: search successfully
        """
        # 1. initialize without data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search collection without data
        log.info("test_search_with_ndarray: Searching collection %s "
                 "using ndarray" % collection_w.name)
        vectors = np.random.randn(default_nq, dim)
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("search_params", [{}, {"params": {}}, {"params": {"nprobe": 10}}])
    def test_search_normal_default_params(self, dim, auto_id, search_params, _async):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim)[0:4]
        # 2. search
        log.info("test_search_normal_default_params: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp, _async=_async,
                            travel_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #13611")
    def test_search_before_after_delete(self, nq, dim, auto_id, _async):
        """
        target: test search function before and after deletion
        method: 1. search the collection
                2. delete a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        # 1. initialize with data
        nb = 1000
        limit = 1000
        partition_num = 1
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info("test_search_before_after_delete: searching before deleting partitions")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})
        # 3. delete partitions
        log.info("test_search_before_after_delete: deleting a partition")
        par = collection_w.partitions
        deleted_entity_num = par[partition_num].num_entities
        print(deleted_entity_num)
        entity_num = nb - deleted_entity_num
        collection_w.drop_partition(par[partition_num].name)
        log.info("test_search_before_after_delete: deleted a partition")
        collection_w.load()
        # 4. search non-deleted part after delete partitions
        log.info("test_search_before_after_delete: searching after deleting partitions")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids[:entity_num],
                                         "limit": limit - deleted_entity_num,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_collection_after_release_load(self, nb, nq, dim, auto_id, _async):
        """
        target: search the pre-released collection after load
        method: 1. create collection
                2. release collection
                3. load collection
                4. search the pre-released collection
        expected: search successfully
        """
        # 1. initialize without data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  1, auto_id=auto_id,
                                                                                  dim=dim)[0:5]
        # 2. release collection
        log.info("test_search_collection_after_release_load: releasing collection %s" % collection_w.name)
        collection_w.release()
        log.info("test_search_collection_after_release_load: released collection %s" % collection_w.name)
        # 3. Search the pre-released collection after load
        log.info("test_search_collection_after_release_load: loading collection %s" % collection_w.name)
        collection_w.load()
        log.info("test_search_collection_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            travel_timestamp=time_stamp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_load_flush_load(self, nb, nq, dim, auto_id, _async):
        """
        target: test search when load before flush
        method: 1. insert data and load
                2. flush, and load
                3. search the collection
        expected: search success with limit(topK)
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, auto_id=auto_id, dim=dim)[0]
        # 2. insert data
        insert_ids = cf.insert_data(collection_w, nb, auto_id=auto_id, dim=dim)[3]
        # 3. load data
        collection_w.load()
        # 4. flush and load
        collection_w.num_entities
        collection_w.load()
        # 5. search
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.skip("enable this later using session/strong consistency")
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_new_data(self, nq, dim, auto_id, _async):
        """
        target: test search new inserted data without load
        method: 1. search the collection
                2. insert new data
                3. search the collection without load again
                4. Use guarantee_timestamp to guarantee data consistency
        expected: new data should be searched
        """
        # 1. initialize with data
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb_old,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim)[0:5]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info("test_search_new_data: searching for original data after load")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            travel_timestamp=time_stamp + 1,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        # 3. insert new data
        nb_new = 300
        _, _, _, insert_ids_new, time_stamp = cf.insert_data(collection_w, nb_new,
                                                             auto_id=auto_id, dim=dim,
                                                             insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        # 4. search for new data without load
        # Using bounded staleness, maybe we could not search the "inserted" entities,
        # since the search requests arrived query nodes earlier than query nodes consume the insert requests.
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            guarantee_timestamp=time_stamp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="debug")
    def test_search_max_dim(self, auto_id, _async):
        """
        target: test search with max configuration
        method: create connection, collection, insert and search with max dim
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 100,
                                                                      auto_id=auto_id,
                                                                      dim=max_dim)[0:4]
        # 2. search
        nq = 2
        log.info("test_search_max_dim: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(max_dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, nq,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nq,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_after_different_index_with_params(self, dim, index, params, auto_id, _async):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=True)[0:5]
        # 2. create index and load
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                travel_timestamp=time_stamp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_after_index_different_metric_type(self, dim, index, params, auto_id, _async):
        """
        target: test search with different metric type
        method: test search with different metric type
        expected: searched successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=True)[0:5]
        # 2. create different index
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        log.info("test_search_after_index_different_metric_type: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_after_index_different_metric_type: Created index-%s" % index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index, "IP")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                travel_timestamp=time_stamp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_collection_multiple_times(self, nb, nq, dim, auto_id, _async):
        """
        target: test search for multiple times
        method: search for multiple times
        expected: searched successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for multiple times
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        for i in range(search_num):
            log.info("test_search_collection_multiple_times: searching round %d" % (i + 1))
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_sync_async_multiple_times(self, nb, nq, dim, auto_id):
        """
        target: test async search after sync search case
        method: create connection, collection, insert,
                sync search and async search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim)[0:5]
        # 2. search
        log.info("test_search_sync_async_multiple_times: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        for i in range(search_num):
            log.info("test_search_sync_async_multiple_times: searching round %d" % (i + 1))
            for _async in [False, True]:
                collection_w.search(vectors[:nq], default_search_field,
                                    default_search_params, default_limit,
                                    default_search_exp, _async=_async,
                                    travel_timestamp=time_stamp,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": nq,
                                                 "ids": insert_ids,
                                                 "limit": default_limit,
                                                 "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #12680")
    # TODO: add one more for binary vectors
    # @pytest.mark.parametrize("vec_fields", [[cf.gen_float_vec_field(name="test_vector1")],
    #                                         [cf.gen_binary_vec_field(name="test_vector1")],
    #                                         [cf.gen_binary_vec_field(), cf.gen_binary_vec_field("test_vector1")]])
    def test_search_multiple_vectors_with_one_indexed(self):
        """
        target: test indexing on one vector fields when there are multi float vec fields
        method: 1. create collection with multiple float vector fields
                2. insert data and build index on one of float vector fields
                3. load collection and search
        expected: load and search successfully
        """
        vec_fields = [cf.gen_float_vec_field(name="test_vector1")]
        schema = cf.gen_schema_multi_vector_fields(vec_fields)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_dataframe_multi_vec_fields(vec_fields=vec_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        _index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        res, ch = collection_w.create_index(field_name="test_vector1", index_params=_index)
        assert ch is True
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(2)]
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        res_1, _ = collection_w.search(data=vectors, anns_field="test_vector1",
                                       param=search_params, limit=1)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_index_one_partition(self, nb, auto_id, _async):
        """
        target: test search from partition
        method: search from one partition
        expected: searched successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  is_index=True)[0:5]

        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search in one partition
        log.info("test_search_index_one_partition: searching (1000 entities) through one partition")
        limit = 1000
        par = collection_w.partitions
        if limit > par[1].num_entities:
            limit_check = par[1].num_entities
        else:
            limit_check = limit
        search_params = {"metric_type": "L2", "params": {"nprobe": 128}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, limit, default_search_exp,
                            [par[1].name], _async=_async,
                            travel_timestamp=time_stamp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids[par[0].num_entities:],
                                         "limit": limit_check,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partitions(self, nb, nq, dim, auto_id, _async):
        """
        target: test search from partitions
        method: search from partitions
        expected: searched successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num=1,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      is_index=True)[0:4]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search through partitions
        log.info("test_search_index_partitions: searching (1000 entities) through partitions")
        par = collection_w.partitions
        log.info("test_search_index_partitions: partitions: %s" % par)
        limit = 1000
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            [par[0].name, par[1].name], _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_names",
                             [["(.*)"], ["search(.*)"]])
    def test_search_index_partitions_fuzzy(self, nb, nq, dim, partition_names, auto_id, _async):
        """
        target: test search from partitions
        method: search from partitions with fuzzy
                partition name
        expected: searched successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num=1,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        # 3. search through partitions
        log.info("test_search_index_partitions_fuzzy: searching through partitions")
        limit = 1000
        limit_check = limit
        par = collection_w.partitions
        if partition_names == ["search(.*)"]:
            insert_ids = insert_ids[par[0].num_entities:]
            if limit > par[1].num_entities:
                limit_check = par[1].num_entities
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit, default_search_exp,
                            partition_names, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit_check,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partition_empty(self, nq, dim, auto_id, _async):
        """
        target: test search the empty partition
        method: search from the empty partition
        expected: searched successfully with 0 results
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, auto_id=auto_id,
                                                    dim=dim, is_index=True)[0]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
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
        log.info("test_search_index_partition_empty: searching %s "
                 "entities through empty partition" % default_limit)
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, [partition_name],
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_jaccard_flat_index(self, nq, dim, auto_id, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with JACCARD
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = self.init_collection_general(prefix, True, 2,
                                                                                                  is_binary=True,
                                                                                                  auto_id=auto_id,
                                                                                                  dim=dim,
                                                                                                  is_index=True,
                                                                                                  is_flush=is_flush)[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.jaccard(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.jaccard(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  travel_timestamp=time_stamp,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_hamming_flat_index(self, nq, dim, auto_id, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with HAMMING
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=True,
                                                                                      is_flush=is_flush)[0:4]
        # 2. create index
        default_index = {"index_type": index, "params": {"nlist": 128}, "metric_type": "HAMMING"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        collection_w.load()
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.hamming(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.hamming(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "HAMMING", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 6843")
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_tanimoto_flat_index(self, nq, dim, auto_id, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with TANIMOTO
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=True,
                                                                                      is_flush=is_flush)[0:4]
        log.info("auto_id= %s, _async= %s" % (auto_id, _async))
        # 2. create index
        default_index = {"index_type": index, "params": {"nlist": 128}, "metric_type": "TANIMOTO"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "TANIMOTO", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_without_flush(self, metrics, auto_id):
        """
        target: test search without flush for binary data (no index)
        method: create connection, collection, insert, load and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize a collection without data
        collection_w = self.init_collection_general(prefix, is_binary=True, auto_id=auto_id)[0]
        # 2. insert data
        insert_ids = cf.insert_data(collection_w, default_nb, is_binary=True, auto_id=auto_id)[3]
        # 3. load data
        collection_w.load()
        # 4. search
        log.info("test_search_binary_without_flush: searching collection %s" % collection_w.name)
        binary_vectors = cf.gen_binary_vectors(default_nq, default_dim)[1]
        search_params = {"metric_type": metrics, "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_travel_time_without_expression(self, auto_id):
        """
        target: test search using travel time without expression
        method: 1. create connections,collection
                2. first insert, and return with timestamp1
                3. second insert, and return with timestamp2
                4. search before timestamp1 and timestamp2
        expected: 1 data inserted at a timestamp could not be searched before it
                  2 data inserted at a timestamp could be searched after it
        """
        # 1. create connection, collection and insert
        nb = 10
        collection_w, _, _, insert_ids_1, time_stamp_1 = \
            self.init_collection_general(prefix, True, nb, auto_id=auto_id, dim=default_dim)[0:5]
        # 2. insert for the second time
        log.info("test_search_travel_time_without_expression: inserting for the second time")
        _, entities, _, insert_ids_2, time_stamp_2 = cf.insert_data(collection_w, nb, auto_id=auto_id,
                                                                    dim=default_dim, insert_offset=nb)[0:5]
        # 3. extract vectors inserted for the second time
        entities_list = np.array(entities[0]).tolist()
        vectors = [entities_list[i][-1] for i in range(default_nq)]
        # 4. search with insert timestamp1
        log.info("test_search_travel_time_without_expression: searching collection %s with time_stamp_1 '%d'"
                 % (collection_w.name, time_stamp_1))
        search_res = collection_w.search(vectors, default_search_field,
                                         default_search_params, default_limit,
                                         travel_timestamp=time_stamp_1,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": insert_ids_1,
                                                      "limit": default_limit})[0]
        log.info("test_search_travel_time_without_expression: checking that data inserted "
                 "after time_stamp_2 is not searched at time_stamp_1")
        for i in range(len(search_res)):
            assert insert_ids_2[i] not in search_res[i].ids
        # 5. search with insert timestamp2
        log.info("test_search_travel_time_without_expression: searching collection %s with time_stamp_2 '%d'"
                 % (collection_w.name, time_stamp_2))
        search_res = collection_w.search(vectors, default_search_field,
                                         default_search_params, default_limit,
                                         travel_timestamp=time_stamp_2,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": insert_ids_1 + insert_ids_2,
                                                      "limit": default_limit})[0]
        log.info("test_search_travel_time_without_expression: checking that data inserted "
                 "after time_stamp_2 is searched at time_stamp_2")
        for i in range(len(search_res)):
            assert insert_ids_2[i] in search_res[i].ids

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_normal_expressions())
    def test_search_with_expression(self, dim, expression, _async):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True,
                                                                             nb, dim=dim,
                                                                             is_index=True)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i, _id in enumerate(insert_ids):
            int64 = _vectors.int64[i]
            float = _vectors.float[i]
            if not expression or eval(expression):
                filter_ids.append(_id)

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false"])
    def test_search_with_expression_bool(self, dim, auto_id, _async, bool_type):
        """
        target: test search with different bool expressions
        method: search with different bool expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                             is_all_data_type=True,
                                                                             auto_id=auto_id,
                                                                             dim=dim)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. filter result with expression in collection
        filter_ids = []
        bool_type_cmp = bool_type
        if bool_type == "true":
            bool_type_cmp = True
        if bool_type == "false":
            bool_type_cmp = False
        for i, _id in enumerate(insert_ids):
            if _vectors[0][f"{default_bool_field_name}"][i] == bool_type_cmp:
                filter_ids.append(_id)

        # 4. search with different expressions
        expression = f"{default_bool_field_name} == {bool_type}"
        log.info("test_search_with_expression_bool: searching with bool expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]

        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_normal_expressions_field(default_float_field_name))
    def test_search_with_expression_auto_id(self, dim, expression, _async):
        """
        target: test search with different expressions
        method: test search with different expressions with auto id
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                             auto_id=True,
                                                                             dim=dim,
                                                                             is_index=True)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i, _id in enumerate(insert_ids):
            exec(f"{default_float_field_name} = _vectors.{default_float_field_name}[i]")
            if not expression or eval(expression):
                filter_ids.append(_id)

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with different expressions
        log.info("test_search_with_expression_auto_id: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_all_data_type(self, nb, nq, dim, auto_id, _async):
        """
        target: test search using all supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      is_all_data_type=True,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search
        log.info("test_search_expression_all_data_type: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        search_exp = "int64 >= 0 && int32 >= 0 && int16 >= 0 " \
                     "&& int8 >= 0 && float >= 0 && double >= 0"
        res = collection_w.search(vectors[:nq], default_search_field,
                                  default_search_params, default_limit,
                                  search_exp, _async=_async,
                                  output_fields=[default_int64_field_name,
                                                 default_float_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert len(res[0][0].entity._row_data) != 0
        assert (default_int64_field_name and default_float_field_name) in res[0][0].entity._row_data

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields_empty(self, nb, nq, dim, auto_id, _async):
        """
        target: test search with output fields
        method: search with empty output_field
        expected: search success
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search
        log.info("test_search_with_output_fields_empty: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        res = collection_w.search(vectors[:nq], default_search_field,
                                  default_search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  output_fields=[],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert len(res[0][0].entity._row_data) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_output_field(self, auto_id, _async):
        """
        target: test search with output fields
        method: search with one output_field
        expected: search success
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id)[0:4]
        # 2. search
        log.info("test_search_with_output_field: Searching collection %s" % collection_w.name)

        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  default_search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  output_fields=[default_int64_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert len(res[0][0].entity._row_data) != 0
        assert default_int64_field_name in res[0][0].entity._row_data

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields(self, nb, nq, dim, auto_id, _async):
        """
        target: test search with output fields
        method: search with multiple output_field
        expected: search success
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      is_all_data_type=True,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search
        log.info("test_search_with_output_fields: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        res = collection_w.search(vectors[:nq], default_search_field,
                                  default_search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  output_fields=[default_int64_field_name,
                                                 default_float_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert len(res[0][0].entity._row_data) != 0
        assert (default_int64_field_name and default_float_field_name) in res[0][0].entity._row_data

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", [["*"], ["*", default_float_field_name]])
    def test_search_with_output_field_wildcard(self, output_fields, auto_id, _async):
        """
        target: test search with output fields using wildcard
        method: search with one output_field (wildcard)
        expected: search success
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id)[0:4]
        # 2. search
        log.info("test_search_with_output_field_wildcard: Searching collection %s" % collection_w.name)

        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  default_search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  output_fields=output_fields,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert len(res[0][0].entity._row_data) != 0
        assert (default_int64_field_name and default_float_field_name) in res[0][0].entity._row_data

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_collections(self, nb, nq, dim, auto_id, _async):
        """
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        """
        self._connect()
        collection_num = 10
        for i in range(collection_num):
            # 1. initialize with data
            log.info("test_search_multi_collections: search round %d" % (i + 1))
            collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                          auto_id=auto_id,
                                                                          dim=dim)[0:4]
            # 2. search
            vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
            log.info("test_search_multi_collections: searching %s entities (nq = %s) from collection %s" %
                     (default_limit, nq, collection_w.name))
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_concurrent_multi_threads(self, nb, nq, dim, auto_id, _async):
        """
        target: test concurrent search with multi-processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        # 1. initialize with data
        threads_num = 10
        threads = []
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim)[0:5]

        def search(collection_w):
            vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                travel_timestamp=time_stamp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info("test_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.skip(reason="Not running for now")
    @pytest.mark.tags(CaseLabel.L2)
    def test_search_insert_in_parallel(self):
        """
        target: test search and insert in parallel
        method: One process do search while other process do insert
        expected: No exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index(ct.default_float_vec_field_name, default_index)
        collection_w.load()

        def do_insert():
            df = cf.gen_default_dataframe_data(10000)
            for i in range(11):
                collection_w.insert(df)
                log.info(f'Collection num entities is : {collection_w.num_entities}')

        def do_search():
            while True:
                results, _ = collection_w.search(cf.gen_vectors(nq, ct.default_dim), default_search_field,
                                                 default_search_params, default_limit, default_search_exp, timeout=30)
                ids = []
                for res in results:
                    ids.extend(res.ids)
                expr = f'{ct.default_int64_field_name} in {ids}'
                collection_w.query(expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                                   timeout=30)

        p_insert = multiprocessing.Process(target=do_insert, args=())
        p_search = multiprocessing.Process(target=do_search, args=(), daemon=True)

        p_insert.start()
        p_search.start()

        p_insert.join()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_search_round_decimal(self, round_decimal):
        """
        target: test search with valid round decimal
        method: search with valid round decimal
        expected: search successfully
        """
        import math
        tmp_nb = 500
        tmp_nq = 1
        tmp_limit = 5
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, nb=tmp_nb)[0]
        # 2. search
        log.info("test_search_round_decimal: Searching collection %s" % collection_w.name)
        res, _ = collection_w.search(vectors[:tmp_nq], default_search_field,
                                     default_search_params, tmp_limit)

        res_round, _ = collection_w.search(vectors[:tmp_nq], default_search_field,
                                           default_search_params, tmp_limit, round_decimal=round_decimal)

        abs_tol = pow(10, 1 - round_decimal)
        # log.debug(f'abs_tol: {abs_tol}')
        for i in range(tmp_limit):
            dis_expect = round(res[0][i].distance, round_decimal)
            dis_actual = res_round[0][i].distance
            # log.debug(f'actual: {dis_actual}, expect: {dis_expect}')
            # abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
            assert math.isclose(dis_actual, dis_expect, rel_tol=0, abs_tol=abs_tol)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large(self, dim):
        """
        target: test search with large expression
        method: test search with large expression
        expected: searched successfully 
        """
        # 1. initialize with data
        nb = 10000
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      nb, dim=dim,
                                                                      is_index=True)[0:4]

        
        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression: searching with expression: %s" % expression)

        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={
                                             "nq": nums,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                            })

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large_two(self, dim):
        """
        target: test search with large expression
        method: test one of the collection ids to another collection search for it, with the large expression
        expected: searched successfully 
        """
        # 1. initialize with data
        nb = 10000
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                               nb, dim=dim,
                                                                               is_index=True)[0:4]                                                                 

        
        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

 
        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        vectors_id = [random.randint(0,nums)for _ in range(nums)]
        expression = f"{default_int64_field_name} in {vectors_id}"
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={
                                             "nq": nums,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                            })

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_bounded(self, nq, dim, auto_id, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "bounded"
        expected: searched successfully 
        """
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4] 
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async,
                                         })
        
        kwargs = {}
        consistency_level = kwargs.get("consistency_level", CONSISTENCY_BOUNDED)
        kwargs.update({"consistency_level": consistency_level})

        nb_new = 400
        _, _, _, insert_ids_new, _= cf.insert_data(collection_w, nb_new,
                                                   auto_id=auto_id, dim=dim,
                                                   insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            ) 

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_strong(self, nq, dim, auto_id, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "Strong"
        expected: searched successfully 
        """
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        
        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get("consistency_level", CONSISTENCY_STRONG)
        kwargs.update({"consistency_level": consistency_level})

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async}) 

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_eventually(self, nq, dim, auto_id, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "eventually"
        expected: searched successfully 
        """
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        nb_new = 400
        _, _, _, insert_ids_new, _= cf.insert_data(collection_w, nb_new,
                                                   auto_id=auto_id, dim=dim,
                                                   insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get("consistency_level", CONSISTENCY_EVENTUALLY)
        kwargs.update({"consistency_level": consistency_level})
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs
                            ) 

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_session(self, nq, dim, auto_id, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "session"
        expected: searched successfully 
        """
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4] 
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        
        kwargs = {}
        consistency_level = kwargs.get("consistency_level", CONSISTENCY_SESSION)
        kwargs.update({"consistency_level": consistency_level})

        nb_new = 400
        _, _, _, insert_ids_new, _= cf.insert_data(collection_w, nb_new,
                                                   auto_id=auto_id, dim=dim,
                                                   insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async}) 

class TestSearchBase(TestcaseBase):
    @pytest.fixture(
        scope="function",
        params=[1, 10]
    )
    def get_top_k(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=[1, 10, 1100]
    )
    def get_nq(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[8, 128])
    def dim(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_flat_top_k(self, get_nq):
        """
        target: test basic search function, all the search params is correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = 16385  # max top k is 16384
        nq = get_nq
        collection_w, data, _, insert_ids = self.init_collection_general(prefix, insert_data=True, nb=nq)[0:4]
        collection_w.load()
        if top_k <= max_top_k:
            res, _ = collection_w.search(vectors[:nq], default_search_field, default_search_params,
                                         top_k)
            assert len(res[0]) <= top_k
        else:
            collection_w.search(vectors[:nq], default_search_field, default_search_params,
                                top_k,
                                check_task=CheckTasks.err_res,
                                check_items={"err_code": 1,
                                             "err_msg": "no Available QueryNode result, "
                                                        "filter reason limit %s is too large," % top_k})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_index_empty_partition(self, index, params):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: add vectors into collection, search with the given vectors, check the result
        expected: the length of the result is top_k, search collection with partition tag return empty
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=True)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create patition
        partition_name = "search_partition_empty"
        collection_w.create_partition(partition_name=partition_name, description="search partition empty")
        par = collection_w.partitions
        collection_w.load()
        # 3. create different index
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 4. search
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     default_search_params, top_k,
                                     default_search_exp)

        assert len(res[0]) <= top_k

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, top_k,
                            default_search_exp, [partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_index_partitions(self, index, params, get_top_k):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=True)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create patition
        partition_name = ct.default_partition_name
        par = collection_w.partitions
        collection_w.load()
        # 3. create different index
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     ct.default_search_params, top_k,
                                     default_search_exp, [partition_name])
        assert len(res[0]) <= top_k

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_ip_flat(self, get_top_k):
        """
        target: test basic search function, all the search params are correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  dim=dim, is_index=True)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create ip index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     ct.default_search_params, top_k,
                                     default_search_exp)
        assert len(res[0]) <= top_k

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_ip_after_index(self, index, params):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim

        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  dim=dim, is_index=True)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create ip index
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     ct.default_search_params, top_k,
                                     default_search_exp)
        assert len(res[0]) <= top_k

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [2, 8, 128, 768])
    @pytest.mark.parametrize("nb", [1, 2, 10, 100])
    def test_search_ip_brute_force(self, nb, dim):
        """
        target: https://github.com/milvus-io/milvus/issues/17378. Ensure the logic of IP distances won't be changed.
        method: search with the given vectors, check the result
        expected: The inner product of vector themselves should be positive.
        """
        top_k = 1

        # 1. initialize with data
        collection_w, insert_entities, _, insert_ids, _ = self.init_collection_general(prefix, True, nb,
                                                                                       is_binary=False,
                                                                                       dim=dim)[0:5]
        insert_vectors = insert_entities[0][default_search_field].tolist()

        # 2. load collection.
        collection_w.load()

        # 3. search and then check if the distances are expected.
        res, _ = collection_w.search(insert_vectors[:nb], default_search_field,
                                     ct.default_search_ip_params, top_k,
                                     default_search_exp)
        for i, v in enumerate(insert_vectors):
            assert len(res[i]) == 1
            ref = ip(v, v)
            got = res[i][0].distance
            assert abs(got - ref) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_ip_index_empty_partition(self, index, params):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: add vectors into collection, search with the given vectors, check the result
        expected: the length of the result is top_k, search collection with partition tag return empty
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=True)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create patition
        partition_name = "search_partition_empty"
        collection_w.create_partition(partition_name=partition_name, description="search partition empty")
        par = collection_w.partitions
        collection_w.load()
        # 3. create different index
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 4. search
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     default_search_params, top_k,
                                     default_search_exp)

        assert len(res[0]) <= top_k

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, top_k,
                            default_search_exp, [partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_ip_index_partitions(self, index, params):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=True)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create patition
        par_name = collection_w.partitions[0].name
        collection_w.load()
        # 3. create different index
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 4. search
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     default_search_params, top_k,
                                     default_search_exp, [par_name])

        assert len(res[0]) <= top_k

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_without_connect(self):
        """
        target: test search vectors without connection
        method: use disconnected instance, call search method and check if search successfully
        expected: raise exception
        """
        self._connect()

        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True,
                                                                                  ct.default_nq, is_index=True)[0:5]
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(nq)]

        collection_w.load()
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list

        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     ct.default_search_params, ct.default_top_k,
                                     default_search_exp,
                                     check_task=CheckTasks.err_res,
                                     check_items={"err_code": 0,
                                                  "err_msg": "'should create connect first.'"})

    @pytest.mark.tags(CaseLabel.L2)
    # @pytest.mark.timeout(300)
    def test_search_concurrent_multithreads_single_connection(self, _async):
        """
        target: test concurrent search with multi processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        threads_num = 10
        threads = []
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, ct.default_nb)[0:5]

        def search(collection_w):
            vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                travel_timestamp=time_stamp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info("test_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_collections(self):
        """
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        """
        num = 10
        top_k = 10
        nq = 20

        for i in range(num):
            collection = gen_unique_str(uid + str(i))
            collection_w, _, _, insert_ids, time_stamp = \
                self.init_collection_general(collection, True, ct.default_nb)[0:5]
            assert len(insert_ids) == default_nb
            vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, top_k,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": top_k})


class TestSearchDSL(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    def test_query_vector_only(self):
        """
        target: test search normal scenario
        method: search vector only
        expected: search status ok, the length of result
        """
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, ct.default_nb)[0:5]
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, ct.default_top_k,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": ct.default_top_k})


class  TestsearchString(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search about string 
    ******************************************************************
    """

    @pytest.fixture(scope="function",
                    params=[default_nb, default_nb_medium])
    def nb(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[2, 500])
    def nq(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[8, 128])
    def dim(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary(self, auto_id, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data 
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim)[0:4]
        # 2. search
        log.info("test_search_string_field_not_primary: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, 
                            default_search_string_exp,
                            output_fields=output_fields,
                            _async=_async,
                            travel_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

        
    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_true(self, dim, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection and insert data 
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, dim=dim, primary_field=ct.default_string_field_name)[0:4]
        # 2. search
        log.info("test_search_string_field_is_primary_true: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_string_exp, 
                            output_fields=output_fields,
                            _async=_async,
                            travel_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})
        

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_mix_expr(self, dim, auto_id, _async):
        """
        target: test search with mix string and int expr
        method: create collection and insert data
                create index and collection load
                collection search uses mix expr 
        expected: Search successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim)[0:4]
        # 2. search
        log.info("test_search_string_mix_expr: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_mix_exp,
                            output_fields=output_fields,
                             _async=_async,
                            travel_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_with_invalid_expr(self, auto_id):
        """
        target: test search data
        method: create collection and insert data
                create index and collection load
                collection search uses invalid string expr  
        expected: Raise exception
        """

        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim)[0:4]
        # 2. search
        log.info("test_search_string_with_invalid_expr: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_invaild_string_exp, 
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": "failed to create query plan: type mismatch"}
                            )

          

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_normal_string_expressions(ct.default_string_field_name))
    def test_search_with_different_string_expr(self, dim, expression, _async):
        """
        target: test search with different string expressions
        method: test search with different string expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True,
                                                                             nb, dim=dim,
                                                                             is_index=True)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        filter_ids = []
        expression = expression.replace("&&", "and").replace("||", "or")
        for i, _id in enumerate(insert_ids):
            int64 = _vectors.int64[i]
            varchar = _vectors.varchar[i]
            if not expression or eval(expression):
                filter_ids.append(_id)

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_binary(self, dim, _async):
        """
        target: test search with string expr and string field is primary
        method: create collection and insert data 
                create index and collection load
                collection search uses string expr in string field ,string field is primary
        expected: Search successfully
        """
        
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      dim=dim,
                                                                                      is_index=True,
                                                                                      primary_field=ct.default_string_field_name)[0:4]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. search with exception
        binary_vectors = cf.gen_binary_vectors(3000, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = [default_string_field_name]
        collection_w.search(binary_vectors[:default_nq], "binary_vector", search_params,
                            default_limit, default_search_string_exp, output_fields=output_fields,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 2,
                                         "_async": _async})
        

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_binary(self, auto_id, dim, _async):
        """
        target: test search with string expr and string field is not primary
        method: create an binary collection and insert data 
                create index and collection load
                collection search uses string expr in string field, string field is not primary 
        expected: Search successfully
        """
         # 1. initialize with binary data
         
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=True)[0:4]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)   
        collection_w.load()                                                                           
        # 2. search with exception
        binary_vectors = cf.gen_binary_vectors(3000, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector", search_params,
                            default_limit, default_search_string_exp,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 2,
                                         "_async": _async})


    @pytest.mark.tags(CaseLabel.L2)
    def test_search_mix_expr_with_binary(self, dim, auto_id, _async):
        """
        target: test search with mix string and int expr
        method: create an binary collection and insert data
                create index and collection load
                collection search uses mix expr 
        expected: Search successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_binary=True, is_index=True)[0:4]
        # 2. create index
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 2. search
        log.info("test_search_mix_expr_with_binary: searching collection %s" % collection_w.name)
        binary_vectors = cf.gen_binary_vectors(3000, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = [default_string_field_name, default_float_field_name]
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            default_search_mix_exp,
                            output_fields=output_fields,
                             _async=_async,
                            travel_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary_perfix(self, auto_id, _async):
        """
        target: test search with string expr and string field is not primary
        method: create collection and insert data 
                create index and collection load
                collection search uses string expr in string field, string field is not primary
        expected: Search successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=default_dim)[0:4]
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param, index_name="a")
        index_param_two ={}
        collection_w.create_index("varchar", index_param_two, index_name="b")
        collection_w.load()
        # 2. search
        log.info("test_search_string_field_not_primary: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        output_fields = [default_float_field_name, default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, 
                            perfix_expr,
                            output_fields=output_fields,
                            _async=_async,
                            travel_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 1,
                                         "_async": _async}
                            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_all_index_with_compare_expr(self, _async):
        """
        target: test delete after creating index
        method: 1.create collection , insert data, primary_field is string field
                2.create string and float index ,delete entities, query 
                3.search
        expected: assert index and deleted id not in search result
        """
        # create collection, insert tmp_nb, flush and load
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, insert_data=True, primary_field=ct.default_string_field_name)[0:4]

        # create index
        index_params_one = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params_one, index_name=index_name1)
        index_params_two ={}
        collection_w.create_index(ct.default_string_field_name, index_params=index_params_two, index_name=index_name2)
        assert collection_w.has_index(index_name=index_name2)

        collection_w.release()
        collection_w.load()
        # delete entity
        expr = 'float >= int64'
        # search with id 0 vectors
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        output_fields = [default_int64_field_name, default_float_field_name,  default_string_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, 
                            expr,
                            output_fields=output_fields,
                            _async=_async,
                            travel_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async}
                            )