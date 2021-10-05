import pytest

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.utils import *
from common.constants import *

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
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
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
default_query, default_query_vecs = gen_query_vectors(field_name, entities, default_top_k, nq)
default_binary_query, default_binary_query_vecs = gen_query_vectors(binary_field_name,
                                                                    binary_entities,
                                                                    default_top_k,
                                                                    nq)


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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_invalid_field_type(self, get_invalid_fields_type):
        """
        target: test search with invalid parameter type
        method: search with invalid field
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_invalid_field_value(self, get_invalid_fields_value):
        """
        target: test search with invalid parameter values
        method: search with invalid field
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 6727")
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
                                                                      is_index=True)
        # 2. create index and load
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        invalid_search_params = cf.gen_invaild_search_params_type()
        for invalid_search_param in invalid_search_params:
            if index == invalid_search_param["index_type"]:
                search_params = {"metric_type": "L2", "params": invalid_search_param["search_params"]}
                collection_w.search(vectors[:default_nq], default_search_field,
                                    search_params, default_limit,
                                    default_search_exp,
                                    check_task=CheckTasks.err_res,
                                    check_items={"err_code": 0,
                                                 "err_msg": "metric type not found"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_invalid_limit_type(self, get_invalid_limit):
        """
        target: test search with invalid limit type
        method: search with invalid limit
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

    @pytest.mark.tags(CaseLabel.L1)
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
        log.info("test_search_param_invalid_limit: searching with "
                 "invalid limit (topK) = %s" % limit)
        err_msg = "limit %d is too large!" % limit
        if limit == 0:
            err_msg = "`limit` value 0 is illegal"
        collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                            limit, default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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
                3. search with specifying the released partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        partition_num = 1
        collection_w = self.init_collection_general(prefix, True, 10, partition_num)[0]
        par = collection_w.partitions
        par_name = par[partition_num].name
        # 2. release partition
        conn = self.connection_wrap.get_connection()[0]
        conn.release_partitions(collection_w.name, [par_name])
        # 3. Search the released partition
        log.info("test_search_release_partition: Searching specifying the released partition")
        limit = 10
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_partition_deleted(self):
        """
        target: test search deleted partition
        method: 1. search the collection
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 6731")
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_different_index_invalid_params(self, index, params):
        """
        target: test search with different index
        method: test search with different index
        expected: searched successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 5000,
                                                                      partition_num=1,
                                                                      is_index=True)
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
        collection_w.search(vectors, default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_output_fields_not_exist(self):
        """
        target: test search with output fields
        method: search with non-exist output_field
        expected: search success
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True)
        # 2. search
        log.info("test_search_with_output_fields_not_exist: Searching collection %s" % collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=["int63"],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: 'Field int63 not exist'})

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
        log.info("test_search_output_field_vector: Searching collection %s" % collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1,
                                         "err_msg": f"Field {output_fields[-1]} not exist"})


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

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_normal(self, nq, dim, auto_id):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim)
        # 2. search
        log.info("test_search_normal: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tag(CaseLabel.L0)
    def test_search_with_hit_vectors(self, nq, dim, auto_id):
        """
        target: test search with vectors in collections
        method: create connections,collection insert and search vectors in collections
        expected: search successfully with limit(topK) and can be hit at top 1 (min distance is 0)
        """
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim)
        # get vectors that inserted into collection
        vectors = np.array(_vectors[0]).tolist()
        vectors = [vectors[i][-1] for i in range(nq)]
        search_res, _ = collection_w.search(vectors[:nq], default_search_field,
                                            default_search_params, default_limit,
                                            default_search_exp,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nq,
                                                         "ids": insert_ids,
                                                         "limit": default_limit})
        for hits in search_res:
            # verify that top 1 hit is itself,so min distance is 0
            assert hits.distances[0] == 0.0

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
    @pytest.mark.parametrize("search_params", [{}, {"params": {}}, {"params": {"nprobe": 10}}])
    def test_search_normal_default_params(self, dim, auto_id, search_params, _async):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim)
        # 2. search
        log.info("test_search_normal: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
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
                                                                      dim=dim)
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_partition_after_release_one(self, nq, dim, auto_id, _async):
        """
        target: test search function before and after release
        method: 1. search the collection
                2. release a partition
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
                                                                      dim=dim)
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info("test_search_partition_after_release_one: searching before deleting partitions")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})
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
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids[:entity_num],
                                         "limit": limit - deleted_entity_num,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_partition_after_release_all(self, nq, dim, auto_id, _async):
        """
        target: test search function before and after release
        method: 1. search the collection
                2. release a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        # 1. initialize with data
        nb = 1000
        limit = 1000
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      1, auto_id=auto_id,
                                                                      dim=dim)
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info("test_search_partition_after_release_all: searching before deleting partitions")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})
        # 3. release all partitions
        log.info("test_search_partition_after_release_all: releasing a partition")
        par = collection_w.partitions
        conn = self.connection_wrap.get_connection()[0]
        conn.release_partitions(collection_w.name, [par[0].name, par[1].name])
        log.info("test_search_partition_after_release_all: released a partition")
        # 4. search collection after release all partitions
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
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
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      1, auto_id=auto_id,
                                                                      dim=dim)
        # 2. release collection
        collection_w.release()
        # 3. Search the pre-released collection after load
        collection_w.load()
        log.info("test_search_collection_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 6997")
    def test_search_partition_after_release_load(self, nb, nq, dim, auto_id, _async):
        """
        target: search the pre-released collection after load
        method: 1. create collection
                2. release a partition
                3. load partition
                4. search the pre-released partition
        expected: search successfully
        """
        # 1. initialize without data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      1, auto_id=auto_id,
                                                                      dim=dim)
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
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})
        # 4. Search the pre-released partition after load
        if limit > par[1].num_entities:
            limit_check = par[1].num_entities
        else:
            limit_check = limit
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            limit, default_search_exp,
                            [par[1].name], _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids[par[0].num_entities:],
                                         "limit": limit_check,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_load_flush_load(self, nb, nq, dim, auto_id, _async):
        """
        target: test search when load before flush
        method: 1. search the collection
                2. insert data and load
                3. flush, and load
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
        # 5. search for new data without load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_new_data(self, nq, dim, auto_id, _async):
        """
        target: test search new inserted data without load
        method: 1. search the collection
                2. insert new data
                3. search the collection without load again
        expected: new data should be searched
        """
        # 1. initialize with data
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info("test_search_new_data: searching for original data after load")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        # 3. insert new data
        nb_new = 300
        insert_ids_new = cf.insert_data(collection_w, nb_new,
                                        auto_id=auto_id, dim=dim)[3]
        insert_ids.extend(insert_ids_new)
        # gracefulTime is default as 1s which allows data
        # could not be searched instantly in gracefulTime
        time.sleep(gracefulTime)
        # 4. search for new data without load
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="skip temporarily for debug")
    def test_search_max_dim(self, nq, auto_id, _async):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, default_nb,
                                                                      auto_id=auto_id,
                                                                      dim=max_dim)
        # 2. search
        log.info("test_search_max_dim: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(max_dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, 2,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": 2,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index, params",
                             zip(ct.all_index_types[:9],
                                 ct.default_index_params[:9]))
    def test_search_after_different_index_with_params(self, dim, index, params, auto_id, _async):
        """
        target: test search with invalid search params
        method: test search with invalid params type
        expected: raise exception and report the error
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 5000,
                                                                      partition_num=1,
                                                                      auto_id=auto_id,
                                                                      dim=dim, is_index=True)
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
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 5000,
                                                                      partition_num=1,
                                                                      auto_id=auto_id,
                                                                      dim=dim, is_index=True)
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
                                                                      dim=dim)
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
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim)
        # 2. search
        log.info("test_search_sync_async_multiple_times: searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        for i in range(search_num):
            log.info("test_search_sync_async_multiple_times: searching round %d" % (i + 1))
            for _async in [False, True]:
                collection_w.search(vectors[:nq], default_search_field,
                                    default_search_params, default_limit,
                                    default_search_exp, _async=_async,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": nq,
                                                 "ids": insert_ids,
                                                 "limit": default_limit,
                                                 "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multiple_vectors(self, nb, nq, dim, auto_id, _async):
        """
        target: test search with multiple vectors
        method: create connection, collection with multiple
                vectors, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. connect
        self._connect()
        # 2. create collection with multiple vectors
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_float_vec_field(dim=dim), cf.gen_float_vec_field(name="tmp", dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        collection_w = self.collection_wrap.init_collection(c_name, schema=schema,
                                                            check_task=CheckTasks.check_collection_property,
                                                            check_items={"name": c_name, "schema": schema})[0]
        # 3. insert
        vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
        vectors_tmp = [[random.random() for _ in range(dim)] for _ in range(nb)]
        data = [[i for i in range(nb)], [np.float32(i) for i in range(nb)], vectors, vectors_tmp]
        if auto_id:
            data = [[np.float32(i) for i in range(nb)], vectors, vectors_tmp]
        res = collection_w.insert(data)
        insert_ids = res.primary_keys
        assert collection_w.num_entities == nb
        # 4. load
        collection_w.load()
        # 5. search all the vectors
        log.info("test_search_multiple_vectors: searching collection %s" % collection_w.name)
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})
        collection_w.search(vectors[:nq], "tmp",
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_index_one_partition(self, nb, auto_id, _async):
        """
        target: test search from partition
        method: search from one partition
        expected: searched successfully
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num=1,
                                                                      auto_id=auto_id,
                                                                      is_index=True)

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
                                                                      is_index=True)
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
                                                                      dim=dim)
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
    def test_search_binary_jaccard_flat_index(self, nq, dim, auto_id, _async, index):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with JACCARD
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=True)
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
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_hamming_flat_index(self, nq, dim, auto_id, _async, index):
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
                                                                                      is_index=True)
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
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue 6843")
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_tanimoto_flat_index(self, nq, dim, auto_id, _async, index):
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
                                                                                      is_index=True)
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
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

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
                                                                             is_index=True)

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
    @pytest.mark.xfail(reason="issue 7910")
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false", 1, 0, 2])
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
                                                                             dim=dim)

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
    @pytest.mark.parametrize("expression", cf.gen_normal_expressions_field(default_float_field_name))
    def test_search_with_expression_auto_id(self, dim, expression, _async):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                             auto_id=True,
                                                                             dim=dim,
                                                                             is_index=True)

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
    def test_search_expression_all_data_type(self, nb, nq, dim, auto_id, _async):
        """
        target: test search using different supported data type
        method: search using different supported data type
        expected: search success
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      is_all_data_type=True,
                                                                      auto_id=auto_id,
                                                                      dim=dim)
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
                                                                      dim=dim)
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
                                                                      auto_id=auto_id)
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
                                                                      dim=dim)
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
                                                                      auto_id=auto_id)
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
                                                                          dim=dim)
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
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim)

        def search(collection_w):
            vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
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


"""
******************************************************************
#  The following cases are copied from test_search.py
******************************************************************
"""


def init_data(connect, collection, nb=3000, partition_names=None, auto_id=True):
    """
    Generate entities and add it in collection
    """
    global entities
    if nb == 3000:
        insert_entities = entities
    else:
        insert_entities = gen_entities(nb, is_normal=True)
    if partition_names is None:
        res = connect.insert(collection, insert_entities)
    else:
        res = connect.insert(collection, insert_entities, partition_name=partition_names)
    connect.flush([collection])
    ids = res.primary_keys
    return insert_entities, ids


def init_binary_data(connect, collection, nb=3000, insert=True, partition_names=None):
    """
    Generate entities and add it in collection
    """
    ids = []
    global binary_entities
    global raw_vectors
    if nb == 3000:
        insert_entities = binary_entities
        insert_raw_vectors = raw_vectors
    else:
        insert_raw_vectors, insert_entities = gen_binary_entities(nb)
    if insert is True:
        if partition_names is None:
            res = connect.insert(collection, insert_entities)
        else:
            res = connect.insert(collection, insert_entities, partition_name=partition_names)
        connect.flush([collection])
        ids = res.primary_keys
    return insert_raw_vectors, insert_entities, ids


def check_id_result(result, id):
    limit_in = 5
    ids = [entity.id for entity in result]
    if len(result) >= limit_in:
        return id in ids[:limit_in]
    else:
        return id in ids


class TestSearchBase:
    """
    generate valid create_index params
    """

    @pytest.fixture(
        scope="function",
        params=gen_index()
    )
    def get_index(self, request, connect):
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("sq8h not support in CPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("sq8h not support in CPU mode")
        return copy.deepcopy(request.param)

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_jaccard_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] in binary_support():
            return request.param
        # else:
        #     pytest.skip("Skip index Temporary")

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_hamming_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] in binary_support():
            return request.param
        # else:
        #     pytest.skip("Skip index Temporary")

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_structure_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == "FLAT":
            return request.param
        # else:
        #     pytest.skip("Skip index Temporary")

    """
    generate top-k params
    """

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

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_flat_top_k(self, connect, collection, get_nq):
        """
        target: test basic search function, all the search params is correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = 16385 # max top k is 16384
        nq = get_nq
        entities, ids = init_data(connect, collection)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq)
        if top_k <= max_top_k:
            connect.load_collection(collection)
            res = connect.search(collection, query)
            assert len(res[0]) == top_k
            assert res[0]._distances[0] <= epsilon
            assert check_id_result(res[0], ids[0])
        else:
            with pytest.raises(Exception) as e:
                res = connect.search(collection, query)

    @pytest.mark.skip("r0.3-test")
    def _test_search_field(self, connect, collection, get_top_k, get_nq):
        """
        target: test basic search function, all the search params is correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = get_nq
        entities, ids = init_data(connect, collection)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq)
        if top_k <= max_top_k:
            connect.load_collection(collection)
            res = connect.search(collection, query, fields=["float_vector"])
            assert len(res[0]) == top_k
            assert res[0]._distances[0] <= epsilon
            assert check_id_result(res[0], ids[0])
            res = connect.search(collection, query, fields=["float"])
            for i in range(nq):
                assert entities[1]["values"][:nq][i] in [r.entity.get('float') for r in res[i]]
        else:
            with pytest.raises(Exception):
                connect.search(collection, query)

    def _test_search_after_delete(self, connect, collection, get_top_k, get_nq):
        """
        target: test basic search function before and after deletion, all the search params is
                correct, change top-k value.
                check issue <a href="https://github.com/milvus-io/milvus/issues/4200">#4200</a>
        method: search with the given vectors, check the result
        expected: the deleted entities do not exist in the result.
        """
        top_k = get_top_k
        nq = get_nq

        entities, ids = init_data(connect, collection, nb=10000)
        first_int64_value = entities[0]["values"][0]
        first_vector = entities[2]["values"][0]

        search_param = get_search_param("FLAT")
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq, search_params=search_param)
        vecs[:] = []
        vecs.append(first_vector)

        res = None
        if top_k > max_top_k:
            with pytest.raises(Exception):
                connect.search(collection, query, fields=['int64'])
            # pytest.skip("top_k value is larger than max_topp_k")
            pass
        else:
            res = connect.search(collection, query, fields=['int64'])
            assert len(res) == 1
            assert len(res[0]) >= top_k
            assert res[0][0].id == ids[0]
            assert res[0][0].entity.get("int64") == first_int64_value
            assert res[0]._distances[0] < epsilon
            assert check_id_result(res[0], ids[0])

        connect.delete_entity_by_id(collection, ids[:1])
        connect.flush([collection])

        res2 = connect.search(collection, query, fields=['int64'])
        assert len(res2) == 1
        assert len(res2[0]) >= top_k
        assert res2[0][0].id != ids[0]
        if top_k > 1:
            assert res2[0][0].id == res[0][1].id
            assert res2[0][0].entity.get("int64") == res[0][1].entity.get("int64")

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_empty_partition(self, connect, collection, get_simple_index, get_top_k, get_nq):
        """
        target: test basic search function, all the search params is correct, test all index params, and build
        method: add vectors into collection, search with the given vectors, check the result
        expected: the length of the result is top_k, search collection with partition tag return empty
        """
        top_k = get_top_k
        nq = get_nq

        index_type = get_simple_index["index_type"]
        if index_type in skip_pq():
            pytest.skip("Skip PQ")
        connect.create_partition(collection, default_tag)
        entities, ids = init_data(connect, collection)
        connect.create_index(collection, field_name, get_simple_index)
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq, search_params=search_param)
        if top_k > max_top_k:
            with pytest.raises(Exception) as e:
                res = connect.search(collection, query)
        else:
            connect.load_collection(collection)
            res = connect.search(collection, query)
            assert len(res) == nq
            assert len(res[0]) >= top_k
            assert res[0]._distances[0] < epsilon
            assert check_id_result(res[0], ids[0])
            connect.release_collection(collection)
            connect.load_partitions(collection, [default_tag])
            res = connect.search(collection, query, partition_names=[default_tag])
            assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partitions(self, connect, collection, get_simple_index, get_top_k):
        """
        target: test basic search function, all the search params is correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = 2
        new_tag = "new_tag"
        index_type = get_simple_index["index_type"]
        if index_type in skip_pq():
            pytest.skip("Skip PQ")
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        entities, ids = init_data(connect, collection, partition_names=default_tag)
        new_entities, new_ids = init_data(connect, collection, nb=6001, partition_names=new_tag)
        connect.create_index(collection, field_name, get_simple_index)
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq, search_params=search_param)
        if top_k > max_top_k:
            with pytest.raises(Exception) as e:
                res = connect.search(collection, query)
        else:
            connect.load_collection(collection)
            res = connect.search(collection, query)
            assert check_id_result(res[0], ids[0])
            assert not check_id_result(res[1], new_ids[0])
            assert res[0]._distances[0] < epsilon
            assert res[1]._distances[0] < epsilon
            res = connect.search(collection, query, partition_names=[new_tag])
            assert res[0]._distances[0] > epsilon
            assert res[1]._distances[0] > epsilon
            connect.release_collection(collection)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_ip_flat(self, connect, collection, get_simple_index, get_top_k, get_nq):
        """
        target: test basic search function, all the search params is correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = get_nq
        entities, ids = init_data(connect, collection)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq, metric_type="IP")
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res[0]) == top_k
        assert res[0]._distances[0] >= 1 - gen_inaccuracy(res[0]._distances[0])
        assert check_id_result(res[0], ids[0])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_ip_after_index(self, connect, collection, get_simple_index, get_top_k, get_nq):
        """
        target: test basic search function, all the search params is correct, test all index params, and build
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = get_nq

        index_type = get_simple_index["index_type"]
        if index_type in skip_pq():
            pytest.skip("Skip PQ")
        entities, ids = init_data(connect, collection)
        get_simple_index["metric_type"] = "IP"
        connect.create_index(collection, field_name, get_simple_index)
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq, metric_type="IP", search_params=search_param)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) >= top_k
        assert check_id_result(res[0], ids[0])
        assert res[0]._distances[0] >= 1 - gen_inaccuracy(res[0]._distances[0])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_ip_index_empty_partition(self, connect, collection, get_simple_index, get_top_k, get_nq):
        """
        target: test basic search function, all the search params is correct, test all index params, and build
        method: add vectors into collection, search with the given vectors, check the result
        expected: the length of the result is top_k, search collection with partition tag return empty
        """
        top_k = get_top_k
        nq = get_nq
        metric_type = "IP"
        index_type = get_simple_index["index_type"]
        if index_type in skip_pq():
            pytest.skip("Skip PQ")
        connect.create_partition(collection, default_tag)
        entities, ids = init_data(connect, collection)
        get_simple_index["metric_type"] = metric_type
        connect.create_index(collection, field_name, get_simple_index)
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq, metric_type=metric_type,
                                        search_params=search_param)
        if top_k > max_top_k:
            with pytest.raises(Exception) as e:
                res = connect.search(collection, query)
        else:
            connect.load_collection(collection)
            res = connect.search(collection, query)
            assert len(res) == nq
            assert len(res[0]) >= top_k
            assert res[0]._distances[0] >= 1 - gen_inaccuracy(res[0]._distances[0])
            assert check_id_result(res[0], ids[0])
            res = connect.search(collection, query, partition_names=[default_tag])
            assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_ip_index_partitions(self, connect, collection, get_simple_index, get_top_k):
        """
        target: test basic search function, all the search params is correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = 2
        metric_type = "IP"
        new_tag = "new_tag"
        index_type = get_simple_index["index_type"]
        if index_type in skip_pq():
            pytest.skip("Skip PQ")
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        entities, ids = init_data(connect, collection, partition_names=default_tag)
        new_entities, new_ids = init_data(connect, collection, nb=6001, partition_names=new_tag)
        get_simple_index["metric_type"] = metric_type
        connect.create_index(collection, field_name, get_simple_index)
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, entities, top_k, nq, metric_type="IP", search_params=search_param)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert check_id_result(res[0], ids[0])
        assert not check_id_result(res[1], new_ids[0])
        assert res[0]._distances[0] >= 1 - gen_inaccuracy(res[0]._distances[0])
        assert res[1]._distances[0] >= 1 - gen_inaccuracy(res[1]._distances[0])
        res = connect.search(collection, query, partition_names=["new_tag"])
        assert res[0]._distances[0] < 1 - gen_inaccuracy(res[0]._distances[0])
        # TODO:
        # assert res[1]._distances[0] >= 1 - gen_inaccuracy(res[1]._distances[0])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_without_connect(self, dis_connect, collection):
        """
        target: test search vectors without connection
        method: use dis connected instance, call search method and check if search successfully
        expected: raise exception
        """
        with pytest.raises(Exception) as e:
            res = dis_connect.search(collection, default_query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_collection_not_existed(self, connect):
        """
        target: search collection not existed
        method: search with the random collection_name, which is not in db
        expected: status not ok
        """
        collection_name = gen_unique_str(uid)
        with pytest.raises(Exception) as e:
            res = connect.search(collection_name, default_query)

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_distance_l2(self, connect, collection):
        """
        target: search collection, and check the result: distance
        method: compare the return distance value with value computed with Euclidean
        expected: the return distance equals to the computed value
        """
        nq = 2
        search_param = {"nprobe": 1}
        entities, ids = init_data(connect, collection, nb=nq)
        query, vecs = gen_query_vectors(field_name, entities, default_top_k, nq, rand_vector=True,
                                        search_params=search_param)
        inside_query, inside_vecs = gen_query_vectors(field_name, entities, default_top_k, nq,
                                                      search_params=search_param)
        distance_0 = l2(vecs[0], inside_vecs[0])
        distance_1 = l2(vecs[0], inside_vecs[1])
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert abs(np.sqrt(res[0]._distances[0]) - min(distance_0, distance_1)) <= gen_inaccuracy(res[0]._distances[0])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_distance_l2_after_index(self, connect, id_collection, get_simple_index):
        """
        target: search collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        """
        index_type = get_simple_index["index_type"]
        nq = 2
        entities, ids = init_data(connect, id_collection, auto_id=False)
        connect.create_index(id_collection, field_name, get_simple_index)
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, entities, default_top_k, nq, rand_vector=True,
                                        search_params=search_param)
        inside_vecs = entities[-1]["values"]
        min_distance = 1.0
        min_id = None
        for i in range(default_nb):
            tmp_dis = l2(vecs[0], inside_vecs[i])
            if min_distance > tmp_dis:
                min_distance = tmp_dis
                min_id = ids[i]
        connect.load_collection(id_collection)
        res = connect.search(id_collection, query)
        tmp_epsilon = epsilon
        check_id_result(res[0], min_id)
        # if index_type in ["ANNOY", "IVF_PQ"]:
        #     tmp_epsilon = 0.1
        # TODO:
        # assert abs(np.sqrt(res[0]._distances[0]) - min_distance) <= tmp_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_distance_ip(self, connect, collection):
        """
        target: search collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        """
        nq = 2
        metirc_type = "IP"
        search_param = {"nprobe": 1}
        entities, ids = init_data(connect, collection, nb=nq)
        query, vecs = gen_query_vectors(field_name, entities, default_top_k, nq, rand_vector=True,
                                        metric_type=metirc_type,
                                        search_params=search_param)
        inside_query, inside_vecs = gen_query_vectors(field_name, entities, default_top_k, nq,
                                                      search_params=search_param)
        distance_0 = ip(vecs[0], inside_vecs[0])
        distance_1 = ip(vecs[0], inside_vecs[1])
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert abs(res[0]._distances[0] - max(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_distance_ip_after_index(self, connect, id_collection, get_simple_index):
        """
        target: search collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        """
        index_type = get_simple_index["index_type"]
        nq = 2
        metirc_type = "IP"
        entities, ids = init_data(connect, id_collection, auto_id=False)
        get_simple_index["metric_type"] = metirc_type
        connect.create_index(id_collection, field_name, get_simple_index)
        search_param = get_search_param(index_type)
        query, vecs = gen_query_vectors(field_name, entities, default_top_k, nq, rand_vector=True,
                                        metric_type=metirc_type,
                                        search_params=search_param)
        inside_vecs = entities[-1]["values"]
        max_distance = 0
        max_id = None
        for i in range(default_nb):
            tmp_dis = ip(vecs[0], inside_vecs[i])
            if max_distance < tmp_dis:
                max_distance = tmp_dis
                max_id = ids[i]
        connect.load_collection(id_collection)
        res = connect.search(id_collection, query)
        tmp_epsilon = epsilon
        check_id_result(res[0], max_id)
        # if index_type in ["ANNOY", "IVF_PQ"]:
        #     tmp_epsilon = 0.1
        # TODO:
        # assert abs(res[0]._distances[0] - max_distance) <= tmp_epsilon

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_distance_jaccard_flat_index(self, connect, binary_collection):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with L2
        expected: the return distance equals to the computed value
        """
        nq = 1
        int_vectors, entities, ids = init_binary_data(connect, binary_collection, nb=2)
        query_int_vectors, query_entities, tmp_ids = init_binary_data(connect, binary_collection, nb=1, insert=False)
        distance_0 = jaccard(query_int_vectors[0], int_vectors[0])
        distance_1 = jaccard(query_int_vectors[0], int_vectors[1])
        query, vecs = gen_query_vectors(binary_field_name, query_entities, default_top_k, nq, metric_type="JACCARD")
        connect.load_collection(binary_collection)
        res = connect.search(binary_collection, query)
        assert abs(res[0]._distances[0] - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_flat_with_L2(self, connect, binary_collection):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with L2
        expected: the return distance equals to the computed value
        """
        nq = 1
        int_vectors, entities, ids = init_binary_data(connect, binary_collection, nb=2)
        query_int_vectors, query_entities, tmp_ids = init_binary_data(connect, binary_collection, nb=1, insert=False)
        query, vecs = gen_query_vectors(binary_field_name, query_entities, default_top_k, nq, metric_type="L2")
        with pytest.raises(Exception) as e:
            connect.search(binary_collection, query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_distance_hamming_flat_index(self, connect, binary_collection):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        """
        nq = 1
        int_vectors, entities, ids = init_binary_data(connect, binary_collection, nb=2)
        query_int_vectors, query_entities, tmp_ids = init_binary_data(connect, binary_collection, nb=1, insert=False)
        distance_0 = hamming(query_int_vectors[0], int_vectors[0])
        distance_1 = hamming(query_int_vectors[0], int_vectors[1])
        query, vecs = gen_query_vectors(binary_field_name, query_entities, default_top_k, nq, metric_type="HAMMING")
        connect.load_collection(binary_collection)
        res = connect.search(binary_collection, query)
        assert abs(res[0][0].distance - min(distance_0, distance_1).astype(float)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_distance_substructure_flat_index(self, connect, binary_collection):
        """
        target: search binary_collection, and check the result: distance
        method: search with new random binary entities and SUBSTRUCTURE metric type
        expected: the return distance equals to the computed value
        """
        nq = 1
        int_vectors, entities, ids = init_binary_data(connect, binary_collection, nb=2)
        query_int_vectors, query_entities, tmp_ids = init_binary_data(connect, binary_collection, nb=1, insert=False)
        distance_0 = substructure(query_int_vectors[0], int_vectors[0])
        distance_1 = substructure(query_int_vectors[0], int_vectors[1])
        query, vecs = gen_query_vectors(binary_field_name, query_entities, default_top_k, nq,
                                        metric_type="SUBSTRUCTURE")
        connect.load_collection(binary_collection)
        res = connect.search(binary_collection, query)
        assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_distance_substructure_flat_index_B(self, connect, binary_collection):
        """
        target: search binary_collection, and check the result: distance
        method: search with entities that related to inserted entities
        expected: the return distance equals to the computed value
        """
        top_k = 3
        int_vectors, entities, ids = init_binary_data(connect, binary_collection, nb=2)
        query_int_vectors, query_vecs = gen_binary_sub_vectors(int_vectors, 2)
        query, vecs = gen_query_vectors(binary_field_name, entities, top_k, nq, metric_type="SUBSTRUCTURE",
                                        replace_vecs=query_vecs)
        connect.load_collection(binary_collection)
        res = connect.search(binary_collection, query)
        assert res[0][0].distance <= epsilon
        assert res[0][0].id == ids[0]
        assert res[1][0].distance <= epsilon
        assert res[1][0].id == ids[1]

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_distance_superstructure_flat_index(self, connect, binary_collection):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        """
        nq = 1
        int_vectors, entities, ids = init_binary_data(connect, binary_collection, nb=2)
        query_int_vectors, query_entities, tmp_ids = init_binary_data(connect, binary_collection, nb=1, insert=False)
        distance_0 = superstructure(query_int_vectors[0], int_vectors[0])
        distance_1 = superstructure(query_int_vectors[0], int_vectors[1])
        query, vecs = gen_query_vectors(binary_field_name, query_entities, default_top_k, nq,
                                        metric_type="SUPERSTRUCTURE")
        connect.load_collection(binary_collection)
        res = connect.search(binary_collection, query)
        assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_distance_superstructure_flat_index_B(self, connect, binary_collection):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with SUPER
        expected: the return distance equals to the computed value
        """
        top_k = 3
        int_vectors, entities, ids = init_binary_data(connect, binary_collection, nb=2)
        query_int_vectors, query_vecs = gen_binary_super_vectors(int_vectors, 2)
        query, vecs = gen_query_vectors(binary_field_name, entities, top_k, nq, metric_type="SUPERSTRUCTURE",
                                        replace_vecs=query_vecs)
        connect.load_collection(binary_collection)
        res = connect.search(binary_collection, query)
        assert len(res[0]) == 2
        assert len(res[1]) == 2
        assert res[0][0].id in ids
        assert res[0][0].distance <= epsilon
        assert res[1][0].id in ids
        assert res[1][0].distance <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_distance_tanimoto_flat_index(self, connect, binary_collection):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with Inner product
        expected: the return distance equals to the computed value
        """
        nq = 1
        int_vectors, entities, ids = init_binary_data(connect, binary_collection, nb=2)
        query_int_vectors, query_entities, tmp_ids = init_binary_data(connect, binary_collection, nb=1, insert=False)
        distance_0 = tanimoto(query_int_vectors[0], int_vectors[0])
        distance_1 = tanimoto(query_int_vectors[0], int_vectors[1])
        query, vecs = gen_query_vectors(binary_field_name, query_entities, default_top_k, nq, metric_type="TANIMOTO")
        connect.load_collection(binary_collection)
        res = connect.search(binary_collection, query)
        assert abs(res[0][0].distance - min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(300)
    def test_search_concurrent_multithreads_single_connection(self, connect, args):
        """
        target: test concurrent search with multiprocessess
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        nb = 100
        top_k = 10
        threads_num = 4
        threads = []
        collection = gen_unique_str(uid)
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        # create collection
        milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
        milvus.create_collection(collection, default_fields)
        entities, ids = init_data(milvus, collection)
        connect.load_collection(collection)

        def search(milvus):
            res = milvus.search(collection, default_query)
            assert len(res) == 1
            assert res[0]._entities[0].id in ids
            assert res[0]._distances[0] < epsilon

        for i in range(threads_num):
            t = MyThread(target=search, args=(milvus,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_collections(self, connect, args):
        """
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        """
        num = 10
        top_k = 10
        nq = 20
        collection_names = []
        for i in range(num):
            collection = gen_unique_str(uid + str(i))
            connect.create_collection(collection, default_fields)
            collection_names.append(collection)
            entities, ids = init_data(connect, collection)
            assert len(ids) == default_nb
            query, vecs = gen_query_vectors(field_name, entities, top_k, nq, search_params=search_param)
            connect.load_collection(collection)
            res = connect.search(collection, query)
            assert len(res) == nq
            for i in range(nq):
                assert check_id_result(res[i], ids[i])
                assert res[i]._distances[0] < epsilon
                assert res[i]._distances[1] > epsilon
        for i in range(num):
            connect.drop_collection(collection_names[i])


class TestSearchDSL(object):
    """
    ******************************************************************
    #  The following cases are used to build invalid query expr
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_no_must(self, connect, collection):
        """
        method: build query without must expr
        expected: error raised
        """
        # entities, ids = init_data(connect, collection)
        query = update_query_expr(default_query, keep_old=False)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_no_vector_term_only(self, connect, collection):
        """
        method: build query without vector only term
        expected: error raised
        """
        # entities, ids = init_data(connect, collection)
        expr = {
            "must": [gen_default_term_expr]
        }
        query = update_query_expr(default_query, keep_old=False, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_no_vector_range_only(self, connect, collection):
        """
        method: build query without vector only range
        expected: error raised
        """
        # entities, ids = init_data(connect, collection)
        expr = {
            "must": [gen_default_range_expr]
        }
        query = update_query_expr(default_query, keep_old=False, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_vector_only(self, connect, collection):
        entities, ids = init_data(connect, collection)
        connect.load_collection(collection)
        res = connect.search(collection, default_query)
        assert len(res) == nq
        assert len(res[0]) == default_top_k

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_wrong_format(self, connect, collection):
        """
        method: build query without must expr, with wrong expr name
        expected: error raised
        """
        # entities, ids = init_data(connect, collection)
        expr = {
            "must1": [gen_default_term_expr]
        }
        query = update_query_expr(default_query, keep_old=False, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_empty(self, connect, collection):
        """
        method: search with empty query
        expected: error raised
        """
        query = {}
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    """
    ******************************************************************
    #  The following cases are used to build valid query expr
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_value_not_in(self, connect, collection):
        """
        method: build query with vector and term expr, with no term can be filtered
        expected: filter pass
        """
        entities, ids = init_data(connect, collection)
        expr = {
            "must": [gen_default_vector_expr(default_query), gen_default_term_expr(values=[100000])]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 0
        # TODO:

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_value_all_in(self, connect, collection):
        """
        method: build query with vector and term expr, with all term can be filtered
        expected: filter pass
        """
        entities, ids = init_data(connect, collection)
        expr = {"must": [gen_default_vector_expr(default_query), gen_default_term_expr(values=[1])]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 1
        # TODO:

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_values_not_in(self, connect, collection):
        """
        method: build query with vector and term expr, with no term can be filtered
        expected: filter pass
        """
        entities, ids = init_data(connect, collection)
        expr = {"must": [gen_default_vector_expr(default_query),
                         gen_default_term_expr(values=[i for i in range(100000, 100010)])]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 0
        # TODO:

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_values_all_in(self, connect, collection):
        """
        method: build query with vector and term expr, with all term can be filtered
        expected: filter pass
        """
        entities, ids = init_data(connect, collection)
        expr = {"must": [gen_default_vector_expr(default_query), gen_default_term_expr()]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == default_top_k
        limit = default_nb // 2
        for i in range(nq):
            for result in res[i]:
                logging.getLogger().info(result.id)
                assert result.id in ids[:limit]
        # TODO:

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_values_parts_in(self, connect, collection):
        """
        method: build query with vector and term expr, with parts of term can be filtered
        expected: filter pass
        """
        entities, ids = init_data(connect, collection)
        expr = {"must": [gen_default_vector_expr(default_query),
                         gen_default_term_expr(
                             values=[i for i in range(default_nb // 2, default_nb + default_nb // 2)])]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == default_top_k
        # TODO:

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_values_repeat(self, connect, collection):
        """
        method: build query with vector and term expr, with the same values
        expected: filter pass
        """
        entities, ids = init_data(connect, collection)
        expr = {
            "must": [gen_default_vector_expr(default_query),
                     gen_default_term_expr(values=[1 for i in range(1, default_nb)])]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 1
        # TODO:

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_value_empty(self, connect, collection):
        """
        method: build query with term value empty
        expected: return null
        """
        expr = {"must": [gen_default_vector_expr(default_query), gen_default_term_expr(values=[])]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_complex_dsl(self, connect, collection):
        """
        method: query with complicated dsl
        expected: no error raised
        """
        expr = {"must": [
            {"must": [{"should": [gen_default_term_expr(values=[1]), gen_default_range_expr()]}]},
            {"must": [gen_default_vector_expr(default_query)]}
        ]}
        logging.getLogger().info(expr)
        query = update_query_expr(default_query, expr=expr)
        logging.getLogger().info(query)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        logging.getLogger().info(res)

    """
    ******************************************************************
    #  The following cases are used to build invalid term query expr
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_key_error(self, connect, collection):
        """
        method: build query with term key error
        expected: Exception raised
        """
        expr = {"must": [gen_default_vector_expr(default_query),
                         gen_default_term_expr(keyword="terrm", values=[i for i in range(default_nb // 2)])]}
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.fixture(
        scope="function",
        params=gen_invalid_term()
    )
    def get_invalid_term(self, request):
        return request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_wrong_format(self, connect, collection, get_invalid_term):
        """
        method: build query with wrong format term
        expected: Exception raised
        """
        entities, ids = init_data(connect, collection)
        term = get_invalid_term
        expr = {"must": [gen_default_vector_expr(default_query), term]}
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_field_named_term(self, connect, collection):
        """
        method: build query with field named "term"
        expected: error raised
        """
        term_fields = add_field_default(default_fields, field_name="term")
        collection_term = gen_unique_str("term")
        connect.create_collection(collection_term, term_fields)
        term_entities = add_field(entities, field_name="term")
        ids = connect.insert(collection_term, term_entities).primary_keys
        assert len(ids) == default_nb
        connect.flush([collection_term])
        # count = connect.count_entities(collection_term)
        # assert count == default_nb
        stats = connect.get_collection_stats(collection_term)
        assert stats["row_count"] == default_nb
        term_param = {"term": {"term": {"values": [i for i in range(default_nb // 2)]}}}
        expr = {"must": [gen_default_vector_expr(default_query),
                         term_param]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection_term)
        res = connect.search(collection_term, query)
        assert len(res) == nq
        assert len(res[0]) == default_top_k
        connect.drop_collection(collection_term)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_term_one_field_not_existed(self, connect, collection):
        """
        method: build query with two fields term, one of it not existed
        expected: exception raised
        """
        entities, ids = init_data(connect, collection)
        term = gen_default_term_expr()
        term["term"].update({"a": [0]})
        expr = {"must": [gen_default_vector_expr(default_query), term]}
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    """
    ******************************************************************
    #  The following cases are used to build valid range query expr
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_range_key_error(self, connect, collection):
        """
        method: build query with range key error
        expected: Exception raised
        """
        range = gen_default_range_expr(keyword="ranges")
        expr = {"must": [gen_default_vector_expr(default_query), range]}
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.fixture(
        scope="function",
        params=gen_invalid_range()
    )
    def get_invalid_range(self, request):
        return request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_range_wrong_format(self, connect, collection, get_invalid_range):
        """
        method: build query with wrong format range
        expected: Exception raised
        """
        entities, ids = init_data(connect, collection)
        range = get_invalid_range
        expr = {"must": [gen_default_vector_expr(default_query), range]}
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_range_string_ranges(self, connect, collection):
        """
        method: build query with invalid ranges
        expected: raise Exception
        """
        entities, ids = init_data(connect, collection)
        ranges = {"GT": "0", "LT": "1000"}
        range = gen_default_range_expr(ranges=ranges)
        expr = {"must": [gen_default_vector_expr(default_query), range]}
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_range_invalid_ranges(self, connect, collection):
        """
        method: build query with invalid ranges
        expected: 0
        """
        entities, ids = init_data(connect, collection)
        ranges = {"GT": default_nb, "LT": 0}
        range = gen_default_range_expr(ranges=ranges)
        expr = {"must": [gen_default_vector_expr(default_query), range]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res[0]) == 0

    @pytest.fixture(
        scope="function",
        params=gen_valid_ranges()
    )
    def get_valid_ranges(self, request):
        return request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_range_valid_ranges(self, connect, collection, get_valid_ranges):
        """
        method: build query with valid ranges
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        ranges = get_valid_ranges
        range = gen_default_range_expr(ranges=ranges)
        expr = {"must": [gen_default_vector_expr(default_query), range]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == default_top_k

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_range_one_field_not_existed(self, connect, collection):
        """
        method: build query with two fields ranges, one of fields not existed
        expected: exception raised
        """
        entities, ids = init_data(connect, collection)
        range = gen_default_range_expr()
        range["range"].update({"a": {"GT": 1, "LT": default_nb // 2}})
        expr = {"must": [gen_default_vector_expr(default_query), range]}
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    """
    ************************************************************************
    #  The following cases are used to build query expr multi range and term
    ************************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_multi_term_has_common(self, connect, collection):
        """
        method: build query with multi term with same field, and values has common
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        term_first = gen_default_term_expr()
        term_second = gen_default_term_expr(values=[i for i in range(default_nb // 3)])
        expr = {"must": [gen_default_vector_expr(default_query), term_first, term_second]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == default_top_k

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_multi_term_no_common(self, connect, collection):
        """
        method: build query with multi range with same field, and ranges no common
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        term_first = gen_default_term_expr()
        term_second = gen_default_term_expr(values=[i for i in range(default_nb // 2, default_nb + default_nb // 2)])
        expr = {"must": [gen_default_vector_expr(default_query), term_first, term_second]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_multi_term_different_fields(self, connect, collection):
        """
        method: build query with multi range with same field, and ranges no common
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        term_first = gen_default_term_expr()
        term_second = gen_default_term_expr(field="float",
                                            values=[float(i) for i in range(default_nb // 2, default_nb)])
        expr = {"must": [gen_default_vector_expr(default_query), term_first, term_second]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_single_term_multi_fields(self, connect, collection):
        """
        method: build query with multi term, different field each term
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        term_first = {"int64": {"values": [i for i in range(default_nb // 2)]}}
        term_second = {"float": {"values": [float(i) for i in range(default_nb // 2, default_nb)]}}
        term = update_term_expr({"term": {}}, [term_first, term_second])
        expr = {"must": [gen_default_vector_expr(default_query), term]}
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_multi_range_has_common(self, connect, collection):
        """
        method: build query with multi range with same field, and ranges has common
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        range_one = gen_default_range_expr()
        range_two = gen_default_range_expr(ranges={"GT": 1, "LT": default_nb // 3})
        expr = {"must": [gen_default_vector_expr(default_query), range_one, range_two]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == default_top_k

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_multi_range_no_common(self, connect, collection):
        """
        method: build query with multi range with same field, and ranges no common
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        range_one = gen_default_range_expr()
        range_two = gen_default_range_expr(ranges={"GT": default_nb // 2, "LT": default_nb})
        expr = {"must": [gen_default_vector_expr(default_query), range_one, range_two]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_multi_range_different_fields(self, connect, collection):
        """
        method: build query with multi range, different field each range
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        range_first = gen_default_range_expr()
        range_second = gen_default_range_expr(field="float", ranges={"GT": default_nb // 2, "LT": default_nb})
        expr = {"must": [gen_default_vector_expr(default_query), range_first, range_second]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_single_range_multi_fields(self, connect, collection):
        """
        method: build query with multi range, different field each range
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        range_first = {"int64": {"GT": 0, "LT": default_nb // 2}}
        range_second = {"float": {"GT": default_nb / 2, "LT": float(default_nb)}}
        range = update_range_expr({"range": {}}, [range_first, range_second])
        expr = {"must": [gen_default_vector_expr(default_query), range]}
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    """
    ******************************************************************
    #  The following cases are used to build query expr both term and range
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_single_term_range_has_common(self, connect, collection):
        """
        method: build query with single term single range
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        term = gen_default_term_expr()
        range = gen_default_range_expr(ranges={"GT": -1, "LT": default_nb // 2})
        expr = {"must": [gen_default_vector_expr(default_query), term, range]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == default_top_k

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_single_term_range_no_common(self, connect, collection):
        """
        method: build query with single term single range
        expected: pass
        """
        entities, ids = init_data(connect, collection)
        term = gen_default_term_expr()
        range = gen_default_range_expr(ranges={"GT": default_nb // 2, "LT": default_nb})
        expr = {"must": [gen_default_vector_expr(default_query), term, range]}
        query = update_query_expr(default_query, expr=expr)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == 0

    """
    ******************************************************************
    #  The following cases are used to build multi vectors query expr
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_multi_vectors_same_field(self, connect, collection):
        """
        method: build query with two vectors same field
        expected: error raised
        """
        entities, ids = init_data(connect, collection)
        vector1 = default_query
        vector2 = gen_query_vectors(field_name, entities, default_top_k, nq=2)
        expr = {
            "must": [vector1, vector2]
        }
        query = update_query_expr(default_query, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)


class TestSearchDSLBools(object):
    """
    ******************************************************************
    #  The following cases are used to build invalid query expr
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_no_bool(self, connect, collection):
        """
        method: build query without bool expr
        expected: error raised
        """
        entities, ids = init_data(connect, collection)
        expr = {"bool1": {}}
        query = expr
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_should_only_term(self, connect, collection):
        """
        method: build query without must, with should.term instead
        expected: error raised
        """
        expr = {"should": gen_default_term_expr}
        query = update_query_expr(default_query, keep_old=False, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_should_only_vector(self, connect, collection):
        """
        method: build query without must, with should.vector instead
        expected: error raised
        """
        expr = {"should": default_query["bool"]["must"]}
        query = update_query_expr(default_query, keep_old=False, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_must_not_only_term(self, connect, collection):
        """
        method: build query without must, with must_not.term instead
        expected: error raised
        """
        expr = {"must_not": gen_default_term_expr}
        query = update_query_expr(default_query, keep_old=False, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_must_not_vector(self, connect, collection):
        """
        method: build query without must, with must_not.vector instead
        expected: error raised
        """
        expr = {"must_not": default_query["bool"]["must"]}
        query = update_query_expr(default_query, keep_old=False, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_must_should(self, connect, collection):
        """
        method: build query must, and with should.term
        expected: error raised
        """
        expr = {"should": gen_default_term_expr}
        query = update_query_expr(default_query, keep_old=True, expr=expr)
        with pytest.raises(Exception) as e:
            res = connect.search(collection, query)
