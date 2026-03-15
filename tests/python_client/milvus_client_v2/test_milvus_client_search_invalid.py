import random
import pytest
from pymilvus import DataType
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base

default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
field_name = ct.default_float_vec_field_name


@pytest.mark.xdist_group("TestSearchInvalidShared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchInvalidShared(TestMilvusClientV2Base):
    """Test search with invalid parameters using shared collection.
    Schema: int64(PK), float, varchar(65535), json, float_vector(128), dynamic=False
    Data: 3000 rows
    Index: COSINE on float_vector
    """

    shared_alias = "TestSearchInvalidShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchInvalidShared" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        data = cf.gen_row_data_by_schema(nb=3000, schema=schema)
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_missing(self):
        """
        target: test search with incomplete parameters
        method: search with incomplete parameters
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_missing: Searching collection %s "
                 "with missing parameters" % self.collection_name)
        self.search(client, self.collection_name,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "Either ids or data must be provided"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_vectors", ct.get_invalid_vectors)
    def test_search_param_invalid_vectors(self, invalid_vectors):
        """
        target: test search with invalid parameter values
        method: search with invalid data
        expected: raise exception and report the error
        """
        if invalid_vectors in [[" "], ['a']]:
            pytest.skip("['a'] and [' '] is valid now")
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_invalid_vectors: searching with "
                 "invalid vectors: {}".format(invalid_vectors))
        if invalid_vectors is None:
            err_msg = "Either ids or data must be provided"
        else:
            err_msg = "`search_data` value {} is illegal".format(invalid_vectors)
        self.search(client, self.collection_name,
                    data=invalid_vectors, anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_dim(self):
        """
        target: test search with invalid parameter values
        method: search with invalid dim
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_invalid_dim: searching with invalid dim")
        wrong_dim = 129
        wrong_vectors = [[random.random() for _ in range(wrong_dim)] for _ in range(default_nq)]
        self.search(client, self.collection_name,
                    data=wrong_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": 'vector dimension mismatch'})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_field_name", ct.invalid_resource_names)
    def test_search_param_invalid_field(self, invalid_field_name):
        """
        target: test search with invalid parameter type
        method: search with invalid field type
        expected: raise exception and report the error
        """
        if invalid_field_name in [None, ""]:
            pytest.skip("None is legal")
        client = self._client(alias=self.shared_alias)
        error = {"err_code": 999, "err_msg": f"failed to create query plan: failed to get field schema by name"}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=invalid_field_name,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_metric", ct.get_invalid_metric_type)
    def test_search_param_invalid_metric_type(self, invalid_metric):
        """
        target: test search with invalid parameter values
        method: search with invalid metric type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_invalid_metric_type: searching with invalid metric_type")
        search_params = {"metric_type": invalid_metric, "params": {"nprobe": 10}}
        if isinstance(invalid_metric, dict):
            self.search(client, self.collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=search_params, limit=default_limit,
                        filter=default_search_exp,
                        check_task=CheckTasks.err_res,
                        check_items={"err_code": 1,
                                     "err_msg": "Dict key must be str"})
        else:
            self.search(client, self.collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=search_params, limit=default_limit,
                        filter=default_search_exp,
                        check_task=CheckTasks.err_res,
                        check_items={"err_code": 65535,
                                     "err_msg": "metric type not match"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_param_metric_type_not_match(self):
        """
        target: test search with invalid parameter values
        method: search with invalid metric type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_metric_type_not_match: searching with not matched metric_type")
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "metric type not match: invalid parameter"
                                            "[expected=COSINE][actual=L2]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_limit", [p for p in ct.get_invalid_ints
                                                if not (isinstance(p, int) and p >= 0)])
    def test_search_param_invalid_limit_type(self, invalid_limit):
        """
        target: test search with invalid limit type
        method: search with invalid limit type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_param_invalid_limit_type: searching with "
                 "invalid limit: %s" % invalid_limit)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=invalid_limit,
                    filter=default_search_exp,
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
        client = self._client(alias=self.shared_alias)
        err_msg = f"topk [{limit}] is invalid, it should be in range [1, 16384]"
        if limit == 0:
            err_msg = "`limit` value 0 is illegal"
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_search_expr", ["'non_existing_field'==2", 1])
    def test_search_param_invalid_expr_type(self, invalid_search_expr):
        """
        target: test search with invalid parameter type
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        error = {"err_code": 999, "err_msg": "failed to create query plan: cannot parse expression"}
        if invalid_search_expr == 1:
            error = {"err_code": 1, "err_msg": "'int' object has no attribute 'lower'"}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=invalid_search_expr,
                    check_task=CheckTasks.err_res,
                    check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_expr_value", ["string", 1.2, None, [1, 2, 3]])
    def test_search_param_invalid_expr_value(self, invalid_expr_value):
        """
        target: test search with invalid parameter values
        method: search with invalid search expressions
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        invalid_search_expr = f"{ct.default_int64_field_name}=={invalid_expr_value}"
        log.info("test_search_param_invalid_expr_value: searching with "
                 "invalid expr: %s" % invalid_search_expr)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=invalid_search_expr,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999,
                                 "err_msg": "failed to create query plan: cannot parse expression: %s"
                                            % invalid_search_expr})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", ["int64 like 33", "float LIKE 33"])
    def test_search_with_expression_invalid_like(self, expression):
        """
        target: test search int64 and float with like
        method: test search int64 and float with like
        expected: searched failed
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        search_vectors = [[random.random() for _ in range(default_dim)]
                          for _ in range(default_nq)]
        self.search(client, self.collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "failed to create query plan: cannot parse "
                                            "expression: %s" % expression})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_partitions", [[None], [1, 2]])
    def test_search_partitions_invalid_type(self, invalid_partitions):
        """
        target: test search invalid partition
        method: search with invalid partition type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        err_msg = "`partition_name_array` value {} is illegal".format(invalid_partitions)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    partition_names=invalid_partitions,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999,
                                 "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_partitions", [["non_existing"], [ct.default_partition_name, "non_existing"]])
    def test_search_partitions_non_existing(self, invalid_partitions):
        """
        target: test search invalid partition
        method: search with invalid partition type
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        err_msg = "partition name non_existing not found"
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    partition_names=invalid_partitions,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_output_fields", [[None], [1, 2], ct.default_int64_field_name])
    def test_search_with_output_fields_invalid_type(self, invalid_output_fields):
        """
        target: test search with output fields
        method: search with invalid output_field
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        err_msg = f"`output_fields` value {invalid_output_fields} is illegal"
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    output_fields=invalid_output_fields,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 999,
                                 ct.err_msg: err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("non_exiting_output_fields",
                             [["non_exiting"], [ct.default_int64_field_name, "non_exiting"]])
    def test_search_with_output_fields_non_existing(self, non_exiting_output_fields):
        """
        target: test search with output fields
        method: search with invalid output_field
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        err_msg = f"field non_exiting not exist"
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    output_fields=non_exiting_output_fields,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 999,
                                 ct.err_msg: err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("output_fields", [[default_search_field], ["*"]])
    def test_search_output_field_vector(self, output_fields):
        """
        target: test search with vector as output field
        method: search with one vector output_field or
                wildcard for vector
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_output_field_vector: Searching collection %s" %
                 self.collection_name)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    output_fields=output_fields)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", [["*%"], ["**"], ["*", "@"]])
    def test_search_output_field_invalid_wildcard(self, output_fields):
        """
        target: test search with invalid output wildcard
        method: search with invalid output_field wildcard
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_output_field_invalid_wildcard: Searching collection %s" %
                 self.collection_name)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": f"field {output_fields[-1]} not exist"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_guarantee_time",
                             [p for p in ct.get_invalid_ints
                              if p != 9999999999 and p is not None])
    def test_search_param_invalid_guarantee_timestamp(self, invalid_guarantee_time):
        """
        target: test search with invalid guarantee timestamp
        method: search with invalid guarantee timestamp
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info(
            "test_search_param_invalid_guarantee_timestamp: searching with invalid guarantee timestamp")
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
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
        client = self._client(alias=self.shared_alias)
        log.info("test_search_invalid_round_decimal: Searching collection %s" %
                 self.collection_name)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    round_decimal=round_decimal,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": f"`round_decimal` value {round_decimal} is illegal"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [16385])
    def test_search_with_invalid_nq(self, nq):
        """
        target: test search with invalid nq
        method: search with invalid nq
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = [[random.random() for _ in range(default_dim)]
                          for _ in range(nq)]
        self.search(client, self.collection_name,
                    data=search_vectors[:nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "nq (number of search vector per search "
                                            "request) should be in range [1, 16384]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_radius", [[0.1], "str"])
    def test_range_search_invalid_radius(self, invalid_radius):
        """
        target: test range search with invalid radius
        method: range search with invalid radius
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_range_search_invalid_radius: Range searching collection %s" %
                 self.collection_name)
        range_search_params = {"metric_type": "COSINE",
                               "params": {"radius": invalid_radius, "range_filter": 0}}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=range_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": "type must be number"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr", [
        "int64 / 0 > 0",
        "int64 / 0 == 1",
        "float / 0 == 1.0",
        "int64 % 0 == 1",
        "int64 % 0 != 0",
        "json_field['number'] / 0 > 0",
        "json_field['number'] % 0 == 1",
    ])
    def test_search_filter_division_by_zero(self, expr):
        """
        target: test search with division/modulo by zero in filter expression (issue #47285)
        method: search with filter containing division or modulo by zero on int64/float/json fields
        expected: raise error with 'by zero' message instead of crashing server (SIGFPE)
        """
        client = self._client(alias=self.shared_alias)
        log.info(f"test_search_filter_division_by_zero: searching with expr: {expr}")
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expr,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": "by zero"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr", [
        "int64 / 0 > 0",
        "int64 % 0 == 1",
    ])
    def test_query_filter_division_by_zero(self, expr):
        """
        target: test query with division/modulo by zero in filter expression (issue #47285)
        method: query with filter containing division or modulo by zero
        expected: raise error with 'by zero' message instead of crashing server (SIGFPE)
        """
        client = self._client(alias=self.shared_alias)
        log.info(f"test_query_filter_division_by_zero: querying with expr: {expr}")
        self.query(client, self.collection_name,
                   filter=expr,
                   check_task=CheckTasks.err_res,
                   check_items={"err_code": 999, "err_msg": "by zero"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr,expr_params", [
        ("int64 / {d} > 0", {"d": 0}),
        ("int64 % {d} == 1", {"d": 0}),
        ("float / {d} == 1.0", {"d": 0}),
    ])
    def test_search_filter_division_by_zero_with_expr_params(self, expr, expr_params):
        """
        target: test search with division/modulo by zero via expr_params (issue #47285)
        method: search with parameterized filter where divisor is zero
        expected: raise error with 'by zero' message instead of crashing server (SIGFPE)
        """
        client = self._client(alias=self.shared_alias)
        log.info(f"test_search_filter_division_by_zero_with_expr_params: "
                 f"searching with expr: {expr}, params: {expr_params}")
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expr, filter_params=expr_params,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": "by zero"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr", [
        "int64 / 2 >= 0",
        "int64 % 3 == 1",
        "float / 2.0 < 1000",
    ])
    def test_search_filter_division_by_nonzero(self, expr):
        """
        target: test search with valid division/modulo expressions still works (issue #47285)
        method: search with filter containing division or modulo by non-zero values
        expected: search succeeds without error
        """
        client = self._client(alias=self.shared_alias)
        log.info(f"test_search_filter_division_by_nonzero: searching with expr: {expr}")
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expr)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_range_filter", [[0.1], "str"])
    def test_range_search_invalid_range_filter(self, invalid_range_filter):
        """
        target: test range search with invalid range_filter
        method: range search with invalid range_filter
        expected: raise exception and report the error
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_range_search_invalid_range_filter: Range searching collection %s" %
                 self.collection_name)
        range_search_params = {"metric_type": "COSINE",
                               "params": {"radius": 1, "range_filter": invalid_range_filter}}
        self.search(client, self.collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=range_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999, "err_msg": "type must be number"})


class TestSearchInvalidIndependent(TestMilvusClientV2Base):
    """ Test case of search interface """

    def _create_standard_schema(self, client, dim=default_dim):
        """Create a standard schema: int64(PK), float, varchar(65535), json, float_vector(dim)."""
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        return schema

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
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. close client connection
        log.info("test_search_no_connection: closing client connection")
        client.close()
        log.info("test_search_no_connection: closed client connection")

        # 3. search without connection
        log.info("test_search_no_connection: searching without connection")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "should create connection first"})

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
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # 2. Drop collection
        self.drop_collection(client, collection_name)

        # 3. Search without collection
        log.info("test_search_no_collection: Searching without collection ")
        self.search(client, collection_name,
                    data=vectors, anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "collection not found"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:8])
    def test_search_invalid_params_type(self, index):
        """
        target: test search with invalid search params
        method: test search with invalid params type
        expected: raise exception and report the error
        """
        if index == "FLAT":
            pytest.skip("skip in FLAT index")
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=2000, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index and load
        params = cf.get_index_params_params(index)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type=index, metric_type="L2", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search
        invalid_search_params = cf.gen_invalid_search_params_type()
        for invalid_search_param in invalid_search_params:
            if index == invalid_search_param["index_type"]:
                search_params = {"metric_type": "L2",
                                 "params": invalid_search_param["search_params"]}
                log.info("search_params: {}".format(search_params))
                self.search(client, collection_name,
                            data=vectors[:default_nq], anns_field=default_search_field,
                            search_params=search_params, limit=default_limit,
                            filter=default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 999,
                                         "err_msg": "fail to search on QueryNode"})

    @pytest.mark.skip("not support now")
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("search_k", [-10, -1, 0, 10, 125])
    def test_search_param_invalid_annoy_index(self, search_k):
        """
        target: test search with invalid search params matched with annoy index
        method: search with invalid param search_k out of [top_k, infinity)
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=3000, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create annoy index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="ANNOY", metric_type="L2", params={"n_trees": 512})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search
        annoy_search_param = {"index_type": "ANNOY",
                              "search_params": {"search_k": search_k}}
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=annoy_search_param, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "Search params check failed"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_field_compare_expressions())
    def test_search_with_expression_join_two_fields(self, expression):
        """
        target: test search with expressions linking two fields such as 'and'
        method: create a collection and search with different conjunction
        expected: raise exception and report the error
        """
        # 1. create a collection
        nb = 1
        dim = 2
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("int64_1", DataType.INT64, is_primary=True)
        schema.add_field("int64_2", DataType.INT64)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data
        data = [{"int64_1": i, "int64_2": i,
                 ct.default_float_vec_field_name: [random.random() for _ in range(dim)]}
                for i in range(nb)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 3. create index, load, and search with expression
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        log.info("test_search_with_expression: searching with expression: %s" % expression)
        expression = expression.replace("&&", "and").replace("||", "or")
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=nb,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999,
                                 "err_msg": "failed to create query plan: "
                                            "cannot parse expression: %s" % expression})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_expr_bool_value", [1.2, 10, "string"])
    def test_search_param_invalid_expr_bool(self, invalid_expr_bool_value):
        """
        target: test search with invalid parameter values
        method: search with invalid bool search expressions
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search with invalid bool expr
        invalid_search_expr_bool = f"{ct.default_bool_field_name} == {invalid_expr_bool_value}"
        log.info("test_search_param_invalid_expr_bool: searching with "
                 "invalid expr: %s" % invalid_search_expr_bool)
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=invalid_search_expr_bool,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "failed to create query plan"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_invalid_bool(self):
        """
        target: test search invalid bool
        method: test search invalid bool
        expected: searched failed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        expressions = ["bool", "true", "false"]
        for expression in expressions:
            log.debug(f"search with expression: {expression}")
            self.search(client, collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=default_search_params, limit=default_limit,
                        filter=expression,
                        check_task=CheckTasks.err_res,
                        check_items={"err_code": 1100,
                                     "err_msg": "failed to create query plan: predicate is not a "
                                                "boolean expression: %s, data type: Bool" % expression})
        expression = "!bool"
        log.debug(f"search with expression: {expression}")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": "cannot parse expression: !bool, "
                                            "error: not op can only be applied on boolean expression"})
        expression = "int64 > 0 and bool"
        log.debug(f"search with expression: {expression}")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": "cannot parse expression: int64 > 0 and bool, "
                                            "error: 'and' can only be used between boolean expressions"})
        expression = "int64 > 0 or false"
        log.debug(f"search with expression: {expression}")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expression,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": "cannot parse expression: int64 > 0 or false, "
                                            "error: 'or' can only be used between boolean expressions"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_invalid_array_one(self):
        """
        target: test search with invalid array expressions
        method: test search with invalid array expressions:
                the order of array > the length of array
        expected: searched successfully with correct limit(topK)
        """
        # 1. create a collection
        nb = ct.default_nb
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_int32_array_field_name, DataType.ARRAY,
                         element_type=DataType.INT32, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_float_array_field_name, DataType.ARRAY,
                         element_type=DataType.FLOAT, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_string_array_field_name, DataType.ARRAY,
                         element_type=DataType.VARCHAR, max_capacity=ct.default_max_capacity,
                         max_length=100, nullable=True)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(schema=schema)
        data[1][ct.default_int32_array_field_name] = [1]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search (subscript > max_capacity)
        expression = "int32_array[101] > 0"
        res, _ = self.search(client, collection_name,
                             data=vectors[:default_nq], anns_field=default_search_field,
                             search_params=default_search_params, limit=nb,
                             filter=expression)
        assert len(res[0]) == 0

        # 3. search (max_capacity > subscript > actual length of array)
        expression = "int32_array[51] > 0"
        res, _ = self.search(client, collection_name,
                             data=vectors[:default_nq], anns_field=default_search_field,
                             search_params=default_search_params, limit=default_limit,
                             filter=expression)
        assert len(res[0]) == default_limit

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_invalid_array_two(self):
        """
        target: test search with invalid array expressions
        method: test search with invalid array expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. create a collection
        nb = ct.default_nb
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_int32_array_field_name, DataType.ARRAY,
                         element_type=DataType.INT32, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_float_array_field_name, DataType.ARRAY,
                         element_type=DataType.FLOAT, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_string_array_field_name, DataType.ARRAY,
                         element_type=DataType.VARCHAR, max_capacity=ct.default_max_capacity,
                         max_length=100, nullable=True)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search
        expression = "int32_array[0] - 1 < 1"
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=nb,
                    filter=expression)

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
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. release collection
        self.release_collection(client, collection_name)

        # 3. Search the released collection
        log.info("test_search_release_collection: Searching without collection ")
        self.search(client, collection_name,
                    data=vectors, anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "collection not loaded"})

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
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition and insert data
        par_name = "search_partition_0"
        self.create_partition(client, collection_name, partition_name=par_name)
        data = cf.gen_row_data_by_schema(nb=10, schema=schema)
        self.insert(client, collection_name, data=data, partition_name=par_name)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT")
        self.create_index(client, collection_name, index_params=idx)
        self.load_partitions(client, collection_name, partition_names=[par_name])

        # 2. release partition
        self.release_partitions(client, collection_name, partition_names=[par_name])

        # 3. Search the released partition
        log.info("test_search_release_partition: Searching the released partition")
        limit = 10
        self.search(client, collection_name,
                    data=vectors, anns_field=default_search_field,
                    search_params=default_search_params, limit=limit,
                    filter=default_search_exp,
                    partition_names=[par_name],
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_search_with_empty_collection(self, vector_data_type):
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
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, vector_data_type, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        # 2. search collection without data before load
        log.info("test_search_with_empty_collection: Searching empty collection %s"
                 % collection_name)
        err_msg = "collection not loaded"
        search_vectors = cf.gen_vectors(default_nq, default_dim, vector_data_type)
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 101,
                                 "err_msg": err_msg})

        # 3. search collection without data after load
        if vector_data_type == DataType.INT8_VECTOR:
            idx = self.prepare_index_params(client)[0]
            idx.add_index(field_name=ct.default_float_vec_field_name,
                          index_type="HNSW", metric_type="L2")
            self.create_index(client, collection_name, index_params=idx)
        else:
            idx = self.prepare_index_params(client)[0]
            idx.add_index(field_name=ct.default_float_vec_field_name,
                          index_type="FLAT", metric_type="COSINE")
            self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

        # 4. search with data inserted but not load again
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": insert_ids,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

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
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        par_name = "search_partition_0"
        self.create_partition(client, collection_name, partition_name=par_name)

        # 2. search collection without data after load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

        # 3. search a partition without data after load
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    partition_names=[par_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": [],
                                 "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

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
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition and insert data
        deleted_par_name = "search_partition_0"
        self.create_partition(client, collection_name, partition_name=deleted_par_name)
        data = cf.gen_row_data_by_schema(nb=1000, schema=schema)
        self.insert(client, collection_name, data=data, partition_name=deleted_par_name)
        self.flush(client, collection_name)

        # 2. delete partition
        log.info("test_search_partition_deleted: deleting a partition")
        self.drop_partition(client, collection_name, partition_name=deleted_par_name)
        log.info("test_search_partition_deleted: deleted a partition")

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search after delete partitions
        log.info("test_search_partition_deleted: searching deleted partition")
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    partition_names=[deleted_par_name],
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "partition name search_partition_0 not found"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partition_not_existed(self):
        """
        target: test search not existed partition
        method: search with not existed partition
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="IVF_FLAT", metric_type="L2", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)

        # 3. search the non exist partition
        partition_name = "search_non_exist"
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=default_search_exp,
                    partition_names=[partition_name],
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "partition name %s not found" % partition_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("reorder_k", [100])
    def test_search_scann_with_invalid_reorder_k(self, reorder_k):
        """
        target: test search with invalid nq
        method: search with invalid nq
        expected: raise exception and report the error
        """
        # initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="SCANN", metric_type="L2", params={"nlist": 1024})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search
        search_params = {"metric_type": "L2", "params": {"nprobe": 10, "reorder_k": reorder_k}}
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=search_params, limit=reorder_k + 1,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "reorder_k(100) should be larger than k(101)"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_param_invalid_binary(self):
        """
        target: test search within binary data (invalid parameter)
        method: search with wrong metric type
        expected: raise exception and report the error
        """
        # 1. initialize with binary data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name,
                      index_type="BIN_IVF_FLAT", metric_type="JACCARD", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search with exception
        _, search_binary_vectors = cf.gen_binary_vectors(3000, default_dim)
        wrong_search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        # err_code 65535: BIN_IVF_FLAT now returns metric type mismatch at search time
        # (previously returned 1100 during index/collection migration)
        self.search(client, collection_name,
                    data=search_binary_vectors[:default_nq],
                    anns_field=ct.default_binary_vec_field_name,
                    search_params=wrong_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "metric type not match"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_flat_with_L2(self):
        """
        target: search binary collection using FlAT with L2
        method: search binary collection using FLAT with L2
        expected: raise exception and report error
        """
        # 1. initialize with binary data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, metric_type="JACCARD")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search and assert
        _, search_binary_vectors = cf.gen_binary_vectors(2, default_dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        self.search(client, collection_name,
                    data=search_binary_vectors[:default_nq],
                    anns_field=ct.default_binary_vec_field_name,
                    search_params=search_params, limit=default_limit,
                    filter="int64 >= 0",
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "metric type not match: invalid "
                                            "parameter[expected=JACCARD][actual=L2]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", ["int63", ""])
    @pytest.mark.parametrize("enable_dynamic", [True, False])
    def test_search_with_output_fields_not_exist(self, output_fields, enable_dynamic):
        """
        target: test search with output fields
        method: search with non-exist output_field
        expected: raise exception for non-dynamic or empty string fields
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search
        log.info("test_search_with_output_fields_not_exist: Searching collection %s" %
                 collection_name)
        if enable_dynamic and output_fields == "int63":
            # dynamic field enabled: non-existent field name returns success (treated as dynamic field)
            self.search(client, collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=default_search_params, limit=default_limit,
                        filter=default_search_exp,
                        output_fields=[output_fields])
        elif output_fields == "":
            # empty string output field
            self.search(client, collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=default_search_params, limit=default_limit,
                        filter=default_search_exp,
                        output_fields=[output_fields],
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 1,
                                     ct.err_msg: "is illegal"})
        else:
            # non-dynamic: non-existent field raises error
            self.search(client, collection_name,
                        data=vectors[:default_nq], anns_field=default_search_field,
                        search_params=default_search_params, limit=default_limit,
                        filter=default_search_exp,
                        output_fields=[output_fields],
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 65535,
                                     ct.err_msg: "field int63 not exist"})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("index", ct.all_index_types[-2:])
    def test_search_output_field_vector_after_gpu_index(self, index):
        """
        target: test search with vector as output field
        method: 1. create a collection and insert data
                2. create an index which doesn't output vectors
                3. load and search
        expected: raise exception and report the error
        """
        # 1. create a collection and insert data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create an index which doesn't output vectors
        params = cf.get_index_params_params(index)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type=index, metric_type="L2", params=params)
        self.create_index(client, collection_name, index_params=idx)

        # 3. load and search
        self.load_collection(client, collection_name)
        search_params = cf.get_search_params_params(index)
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=field_name,
                    search_params={"params": search_params}, limit=default_limit,
                    output_fields=[field_name],
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1,
                                 "err_msg": "not supported"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ignore_growing", [1.2, "string", [True]])
    def test_search_invalid_ignore_growing_param(self, ignore_growing):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. search with param ignore_growing invalid
        expected: raise exception
        """
        # 1. create a collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=10, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. insert data again
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=100)
        self.insert(client, collection_name, data=data)

        # 3. search with param ignore_growing=invalid
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}, "ignore_growing": ignore_growing}
        vector = [[random.random() for _ in range(default_dim)] for _ in range(1)]
        self.search(client, collection_name,
                    data=vector[:default_nq], anns_field=default_search_field,
                    search_params=search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 999,
                                 "err_msg": "parse ignore growing field failed"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_invalid_radius_range_filter_L2(self):
        """
        target: test range search with invalid radius and range_filter for L2
        method: range search with radius smaller than range_filter
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=100, schema=schema)
        self.insert(client, collection_name, data=data)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="FLAT", metric_type="L2")
        self.create_index(client, collection_name, index_params=idx)
        # 3. load
        self.load_collection(client, collection_name)

        # 4. range search
        log.info("test_range_search_invalid_radius_range_filter_L2: Range searching collection %s" %
                 collection_name)
        range_search_params = {"metric_type": "L2", "params": {"nprobe": 10, "radius": 1, "range_filter": 10}}
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=range_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "must be less than radius"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_invalid_radius_range_filter_IP(self):
        """
        target: test range search with invalid radius and range_filter for IP
        method: range search with radius larger than range_filter
        expected: raise exception and report the error
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=100, schema=schema)
        self.insert(client, collection_name, data=data)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="FLAT", metric_type="IP")
        self.create_index(client, collection_name, index_params=idx)
        # 3. load
        self.load_collection(client, collection_name)

        # 4. range search
        log.info("test_range_search_invalid_radius_range_filter_IP: Range searching collection %s" %
                 collection_name)
        range_search_params = {"metric_type": "IP",
                               "params": {"nprobe": 10, "radius": 10, "range_filter": 1}}
        self.search(client, collection_name,
                    data=vectors[:default_nq], anns_field=default_search_field,
                    search_params=range_search_params, limit=default_limit,
                    filter=default_search_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "must be greater than radius"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_dynamic_compare_two_fields(self):
        """
        target: test search compare with two fields for dynamic collection
        method: 1.create collection , insert data, enable dynamic function
                2.search with two fields comparisons
        expected: Raise exception
        """
        # create collection, insert data, enable dynamic field
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        data = [{ct.default_string_field_name: str(i),
                 ct.default_float_vec_field_name: [random.random() for _ in range(default_dim)]}
                for i in range(1)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # create indexes
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="IVF_SQ8", metric_type="COSINE", params={"nlist": 64})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search with two fields comparison
        expr = 'float >= int64'
        search_vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        self.search(client, collection_name,
                    data=search_vectors[:default_nq], anns_field=default_search_field,
                    search_params=default_search_params, limit=default_limit,
                    filter=expr,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": "error: two column comparison with JSON type is not supported"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_ef_less_than_limit(self):
        """
        target: test the scenario which search with ef less than limit
        method: 1. create collection
                2. search with ef less than limit
        expected: raise exception and report the error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self._create_standard_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=2000, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="HNSW", metric_type="L2",
                      params={"M": 8, "efConstruction": 256})
        self.create_index(client, collection_name, index_params=idx)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_params = {"metric_type": "L2", "params": {"ef": 10}}
        self.search(client, collection_name,
                    data=vectors, anns_field=ct.default_float_vec_field_name,
                    search_params=search_params, limit=100,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 65535,
                                 "err_msg": "query failed: N6milvus21ExecOperatorExceptionE :Operator::GetOutput failed"})
