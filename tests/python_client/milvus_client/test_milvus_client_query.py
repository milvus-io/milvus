import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

prefix = "milvus_client_api_query"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name


class TestMilvusClientQueryInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_not_all_required_params(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. query using ids
        error = {ct.err_code: 65535, ct.err_msg: f"empty expression should be used with limit"}
        self.query(client, collection_name,
                   check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientQueryValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_default(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. query using ids
        self.query(client, collection_name, ids=[i for i in range(default_nb)],
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name})
        # 4. query using filter
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_output_fields(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. query using ids
        self.query(client, collection_name, ids=[i for i in range(default_nb)],
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name})
        # 4. query using filter
        res = self.query(client, collection_name, filter=default_search_exp,
                         output_fields=[default_primary_key_field_name, default_float_field_name,
                                        default_string_field_name, default_vector_field_name],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: rows,
                                      "with_vec": True,
                                      "primary_field": default_primary_key_field_name})[0]
        assert set(res[0].keys()) == {default_primary_key_field_name, default_vector_field_name,
                                      default_float_field_name, default_string_field_name}
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_output_fields_all(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. query using ids
        self.query(client, collection_name, ids=[i for i in range(default_nb)],
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name})
        # 4. query using filter
        res = self.query(client, collection_name, filter=default_search_exp,
                         output_fields=["*"],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: rows,
                                      "with_vec": True,
                                      "primary_field": default_primary_key_field_name})[0]
        assert set(res[0].keys()) == {default_primary_key_field_name, default_vector_field_name,
                                      default_float_field_name, default_string_field_name}
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_query_limit(self):
        """
        target: test query (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. query using ids
        limit = 5
        self.query(client, collection_name, ids=[i for i in range(default_nb)],
                   limit=limit,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[:limit],
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name[:limit]})
        # 4. query using filter
        self.query(client, collection_name, filter=default_search_exp,
                   limit=limit,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows[:limit],
                                "with_vec": True,
                                "primary_field": default_primary_key_field_name[:limit]})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("sample_rate", [0.7, 0.5, 0.01])
    def test_milvus_client_query_random_sample(self, sample_rate):
        """
        target: test query random sample
        method: create connection, collection, insert and query
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        vectors = cf.gen_vectors(default_nb, default_dim)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: vectors[i],
                default_float_field_name: i * 1.0,
                default_string_field_name: cf.generate_random_sentence("English")
            } for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        expr = f"{default_string_field_name} like '%red%'"

        # 3. query without sample rate
        all_res = self.query(client, collection_name, filter=expr)[0]
        exp_num = max(1, int(len(all_res) * sample_rate))

        # 4. query using sample rate
        expr = expr + f" && random_sample({sample_rate})"
        sample_res = self.query(client, collection_name, filter=expr)[0]
        log.info(exp_num)
        assert len(sample_res) == exp_num

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("sample_rate", [1, 0, -9])
    def test_milvus_client_query_invalid_sample_rate(self, sample_rate):
        """
        target: test query random sample
        method: create connection, collection, insert and query
        expected: query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        vectors = cf.gen_vectors(1, default_dim)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: vectors[i],
                default_float_field_name: i * 1.0,
                default_string_field_name: cf.generate_random_sentence("English")
            } for i in range(1)
        ]
        self.insert(client, collection_name, rows)
        expr = f"{default_string_field_name} like '%red%' && random_sample({sample_rate})"

        # 3. query
        error = {ct.err_code: 999,
                 ct.err_msg: "the sample factor should be between 0 and 1 and not too close to 0 or 1"}
        self.query(client, collection_name, filter=expr,
                   check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientGetInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("name",
                             ["12-s", "12 s", "(mn)", "中文", "%$#",
                              "".join("a" for i in range(ct.max_name_length + 1))])
    def test_milvus_client_get_invalid_collection_name(self, name):
        """
        target: test get interface invalid cases
        method: invalid collection name
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [i for i in range(default_nb)]
        # 3. get first primary key
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name"}
        self.get(client, name, ids=pks[0:1],
                 check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_get_not_exist_collection_name(self):
        """
        target: test get interface invalid cases
        method: invalid collection name
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [i for i in range(default_nb)]
        # 3. get first primary key
        name = "invalid"
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection[database=default][collection={name}]"}
        self.get(client, name, ids=pks[0:1],
                 check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_ids", ["中文", "%$#"])
    def test_milvus_client_get_invalid_ids(self, invalid_ids):
        """
        target: test get interface invalid cases
        method: invalid collection name
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. get first primary key
        error = {ct.err_code: 1100, ct.err_msg: f"cannot parse expression"}
        self.get(client, collection_name, ids=invalid_ids,
                 check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)


class TestMilvusClientGetValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_get_normal(self):
        """
        target: test get interface
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [i for i in range(default_nb)]
        # 3. get first primary key
        first_pk_data = self.get(client, collection_name, ids=pks[0:1])[0]
        assert len(first_pk_data) == len(pks[0:1])
        first_pk_data_1 = self.get(client, collection_name, ids=0)[0]
        assert first_pk_data == first_pk_data_1
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_get_output_fields(self):
        """
        target: test get interface with output fields
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. insert
        default_nb = 1000
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [i for i in range(default_nb)]
        # 3. get first primary key
        output_fields_array = [default_primary_key_field_name, default_vector_field_name,
                               default_float_field_name, default_string_field_name]
        first_pk_data = self.get(client, collection_name, ids=pks[0:1], output_fields=output_fields_array)[0]
        assert len(first_pk_data) == len(pks[0:1])
        assert len(first_pk_data[0]) == len(output_fields_array)
        first_pk_data_1 = self.get(client, collection_name, ids=0, output_fields=output_fields_array)[0]
        assert first_pk_data == first_pk_data_1
        assert len(first_pk_data_1[0]) == len(output_fields_array)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2056")
    def test_milvus_client_get_normal_string(self):
        """
        target: test get interface for string field
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [str(i) for i in range(default_nb)]
        # 3. get first primary key
        first_pk_data = self.get(client, collection_name, ids=pks[0:1])[0]
        assert len(first_pk_data) == len(pks[0:1])
        first_pk_data_1 = self.get(client, collection_name, ids="0")[0]
        assert first_pk_data == first_pk_data_1

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="pymilvus issue 2056")
    def test_milvus_client_get_normal_string_output_fields(self):
        """
        target: test get interface for string field
        method: create connection, collection, insert delete, and search
        expected: search/query successfully without deleted data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, id_type="string", max_length=ct.default_length)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: str(i), default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        pks = [str(i) for i in range(default_nb)]
        # 3. get first primary key
        output_fields_array = [default_primary_key_field_name, default_vector_field_name,
                               default_float_field_name, default_string_field_name]
        first_pk_data = self.get(client, collection_name, ids=pks[0:1], output_fields=output_fields_array)[0]
        assert len(first_pk_data) == len(pks[0:1])
        assert len(first_pk_data[0]) == len(output_fields_array)
        first_pk_data_1 = self.get(client, collection_name, ids="0", output_fields=output_fields_array)[0]
        assert first_pk_data == first_pk_data_1
        assert len(first_pk_data_1[0]) == len(output_fields_array)
        self.drop_collection(client, collection_name)
