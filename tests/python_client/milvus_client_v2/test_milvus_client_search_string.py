from pymilvus import DataType
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base
import pytest

default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invalid_string_exp = "varchar >= 0"
prefix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name


@pytest.mark.xdist_group("TestSearchStringAutoId")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchStringAutoId(TestMilvusClientV2Base):
    """Shared collection with auto_id=True
    Schema: int64(PK, auto_id=True), float, varchar(65535), json, float_vector(128), dynamic=False
    Data: 3000 rows, varchar overridden with str(i) for predictable prefix/comparison expressions
    Index: COSINE on float_vector
    """
    shared_alias = "TestSearchStringAutoId"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchStringAutoId" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        data = cf.gen_row_data_by_schema(nb=3000, schema=schema)
        # Override varchar with str(i) so prefix/comparison expressions work predictably
        # (gen_row_data_by_schema generates random strings without predictable ordering)
        for i in range(len(data)):
            data[i][ct.default_string_field_name] = str(i)
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
    def test_search_string_field_not_primary(self):
        """
        target: verify search with string equality filter on non-primary varchar field
        method: 1. query to get a valid varchar value
                2. search with filter varchar == 'value' on shared collection
                3. check nq, limit, distance order via check_task
                4. manually assert returned varchar matches search string
        expected: exactly 1 result with matching varchar value, distances in COSINE order
        """
        client = self._client(alias=self.shared_alias)
        # query to get a valid string value from the collection
        query_res, _ = self.query(client, self.collection_name, filter="int64 >= 0",
                                  output_fields=[default_string_field_name], limit=10)
        search_str = query_res[1][default_string_field_name]
        search_exp = f"{default_string_field_name} == '{search_str}'"
        log.info("test_search_string_field_not_primary: searching collection %s" % self.collection_name)
        log.info("search expr: %s" % search_exp)
        output_fields = [default_string_field_name, default_float_field_name]
        vectors = cf.gen_vectors(default_nq, default_dim)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=search_exp,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "pk_name": default_int64_field_name,
                                          "limit": 1,
                                          "metric": "COSINE",
                                          "enable_milvus_client_api": True})
        assert res[0][0]["entity"][default_string_field_name] == search_str

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_mix_expr(self):
        """
        target: verify search with mixed int64 and varchar comparison filter
        method: 1. search with filter "int64 >= 0 && varchar >= '0'" on shared collection
                2. check nq, limit, distance order via check_task
                3. manually assert all results satisfy both filter conditions
        expected: all results have int64 >= 0 and varchar >= "0", distances in COSINE order
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_string_mix_expr: searching collection %s" %
                 self.collection_name)
        output_fields = [default_string_field_name, default_float_field_name]
        vectors = cf.gen_vectors(default_nq, default_dim)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=default_search_mix_exp,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "pk_name": default_int64_field_name,
                                          "limit": default_limit,
                                          "metric": "COSINE",
                                          "enable_milvus_client_api": True})
        # manually verify filter effectiveness
        for hits in res:
            for hit in hits:
                assert hit.entity.get(default_string_field_name) >= "0"
                assert hit.entity.get(default_int64_field_name) >= 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_with_invalid_expr(self):
        """
        target: verify search with invalid string expression raises error
        method: 1. search with filter "varchar >= 0" (int comparison on varchar)
                2. check error response
        expected: error 1100 with "cannot parse expression" message
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_string_with_invalid_expr: searching collection %s" %
                 self.collection_name)
        vectors = cf.gen_vectors(default_nq, default_dim)
        self.search(client, self.collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_invalid_string_exp,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": "failed to create query plan: cannot "
                                            "parse expression: varchar >= 0"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary_prefix(self):
        """
        target: verify search with prefix (LIKE) filter on non-primary varchar field
        method: 1. search with filter 'varchar like "0%"' on shared collection
                2. check nq, limit, distance order via check_task
                3. manually assert all returned varchar values start with "0"
        expected: results have varchar starting with "0", distances in COSINE order
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_string_field_not_primary_prefix: searching collection %s" %
                 self.collection_name)
        output_fields = [default_float_field_name, default_string_field_name]
        vectors = cf.gen_vectors(default_nq, default_dim)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=prefix_expr,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "limit": 1,
                                          "metric": "COSINE",
                                          "pk_name": default_int64_field_name,
                                          "enable_milvus_client_api": True})
        # manually verify prefix filter effectiveness
        for hits in res:
            for hit in hits:
                assert str(hit.entity.get(default_string_field_name, "")).startswith("0")

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_not_primary_is_empty(self):
        """
        target: verify search with empty-string comparison filter on varchar field
        method: 1. search with filter 'varchar >= ""' on shared collection
                2. check nq, limit, distance order via check_task
        expected: all rows match (every varchar >= ""), distances in COSINE order
        """
        client = self._client(alias=self.shared_alias)
        search_string_exp = "varchar >= \"\""
        log.info("test_search_string_field_not_primary_is_empty: searching collection %s" %
                 self.collection_name)
        output_fields = [default_string_field_name, default_float_field_name]
        vectors = cf.gen_vectors(default_nq, default_dim)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=search_string_exp,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "pk_name": default_int64_field_name,
                                          "limit": default_limit,
                                          "metric": "COSINE",
                                          "enable_milvus_client_api": True})
        # manually verify filter effectiveness
        for hits in res:
            for hit in hits:
                assert hit.entity.get(default_string_field_name, "") >= ""


@pytest.mark.xdist_group("TestSearchStringVarcharPK")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchStringVarcharPK(TestMilvusClientV2Base):
    """Shared collection with varchar PK
    Schema: varchar(65535, PK), int64, float, json, float_vector(128), dynamic=False
    Data: 3000 rows
    Index: COSINE on float_vector, TRIE on varchar
    """
    shared_alias = "TestSearchStringVarcharPK"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchStringVarcharPK" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, is_primary=True)
        schema.add_field(ct.default_int64_field_name, DataType.INT64)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        data = cf.gen_row_data_by_schema(nb=3000, schema=schema)
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        idx.add_index(field_name=ct.default_string_field_name, index_type="Trie")
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_true(self):
        """
        target: verify search with string equality filter when varchar is primary key
        method: 1. query to get a valid varchar PK value
                2. search with filter varchar == 'value' on shared varchar-PK collection
                3. check nq, limit, distance order via check_task
                4. manually assert returned varchar matches search string
        expected: exactly 1 result with matching varchar PK, distances in COSINE order
        """
        client = self._client(alias=self.shared_alias)
        # query to get a valid string value from the collection
        query_res, _ = self.query(client, self.collection_name, filter="int64 >= 0",
                                  output_fields=[default_string_field_name], limit=10)
        search_str = query_res[1][default_string_field_name]
        search_exp = f"{default_string_field_name} == '{search_str}'"
        log.info("test_search_string_field_is_primary_true: searching collection %s" % self.collection_name)
        log.info("search expr: %s" % search_exp)
        output_fields = [default_string_field_name, default_float_field_name]
        vectors = cf.gen_vectors(default_nq, default_dim)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=search_exp,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "pk_name": ct.default_string_field_name,
                                          "limit": 1,
                                          "metric": "COSINE",
                                          "enable_milvus_client_api": True})
        assert res[0][0]["entity"][default_string_field_name] == search_str

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_index(self):
        """
        target: verify search with prefix (LIKE) filter on varchar PK with Trie index
        method: 1. search with filter 'varchar like "0%"' on varchar-PK collection with Trie index
                2. check nq, limit, distance order via check_task
        expected: results match prefix filter, distances in COSINE order
        """
        client = self._client(alias=self.shared_alias)
        log.info("test_search_string_field_index: searching collection %s" %
                 self.collection_name)
        output_fields = [default_float_field_name, default_string_field_name]
        vectors = cf.gen_vectors(default_nq, default_dim)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=prefix_expr,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "limit": 1,
                                          "metric": "COSINE",
                                          "pk_name": ct.default_string_field_name,
                                          "enable_milvus_client_api": True})
        # manually verify prefix filter effectiveness
        for hits in res:
            for hit in hits:
                assert str(hit.entity.get(default_string_field_name, "")).startswith("0")

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_insert_empty(self):
        """
        target: verify search with empty-string comparison filter on varchar PK
        method: 1. search with filter 'varchar >= ""' (matches all rows) on varchar-PK collection
                2. check nq, limit, distance order via check_task
        expected: results returned (all rows match), distances in COSINE order
        """
        client = self._client(alias=self.shared_alias)
        search_string_exp = "varchar >= \"\""
        limit = 1
        log.info("test_search_string_field_is_primary_insert_empty: searching collection %s" %
                 self.collection_name)
        output_fields = [default_string_field_name, default_float_field_name]
        vectors = cf.gen_vectors(default_nq, default_dim)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=limit,
                             filter=search_string_exp,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "limit": limit,
                                          "metric": "COSINE",
                                          "enable_milvus_client_api": True,
                                          "pk_name": ct.default_string_field_name})
        # manually verify filter effectiveness
        for hits in res:
            for hit in hits:
                assert hit.entity.get(default_string_field_name, "") >= ""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_normal_string_expressions([ct.default_string_field_name]))
    def test_search_with_different_string_expr(self, expression):
        """
        target: verify search with various string expressions returns only matching rows
        method: 1. query all rows from varchar-PK collection
                2. evaluate expression locally to get expected matching PKs
                3. search with the expression
                4. assert all returned PKs are a subset of expected matching PKs
        expected: all returned results satisfy the string expression
        """
        client = self._client(alias=self.shared_alias)
        nb = 3000
        # query to get all data for filter evaluation (varchar is PK, use varchar > "" to match all)
        query_res, _ = self.query(client, self.collection_name, filter='varchar > ""',
                                  output_fields=[ct.default_int64_field_name, ct.default_string_field_name],
                                  limit=nb)
        # filter result with expression in collection
        filter_ids = []
        expression_eval = expression.replace("&&", "and").replace("||", "or")
        for item in query_res:
            int64 = item[ct.default_int64_field_name]
            varchar = item[ct.default_string_field_name]
            if not expression_eval or eval(expression_eval):
                filter_ids.append(item[ct.default_string_field_name])

        # search with expression (AUTOINDEX/HNSW may not return all matches, use subset check)
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        vectors = cf.gen_vectors(default_nq, default_dim)
        search_res, _ = self.search(client, self.collection_name,
                                    data=vectors[:default_nq],
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=nb,
                                    filter=expression)

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = [hit[ct.default_string_field_name] for hit in hits]
            assert set(ids).issubset(filter_ids_set)


@pytest.mark.xdist_group("TestSearchStringBinary")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchStringBinary(TestMilvusClientV2Base):
    """Shared collection with binary vectors
    Schema: int64(PK, auto_id=True), float, varchar(65535), binary_vector(128), dynamic=False
    Data: 3000 rows with binary vectors, varchar=str(i), float=i*1.0
    Index: BIN_IVF_FLAT/JACCARD
    """
    shared_alias = "TestSearchStringBinary"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchStringBinary" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        dim = 128
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        nb = 3000
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        # Override varchar and float with deterministic values for predictable filter expressions
        _, binary_vectors = cf.gen_binary_vectors(nb, dim)
        for i in range(nb):
            data[i][ct.default_float_field_name] = i * 1.0
            data[i][ct.default_string_field_name] = str(i)
            data[i][ct.default_binary_vec_field_name] = binary_vectors[i]
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name,
                      index_type="BIN_IVF_FLAT", metric_type="JACCARD",
                      params={"nlist": 128})
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_binary(self):
        """
        target: verify search with string comparison filter on binary vector collection
        method: 1. search with filter 'varchar >= "0"' on binary vector collection
                2. check nq, limit, distance order via check_task
        expected: results match filter, distances in JACCARD ascending order
        """
        client = self._client(alias=self.shared_alias)
        dim = 128
        _, search_binary_vectors = cf.gen_binary_vectors(default_nq, dim)
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = [default_string_field_name]
        res, _ = self.search(client, self.collection_name,
                             data=search_binary_vectors[:default_nq],
                             anns_field=ct.default_binary_vec_field_name,
                             search_params=search_params,
                             limit=default_limit,
                             filter=default_search_string_exp,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "limit": default_limit,
                                          "metric": "JACCARD",
                                          "pk_name": default_int64_field_name,
                                          "enable_milvus_client_api": True})
        # manually verify filter effectiveness
        for hits in res:
            for hit in hits:
                assert hit.entity.get(default_string_field_name) >= "0"

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_binary(self):
        """
        target: verify search with string comparison filter on binary vector collection (no output fields)
        method: 1. search with filter 'varchar >= "0"' on binary vector collection
                2. check nq, limit, distance order via check_task
        expected: results match filter, distances in JACCARD ascending order
        """
        client = self._client(alias=self.shared_alias)
        dim = 128
        _, search_binary_vectors = cf.gen_binary_vectors(default_nq, dim)
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        res, _ = self.search(client, self.collection_name,
                             data=search_binary_vectors[:default_nq],
                             anns_field=ct.default_binary_vec_field_name,
                             search_params=search_params,
                             limit=default_limit,
                             filter=default_search_string_exp,
                             output_fields=[default_string_field_name],
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "limit": default_limit,
                                          "metric": "JACCARD",
                                          "pk_name": default_int64_field_name,
                                          "enable_milvus_client_api": True})
        # manually verify filter effectiveness
        for hits in res:
            for hit in hits:
                assert hit.entity.get(default_string_field_name) >= "0"

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_mix_expr_with_binary(self):
        """
        target: verify search with mixed int64+varchar filter on binary vector collection
        method: 1. search with filter "int64 >= 0 && varchar >= '0'" on binary collection
                2. check nq, limit, distance order via check_task
                3. manually assert all results satisfy both filter conditions
        expected: all results have int64 >= 0 and varchar >= "0", distances in JACCARD order
        """
        client = self._client(alias=self.shared_alias)
        dim = 128
        log.info("test_search_mix_expr_with_binary: searching collection %s" %
                 self.collection_name)
        _, search_binary_vectors = cf.gen_binary_vectors(default_nq, dim)
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = [default_string_field_name, default_float_field_name]
        res, _ = self.search(client, self.collection_name,
                             data=search_binary_vectors[:default_nq],
                             anns_field=ct.default_binary_vec_field_name,
                             search_params=search_params,
                             limit=default_limit,
                             filter=default_search_mix_exp,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "pk_name": default_int64_field_name,
                                          "limit": default_limit,
                                          "metric": "JACCARD",
                                          "enable_milvus_client_api": True})
        # manually verify filter effectiveness
        for hits in res:
            for hit in hits:
                assert hit.entity.get(default_string_field_name) >= "0"
                assert hit.entity.get(default_int64_field_name) >= 0


class TestSearchStringIndependent(TestMilvusClientV2Base):
    """Independent tests for string search scenarios requiring unique schemas
    (multi-language, multi-vector, range search, cross-field comparison)
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("language", ["en", "zh", "de"])
    def test_search_string_different_language(self, language):
        """
        target: verify search with string equality filter using different language data
        method: 1. create collection with multi-language string data
                2. query to get a valid varchar value
                3. search with filter varchar == 'value'
                4. manually assert returned varchar matches search string
        expected: exactly 1 result with matching varchar, distances in COSINE order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = ct.default_nb
        dim = 64
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_default_rows_data(nb=nb, dim=dim, auto_id=True, with_json=False, language=language)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # query to get a valid string value from the collection
        query_res, _ = self.query(client, collection_name, filter='varchar > ""',
                                  output_fields=[default_string_field_name], limit=10)
        search_str = query_res[0][default_string_field_name]
        search_exp = f"{default_string_field_name} == '{search_str}'"
        log.info("test_search_string_different_language: searching with language=%s" % language)
        search_vectors = cf.gen_vectors(default_nq, dim)
        output_fields = [default_string_field_name, default_float_field_name]
        res, _ = self.search(client, collection_name,
                             data=search_vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=search_exp,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "limit": 1,
                                          "metric": "COSINE",
                                          "pk_name": default_int64_field_name,
                                          "enable_milvus_client_api": True})
        assert res[0][0]["entity"][default_string_field_name] == search_str

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_string_field_is_primary_true_multi_vector_fields(self):
        """
        target: verify search with string filter across multiple vector fields when varchar is PK
        method: 1. create collection with varchar PK and 3 float vector fields
                2. search each vector field with filter 'varchar >= "0"'
                3. check nq, limit, returned IDs via check_task
        expected: search succeeds on all 3 vector fields, returned IDs are valid, distances in COSINE order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 64
        multiple_vector_field_1 = cf.gen_unique_str("multiple_vector")
        multiple_vector_field_2 = cf.gen_unique_str("multiple_vector")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, is_primary=True)
        schema.add_field(ct.default_int64_field_name, DataType.INT64)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(multiple_vector_field_1, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(multiple_vector_field_2, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        idx.add_index(field_name=multiple_vector_field_1, metric_type="COSINE")
        idx.add_index(field_name=multiple_vector_field_2, metric_type="COSINE")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        log.info("test_search_string_field_is_primary_true_multi_vector_fields: searching collection %s" %
                 collection_name)
        search_vectors = cf.gen_vectors(default_nq, dim)
        output_fields = [default_string_field_name, default_float_field_name]
        vector_list = [ct.default_float_vec_field_name, multiple_vector_field_1, multiple_vector_field_2]
        for search_field in vector_list:
            res, _ = self.search(client, collection_name,
                                 data=search_vectors[:default_nq],
                                 anns_field=search_field,
                                 search_params=default_search_params,
                                 limit=default_limit,
                                 filter=default_search_string_exp,
                                 output_fields=output_fields,
                                 check_task=CheckTasks.check_search_results,
                                 check_items={"nq": default_nq,
                                              "ids": insert_ids,
                                              "pk_name": ct.default_string_field_name,
                                              "limit": default_limit,
                                              "metric": "COSINE",
                                              "enable_milvus_client_api": True})
            # manually verify filter effectiveness
            for hits in res:
                for hit in hits:
                    assert hit.entity.get(default_string_field_name) >= "0"

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_string_field_is_primary_true(self):
        """
        target: verify range search with string filter across multiple vector fields when varchar is PK
        method: 1. create collection with varchar PK, dynamic field, and 3 float vector fields (L2)
                2. range search each vector field with filter 'varchar >= "0"'
                3. check nq, limit, returned IDs via check_task
        expected: range search succeeds on all 3 vector fields, returned IDs are valid, distances in L2 order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 64
        multiple_vector_field_1 = cf.gen_unique_str("multiple_vector")
        multiple_vector_field_2 = cf.gen_unique_str("multiple_vector")
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, is_primary=True)
        schema.add_field(ct.default_int64_field_name, DataType.INT64)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(multiple_vector_field_1, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(multiple_vector_field_2, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        # Create index with L2 metric
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        idx.add_index(field_name=multiple_vector_field_1, metric_type="L2")
        idx.add_index(field_name=multiple_vector_field_2, metric_type="L2")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        log.info("test_range_search_string_field_is_primary_true: searching collection %s" %
                 collection_name)
        range_search_params = {"metric_type": "L2",
                               "params": {"radius": 1000, "range_filter": 0}}
        search_vectors = cf.gen_vectors(default_nq, dim)
        output_fields = [default_string_field_name, default_float_field_name]
        vector_list = [ct.default_float_vec_field_name, multiple_vector_field_1, multiple_vector_field_2]
        for search_field in vector_list:
            res, _ = self.search(client, collection_name,
                                 data=search_vectors[:default_nq],
                                 anns_field=search_field,
                                 search_params=range_search_params,
                                 limit=default_limit,
                                 filter=default_search_string_exp,
                                 output_fields=output_fields,
                                 check_task=CheckTasks.check_search_results,
                                 check_items={"nq": default_nq,
                                              "ids": insert_ids,
                                              "limit": default_limit,
                                              "metric": "L2",
                                              "pk_name": ct.default_string_field_name,
                                              "enable_milvus_client_api": True})
            # manually verify filter effectiveness
            for hits in res:
                for hit in hits:
                    assert hit.entity.get(default_string_field_name) >= "0"

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_all_index_with_compare_expr(self):
        """
        target: verify search with cross-field comparison filter (float >= int64) on varchar-PK collection
        method: 1. create collection with varchar PK, Trie index on varchar, IVF_SQ8 on vector
                2. verify Trie index exists
                3. search with filter 'float >= int64' and output scalar fields
                4. manually verify filter effectiveness on returned results
        expected: all results satisfy float >= int64, distances in COSINE order
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, is_primary=True)
        schema.add_field(ct.default_int64_field_name, DataType.INT64)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        # create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="IVF_SQ8", metric_type="COSINE",
                      params={"nlist": 64})
        idx.add_index(field_name=ct.default_string_field_name, index_type="Trie")
        self.create_index(client, collection_name, index_params=idx)

        # verify index exists
        indexes, _ = self.list_indexes(client, collection_name)
        assert ct.default_string_field_name in indexes

        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        # search with compare expr
        expr = 'float >= int64'
        search_vectors = cf.gen_vectors(default_nq, default_dim)
        output_fields = [default_int64_field_name,
                         default_float_field_name, default_string_field_name]
        res, _ = self.search(client, collection_name,
                             data=search_vectors[:default_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=expr,
                             output_fields=output_fields,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": default_nq,
                                          "ids": insert_ids,
                                          "limit": default_limit,
                                          "metric": "COSINE",
                                          "pk_name": ct.default_string_field_name,
                                          "enable_milvus_client_api": True})
        # manually verify cross-field comparison filter
        for hits in res:
            for hit in hits:
                assert hit.entity.get(default_float_field_name) >= hit.entity.get(default_int64_field_name)
