import pytest
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base

prefix = "search_collection"
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit


def _sparse_column_to_rows(data, nb):
    """Convert column-oriented sparse data to row-oriented dicts for Client V2 insert.

    ``data`` is [int64_list, float_list, varchar_list, sparse_vector_list]
    returned by ``cf.gen_default_list_sparse_data``.
    """
    rows = []
    for i in range(nb):
        rows.append({
            ct.default_int64_field_name: data[0][i],
            ct.default_float_field_name: data[1][i],
            ct.default_string_field_name: data[2][i],
            ct.default_sparse_vec_field_name: data[3][i],
        })
    return rows


class TestSparseSearchIndependent(TestMilvusClientV2Base):
    """ Test cases for sparse vector search using Client V2 API """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ct.all_index_types[10:12])
    @pytest.mark.parametrize("inverted_index_algo", ct.inverted_index_algo)
    def test_sparse_index_search(self, index, inverted_index_algo):
        """
        target: verify that sparse index for sparse vectors can be searched properly
        method: create connection, collection, insert and search
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        nb = 3000

        # create collection with sparse schema
        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, collection_name, schema=schema)

        # insert data (convert column-oriented to rows)
        data = cf.gen_default_list_sparse_data(nb=nb)
        rows = _sparse_column_to_rows(data, nb)
        self.insert(client, collection_name, data=rows)

        # create sparse index
        params = cf.get_index_params_params(index)
        params.update({"inverted_index_algo": inverted_index_algo})
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search
        _params = cf.get_search_params_params(index)
        _params.update({"dim_max_score_ratio": 1.05})
        search_params = {"metric_type": "IP", "params": _params}
        self.search(client, collection_name,
                    data=data[-1][0:default_nq],
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    output_fields=[ct.default_sparse_vec_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": [ct.default_sparse_vec_field_name]})

        # search with filter
        expr = "int64 < 100 "
        self.search(client, collection_name,
                    data=data[-1][0:default_nq],
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    filter=expr,
                    output_fields=[ct.default_sparse_vec_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": [ct.default_sparse_vec_field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[10:12])
    @pytest.mark.parametrize("dim", [32768, ct.max_sparse_vector_dim])
    def test_sparse_index_dim(self, index, dim):
        """
        target: validating the sparse index in different dimensions
        method: create connection, collection, insert and hybrid search
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        nb = default_nb

        # create collection with sparse schema
        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        data = cf.gen_default_list_sparse_data(dim=dim)
        rows = _sparse_column_to_rows(data, nb)
        self.insert(client, collection_name, data=rows)

        # create sparse index
        params = cf.get_index_params_params(index)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search
        self.search(client, collection_name,
                    data=data[-1][0:default_nq],
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=ct.default_sparse_search_params,
                    limit=1,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": 1,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[10:12])
    @pytest.mark.parametrize("inverted_index_algo", ct.inverted_index_algo)
    def test_sparse_index_enable_mmap_search(self, index, inverted_index_algo):
        """
        target: verify that the sparse indexes of sparse vectors can be searched properly after turning on mmap
        method: create connection, collection, enable mmap, insert and search
        expected: search successfully, query result is correct
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        first_nb = 3000

        # create collection with sparse schema
        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, collection_name, schema=schema)

        # insert first batch
        data = cf.gen_default_list_sparse_data(nb=first_nb, start=0)
        rows = _sparse_column_to_rows(data, first_nb)
        self.insert(client, collection_name, data=rows)

        # create sparse index
        params = cf.get_index_params_params(index)
        params.update({"inverted_index_algo": inverted_index_algo})
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP", params=params)
        self.create_index(client, collection_name, index_params=idx)

        # enable mmap on collection
        self.alter_collection_properties(client, collection_name, properties={'mmap.enabled': True})
        desc, _ = self.describe_collection(client, collection_name)
        assert desc.get("properties", {}).get("mmap.enabled") == 'True'

        # enable mmap on index (index name defaults to field name in Client V2)
        self.alter_index_properties(client, collection_name,
                                    index_name=ct.default_sparse_vec_field_name,
                                    properties={'mmap.enabled': True})
        index_info, _ = self.describe_index(client, collection_name,
                                            index_name=ct.default_sparse_vec_field_name)
        assert index_info.get("mmap.enabled") == 'True'

        # insert second batch
        second_nb = 2000
        data2 = cf.gen_default_list_sparse_data(nb=second_nb, start=first_nb)
        rows2 = _sparse_column_to_rows(data2, second_nb)
        self.insert(client, collection_name, data=rows2)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # search
        self.search(client, collection_name,
                    data=data[-1][0:default_nq],
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=ct.default_sparse_search_params,
                    limit=default_limit,
                    output_fields=[ct.default_sparse_vec_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": [ct.default_sparse_vec_field_name]})

        # query to verify data
        expr_id_list = [0, 1, 10, 100]
        term_expr = f'{ct.default_int64_field_name} in {expr_id_list}'
        res, _ = self.query(client, collection_name, filter=term_expr)
        assert len(res) == 4

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("drop_ratio_build", [0.01])
    @pytest.mark.parametrize("index", ct.all_index_types[10:12])
    def test_search_sparse_ratio(self, drop_ratio_build, index):
        """
        target: create a sparse index by adjusting the ratio parameter.
        method: create a sparse index by adjusting the ratio parameter.
        expected: search successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        nb = 4000

        # create collection with sparse schema
        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        data = cf.gen_default_list_sparse_data(nb=nb)
        rows = _sparse_column_to_rows(data, nb)
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        # create sparse index with drop_ratio_build
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP",
                      params={"drop_ratio_build": drop_ratio_build})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # verify index exists (list_indexes returns field names in Client V2)
        indexes, _ = self.list_indexes(client, collection_name)
        assert ct.default_sparse_vec_field_name in indexes

        # search with valid dim_max_score_ratio values
        _params = {"drop_ratio_search": 0.2}
        for dim_max_score_ratio in [0.5, 0.99, 1, 1.3]:
            _params.update({"dim_max_score_ratio": dim_max_score_ratio})
            search_params = {"metric_type": "IP", "params": _params}
            self.search(client, collection_name,
                        data=data[-1][0:default_nq],
                        anns_field=ct.default_sparse_vec_field_name,
                        search_params=search_params,
                        limit=default_limit,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": default_nq,
                                     "limit": default_limit,
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

        # search with invalid dim_max_score_ratio values
        error = {ct.err_code: 999,
                 ct.err_msg: "should be in range [0.500000, 1.300000]"}
        for invalid_ratio in [0.49, 1.4]:
            _params.update({"dim_max_score_ratio": invalid_ratio})
            search_params = {"metric_type": "IP", "params": _params}
            self.search(client, collection_name,
                        data=data[-1][0:default_nq],
                        anns_field=ct.default_sparse_vec_field_name,
                        search_params=search_params,
                        limit=default_limit,
                        check_task=CheckTasks.err_res,
                        check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[10:12])
    def test_sparse_vector_search_output_field(self, index):
        """
        target: create sparse vectors and search
        method: create sparse vectors and search
        expected: normal search
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        nb = 4000

        # create collection with sparse schema (auto_id default)
        schema = cf.gen_default_sparse_schema()
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        data = cf.gen_default_list_sparse_data(nb=nb)
        rows = _sparse_column_to_rows(data, nb)
        self.insert(client, collection_name, data=rows)

        # create sparse index
        params = cf.get_index_params_params(index)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search with specific output_fields
        d = cf.gen_default_list_sparse_data(nb=10)
        self.search(client, collection_name,
                    data=d[-1][0:default_nq],
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=ct.default_sparse_search_params,
                    limit=default_limit,
                    output_fields=["float", "sparse_vector"],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": ["float", "sparse_vector"]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[10:12])
    @pytest.mark.parametrize("inverted_index_algo", ct.inverted_index_algo)
    def test_sparse_vector_search_iterator(self, index, inverted_index_algo):
        """
        target: create sparse vectors and search iterator
        method: create sparse vectors and search iterator
        expected: normal search
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        nb = 4000

        # create collection with sparse schema (auto_id default)
        schema = cf.gen_default_sparse_schema()
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        data = cf.gen_default_list_sparse_data(nb=nb)
        rows = _sparse_column_to_rows(data, nb)
        self.insert(client, collection_name, data=rows)

        # create sparse index
        params = cf.get_index_params_params(index)
        params.update({"inverted_index_algo": inverted_index_algo})
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search iterator
        batch_size = 100
        self.search_iterator(client, collection_name,
                             data=data[-1][0:1],
                             batch_size=batch_size,
                             limit=500,
                             anns_field=ct.default_sparse_vec_field_name,
                             search_params=ct.default_sparse_search_params,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={"batch_size": batch_size})
