import pytest
from pymilvus import DataType
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base

default_nq = ct.default_nq
default_limit = ct.default_limit
default_search_exp = f"{ct.default_int64_field_name} >= 0"
default_search_field = ct.default_float_vec_field_name
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name
half_nb = ct.default_nb // 2


class TestSearchDiskannIndependent(TestMilvusClientV2Base):
    """
    ******************************************************************
      The following cases are used to test search about diskann index
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_delete_data(self):
        """
        target: test delete after creating index
        method: 1.create collection , insert data,
                2.create  diskann index
                3.delete data, the search
        expected: assert index and deleted id not in search result
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 100

        # Create schema with auto_id and dynamic field
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        ids = insert_res["ids"]
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="DISKANN", metric_type="L2", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # delete half of data
        expr = f'{ct.default_int64_field_name} in {ids[:half_nb]}'
        self.delete(client, collection_name, filter=expr)

        tmp_expr = f'{ct.default_int64_field_name} in {[0]}'
        self.delete(client, collection_name, filter=tmp_expr)

        # search
        default_search_params = {"metric_type": "L2", "params": {"search_list": 30}}
        vectors = cf.gen_vectors(default_nq, dim)
        output_fields = [default_int64_field_name,
                         default_float_field_name, default_string_field_name]
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=default_search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": ids[half_nb:],
                                 "limit": default_limit,
                                 "metric": "L2",
                                 "pk_name": ct.default_int64_field_name,
                                 "enable_milvus_client_api": True})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_scalar_field(self):
        """
        target: test search with scalar field
        method: 1.create collection , insert data
                2.create more index ,then load
                3.search with expr
        expected: assert index and search successfully
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 66

        # Create schema with varchar PK and dynamic field
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535, is_primary=True)
        schema.add_field(ct.default_int64_field_name, DataType.INT64)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        ids = insert_res["ids"]
        self.flush(client, collection_name)

        # 2. create index
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name,
                      index_type="DISKANN", metric_type="L2", params={})
        idx.add_index(field_name=ct.default_string_field_name, index_type="")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search with expr (use varchar PK filter — gen_row_data_by_schema generates "0","1","2",...)
        default_expr = f'{ct.default_string_field_name} in ["0", "1", "2", "3"]'
        limit = 4
        default_search_params = {"metric_type": "L2", "params": {"search_list": 30}}
        vectors = cf.gen_vectors(default_nq, dim)
        output_fields = [default_int64_field_name,
                         default_float_field_name, default_string_field_name]
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=limit,
                    filter=default_expr,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "ids": ids,
                                 "limit": limit,
                                 "metric": "L2",
                                 "pk_name": ct.default_string_field_name,
                                 "enable_milvus_client_api": True})
