import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

prefix = "client_insert"
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
default_dynamic_field_name = "field_new"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name
default_int32_field_name = ct.default_int32_field_name
default_int32_value = ct.default_int32_value
default_timestamp_field_name = "timestamp"

class TestMilvusClientTimestamptzValid(TestMilvusClientV2Base):

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_UTC(self):
        """
        target:  Test timestamptz can be successfully inserted and queried
        method:
            1. Create a collection
            2. Generate rows with timestamptz and insert the rows
            3. Insert the rows
        expected: Step 3 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: generate rows and insert the rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)

        # step 3: query the rows
        rows = cf.convert_timestamptz(rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: rows,
                                         "pk_name": default_primary_key_field_name})

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_Asia_Shanghai(self):
        # BUG: https://github.com/milvus-io/milvus/issues/44595 
        """
        target:  Test timestamptz can be successfully inserted and queried
        method:
            1. Create a collection
            2. Generate rows with timestamptz and insert the rows
            3. Insert the rows
        expected: Step 3 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        db_name = self.list_databases(client)[0]
        self.alter_database_properties(client, db_name, properties={"database.timezone": "Asia/Shanghai"})
        
        # step 2: generate rows and insert the rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)

        # step 3: query the rows
        rows = cf.convert_timestamptz(rows, default_timestamp_field_name, "Asia/Shanghai")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: rows,
                                         "pk_name": default_primary_key_field_name})
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_edge_case(self):
        """
        target:  Test timestamptz can be successfully inserted and queried
        method:
            1. Create a collection
            2. Generate rows with edge timestamptz and insert the rows
            3. Insert the rows
        expected: Step 3 should result success
        """
        # step 1: create collection
        default_dim = 3
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: generate rows with edge timestamptz and insert the rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        rows = [{default_primary_key_field_name: 0, default_vector_field_name: [1,2,3], default_timestamp_field_name: "0000-01-01 00:00:00"},
                {default_primary_key_field_name: 1, default_vector_field_name: [4,5,6], default_timestamp_field_name: "9999-12-31T23:59:59"},
                {default_primary_key_field_name: 2, default_vector_field_name: [10,11,12], default_timestamp_field_name: "1970-01-01T00:00:00+01:00"},
                {default_primary_key_field_name: 3, default_vector_field_name: [13,14,15], default_timestamp_field_name: "2000-01-01T00:00:00+01:00"}]
        self.insert(client, collection_name, rows)

        # step 3: query the rows
        rows = cf.convert_timestamptz(rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: rows,
                                         "pk_name": default_primary_key_field_name})
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_Feb_29(self):
        """
        target:  Milvus raise error when input data with Feb 29
        method:
            1. Create a collection
            2. Generate rows with Feb 29 on a leap year and insert the rows
            3. Insert the rows
        expected: Step 3 should result success
        """
        # step 1: create collection
        default_dim = 3
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: generate rows with Feb 29 on a leap year and insert the rows
        rows = [{default_primary_key_field_name: 0, default_vector_field_name: [1,2,3], default_timestamp_field_name: "2024-02-29T00:00:00+03:00"}]
        self.insert(client, collection_name, rows)
        
        # step 3: query the rows
        rows = cf.convert_timestamptz(rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: rows,
                                         "pk_name": default_primary_key_field_name})
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_partial_update(self):
        # BUG: https://github.com/milvus-io/milvus/issues/44527
        """
        target:  Test timestamptz can be successfully inserted and queried
        method:
            1. Create a collection
            2. Generate rows with timestamptz and insert the rows
            3. partial update the rows
        expected: Step 3 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: generate rows with timestamptz and insert the rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, rows, partial_update=True)
        
        # step 3: partial update the rows
        partial_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                                  desired_field_names=[default_primary_key_field_name, default_timestamp_field_name])
        self.upsert(client, collection_name, partial_rows, partial_update=True)
        
        # step 4: query the rows
        partial_rows = cf.convert_timestamptz(partial_rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            output_fields=[default_timestamp_field_name],
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: partial_rows,
                                         "pk_name": default_primary_key_field_name})
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_default_value(self):
        # BUG: https://github.com/milvus-io/milvus/issues/44585
        """
        target:  Test timestamptz can be successfully inserted and queried with default value
        method:
            1. Create a collection
            2. Generate rows without timestamptz and insert the rows
            3. Insert the rows
        expected: Step 3 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True, default_value="2025-01-01T00:00:00")
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)

        # step 2: generate rows without timestamptz and insert the rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, skip_field_names=[default_timestamp_field_name])
        self.insert(client, collection_name, rows)

        # step 3: query the rows
        for row in rows:
            row[default_timestamp_field_name] = "2025-01-01T00:00:00+08:00"
        rows = cf.convert_timestamptz(rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: rows,
                                         "pk_name": default_primary_key_field_name})
        
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_search(self):
        # BUG: https://github.com/milvus-io/milvus/issues/44594
        """
        target:  Milvus can search with timestamptz expr
        method:
            1. Create a collection
            2. Generate rows with timestamptz and insert the rows
            3. Search with timestamptz expr
        expected: Step 3 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: generate rows with timestamptz and insert the rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        # step 3: search with timestamptz expr
        vectors_to_search = cf.gen_vectors(1, default_dim, vector_data_type=DataType.FLOAT_VECTOR)
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search, 
                    timezone="Asia/Shanghai",
                    time_fields="year, month, day, hour, minute, second, microsecond",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                            "nq": len(vectors_to_search),
                            "ids": insert_ids,
                            "pk_name": default_primary_key_field_name,
                            "limit": default_limit})

        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_query(self):
        # BUG: https://github.com/milvus-io/milvus/issues/44598
        """
        target:  Milvus can query with timestamptz expr
        method:
            1. Create a collection
            2. Generate rows with timestamptz and insert the rows
            3. Query with timestamptz expr
        expected: Step 3 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)

        # step 2: generate rows with timestamptz and insert the rows
        rows = [{default_primary_key_field_name: 0, default_vector_field_name: [1,2,3], default_timestamp_field_name: "0000-01-01 00:00:00"},
                {default_primary_key_field_name: 1, default_vector_field_name: [4,5,6], default_timestamp_field_name: "2021-02-28T00:00:00Z"},
                {default_primary_key_field_name: 2, default_vector_field_name: [7,8,9], default_timestamp_field_name: "2025-05-25T23:46:05"},
                {default_primary_key_field_name: 3, default_vector_field_name: [10,11,12], default_timestamp_field_name:"2025-05-30T23:46:05+05:30"},
                {default_primary_key_field_name: 4, default_vector_field_name: [13,14,15], default_timestamp_field_name: "2025-10-05 12:56:34"},
                {default_primary_key_field_name: 5, default_vector_field_name: [16,17,18], default_timestamp_field_name: "9999-12-31T23:46:05"}]
        self.insert(client, collection_name, rows)
        
        # step 3: query with timestamptz expr
        shanghai_time_row = cf.convert_timestamptz(rows, default_timestamp_field_name, "Asia/Shanghai")
        self.query(client, collection_name, filter=default_search_exp,
                            timezone="Asia/Shanghai",
                            time_fields="year, month, day, hour, minute, second, microsecond",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: shanghai_time_row,
                                         "pk_name": default_primary_key_field_name})
        # >=
        expr = f"{default_timestamp_field_name} >= ISO '2025-05-30T23:46:05+05:30'"
        self.query(client, collection_name, filter=expr,
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: shanghai_time_row,
                                         "pk_name": default_primary_key_field_name})
        # ==
        expr = f"{default_timestamp_field_name} == ISO '9999-12-31T23:46:05Z'"
        self.query(client, collection_name, filter=expr,
                    check_task=CheckTasks.check_query_results,
                    check_items={exp_res: shanghai_time_row,
                                    "pk_name": default_primary_key_field_name})
        
        # <=
        expr = f"{default_timestamp_field_name} <= ISO '2025-01-01T00:00:00+08:00'"
        self.query(client, collection_name, filter=expr,
                    check_task=CheckTasks.check_query_results,
                    check_items={exp_res: shanghai_time_row,
                                    "pk_name": default_primary_key_field_name})
        # !=
        expr = f"{default_timestamp_field_name} != ISO '9999-12-31T23:46:05'"
        self.query(client, collection_name, filter=expr,
                    check_task=CheckTasks.check_query_results,
                    check_items={exp_res: shanghai_time_row,
                                    "pk_name": default_primary_key_field_name})
        # INTERVAL
        expr = f"{default_timestamp_field_name} + INTERVAL 'P3D' != ISO '0000-01-02T00:00:00Z'"
        self.query(client, collection_name, filter=expr,
                    check_task=CheckTasks.check_query_results,
                    check_items={exp_res: shanghai_time_row,
                                    "pk_name": default_primary_key_field_name})
        
        # lower < tz < upper
        # BUG: https://github.com/milvus-io/milvus/issues/44600
        expr = f"ISO '2025-01-01T00:00:00+08:00' < {default_timestamp_field_name} < ISO '2026-10-05T12:56:34+08:00'"
        self.query(client, collection_name, filter=expr,
                    check_task=CheckTasks.check_query_results,
                    check_items={exp_res: shanghai_time_row,
                                    "pk_name": default_primary_key_field_name})
        
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_alter_collection(self):
        """
        target:  Milvus raise error when alter collection properties
        method:
            1. Create a collection
            2. Alter collection properties
            3. Query the rows
        expected: Step 3 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: alter collection properties
        self.alter_collection_properties(client, collection_name, properties={"timezone": "Asia/Shanghai"})
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        # step 3: query the rows
        rows = cf.convert_timestamptz(rows, default_timestamp_field_name, "Asia/Shanghai")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: rows,
                                         "pk_name": default_primary_key_field_name})
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_add_collection_field(self):
        # BUG: https://github.com/milvus-io/milvus/issues/44527
        """
        target:  Milvus raise error when add collection field with timestamptz
        method:
            1. Create a collection
            2. Add collection field with timestamptz
            3. Query the rows
            4. Insert new rows and query the rows
            5. Partial update the rows and query the rows
        expected: Step 3, Step 4, and Step 5 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: add collection field with timestamptz
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.add_collection_field(client, collection_name, field_name=default_timestamp_field_name, data_type=DataType.TIMESTAMPTZ,
                                  nullable=True)
        index_params.add_index(default_timestamp_field_name, index_type="STL_SORT")
        self.create_index(client, collection_name, index_params=index_params)

        # step 3: query the rows
        for row in rows:
            row[default_timestamp_field_name] = None
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: rows,
                                         "pk_name": default_primary_key_field_name})

        # step 4: insert new rows and query the rows
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=default_nb)
        self.insert(client, collection_name, new_rows)
        new_rows = cf.convert_timestamptz(new_rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= {default_nb}",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: new_rows,
                                         "pk_name": default_primary_key_field_name})
        
        # step 5: partial update the rows and query the rows
        pu_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                            desired_field_names=[default_primary_key_field_name, default_timestamp_field_name])
        self.upsert(client, collection_name, pu_rows, partial_update=True)
        pu_rows = cf.convert_timestamptz(pu_rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"0 <= {default_primary_key_field_name} <= {default_nb}",
                            check_task=CheckTasks.check_query_results,
                            output_fields=[default_timestamp_field_name],
                            check_items={exp_res: pu_rows,
                                         "pk_name": default_primary_key_field_name})

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_add_field_compaction(self):
        """
        target: test compaction with added timestamptz field
        method:
            1. Create a collection
            2. insert rows
            3. add field with timestamptz
            4. compact
        expected: Step 4 should success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        # step 3: add field with timestamptz
        self.add_collection_field(client, collection_name, field_name=default_timestamp_field_name, data_type=DataType.TIMESTAMPTZ,
                                  nullable=True)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params.add_index(default_timestamp_field_name, index_type="STL_SORT")
        self.create_index(client, collection_name, index_params=index_params)
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=default_nb)
        self.insert(client, collection_name, new_rows)
        
        # step 4: compact
        compact_id = self.compact(client, collection_name, is_clustering=False)[0]
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id, is_clustering=False)[0]
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(1, f"Compact after index cost more than {cost}s")
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_add_field_search(self):
        # BUG: https://github.com/milvus-io/milvus/issues/44622
        """
        target: test add field with timestamptz and search
        method:
            1. Create a collection
            2. Insert rows
            3. Add field with timestamptz
            4. Search the rows
        expected: Step 4 should success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        # step 3: add field with timestamptz
        self.add_collection_field(client, collection_name, field_name=default_timestamp_field_name, data_type=DataType.TIMESTAMPTZ,
                                  nullable=True)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params.add_index(default_timestamp_field_name, index_type="STL_SORT")
        self.create_index(client, collection_name, index_params=index_params)
        
        # step 4: search the rows
        vectors_to_search = cf.gen_vectors(1, default_dim, vector_data_type=DataType.FLOAT_VECTOR)
        insert_ids = [i for i in range(default_nb)]
        check_items = {"enable_milvus_client_api": True,
                        "nq": len(vectors_to_search),
                        "ids": insert_ids,
                        "pk_name": default_primary_key_field_name,
                        "limit": default_limit}
        self.search(client, collection_name, vectors_to_search, 
                    filter=f"{default_timestamp_field_name} is null",
                    check_task=CheckTasks.check_search_results,
                    check_items=check_items)
        
        self.search(client, collection_name, vectors_to_search,
                    filter=f"{default_timestamp_field_name} is not null",
                    check_task=CheckTasks.check_search_results,
                    check_items=check_items)
        
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_add_field_with_default_value(self):
        """
        target:  Milvus raise error when add field with timestamptz and default value
        method:
            1. Create a collection
            2. Add field with timestamptz and default value
            3. Query the rows
        expected: Step 3 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: add field with timestamptz and default value
        default_timestamp_value = "2025-01-01T00:00:00"
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.add_collection_field(client, collection_name, field_name=default_timestamp_field_name, data_type=DataType.TIMESTAMPTZ,
                                  nullable=True, default_value=default_timestamp_value)
        index_params.add_index(default_timestamp_field_name, index_type="STL_SORT")
        self.create_index(client, collection_name, index_params=index_params)
        
        # step 3: query the rows
        for row in rows:
            row[default_timestamp_field_name] = default_timestamp_value
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: rows,
                                         "pk_name": default_primary_key_field_name})
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_add_another_timestamptz_field(self):
        """
        target:  Milvus raise error when add another timestamptz field
        method:
            1. Create a collection
            2. Insert rows and then add another timestamptz field
            3. Insert new rows
            4. Insert new rows
            5. Query the new rows
        expected: Step 2,3,4,and 5 should result success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="STL_SORT")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        # step 3: add another timestamptz field
        self.add_collection_field(client, collection_name, field_name=default_timestamp_field_name + "_new", data_type=DataType.TIMESTAMPTZ,
                                  nullable=True)
        schema.add_field(default_timestamp_field_name + "_new", DataType.TIMESTAMPTZ, nullable=True) 
        index_params.add_index(default_timestamp_field_name + "_new", index_type="STL_SORT")

        # step 4: insert new rows
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=default_nb)
        self.upsert(client, collection_name, new_rows)
        self.create_index(client, collection_name, index_params=index_params)

        # step 5: query the new rows
        new_rows = cf.convert_timestamptz(new_rows, default_timestamp_field_name + "_new", "UTC")
        new_rows = cf.convert_timestamptz(new_rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= {default_nb}",
                    check_task=CheckTasks.check_query_results,
                    check_items={exp_res: new_rows,
                                    "pk_name": default_primary_key_field_name})

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_insert_delete_upsert_with_flush(self):
        """
        target: test insert, delete, upsert with flush on timestamptz
        method:
            1. Create a collection
            2. Insert rows
            3. Delete the rows
            4. flush the rows and query
            5. upsert the rows and flush
            6. query the rows
        expected: Step 2-6 should success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        # step 3: delete the rows
        self.delete(client, collection_name, filter=f"{default_primary_key_field_name} < {default_nb//2}")
        
        # step 4: flush the rows and query
        self.flush(client, collection_name)
        rows = cf.convert_timestamptz(rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: rows[default_nb//2:],
                                         "pk_name": default_primary_key_field_name})
        
        # step 5: upsert the rows and flush
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, new_rows)
        self.flush(client, collection_name)
        
        # step 6: query the rows
        new_rows = cf.convert_timestamptz(new_rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: new_rows,
                                         "pk_name": default_primary_key_field_name})

        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_insert_upsert_flush_delete_upsert_flush(self):
        # BUG: blocked by partial update
        """
        target: test insert, upsert, flush, delete, upsert with flush on timestamptz
        method:
            1. Create a collection
            2. Insert rows
            3. Upsert the rows
            4. Flush the rows
            5. Delete the rows
            6. Upsert the rows and flush
            7. Query the rows
        expected: Step 2-7 should success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        # step 3: upsert the rows
        partial_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, 
                                                 desired_field_names=[default_primary_key_field_name, default_timestamp_field_name])
        self.upsert(client, collection_name, partial_rows, partial_update=True)

        # step 4: flush the rows
        self.flush(client, collection_name)
        
        # step 5: delete the rows
        self.delete(client, collection_name, filter=f"{default_primary_key_field_name} < {default_nb//2}")
        
        # step 6: upsert the rows and flush
        new_rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.upsert(client, collection_name, new_rows)
        self.flush(client, collection_name)
        
        # step 7: query the rows
        new_rows = cf.convert_timestamptz(new_rows, default_timestamp_field_name, "UTC")
        self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                            check_task=CheckTasks.check_query_results,
                            check_items={exp_res: new_rows,
                                         "pk_name": default_primary_key_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_read_from_different_client(self):
        """
        target: test read from different client in different timezone
        method:
            1. Create a collection
            2. Insert rows
            3. Query the rows from different client in different timezone
        expected: Step 3 should success
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                                consistency_level="Strong", index_params=index_params)
        
        # step 2: insert rows
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        
        # step 3: query the rows from different client in different timezone
        client2 = self._client()
        shanghai_rows = cf.convert_timestamptz(rows, default_timestamp_field_name, "Asia/Shanghai")
        LA_rows = cf.convert_timestamptz(rows, default_timestamp_field_name, "America/Los_Angeles")
        result_1 = self.query(client, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                        check_task=CheckTasks.check_query_results,
                        timezone="Asia/Shanghai",
                        check_items={exp_res: shanghai_rows,
                                        "pk_name": default_primary_key_field_name})[0]
        result_2 = self.query(client2, collection_name, filter=f"{default_primary_key_field_name} >= 0",
                        check_task=CheckTasks.check_query_results,
                        timezone="America/Los_Angeles",
                        check_items={exp_res: LA_rows,
                                        "pk_name": default_primary_key_field_name})[0]
        
        assert len(result_1) == len(result_2) == default_nb
        self.drop_collection(client, collection_name)



class TestMilvusClientTimestamptzInvalid(TestMilvusClientV2Base):

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_input_data_invalid_time_format(self):
        # BUG: https://github.com/milvus-io/milvus/issues/44537
        """
        target:  Milvus raise error when input data with invalid time format
        method:
            1. Create a collection
            2. Generate rows with invalid timestamptz and insert the rows
            3. Insert the rows
        expected: Step 3 should result fail
        """
        # step 1: create collection
        default_dim = 3
        client = self._client() 
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="AUTOINDEX")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)
        
        # step 2: generate rows with invalid timestamptz and insert the rows
        rows = [{default_primary_key_field_name: 0, default_vector_field_name: [1,2,3], default_timestamp_field_name: "invalid_time_format"},
                # April 31 does not exist
                {default_primary_key_field_name: 1, default_vector_field_name: [4,5,6], default_timestamp_field_name: "2025-04-31 00:00:00"},
                # 2025 is not a leap year
                {default_primary_key_field_name: 2, default_vector_field_name: [7,8,9], default_timestamp_field_name: "2025-02-29 00:00:00"},
                # UTC+24:00 is not a valid timezone
                {default_primary_key_field_name: 3, default_vector_field_name: [10,11,12], default_timestamp_field_name: "2025-01-01T00:00:00+24:00"}]
        
        # step 3: query the rows
        for row in rows:
            error = {ct.err_code: 1, ct.err_msg: f"got invalid timestamptz string: {row[default_timestamp_field_name]}"}
            self.insert(client, collection_name, row, 
                        check_task=CheckTasks.err_res, 
                        check_items=error)
        
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_wrong_index_type(self):
        """
        target:  Milvus raise error when input data with wrong index type
        method:
            1. Create a collection with wrong index type for timestamptz field
        expected: Step 1 should result fail
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True)
        index_params = self.prepare_index_params(client)[0] 
        index_params.add_index(default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(default_timestamp_field_name, index_type="INVERTED")
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong", index_params=index_params)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_wrong_default_value(self):
        """
        target:  Milvus raise error when input data with wrong default value
        method:
            1. Create a collection with wrong string default value for timestamptz field
            2. Create a collection with wrong int default value for timestamptz field
        expected: Step 1 and Step 2 should result fail
        """
        # step 1: create collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True, default_value="timestamp")

        error = {ct.err_code: 1100, ct.err_msg: "type (Timestamptz) of field (timestamp) is not equal to the type(DataType_VarChar) of default_value: invalid parameter"}
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong",
                               check_task=CheckTasks.err_res, check_items=error)
        
        # step 2: create collection 
        new_schema = self.create_schema(client, enable_dynamic_field=False)[0]
        new_schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        new_schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        new_schema.add_field(default_timestamp_field_name, DataType.TIMESTAMPTZ, nullable=True, default_value=10)

        error = {ct.err_code: 1100, ct.err_msg: "type (Timestamptz) of field (timestamp) is not equal to the type(DataType_VarChar) of default_value: invalid parameter"}
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong",
                               check_task=CheckTasks.err_res, check_items=error)
        
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="timesptamptz is not ready")
    def test_milvus_client_timestamptz_add_field_not_nullable(self):
        """
        target:  Milvus raise error when add non-nullable timestamptz field
        method:
            1. Create a collection with non-nullable timestamptz field
            2. Add non-nullable timestamptz field
        expected: Step 2 should result fail
        """
        # step 1: create collection 
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, default_dim, schema=schema, 
                               consistency_level="Strong")
        
        # step 2: add non-nullable timestamptz field
        error = {ct.err_code: 1100, ct.err_msg: f"added field must be nullable, please check it, field name = {default_timestamp_field_name}: invalid parameter"}
        self.add_collection_field(client, collection_name, field_name=default_timestamp_field_name, data_type=DataType.TIMESTAMPTZ,
                                  nullable=False, check_task=CheckTasks.err_res, check_items=error)
        
        self.drop_collection(client, collection_name)