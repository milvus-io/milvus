import pytest

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

default_dim = ct.default_dim


class TestPartitionKeyParams(TestMilvusClientV2Base):
    """Test case of partition key params"""

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("par_key_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_partition_key_on_field_schema(self, par_key_field):
        """
        Method：
        1. create a collection with partition key on
        2. verify insert, build, load and search successfully
        3. drop collection
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=True)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(
            schema,
            ct.default_int64_field_name,
            DataType.INT64,
            is_partition_key=(par_key_field == ct.default_int64_field_name),
        )
        self.add_field(
            schema,
            ct.default_string_field_name,
            DataType.VARCHAR,
            max_length=ct.default_length,
            is_partition_key=(par_key_field == ct.default_string_field_name),
        )
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, c_name, schema=schema)
        partitions = self.list_partitions(client, c_name)[0]
        assert len(partitions) == ct.default_partition_num

        # insert
        nb = 1000
        string_prefix = cf.gen_str_by_length(length=6)
        entities_per_parkey = 10
        for _ in range(entities_per_parkey):
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [
                {
                    ct.default_int64_field_name: i,
                    ct.default_string_field_name: string_prefix + str(i),
                    ct.default_float_vec_field_name: float_vec_values[i],
                }
                for i in range(nb)
            ]
            self.insert(client, c_name, data)

        # flush
        self.flush(client, c_name)
        # build index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=ct.default_float_vec_field_name,
            index_type="IVF_SQ8",
            metric_type="COSINE",
            params={"nlist": 128},
        )
        self.create_index(client, c_name, index_params)
        # load
        self.load_collection(client, c_name)
        # search
        nq = 10
        search_vectors = gen_vectors(nq, ct.default_dim)
        # search with mixed filtered
        res1 = self.search(
            client,
            c_name,
            data=search_vectors,
            anns_field=ct.default_float_vec_field_name,
            search_params=ct.default_search_params,
            limit=entities_per_parkey,
            filter=f'{ct.default_int64_field_name} in [1,3,5] && {ct.default_string_field_name} in ["{string_prefix}1","{string_prefix}3","{string_prefix}5"]',
            output_fields=[ct.default_int64_field_name, ct.default_string_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": entities_per_parkey},
        )[0]
        # search with partition key filter only or with non partition key
        res2 = self.search(
            client,
            c_name,
            data=search_vectors,
            anns_field=ct.default_float_vec_field_name,
            search_params=ct.default_search_params,
            limit=entities_per_parkey,
            filter=f"{ct.default_int64_field_name} in [1,3,5]",
            output_fields=[ct.default_int64_field_name, ct.default_string_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": entities_per_parkey},
        )[0]
        # search with partition key filter only or with non partition key
        res3 = self.search(
            client,
            c_name,
            data=search_vectors,
            anns_field=ct.default_float_vec_field_name,
            search_params=ct.default_search_params,
            limit=entities_per_parkey,
            filter=f'{ct.default_string_field_name} in ["{string_prefix}1","{string_prefix}3","{string_prefix}5"]',
            output_fields=[ct.default_int64_field_name, ct.default_string_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": entities_per_parkey},
        )[0]
        # assert the results persist
        for i in range(nq):
            assert res1[i].ids == res2[i].ids == res3[i].ids

        # search with 'or' to verify no partition key optimization local with or binary expr
        query_res1 = self.query(
            client,
            c_name,
            filter=f'{ct.default_string_field_name} == "{string_prefix}5" || {ct.default_int64_field_name} in [2,4,6]',
            output_fields=["count(*)"],
        )[0]
        query_res2 = self.query(
            client,
            c_name,
            filter=f'{ct.default_string_field_name} in ["{string_prefix}2","{string_prefix}4", "{string_prefix}6"] || {ct.default_int64_field_name}==5',
            output_fields=["count(*)"],
        )[0]
        query_res3 = self.query(
            client,
            c_name,
            filter=f'{ct.default_int64_field_name}==5 or {ct.default_string_field_name} in ["{string_prefix}2","{string_prefix}4", "{string_prefix}6"]',
            output_fields=["count(*)"],
        )[0]
        query_res4 = self.query(
            client,
            c_name,
            filter=f'{ct.default_int64_field_name} in [2,4,6] || {ct.default_string_field_name} == "{string_prefix}5"',
            output_fields=["count(*)"],
        )[0]
        # assert the results persist
        assert (
            query_res1[0].get("count(*)")
            == query_res2[0].get("count(*)")
            == query_res3[0].get("count(*)")
            == query_res4[0].get("count(*)")
            == 40
        )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("par_key_field", [ct.default_int64_field_name, ct.default_string_field_name])
    @pytest.mark.parametrize("index_on_par_key_field", [True, False])
    def test_partition_key_on_collection_schema(self, par_key_field, index_on_par_key_field):
        """
        Method：
        1. create a collection with partition key on collection schema with customized num_partitions
        2. verify insert, build, load and search successfully
        3. drop collection
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=False, partition_key_field=par_key_field)[0]
        self.add_field(schema, "pk", DataType.VARCHAR, max_length=ct.default_length, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, c_name, schema=schema, num_partitions=9)
        # verify partition num is 9 as specified, not default 16
        partitions = self.list_partitions(client, c_name)[0]
        assert len(partitions) == 9

        # insert
        nb = 1000
        string_prefix = cf.gen_str_by_length(length=6)
        entities_per_parkey = 20
        for n in range(entities_per_parkey):
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [
                {
                    "pk": str(n * nb + i),
                    ct.default_int64_field_name: i,
                    ct.default_string_field_name: string_prefix + str(i),
                    ct.default_float_vec_field_name: float_vec_values[i],
                }
                for i in range(nb)
            ]
            self.insert(client, c_name, data)

        # flush
        self.flush(client, c_name)
        # build index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE", params={}
        )
        if index_on_par_key_field:
            index_params.add_index(field_name=par_key_field)
        self.create_index(client, c_name, index_params)
        # load
        self.load_collection(client, c_name)
        # search
        nq = 10
        search_vectors = gen_vectors(nq, ct.default_dim)
        # search with mixed filtered
        self.search(
            client,
            c_name,
            data=search_vectors,
            anns_field=ct.default_float_vec_field_name,
            search_params=ct.default_search_params,
            limit=entities_per_parkey,
            filter=f'{ct.default_int64_field_name} in [1,3,5] && {ct.default_string_field_name} in ["{string_prefix}1","{string_prefix}3","{string_prefix}5"]',
            output_fields=[ct.default_int64_field_name, ct.default_string_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": entities_per_parkey},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_key_off_in_field_but_enable_in_schema(self):
        """
        Method:
        1. create a collection with partition key off in field but enabled in schema via partition_key_field
        2. verify the collection created successfully and partition key is enabled
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=True, partition_key_field=ct.default_int64_field_name)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64, is_partition_key=False)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)

        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, c_name, schema=schema, num_partitions=10)
        partitions = self.list_partitions(client, c_name)[0]
        assert len(partitions) == 10
        # verify partition key is enabled via describe_collection
        desc = self.describe_collection(client, c_name)[0]
        par_key_fields = [f for f in desc["fields"] if f.get("is_partition_key")]
        assert len(par_key_fields) == 1
        assert par_key_fields[0]["name"] == ct.default_int64_field_name


class TestPartitionKeyInvalidParams(TestMilvusClientV2Base):
    """Test case of partition key invalid params"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_max_partitions(self):
        """
        Method：
        1. create a collection with max partitions
        2. insert
        3. drop collection
        4. create a collection with max partitions + 1
        5. verify the error raised
        """
        max_partition = ct.max_partition_num
        client = self._client()
        schema = self.create_schema(client, auto_id=True)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(
            schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length, is_partition_key=True
        )
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, c_name, schema=schema, num_partitions=max_partition)
        partitions = self.list_partitions(client, c_name)[0]
        assert len(partitions) == max_partition

        # insert
        nb = 100
        string_prefix = cf.gen_str_by_length(length=6)
        for _ in range(5):
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [
                {
                    ct.default_int64_field_name: i,
                    ct.default_string_field_name: string_prefix + str(i),
                    ct.default_float_vec_field_name: float_vec_values[i],
                }
                for i in range(nb)
            ]
            self.insert(client, c_name, data)

        # drop collection
        self.drop_collection(client, c_name)

        # create a collection with max partitions + 1
        num_partitions = max_partition + 1
        err_msg = f"partition number ({num_partitions}) exceeds max configuration ({max_partition})"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            num_partitions=num_partitions,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_min_partitions(self):
        """
        Method：
        1. create a collection with min partitions
        2. insert
        3. drop collection
        4. create a collection with min partitions - 1
        5. verify the error raised
        """
        min_partition = 1
        client = self._client()
        schema = self.create_schema(client, auto_id=False)[0]
        self.add_field(schema, "pk", DataType.VARCHAR, max_length=ct.default_length, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64, is_partition_key=True)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, c_name, schema=schema, num_partitions=min_partition)
        partitions = self.list_partitions(client, c_name)[0]
        assert len(partitions) == min_partition

        # insert
        nb = 100
        string_prefix = cf.gen_str_by_length(length=6)
        for n in range(5):
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [
                {
                    "pk": str(n * nb + i),
                    ct.default_int64_field_name: i,
                    ct.default_string_field_name: string_prefix + str(i),
                    ct.default_float_vec_field_name: float_vec_values[i],
                }
                for i in range(nb)
            ]
            self.insert(client, c_name, data)
            self.flush(client, c_name)

        # drop collection
        self.drop_collection(client, c_name)

        # create a collection with min partitions - 1
        err_msg = "The specified num_partitions should be greater than or equal to 1"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            num_partitions=min_partition - 1,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )
        self.create_collection(
            client,
            c_name,
            schema=schema,
            num_partitions=min_partition - 3,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("is_par_key", [None, "", "invalid", 0.1, [], {}, ()])
    def test_invalid_partition_key_values(self, is_par_key):
        """
        Method：
        1. create a schema and add field with invalid is_partition_key values
        2. verify the error raised
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=True)[0]
        err_msg = "Param is_partition_key must be bool type"
        self.add_field(
            schema,
            ct.default_int64_field_name,
            DataType.INT64,
            is_partition_key=is_par_key,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("num_partitions", [True, False, "", "invalid", 0.1, [], {}, ()])
    def test_invalid_partitions_values(self, num_partitions):
        """
        Method：
        1. create a collection with invalid num_partitions
        2. verify the error raised
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=True)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64, is_partition_key=True)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "invalid num_partitions type"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            num_partitions=num_partitions,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_key_on_multi_fields(self):
        """
        Method：
        1. create a collection with partition key on multi fields
        2. verify the error raised
        """
        client = self._client()

        # sub-case 1: both defined in field schema via add_field
        schema = self.create_schema(client, auto_id=True)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64, is_partition_key=True)
        self.add_field(
            schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length, is_partition_key=True
        )
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "Expected only one partition key field"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # sub-case 2: partition_key_field passed as list in create_schema
        err_msg = "Param partition_key_field must be str type"
        self.create_schema(
            client,
            auto_id=True,
            partition_key_field=[ct.default_int64_field_name, ct.default_string_field_name],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # sub-case 3: one defined in field schema, one defined in create_schema
        schema = self.create_schema(client, auto_id=True, partition_key_field=ct.default_string_field_name)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64, is_partition_key=True)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "Expected only one partition key field"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("is_int64_primary", [True, False])
    def test_partition_key_on_primary_key(self, is_int64_primary):
        """
        Method：
        1. create a collection with partition key on primary key
        2. verify the error raised
        """
        client = self._client()

        # sub-case 1: partition key set on primary field via add_field
        schema = self.create_schema(client, auto_id=False)[0]
        if is_int64_primary:
            self.add_field(schema, "pk", DataType.INT64, is_primary=True, is_partition_key=True)
        else:
            self.add_field(
                schema, "pk", DataType.VARCHAR, max_length=ct.default_length, is_primary=True, is_partition_key=True
            )
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "the partition key field must not be primary field"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # sub-case 2: partition key set on primary field via create_schema partition_key_field
        schema = self.create_schema(client, auto_id=False, partition_key_field="pk")[0]
        if is_int64_primary:
            self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        else:
            self.add_field(schema, "pk", DataType.VARCHAR, max_length=ct.default_length, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "the partition key field must not be primary field"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_key_on_and_off(self):
        """
        Method：
        1. create a schema with one field partition key on via add_field and another via create_schema
        2. verify the error raised
        """
        client = self._client()

        # sub-case 1: int64 field is_partition_key=True, schema partition_key_field=vector field
        schema = self.create_schema(client, auto_id=True, partition_key_field=ct.default_float_vec_field_name)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64, is_partition_key=True)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "Expected only one partition key field"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # sub-case 2: string1 is_partition_key=True, schema partition_key_field=string2
        schema = self.create_schema(client, auto_id=True, partition_key_field="string2")[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, "string1", DataType.VARCHAR, max_length=ct.default_length, is_partition_key=True)
        self.add_field(schema, "string2", DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "Expected only one partition key field"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "field_type",
        [
            DataType.FLOAT_VECTOR,
            DataType.BINARY_VECTOR,
            DataType.FLOAT,
            DataType.DOUBLE,
            DataType.BOOL,
            DataType.INT8,
            DataType.INT16,
            DataType.INT32,
            DataType.JSON,
        ],
    )
    def test_partition_key_on_invalid_type_fields(self, field_type):
        """
        Method：
        1. create a collection with partition key on invalid type fields
        2. verify the error raised
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=True)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, "int8", DataType.INT8, is_partition_key=(field_type == DataType.INT8))
        self.add_field(schema, "int16", DataType.INT16, is_partition_key=(field_type == DataType.INT16))
        self.add_field(schema, "int32", DataType.INT32, is_partition_key=(field_type == DataType.INT32))
        self.add_field(schema, "bool", DataType.BOOL, is_partition_key=(field_type == DataType.BOOL))
        self.add_field(schema, "float", DataType.FLOAT, is_partition_key=(field_type == DataType.FLOAT))
        self.add_field(schema, "double", DataType.DOUBLE, is_partition_key=(field_type == DataType.DOUBLE))
        self.add_field(schema, "json_field", DataType.JSON, is_partition_key=(field_type == DataType.JSON))
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        if field_type == DataType.BINARY_VECTOR:
            self.add_field(
                schema, ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim, is_partition_key=True
            )
        else:
            self.add_field(
                schema,
                ct.default_float_vec_field_name,
                DataType.FLOAT_VECTOR,
                dim=default_dim,
                is_partition_key=(field_type == DataType.FLOAT_VECTOR),
            )
        err_msg = "Partition key field type must be DataType.INT64 or DataType.VARCHAR"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_key_on_not_existed_fields(self):
        """
        Method：
        1. create a collection with partition key on not existed fields
        2. verify the error raised
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=True, partition_key_field="non_existing_field")[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "the specified partition key field {non_existing_field} not exist"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_key_on_empty_and_num_partitions_set(self):
        """
        Method：
        1. create a collection with partition key on empty and num_partitions set
        2. verify the error raised
        """
        client = self._client()

        # sub-case 1: partition_key_field="" → SDK error
        schema = self.create_schema(client, auto_id=True, partition_key_field="")[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "the specified partition key field {} not exist"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # sub-case 2: no partition key but num_partitions set → server error
        schema = self.create_schema(client, auto_id=True)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        err_msg = "num_partitions should only be specified with partition key field enabled"
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            c_name,
            schema=schema,
            num_partitions=200,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )


class TestPartitionKeyInsertInvalid(TestMilvusClientV2Base):
    """Test case of partition key insert invalid data"""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_data", [99, True, None, [], {}, ()])
    def test_partition_key_insert_invalid_data(self, invalid_data):
        """
        Method:
        1. create a collection with partition key on varchar field
        2. insert entities with invalid partition key value
        3. verify the error raised
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=False, partition_key_field=ct.default_string_field_name)[0]
        self.add_field(schema, "pk", DataType.VARCHAR, max_length=ct.default_length, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, c_name, schema=schema)

        # insert with invalid partition key value at index 1
        nb = 10
        string_prefix = cf.gen_str_by_length(length=6)
        float_vec_values = gen_vectors(nb, ct.default_dim)
        data = [
            {
                "pk": str(i),
                ct.default_int64_field_name: i,
                ct.default_string_field_name: string_prefix + str(i),
                ct.default_float_vec_field_name: float_vec_values[i],
            }
            for i in range(nb)
        ]
        data[1][ct.default_string_field_name] = invalid_data  # inject invalid data

        if invalid_data is None:
            # None is skipped in row-based insert, causing row count mismatch
            err_msg = "the num_rows"
            err_code = 1100
        else:
            # non-string types trigger DataNotMatchException in row-based insert
            err_msg = "The Input data type is inconsistent with defined schema"
            err_code = 1
        self.insert(
            client, c_name, data, check_task=CheckTasks.err_res, check_items={"err_code": err_code, "err_msg": err_msg}
        )


class TestPartitionApiForbidden(TestMilvusClientV2Base):
    """Test case of partition api forbidden when partition key is on"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_partition(self):
        """
        Method:
        1. return error if create partition when partition key is on
        2. return error if insert with partition_name when partition key is on
        3. return error if drop partition when partition key is on
        4. return success if list/has partition when partition key is on
        5. return error if load partition when partition key is on
        6. return error if release partition when partition key is on
        7. return error if search/query/delete with partition_names when partition key is on
        Expected: raise exception for partition-specific operations
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=True)[0]
        self.add_field(schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(schema, ct.default_int64_field_name, DataType.INT64)
        self.add_field(
            schema, ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length, is_partition_key=True
        )
        self.add_field(schema, ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        c_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, c_name, schema=schema)

        # create partition → error
        err_msg = "disable create partition if partition key mode is used"
        partition_name = cf.gen_unique_str("partition")
        self.create_partition(
            client,
            c_name,
            partition_name,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # list/has partition → allowed
        partitions = self.list_partitions(client, c_name)[0]
        assert len(partitions) > 0
        assert self.has_partition(client, c_name, partitions[0])[0]

        # insert without partition_name → allowed
        nb = 100
        string_prefix = cf.gen_str_by_length(length=6)
        entities_per_parkey = 10
        for _ in range(entities_per_parkey):
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [
                {
                    ct.default_int64_field_name: i,
                    ct.default_string_field_name: string_prefix + str(i),
                    ct.default_float_vec_field_name: float_vec_values[i],
                }
                for i in range(nb)
            ]
            self.insert(client, c_name, data)

        # insert with partition_name → error
        err_msg = "not support manually specifying the partition names if partition key mode is used"
        self.insert(
            client,
            c_name,
            data,
            partition_name=partitions[0],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # load partitions → error
        err_msg = "disable load partitions if partition key mode is used"
        self.load_partitions(
            client,
            c_name,
            [partitions[0]],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # flush + index + load collection → allowed
        self.flush(client, c_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=ct.default_float_vec_field_name,
            index_type="IVF_SQ8",
            metric_type="COSINE",
            params={"nlist": 128},
        )
        self.create_index(client, c_name, index_params)
        self.load_collection(client, c_name)

        # search without partition_names → allowed
        nq = 10
        search_vectors = gen_vectors(nq, ct.default_dim)
        res1 = self.search(
            client,
            c_name,
            data=search_vectors,
            anns_field=ct.default_float_vec_field_name,
            search_params=ct.default_search_params,
            limit=entities_per_parkey,
            filter=f'{ct.default_int64_field_name} in [1,3,5] && {ct.default_string_field_name} in ["{string_prefix}1","{string_prefix}3","{string_prefix}5"]',
            output_fields=[ct.default_int64_field_name, ct.default_string_field_name],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": ct.default_limit},
        )[0]
        pks = res1[0].ids[:3]

        # search with partition_names → error
        err_msg = "not support manually specifying the partition names if partition key mode is used"
        self.search(
            client,
            c_name,
            data=search_vectors,
            anns_field=ct.default_float_vec_field_name,
            search_params=ct.default_search_params,
            limit=entities_per_parkey,
            filter=f"{ct.default_int64_field_name} in [1,3,5]",
            output_fields=[ct.default_int64_field_name, ct.default_string_field_name],
            partition_names=[partitions[0]],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # get_load_state with partition → allowed (v1: loading_progress/wait_for_loading_complete)
        load_state = self.get_load_state(client, c_name, partition_name=partitions[0])[0]
        assert "state" in load_state

        # get_partition_stats → allowed
        stats = self.get_partition_stats(client, c_name, partitions[0])[0]
        assert "row_count" in stats

        # flush → allowed (v1: partition_w.flush())
        self.flush(client, c_name)

        # delete with partition_name → error
        err_msg = "not support manually specifying the partition names if partition key mode is used"
        self.delete(
            client,
            c_name,
            filter=f"pk in {pks}",
            partition_name=partitions[0],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # query with partition_names → error
        self.query(
            client,
            c_name,
            filter=f"pk in {pks}",
            partition_names=[partitions[0]],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # release partitions → error
        err_msg = "disable release partitions if partition key mode is used"
        self.release_partitions(
            client,
            c_name,
            [partitions[0]],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )

        # drop partition → error
        err_msg = "disable drop partition if partition key mode is used"
        self.drop_partition(
            client,
            c_name,
            partitions[0],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2, "err_msg": err_msg},
        )
