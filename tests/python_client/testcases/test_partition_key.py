import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *


class TestPartitionKeyParams(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("par_key_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_partition_key_on_field_schema(self, par_key_field):
        """
        Method：
        1. create a collection with partition key on
        2. verify insert, build, load and search successfully
        3. drop collection
        """
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field(is_partition_key=(par_key_field == ct.default_int64_field_name))
        string_field = cf.gen_string_field(is_partition_key=(par_key_field == ct.default_string_field_name))
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field], auto_id=True)
        c_name = cf.gen_unique_str("par_key")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        assert len(collection_w.partitions) == ct.default_partition_num

        # insert
        nb = 1000
        string_prefix = cf.gen_str_by_length(length=6)
        entities_per_parkey = 10
        for _ in range(entities_per_parkey):
            int64_values = [i for i in range(0, nb)]
            string_values = [string_prefix + str(i) for i in range(0, nb)]
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [int64_values, string_values, float_vec_values]
            collection_w.insert(data)

        # flush
        collection_w.flush()
        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        # load
        collection_w.load()
        # search
        nq = 10
        search_vectors = gen_vectors(nq, ct.default_dim)
        # search with mixed filtered
        res1 = collection_w.search(data=search_vectors, anns_field=vector_field.name,
                                   param=ct.default_search_params, limit=entities_per_parkey,
                                   expr=f'{int64_field.name} in [1,3,5] && {string_field.name} in ["{string_prefix}1","{string_prefix}3","{string_prefix}5"]',
                                   output_fields=[int64_field.name, string_field.name],
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq, "limit": entities_per_parkey})[0]
        # search with partition key filter only or with non partition key
        res2 = collection_w.search(data=search_vectors, anns_field=vector_field.name,
                                   param=ct.default_search_params, limit=entities_per_parkey,
                                   expr=f'{int64_field.name} in [1,3,5]',
                                   output_fields=[int64_field.name, string_field.name],
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq, "limit": entities_per_parkey})[0]
        # search with partition key filter only or with non partition key
        res3 = collection_w.search(data=search_vectors, anns_field=vector_field.name,
                                   param=ct.default_search_params, limit=entities_per_parkey,
                                   expr=f'{string_field.name} in ["{string_prefix}1","{string_prefix}3","{string_prefix}5"]',
                                   output_fields=[int64_field.name, string_field.name],
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq, "limit": entities_per_parkey})[0]
        # assert the results persist
        for i in range(nq):
            assert res1[i].ids == res2[i].ids == res3[i].ids

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
        self._connect()
        pk_field = cf.gen_string_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                          auto_id=False, partition_key_field=par_key_field)
        c_name = cf.gen_unique_str("par_key")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema, num_partitions=9)

        # insert
        nb = 1000
        string_prefix = cf.gen_str_by_length(length=6)
        entities_per_parkey = 20
        for n in range(entities_per_parkey):
            pk_values = [str(i) for i in range(n * nb, (n + 1) * nb)]
            int64_values = [i for i in range(0, nb)]
            string_values = [string_prefix + str(i) for i in range(0, nb)]
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [pk_values, int64_values, string_values, float_vec_values]
            collection_w.insert(data)

        # flush
        collection_w.flush()
        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        if index_on_par_key_field:
            collection_w.create_index(field_name=par_key_field, index_params={})
        # load
        collection_w.load()
        # search
        nq = 10
        search_vectors = gen_vectors(nq, ct.default_dim)
        # search with mixed filtered
        res1 = collection_w.search(data=search_vectors, anns_field=vector_field.name,
                                   param=ct.default_search_params, limit=entities_per_parkey,
                                   expr=f'{int64_field.name} in [1,3,5] && {string_field.name} in ["{string_prefix}1","{string_prefix}3","{string_prefix}5"]',
                                   output_fields=[int64_field.name, string_field.name],
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq, "limit": entities_per_parkey})[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_key_off_in_field_but_enable_in_schema(self):
        """
        Method:
        1. create a collection with partition key off in field schema but enable in collection schema
        2. verify the collection created successfully
        """
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field(is_partition_key=False)
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                          partition_key_field=int64_field.name, auto_id=True)

        err_msg = "fail to create collection"
        c_name = cf.gen_unique_str("par_key")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema, num_partitions=10)
        assert len(collection_w.partitions) == 10

    @pytest.mark.skip("need more investigation")
    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_key_bulk_insert(self):
        """
        Method:
        1. create a collection with partition key on
        2. bulk insert data
        3. verify the data bulk inserted and be searched successfully
        """
        pass


class TestPartitionKeyInvalidParams(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_max_partitions(self):
        """
        Method：
        1. create a collection with max partitions
        2. insert, build, load and search
        3. drop collection
        4. create a collection with max partitions + 1
        5. verify the error raised
        """
        max_partition = 1024
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field(is_partition_key=True)
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                          auto_id=True)
        c_name = cf.gen_unique_str("par_key")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema, num_partitions=max_partition)
        assert len(collection_w.partitions) == max_partition

        # insert
        nb = 100
        string_prefix = cf.gen_str_by_length(length=6)
        for _ in range(5):
            int64_values = [i for i in range(0, nb)]
            string_values = [string_prefix + str(i) for i in range(0, nb)]
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [int64_values, string_values, float_vec_values]
            collection_w.insert(data)

        # drop collection
        collection_w.drop()

        # create a collection with min partitions - 1
        num_partitions = max_partition + 1
        err_msg = f"partition number ({num_partitions}) exceeds max configuration ({max_partition})"
        c_name = cf.gen_unique_str("par_key")
        self.init_collection_wrap(name=c_name, schema=schema, num_partitions=num_partitions,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 1100, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_min_partitions(self):
        """
        Method：
        1. create a collection with min partitions
        2. insert, build, load and search
        3. drop collection
        4. create a collection with min partitions - 1
        5. verify the error raised
        """
        min_partition = 1
        self._connect()
        pk_field = cf.gen_string_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                          partition_key_field=int64_field.name)
        c_name = cf.gen_unique_str("par_key")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema, num_partitions=min_partition)
        assert len(collection_w.partitions) == min_partition

        # insert
        nb = 100
        string_prefix = cf.gen_str_by_length(length=6)
        for _ in range(5):
            pk_values = [str(i) for i in range(0, nb)]
            int64_values = [i for i in range(0, nb)]
            string_values = [string_prefix + str(i) for i in range(0, nb)]
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [pk_values, int64_values, string_values, float_vec_values]
            collection_w.insert(data)
            collection_w.flush()

        # drop collection
        collection_w.drop()

        # create a collection with min partitions - 1
        err_msg = "The specified num_partitions should be greater than or equal to 1"
        c_name = cf.gen_unique_str("par_key")
        self.init_collection_wrap(name=c_name, schema=schema, num_partitions=min_partition - 1,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 2, "err_msg": err_msg})
        self.init_collection_wrap(name=c_name, schema=schema, num_partitions=min_partition - 3,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 2, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("is_par_key", [None, "", "invalid", 0.1, [], {}, ()])
    def test_invalid_partition_key_values(self, is_par_key):
        """
        Method：
        1. create a collection with invalid partition keys
        2. verify the error raised
        """
        self._connect()
        err_msg = "Param is_partition_key must be bool type"
        cf.gen_int64_field(is_partition_key=is_par_key,
                           check_task=CheckTasks.err_res,
                           check_items={"err_code": 2, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("num_partitions", [True, False, "", "invalid", 0.1, [], {}, ()])
    def test_invalid_partitions_values(self, num_partitions):
        """
        Method：
        1. create a collection with invalid num_partitions
        2. verify the error raised
        """
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field(is_partition_key=True)
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field])

        err_msg = "invalid num_partitions type"
        c_name = cf.gen_unique_str("par_key")
        self.init_collection_wrap(name=c_name, schema=schema, num_partitions=num_partitions,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 2, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_key_on_multi_fields(self):
        """
        Method：
        1. create a collection with partition key on multi fields
        2. verify the error raised
        """
        self._connect()
        # both defined in field schema
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field(is_partition_key=True)
        string_field = cf.gen_string_field(is_partition_key=True)
        vector_field = cf.gen_float_vec_field()
        err_msg = "Expected only one partition key field"
        cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                 auto_id=True,
                                 check_task=CheckTasks.err_res,
                                 check_items={"err_code": 2, "err_msg": err_msg})

        # both defined in collection schema
        err_msg = "Param partition_key_field must be str type"
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field()
        cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                 partition_key_field=[int64_field.name, string_field.name],
                                 auto_id=True,
                                 check_task=CheckTasks.err_res,
                                 check_items={"err_code": 2, "err_msg": err_msg})

        # one defined in field schema, one defined in collection schema
        err_msg = "Expected only one partition key field"
        int64_field = cf.gen_int64_field(is_partition_key=True)
        string_field = cf.gen_string_field()
        cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                 partition_key_field=string_field.name,
                                 auto_id=True,
                                 check_task=CheckTasks.err_res,
                                 check_items={"err_code": 2, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("is_int64_primary", [True, False])
    def test_partition_key_on_primary_key(self, is_int64_primary):
        """
        Method：
        1. create a collection with partition key on primary key
        2. verify the error raised
        """
        self._connect()
        if is_int64_primary:
            pk_field = cf.gen_int64_field(name='pk', is_primary=True, is_partition_key=True)
        else:
            pk_field = cf.gen_string_field(name='pk', is_primary=True, is_partition_key=True)
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                          auto_id=False)

        err_msg = "the partition key field must not be primary field"
        c_name = cf.gen_unique_str("par_key")
        self.init_collection_wrap(name=c_name, schema=schema,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 2, "err_msg": err_msg})

        # if settings on collection schema
        if is_int64_primary:
            pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        else:
            pk_field = cf.gen_string_field(name='pk', is_primary=True)
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                          partition_key_field=pk_field.name,
                                          auto_id=False)

        err_msg = "the partition key field must not be primary field"
        c_name = cf.gen_unique_str("par_key")
        self.init_collection_wrap(name=c_name, schema=schema,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 2, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_key_on_and_off(self):
        """
        Method：
        1. create a collection with one field partition key on and the other field partition key on in schema
        2. verify the error raised
        """
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field(is_partition_key=True)
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        err_msg = "Expected only one partition key field"
        cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                 partition_key_field=vector_field.name,
                                 auto_id=True,
                                 check_task=CheckTasks.err_res,
                                 check_items={"err_code": 2, "err_msg": err_msg})

        # if two fields with same type
        string_field = cf.gen_string_field(name="string1", is_partition_key=True)
        string_field2 = cf.gen_string_field(name="string2")
        err_msg = "Expected only one partition key field"
        cf.gen_collection_schema(fields=[pk_field, string_field, string_field2, vector_field],
                                 partition_key_field=string_field2.name,
                                 auto_id=True,
                                 check_task=CheckTasks.err_res,
                                 check_items={"err_code": 2, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("field_type", [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR, DataType.FLOAT,
                                            DataType.DOUBLE, DataType.BOOL,  DataType.INT8,
                                            DataType.INT16, DataType.INT32, DataType.JSON])
    def test_partition_key_on_invalid_type_fields(self, field_type):
        """
        Method：
        1. create a collection with partition key on invalid type fields
        2. verify the error raised
        """
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int8_field = cf.gen_int8_field(is_partition_key=(field_type == DataType.INT8))
        int16_field = cf.gen_int16_field(is_partition_key=(field_type == DataType.INT16))
        int32_field = cf.gen_int32_field(is_partition_key=(field_type == DataType.INT32))
        bool_field = cf.gen_bool_field(is_partition_key=(field_type == DataType.BOOL))
        float_field = cf.gen_float_field(is_partition_key=(field_type == DataType.FLOAT))
        double_field = cf.gen_double_field(is_partition_key=(field_type == DataType.DOUBLE))
        json_field = cf.gen_json_field(is_partition_key=(field_type == DataType.JSON))
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field(is_partition_key=(field_type == DataType.FLOAT_VECTOR))
        if field_type == DataType.BINARY_VECTOR:
            vector_field = cf.gen_binary_vec_field(is_partition_key=(field_type == DataType.BINARY_VECTOR))

        err_msg = "Partition key field type must be DataType.INT64 or DataType.VARCHAR"
        cf.gen_collection_schema(fields=[pk_field, int8_field, int16_field, int32_field,
                                         bool_field, float_field, double_field, json_field,
                                         int64_field, string_field, vector_field],
                                 auto_id=True,
                                 check_task=CheckTasks.err_res,
                                 check_items={"err_code": 2, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_key_on_not_existed_fields(self):
        """
        Method：
        1. create a collection with partition key on not existed fields
        2. verify the error raised
        """
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        err_msg = "the specified partition key field {non_existing_field} not exist"
        cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                 partition_key_field="non_existing_field",
                                 auto_id=True,
                                 check_task=CheckTasks.err_res,
                                 check_items={"err_code": 2, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_key_on_empty_and_num_partitions_set(self):
        """
        Method：
        1. create a collection with partition key on empty and num_partitions set
        2. verify the error raised
        """
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        err_msg = "the specified partition key field {} not exist"
        cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                 partition_key_field="", auto_id=True,
                                 check_task=CheckTasks.err_res,
                                 check_items={"err_code": 2, "err_msg": err_msg})

        schema = cf.gen_default_collection_schema()
        err_msg = "num_partitions should only be specified with partition key field enabled"
        c_name = cf.gen_unique_str("par_key")
        self.init_collection_wrap(name=c_name, schema=schema, num_partitions=200,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 2, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_data", [99, True, None, [], {}, ()])
    def test_partition_key_insert_invalid_data(self, invalid_data):
        """
        Method:
        1. create a collection with partition key on int64 field
        2. insert entities with invalid partition key
        3. verify the error raised
        """
        self._connect()
        pk_field = cf.gen_string_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                          partition_key_field=string_field.name, auto_id=False)

        c_name = cf.gen_unique_str("par_key")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # insert
        nb = 10
        string_prefix = cf.gen_str_by_length(length=6)
        pk_values = [str(i) for i in range(0, nb)]
        int64_values = [i for i in range(0, nb)]
        string_values = [string_prefix + str(i) for i in range(0, nb)]
        string_values[1] = invalid_data     # inject invalid data
        float_vec_values = gen_vectors(nb, ct.default_dim)
        data = [pk_values, int64_values, string_values, float_vec_values]

        err_msg = "field (varchar) expect string input"
        collection_w.insert(data, check_task=CheckTasks.err_res, check_items={"err_code": 2, "err_msg": err_msg})


class TestPartitionApiForbidden(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_create_partition(self):
        """
        Method:
        1. return error if create partition when partition key is on
        2. return error if insert partition when partition key is on
        3. return error if drop partition when partition key is on
        4. return success if show partition when partition key is on
        5. return error if load partition when partition key is on
        6. return error if release partition when partition key is on
        Expected: raise exception
        """
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field(is_partition_key=True)
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field], auto_id=True)
        c_name = cf.gen_unique_str("par_key")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # create partition
        err_msg = "disable create partition if partition key mode is used"
        partition_name = cf.gen_unique_str("partition")
        collection_w.create_partition(partition_name,
                                      check_task=CheckTasks.err_res,
                                      check_items={"err_code": 2, "err_msg": err_msg})
        self.init_partition_wrap(collection_w, partition_name,
                                 check_task=CheckTasks.err_res,
                                 check_items={"err_code": 2, "err_msg": err_msg})

        # get partition is allowed
        partitions = collection_w.partitions
        collection_w.partition(partitions[0].name)
        partition_w = self.init_partition_wrap(collection_w, partitions[0].name)
        assert partition_w.name == partitions[0].name
        # has partition is allowed
        assert collection_w.has_partition(partitions[0].name)
        assert self.utility_wrap.has_partition(collection_w.name, partitions[0].name)

        # insert
        nb = 100
        string_prefix = cf.gen_str_by_length(length=6)
        entities_per_parkey = 10
        for _ in range(entities_per_parkey):
            int64_values = [i for i in range(0, nb)]
            string_values = [string_prefix + str(i) for i in range(0, nb)]
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [int64_values, string_values, float_vec_values]
            collection_w.insert(data)

        err_msg = "not support manually specifying the partition names if partition key mode is used"
        partition_w.insert(data, check_task=CheckTasks.err_res,
                           check_items={"err_code": 2, "err_msg": err_msg})
        collection_w.insert(data, partition_name=partitions[0].name,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 2, "err_msg": err_msg})

        err_msg = "disable load partitions if partition key mode is used"
        partition_w.load(check_task=CheckTasks.err_res,
                         check_items={"err_code": 2, "err_msg": err_msg})
        collection_w.load(partition_names=[partitions[0].name],
                          check_task=CheckTasks.err_res,
                          check_items={"err_code": 2, "err_msg": err_msg})

        # flush
        collection_w.flush()

        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        # load
        collection_w.load()
        # search
        nq = 10
        search_vectors = gen_vectors(nq, ct.default_dim)
        # search with mixed filtered
        res1 = collection_w.search(data=search_vectors, anns_field=vector_field.name,
                                   param=ct.default_search_params, limit=entities_per_parkey,
                                   expr=f'{int64_field.name} in [1,3,5] && {string_field.name} in ["{string_prefix}1","{string_prefix}3","{string_prefix}5"]',
                                   output_fields=[int64_field.name, string_field.name],
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq, "limit": ct.default_limit})[0]
        pks = res1[0].ids[:3]
        err_msg = "not support manually specifying the partition names if partition key mode is used"
        collection_w.search(data=search_vectors, anns_field=vector_field.name, partition_names=[partitions[0].name],
                            param=ct.default_search_params, limit=entities_per_parkey,
                            expr=f'{int64_field.name} in [1,3,5]',
                            output_fields=[int64_field.name, string_field.name],
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": nq, "err_msg": err_msg})
        partition_w.search(data=search_vectors, anns_field=vector_field.name,
                           params=ct.default_search_params, limit=entities_per_parkey,
                           expr=f'{string_field.name} in ["{string_prefix}1","{string_prefix}3","{string_prefix}5"]',
                           output_fields=[int64_field.name, string_field.name],
                           check_task=CheckTasks.err_res,
                           check_items={"err_code": nq, "err_msg": err_msg})

        # partition loading progress is allowed
        self.utility_wrap.loading_progress(collection_name=collection_w.name)
        self.utility_wrap.loading_progress(collection_name=collection_w.name,
                                           partition_names=[partitions[0].name])

        # partition wait for loading complete is allowed
        self.utility_wrap.wait_for_loading_complete(collection_name=collection_w.name)
        self.utility_wrap.wait_for_loading_complete(collection_name=collection_w.name,
                                                    partition_names=[partitions[0].name])
        # partition flush is allowed: #24165
        partition_w.flush()

        # partition delete is not allowed
        partition_w.delete(expr=f'{pk_field.name} in {pks}',
                           check_task=CheckTasks.err_res,
                           check_items={"err_code": 2, "err_msg": err_msg})
        collection_w.delete(expr=f'{pk_field.name} in {pks}', partition_name=partitions[0].name,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 2, "err_msg": err_msg})
        # partition query is not allowed
        partition_w.query(expr=f'{pk_field.name} in {pks}',
                          check_task=CheckTasks.err_res,
                          check_items={"err_code": 2, "err_msg": err_msg})
        collection_w.query(expr=f'{pk_field.name} in {pks}', partition_names=[partitions[0].name],
                           check_task=CheckTasks.err_res,
                           check_items={"err_code": 2, "err_msg": err_msg})
        # partition upsert is not allowed
        # self.partition_wrap.upsert(data=data, check_task=CheckTasks.err_res,
        #                            check_items={"err_code": 2, "err_msg": err_msg})
        # self.collection_wrap.upsert(data=data, partition_name=partitions[0].name,
        #                             chek_task=CheckTasks.err_res, check_items={"err_code": 2, "err_msg": err_msg})
        # partition release
        err_msg = "disable release partitions if partition key mode is used"
        partition_w.release(check_task=CheckTasks.err_res, check_items={"err_code": 2, "err_msg": err_msg})
        # partition drop
        err_msg = "disable drop partition if partition key mode is used"
        partition_w.drop(check_task=CheckTasks.err_res, check_items={"err_code": 2, "err_msg": err_msg})

        # # partition bulk insert
        # self.utility_wrap.do_bulk_insert(collection_w.name, files, partition_names=[partitions[0].name],
        #                                  check_task=CheckTasks.err_res,
        #                                  check_items={"err_code": 2, "err_msg": err_msg})
