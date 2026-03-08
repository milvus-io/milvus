import pytest
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

@pytest.mark.skip(reason="field partial load behavior changing @congqixia")
class TestFieldPartialLoad(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    def test_field_partial_load_default(self):
        """
        target: test field partial load
        method:
        1. create a collection with fields
        2. index/not index fields to be loaded; index/not index fields to be skipped
        3. load a part of the fields
        expected:
        1.  verify the collection loaded successfully
        2.  verify the loaded fields can be searched in expr and output_fields
        3.  verify the skipped fields not loaded, and cannot search with them in expr or output_fields
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 128
        nb = 2000
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        load_int64_field = cf.gen_int64_field(name="int64_load")
        not_load_int64_field = cf.gen_int64_field(name="int64_not_load")
        load_string_field = cf.gen_string_field(name="string_load")
        not_load_string_field = cf.gen_string_field(name="string_not_load")
        vector_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[pk_field, load_int64_field, not_load_int64_field,
                                                  load_string_field, not_load_string_field, vector_field],
                                          auto_id=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        int_values = [i for i in range(nb)]
        string_values = [str(i) for i in range(nb)]
        float_vec_values = gen_vectors(nb, dim)
        collection_w.insert([int_values, int_values, string_values, string_values, float_vec_values])

        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        collection_w.load(load_fields=[pk_field.name, vector_field.name, load_string_field.name, load_int64_field.name],
                          replica_number=1)
        # search
        search_params = ct.default_search_params
        nq = 2
        search_vectors = float_vec_values[0:nq]
        res, _ = collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                                     limit=100, output_fields=["*"])
        assert pk_field.name in res[0][0].fields.keys() \
               and vector_field.name in res[0][0].fields.keys() \
               and load_string_field.name in res[0][0].fields.keys() \
               and load_int64_field.name in res[0][0].fields.keys() \
               and not_load_string_field.name not in res[0][0].fields.keys() \
               and not_load_int64_field.name not in res[0][0].fields.keys()

        # release and reload with some other fields
        collection_w.release()
        collection_w.load(load_fields=[pk_field.name, vector_field.name,
                                        not_load_string_field.name, not_load_int64_field.name])
        res, _ = collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                                     limit=100, output_fields=["*"])
        assert pk_field.name in res[0][0].fields.keys() \
               and vector_field.name in res[0][0].fields.keys() \
               and load_string_field.name not in res[0][0].fields.keys() \
               and load_int64_field.name not in res[0][0].fields.keys() \
               and not_load_string_field.name in res[0][0].fields.keys() \
               and not_load_int64_field.name in res[0][0].fields.keys()

    @pytest.mark.tags(CaseLabel.L1)
    def test_skip_load_dynamic_field(self):
        """
        target: test skip load dynamic field
        method:
        1. create a collection with dynamic field
        2. load
        3. search on dynamic field in expr and/or output_fields
        expected:  search successfully
        4. release and reload with skip load dynamic field=true
        5. search on dynamic field in expr and/or output_fields
        expected: raise exception
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 128
        nb = 2000
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        load_int64_field = cf.gen_int64_field(name="int64_load")
        load_string_field = cf.gen_string_field(name="string_load")
        vector_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[pk_field, load_int64_field, load_string_field, vector_field],
                                          auto_id=True, enable_dynamic_field=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        data = []
        for i in range(nb):
            data.append({
                f"{load_int64_field.name}": i,
                f"{load_string_field.name}": str(i),
                f"{vector_field.name}": [random.uniform(-1, 1) for _ in range(dim)],
                "color": i,
                "tag": i,
            })
        collection_w.insert(data)

        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        collection_w.load()
        # search
        search_params = ct.default_search_params
        nq = 2
        search_vectors = cf.gen_vectors(nq, dim)
        res, _ = collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                                     expr="color > 0",
                                     limit=100, output_fields=["*"],
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": nq, "limit": 100})

        collection_w.release()
        collection_w.load(load_fields=[pk_field.name, vector_field.name, load_string_field.name],
                          skip_load_dynamic_field=True)
        error = {ct.err_code: 999, ct.err_msg: f"field color cannot be returned since dynamic field not loaded"}
        collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                            limit=100, output_fields=["color"],
                            check_task=CheckTasks.err_res, check_items=error)
        error = {ct.err_code: 999, ct.err_msg: f"field color is dynamic but dynamic field is not loaded"}
        collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                            expr="color > 0", limit=100, output_fields=["*"],
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_skip_load_some_vector_fields(self):
        """
        target: test skip load some vector fields
        method:
        1. create a collection with multiple vector fields
        2. not create index for skip load vector fields
        2. load some vector fields
        3. search on vector fields in expr and/or output_fields
        expected:  search successfully
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 128
        nb = 2000
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        load_string_field = cf.gen_string_field(name="string_load")
        vector_field = cf.gen_float_vec_field(name="vec_float32", dim=dim)
        sparse_vector_field = cf.gen_float_vec_field(name="sparse", vector_data_type=DataType.SPARSE_FLOAT_VECTOR)
        schema = cf.gen_collection_schema(fields=[pk_field, load_string_field, vector_field, sparse_vector_field],
                                          auto_id=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        string_values = [str(i) for i in range(nb)]
        float_vec_values = cf.gen_vectors(nb, dim)
        sparse_vec_values = cf.gen_vectors(nb, dim, vector_data_type=DataType.SPARSE_FLOAT_VECTOR)
        collection_w.insert([string_values, float_vec_values, sparse_vec_values])

        # build index on one of vector fields
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        # not load sparse vector field
        collection_w.load(load_fields=[pk_field.name, vector_field.name, load_string_field.name])
        # search
        search_params = ct.default_search_params
        nq = 2
        search_vectors = float_vec_values[0:nq]
        res, _ = collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                                     limit=100, output_fields=["*"],
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": nq, "limit": 100})

    @pytest.mark.tags(CaseLabel.L1)
    def test_partial_load_with_partition(self):
        """
        target: test partial load with partitions
        method:
        1. create a collection with fields
        2. create 2 partitions: p1, p2
        3. partial load p1
        4. search on p1
        5. load p2 with different fields
        expected: p2 load fail
        6. load p2 with the same partial fields
        7. search on p2
        expected: search successfully
        8. load the collection with all fields
        expected: load fail
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 128
        nb = 2000
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        load_int64_field = cf.gen_int64_field(name="int64_load")
        not_load_int64_field = cf.gen_int64_field(name="int64_not_load")
        load_string_field = cf.gen_string_field(name="string_load")
        not_load_string_field = cf.gen_string_field(name="string_not_load")
        vector_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[pk_field, load_int64_field, not_load_int64_field,
                                                  load_string_field, not_load_string_field, vector_field],
                                          auto_id=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        p1 = self.init_partition_wrap(collection_w, name='p1')
        p2 = self.init_partition_wrap(collection_w, name='p2')
        int_values = [i for i in range(nb)]
        string_values = [str(i) for i in range(nb)]
        float_vec_values = gen_vectors(nb, dim)
        p1.insert([int_values, int_values, string_values, string_values, float_vec_values])
        p2.insert([int_values, int_values, string_values, string_values, float_vec_values])

        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        # p1 load with partial fields
        p1.load(load_fields=[pk_field.name, vector_field.name, load_string_field.name, load_int64_field.name])
        # search
        search_params = ct.default_search_params
        nq = 2
        search_vectors = float_vec_values[0:nq]
        res, _ = p1.search(data=search_vectors, anns_field=vector_field.name, params=search_params,
                           limit=100, output_fields=["*"])
        assert pk_field.name in res[0][0].fields.keys() \
               and vector_field.name in res[0][0].fields.keys()
        # load p2 with different fields
        error = {ct.err_code: 999, ct.err_msg: f"can't change the load field list for loaded collection"}
        p2.load(load_fields=[pk_field.name, vector_field.name, not_load_string_field.name, not_load_int64_field.name],
                check_task=CheckTasks.err_res, check_items=error)
        # load p2 with the same partial fields
        p2.load(load_fields=[pk_field.name, vector_field.name, load_string_field.name, load_int64_field.name])
        res, _ = p2.search(data=search_vectors, anns_field=vector_field.name, params=search_params,
                           limit=100, output_fields=["*"])
        assert pk_field.name in res[0][0].fields.keys() \
               and vector_field.name in res[0][0].fields.keys()

        # load the collection with all fields
        collection_w.load(check_task=CheckTasks.err_res, check_items=error)
        collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                            limit=100, output_fields=["*"],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_skip_load_on_all_scalar_field_types(self):
        """
        target: test skip load on all scalar field types
        method:
        1. create a collection with fields define skip load on all scalar field types
        expected:
        1.  load and search successfully
        """
        prefix = "partial_load"
        collection_w = self.init_collection_general(prefix, insert_data=True, is_index=True,
                                                    is_all_data_type=True, with_json=True)[0]
        collection_w.release()
        # load with only pk field and vector field
        collection_w.load(load_fields=[ct.default_int64_field_name, ct.default_float_vec_field_name])
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        search_params = {"params": {}}
        res = collection_w.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                  param=search_params, limit=10, output_fields=["*"],
                                  check_tasks=CheckTasks.check_search_results, check_items={"nq": 1, "limit": 10})[0]
        assert len(res[0][0].fields.keys()) == 2


@pytest.mark.skip(reason="field partial load behavior changing @congqixia")
class TestFieldPartialLoadInvalid(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_skip_load_on_pk_field_or_vector_field(self):
        """
        target: test skip load on pk field
        method:
        1. create a collection with fields define skip load on pk field
        expected:
        1.  raise exception
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 32
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        load_int64_field = cf.gen_int64_field(name="int64_load")
        vector_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[pk_field, load_int64_field, vector_field], auto_id=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        # load without pk field
        error = {ct.err_code: 999, ct.err_msg: f"does not contain primary key field {pk_field.name}"}
        collection_w.load(load_fields=[vector_field.name, load_int64_field.name],
                          check_task=CheckTasks.err_res, check_items=error)
        error = {ct.err_code: 999, ct.err_msg: f"does not contain vector field"}
        collection_w.load(load_fields=[pk_field.name, load_int64_field.name],
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_skip_load_on_partition_key_field(self):
        """
        target: test skip load on partition key field
        method:
        1. create a collection with fields define skip load on partition key field
        expected:
        1.  raise exception
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 32
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        partition_key_field = cf.gen_int64_field(name="int64_load", is_partition_key=True)
        vector_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[pk_field, partition_key_field, vector_field], auto_id=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        # load without pk field
        error = {ct.err_code: 999, ct.err_msg: f"does not contain partition key field {partition_key_field.name}"}
        collection_w.load(load_fields=[vector_field.name, pk_field.name],
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_skip_load_on_clustering_key_field(self):
        """
        target: test skip load on clustering key field
        method:
        1. create a collection with fields define skip load on clustering key field
        expected:
        1.  raise exception
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 32
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        clustering_key_field = cf.gen_int64_field(name="int64_load", is_clustering_key=True)
        vector_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[pk_field, clustering_key_field, vector_field], auto_id=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        # load without pk field
        error = {ct.err_code: 999, ct.err_msg: f"does not contain clustering key field {clustering_key_field.name}"}
        collection_w.load(load_fields=[vector_field.name, pk_field.name],
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_update_load_fields_list_when_reloading_collection(self):
        """
        target: test update load fields list when reloading collection
        method:
        1. create a collection with fields
        2. load a part of the fields
        3. update load fields list when reloading collection
        expected:
        1.  raise exception
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 32
        nb = 2000
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        not_load_int64_field = cf.gen_int64_field(name="not_int64_load")
        load_string_field = cf.gen_string_field(name="string_load")
        vector_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[pk_field, not_load_int64_field, load_string_field, vector_field],
                                          auto_id=True, enable_dynamic_field=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        int_values = [i for i in range(nb)]
        string_values = [str(i) for i in range(nb)]
        float_vec_values = cf.gen_vectors(nb, dim)
        collection_w.insert([int_values, string_values, float_vec_values])

        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        collection_w.load(load_fields=[pk_field.name, vector_field.name, load_string_field.name])
        # search
        search_params = ct.default_search_params
        nq = 1
        search_vectors = float_vec_values[0:nq]
        collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                            limit=10, output_fields=[load_string_field.name],
                            check_task=CheckTasks.check_search_results, check_items={"nq": nq, "limit": 10})
        # try to add more fields in load fields list when reloading
        error = {ct.err_code: 999, ct.err_msg: f"can't change the load field list for loaded collection"}
        collection_w.load(load_fields=[pk_field.name, vector_field.name,
                                        load_string_field.name, not_load_int64_field.name],
                          check_task=CheckTasks.err_res, check_items=error)
        # try to remove fields in load fields list when reloading
        collection_w.load(load_fields=[pk_field.name, vector_field.name],
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_one_of_dynamic_fields_in_load_fields_list(self):
        """
        target: test one of dynamic fields in load fields list
        method:
        1. create a collection with fields
        3. add one of dynamic fields in load fields list when loading
        expected: raise exception
        4. add non_existing field in load fields list when loading
        expected: raise exception
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 32
        nb = 2000
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        load_int64_field = cf.gen_int64_field(name="int64_load")
        load_string_field = cf.gen_string_field(name="string_load")
        vector_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[pk_field, load_int64_field, load_string_field, vector_field],
                                          auto_id=True, enable_dynamic_field=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        data = []
        for i in range(nb):
            data.append({
                f"{load_int64_field.name}": i,
                f"{load_string_field.name}": str(i),
                f"{vector_field.name}": [random.uniform(-1, 1) for _ in range(dim)],
                "color": i,
                "tag": i,
            })
        collection_w.insert(data)
        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        # add one of dynamic fields in load fields list
        error = {ct.err_code: 999,
                 ct.err_msg: f"failed to get field schema by name: fieldName(color) not found"}
        collection_w.load(load_fields=[pk_field.name, vector_field.name, "color"],
                          check_task=CheckTasks.err_res, check_items=error)
        # add non_existing field in load fields list
        error = {ct.err_code: 999,
                 ct.err_msg: f"failed to get field schema by name: fieldName(not_existing) not found"}
        collection_w.load(load_fields=[pk_field.name, vector_field.name, "not_existing"],
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_on_not_loaded_fields(self):
        """
        target: test search on skipped fields
        method:
        1. create a collection with fields
        2. load a part of the fields
        3. search on skipped fields in expr and/or output_fields
        expected:
        1.  raise exception
        """
        self._connect()
        name = cf.gen_unique_str()
        dim = 32
        nb = 2000
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        not_load_int64_field = cf.gen_int64_field(name="not_int64_load")
        load_string_field = cf.gen_string_field(name="string_load")
        vector_field = cf.gen_float_vec_field(dim=dim)
        schema = cf.gen_collection_schema(fields=[pk_field, not_load_int64_field, load_string_field, vector_field],
                                          auto_id=True, enable_dynamic_field=True)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        int_values = [i for i in range(nb)]
        string_values = [str(i) for i in range(nb)]
        float_vec_values = cf.gen_vectors(nb, dim)
        collection_w.insert([int_values, string_values, float_vec_values])

        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)
        collection_w.load(load_fields=[pk_field.name, vector_field.name, load_string_field.name])
        # search
        search_params = ct.default_search_params
        nq = 1
        search_vectors = float_vec_values[0:nq]
        error = {ct.err_code: 999, ct.err_msg: f"field {not_load_int64_field.name} is not loaded"}
        collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                            limit=10, output_fields=[not_load_int64_field.name, load_string_field.name],
                            check_task=CheckTasks.err_res, check_items=error)
        error = {ct.err_code: 999, ct.err_msg: f"cannot parse expression"}
        collection_w.search(data=search_vectors, anns_field=vector_field.name, param=search_params,
                            expr=f"{not_load_int64_field.name} > 0",
                            limit=10, output_fields=[load_string_field.name],
                            check_task=CheckTasks.err_res, check_items=error)
