import pytest
import numbers
from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import DataType
import numpy as np

prefix = "alter"
default_vector_field_name = "vector"
default_primary_key_field_name = "id"
default_string_field_name = "varchar"
default_float_field_name = "float"
default_new_field_name = "field_new"
default_dynamic_field_name = "dynamic_field"
exp_res = "exp_res"
default_nb = 20
default_dim = 128
default_limit = 10


class TestMilvusClientAlterIndex(TestMilvusClientV2Base):

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_alter_index_default(self):
        """
        target: test alter index
        method: 1. alter index after load
                verify alter fail
                2. alter index after release
                verify alter successfully
                3. drop index properties after load
                verify drop fail
                4. drop index properties after release
                verify drop successfully
        expected: alter successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, ct.default_dim, consistency_level="Strong")
        idx_names, _ = self.list_indexes(client, collection_name, field_name=default_vector_field_name)
        self.load_collection(client, collection_name)
        res1 = self.describe_index(client, collection_name, index_name=idx_names[0])[0]
        assert res1.get('mmap.enabled', None) is None
        error = {ct.err_code: 104,
                 ct.err_msg: f"can't alter index on loaded collection, "
                             f"please release the collection first: collection already loaded[collection={collection_name}]"}
        # 1. alter index after load
        self.alter_index_properties(client, collection_name, idx_names[0], properties={"mmap.enabled": True},
                                    check_task=CheckTasks.err_res, check_items=error)
        self.drop_index_properties(client, collection_name, idx_names[0], property_keys=["mmap.enabled"],
                                   check_task=CheckTasks.err_res, check_items=error)
        self.release_collection(client, collection_name)
        # 2. alter index after release
        self.alter_index_properties(client, collection_name, idx_names[0], properties={"mmap.enabled": True})
        res2 = self.describe_index(client, collection_name, index_name=idx_names[0])[0]
        assert res2.get('mmap.enabled', None) == 'True'
        self.drop_index_properties(client, collection_name, idx_names[0], property_keys=["mmap.enabled"])
        res3 = self.describe_index(client, collection_name, index_name=idx_names[0])[0]
        assert res3.get('mmap.enabled', None) is None

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_index_unsupported_properties(self):
        """
        target: test alter index with unsupported properties
        method: 1. alter index with unsupported properties
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        dim = 32
        pk_field_name = 'id_string'
        vector_field_name = 'embeddings'
        str_field_name = 'title'
        max_length = 16
        schema.add_field(pk_field_name, DataType.VARCHAR, max_length=max_length, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, DataType.FLOAT_VECTOR, dim=dim, mmap_enabled=True)
        schema.add_field(str_field_name, DataType.VARCHAR, max_length=max_length, mmap_enabled=True)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name, metric_type="COSINE",
                               index_type="HNSW", params={"M": 16, "efConstruction": 100, "mmap.enabled": True})
        index_params.add_index(field_name=str_field_name)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={"mmap.enabled": True})
        self.describe_collection(client, collection_name, check_task=CheckTasks.check_collection_fields_properties,
                                 check_items={str_field_name: {"max_length": max_length, "mmap_enabled": True},
                                              vector_field_name: {"mmap_enabled": True},
                                              'properties': {'mmap.enabled': 'False'}})
        res = self.describe_index(client, collection_name, index_name=vector_field_name)[0]
        assert res.get('mmap.enabled', None) == 'True'
        self.release_collection(client, collection_name)
        properties = self.describe_index(client, collection_name, index_name=vector_field_name)[0]
        for p in properties.items():
            if p[0] not in ["mmap.enabled"]:
                log.debug(f"try to alter index property: {p[0]}")
                error = {ct.err_code: 1, ct.err_msg: f"{p[0]} is not a configable index property"}
                new_value = p[1] + 1 if isinstance(p[1], numbers.Number) else "new_value"
                self.alter_index_properties(client, collection_name, vector_field_name,
                                            properties={p[0]: new_value},
                                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_index_unsupported_value(self):
        """
        target: test alter index with unsupported properties
        method: 1. alter index with unsupported properties
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, ct.default_dim, consistency_level="Strong")
        idx_names, _ = self.list_indexes(client, collection_name, field_name=default_vector_field_name)
        self.release_collection(client, collection_name)
        res1 = self.describe_index(client, collection_name, index_name=idx_names[0])[0]
        assert res1.get('mmap.enabled', None) is None
        unsupported_values = [None, [], '', 20, '  ', 0.01, "new_value"]
        for value in unsupported_values:
            error = {ct.err_code: 1, ct.err_msg: f"invalid mmap.enabled value: {value}, expected: true, false"}
            self.alter_index_properties(client, collection_name, idx_names[0],
                                        properties={"mmap.enabled": value},
                                        check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientAlterCollection(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_alter_collection_default(self):
        """
        target: test alter collection
        method:
            1. alter collection properties after load
            verify alter successfully if trying to altering lazyload.enabled, mmap.enabled or collection.ttl.seconds
            2. alter collection properties after release
            verify alter successfully
            3. drop collection properties after load
            verify drop successfully
        expected: alter successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, ct.default_dim, consistency_level="Strong")
        self.load_collection(client, collection_name)
        res1 = self.describe_collection(client, collection_name)[0]
        assert len(res1.get('properties', {})) == 1
        # 1. alter collection properties after load
        self.load_collection(client, collection_name)
        error = {ct.err_code: 999,
                 ct.err_msg: "can not alter mmap properties if collection loaded"}
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True},
                                         check_task=CheckTasks.err_res, check_items=error)
        self.alter_collection_properties(client, collection_name, properties={"lazyload.enabled": True},
                                         check_task=CheckTasks.err_res, check_items=error)
        error = {ct.err_code: 999,
                 ct.err_msg: "dynamic schema cannot supported to be disabled: invalid parameter"}
        self.alter_collection_properties(client, collection_name, properties={"dynamicfield.enabled": False},
                                         check_task=CheckTasks.err_res, check_items=error)
        error = {ct.err_code: 999,
                 ct.err_msg: "can not delete mmap properties if collection loaded"}
        self.drop_collection_properties(client, collection_name, property_keys=["mmap.enabled"],
                                        check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection_properties(client, collection_name, property_keys=["lazyload.enabled"],
                                        check_task=CheckTasks.err_res, check_items=error)
        # TODO                                
        # error = {ct.err_code: 999,
        #          ct.err_msg: "can not delete dynamicfield properties"}
        # self.drop_collection_properties(client, collection_name, property_keys=["dynamicfield.enabled"],
        #                                 check_task=CheckTasks.err_res, check_items=error)
        res3 = self.describe_collection(client, collection_name)[0]
        assert len(res1.get('properties', {})) == 1
        self.drop_collection_properties(client, collection_name, property_keys=["collection.ttl.seconds"])
        assert len(res1.get('properties', {})) == 1
        # 2. alter collection properties after release
        self.release_collection(client, collection_name)
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        res2 = self.describe_collection(client, collection_name)[0]
        assert {'mmap.enabled': 'True'}.items() <= res2.get('properties', {}).items()
        self.alter_collection_properties(client, collection_name,
                                         properties={"collection.ttl.seconds": 100, "lazyload.enabled": True})
        res2 = self.describe_collection(client, collection_name)[0]
        assert {'mmap.enabled': 'True', 'collection.ttl.seconds': '100', 'lazyload.enabled': 'True'}.items()  \
                <= res2.get('properties', {}).items()
        self.drop_collection_properties(client, collection_name,
                                        property_keys=["mmap.enabled", "lazyload.enabled",
                                                       "collection.ttl.seconds"])
        res3 = self.describe_collection(client, collection_name)[0]
        assert len(res1.get('properties', {})) == 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_enable_dynamic_collection_field(self):
        """
        target: test enable dynamic field and mixed field operations
        method: create collection, add field, enable dynamic field, insert mixed data, query/search
        expected: dynamic field works with new field and static field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, max_length=64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. Prepare and insert data
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb
        # 3. add new field
        default_value = 100
        self.add_collection_field(client, collection_name, field_name="field_new", data_type=DataType.INT64,
                                  nullable=True, default_value=default_value)
        # 4. alter collection dynamic field enable
        self.alter_collection_properties(client, collection_name, {"dynamicfield.enabled": True})
        res = self.describe_collection(client, collection_name)[0]
        assert res.get('enable_dynamic_field', None) is True
        # 5. insert data with dynamic field and new field
        vectors = cf.gen_vectors(default_nb, dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows_new = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                     default_string_field_name: str(i), default_new_field_name: i,
                     default_dynamic_field_name: i} for i in range(default_nb)]
        self.insert(client, collection_name, rows_new)
        # 6. query using filter with dynamic field and new field
        res = self.query(client, collection_name,
                         filter="{} >= 0 and field_new < {}".format(default_dynamic_field_name, default_value),
                         output_fields=[default_dynamic_field_name],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: [{"id": item["id"],
                                                 default_dynamic_field_name: item[default_dynamic_field_name]}
                                                 for item in rows_new]})[0]
        assert set(res[0].keys()) == {default_dynamic_field_name, default_primary_key_field_name}
        # 7. search using filter with dynamic field and new field
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter="{} >= 0 and field_new < {}".format(default_dynamic_field_name, default_value),
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 8. add new field same as dynamic field name
        self.add_collection_field(client, collection_name, field_name=default_dynamic_field_name,
                                  data_type=DataType.INT64, nullable=True, default_value=default_value)
        # 9. query using filter with dynamic field and new field
        res = self.query(client, collection_name,
                         filter='$meta["{}"] >= 0 and {} == {}'.format(default_dynamic_field_name,
                                                                       default_dynamic_field_name, default_value),
                         output_fields=[default_dynamic_field_name, f'$meta["{default_dynamic_field_name}"]'],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: [{"id": item["id"], default_dynamic_field_name: default_value}
                                                for item in rows_new]})[0]
        # dynamic field same as new field name, output_fields contain dynamic field, result do not contain dynamic field
        # https://github.com/milvus-io/milvus/issues/41702
        assert set(res[0].keys()) == {default_dynamic_field_name, default_primary_key_field_name}
        # 10. search using filter with dynamic field and new field
        self.search(client, collection_name, vectors_to_search,
                    filter='$meta["{}"] >= 0 and {} == {}'.format(default_dynamic_field_name,
                                                                  default_dynamic_field_name, default_value),
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("old_dynamic_flag, new_dynamic_flag", [(True, True), (False, False)])
    def test_milvus_client_alter_dynamic_collection_field_no_op(self, old_dynamic_flag, new_dynamic_flag):
        """
        target: test dynamic field no-op alter operations
        method: create collection with dynamic flag, alter to same flag, verify unchanged
        expected: no-op alter succeeds without state change
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=old_dynamic_flag)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, max_length=64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. alter collection dynamic field
        self.alter_collection_properties(client, collection_name, properties={"dynamicfield.enabled": new_dynamic_flag})
        res = self.describe_collection(client, collection_name)[0]
        assert res.get('enable_dynamic_field', None) is new_dynamic_flag


class TestMilvusClientAlterCollectionField(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("add_field", [True, False])
    def test_milvus_client_alter_collection_field_default(self, add_field):
        """
        target: test alter collection field before load
        method: alter varchar field max length
        expected: alter successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        dim = 32
        pk_field_name = 'id_string'
        vector_field_name = 'embeddings'
        str_field_name = 'title'
        json_field_name = 'json_field'
        array_field_name = 'tags'
        new_field_name = 'field_new'
        max_length = 16
        schema.add_field(pk_field_name, DataType.VARCHAR, max_length=max_length, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, DataType.FLOAT_VECTOR, dim=dim, mmap_enabled=True)
        schema.add_field(str_field_name, DataType.VARCHAR, max_length=max_length, mmap_enabled=True)
        schema.add_field(json_field_name, DataType.JSON, mmap_enabled=False)
        schema.add_field(field_name=array_field_name, datatype=DataType.ARRAY, element_type=DataType.VARCHAR,
                         max_capacity=10, max_length=max_length)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name, metric_type="COSINE",
                               index_type="IVF_FLAT", params={"nlist": 128})
        index_params.add_index(field_name=str_field_name)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        check_items = {str_field_name: {"max_length": max_length, "mmap_enabled": True},
                       vector_field_name: {"mmap_enabled": True},
                       json_field_name: {"mmap_enabled": False}}
        if add_field:
            self.add_collection_field(client, collection_name, field_name="field_new", data_type=DataType.VARCHAR,
                                      nullable=True, max_length=max_length)
            check_items["field_new"] = {"max_length": max_length}
        self.describe_collection(client, collection_name, check_task=CheckTasks.check_collection_fields_properties,
                                 check_items=check_items)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            pk_field_name: f'id_{i}',
            vector_field_name: list(rng.random((1, dim))[0]),
            str_field_name: cf.gen_str_by_length(max_length),
            json_field_name: {"number": i},
            array_field_name: [cf.gen_str_by_length(max_length) for _ in range(10)],
            # add new field data (only when add_field is True)
            **({"field_new": cf.gen_str_by_length(max_length)} if add_field else {})
        } for i in range(ct.default_nb)]
        self.insert(client, collection_name, rows)

        # 1. alter collection field before load
        self.release_collection(client, collection_name)
        new_max_length = max_length // 2
        # TODO: use one format of mmap_enabled after #38443 fixed
        self.alter_collection_field(client, collection_name, field_name=str_field_name,
                                    field_params={"max_length": new_max_length, "mmap.enabled": False})
        self.alter_collection_field(client, collection_name, field_name=pk_field_name,
                                    field_params={"max_length": new_max_length})
        self.alter_collection_field(client, collection_name, field_name=json_field_name,
                                    field_params={"mmap.enabled": True})
        self.alter_collection_field(client, collection_name, field_name=vector_field_name,
                                    field_params={"mmap.enabled": False})
        self.alter_collection_field(client, collection_name, field_name=array_field_name,
                                    field_params={"max_length": new_max_length})
        self.alter_collection_field(client, collection_name, field_name=array_field_name,
                                    field_params={"max_capacity": 20})
        error = {ct.err_code: 999, ct.err_msg: f"can not modify the maxlength for non-string types"}
        self.alter_collection_field(client, collection_name, field_name=vector_field_name,
                                    field_params={"max_length": new_max_length},
                                    check_task=CheckTasks.err_res, check_items=error)
        error = {ct.err_code: 999, ct.err_msg: "element_type does not allow update in collection field param"}
        self.alter_collection_field(client, collection_name, field_name=array_field_name,
                                    field_params={"element_type": DataType.INT64},
                                    check_task=CheckTasks.err_res, check_items=error)
        check_items_new = {str_field_name: {"max_length": new_max_length, "mmap_enabled": False},
                           vector_field_name: {"mmap_enabled": False},
                           json_field_name: {"mmap_enabled": True},
                           array_field_name: {"max_length": new_max_length, "max_capacity": 20}}
        if add_field:
            self.alter_collection_field(client, collection_name, field_name="field_new",
                                        field_params={"max_length": new_max_length})
            check_items_new["field_new"] = {"max_length": new_max_length}
        self.describe_collection(client, collection_name, check_task=CheckTasks.check_collection_fields_properties,
                                 check_items=check_items_new)
        # verify that cannot insert data with the old max_length
        fields_to_verify = [pk_field_name, str_field_name, array_field_name]
        if add_field:
            fields_to_verify.append(new_field_name)
        for alter_field in fields_to_verify:
            error = {ct.err_code: 999, ct.err_msg: f"length of varchar field {alter_field} exceeds max length"}
            if alter_field == array_field_name:
                error = {ct.err_code: 999,
                         ct.err_msg: f'length of Array array field "{array_field_name}" exceeds max length'}
            rows = [{
                pk_field_name: cf.gen_str_by_length(max_length) if alter_field == pk_field_name else f'id_{i}',
                vector_field_name: list(rng.random((1, dim))[0]),
                str_field_name: cf.gen_str_by_length(max_length) if alter_field == str_field_name else f'ti_{i}',
                json_field_name: {"number": i},
                array_field_name: [cf.gen_str_by_length(max_length) for _ in
                                   range(10)] if alter_field == array_field_name else [f"tags_{j}" for j in range(10)],
                **({"field_new": cf.gen_str_by_length(max_length)} if add_field and alter_field == new_field_name else {})
            } for i in range(ct.default_nb, ct.default_nb + 10)]
            self.insert(client, collection_name, rows, check_task=CheckTasks.err_res, check_items=error)

        # verify that can insert data with the new max_length
        rows = [{
            pk_field_name: f"new_{cf.gen_str_by_length(new_max_length - 4)}",
            vector_field_name: list(rng.random((1, dim))[0]),
            str_field_name: cf.gen_str_by_length(new_max_length),
            json_field_name: {"number": i},
            array_field_name: [cf.gen_str_by_length(new_max_length) for _ in range(10)],
            **({"field_new": cf.gen_str_by_length(new_max_length)} if add_field else {})
        } for i in range(ct.default_nb, ct.default_nb + 10)]
        self.insert(client, collection_name, rows)

        # 2. alter collection field after load
        self.load_collection(client, collection_name)
        error = {ct.err_code: 999,
                 ct.err_msg: "can not alter collection field properties if collection loaded"}
        self.alter_collection_field(client, collection_name, field_name=str_field_name,
                                    field_params={"max_length": max_length, "mmap.enabled": True},
                                    check_task=CheckTasks.err_res, check_items=error)
        self.alter_collection_field(client, collection_name, field_name=vector_field_name,
                                    field_params={"mmap.enabled": True},
                                    check_task=CheckTasks.err_res, check_items=error)
        self.alter_collection_field(client, collection_name, field_name=pk_field_name,
                                    field_params={"max_length": max_length})
        if add_field:
            self.alter_collection_field(client, collection_name, field_name=new_field_name,
                                        field_params={"max_length": max_length})
        res = self.query(client, collection_name, filter=f"{pk_field_name} in ['id_10', 'id_20']",
                         output_fields=["*"])[0]
        assert (len(res)) == 2
        res = self.query(client, collection_name, filter=f"{pk_field_name} like 'new_%'",
                         output_fields=["*"])[0]
        assert (len(res)) == 10


class TestMilvusClientAlterDatabase(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_alter_database_default(self):
        """
        target: test alter database
        method:
            1. alter database properties before load
            alter successfully
            2. alter database properties after load
            alter successfully
        expected: alter successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, ct.default_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)
        default_db = 'default'
        res1 = self.describe_database(client, db_name=default_db)[0]
        if len(res1.keys()) != 1:
            self.drop_database_properties(client, db_name=default_db, property_keys=res1.keys())
        assert len(self.describe_database(client, default_db)[0].keys()) == 1
        for need_load in [True, False]:
            if need_load:
                log.debug("alter database after load collection")
                self.load_collection(client, collection_name)

            # 1. alter default database properties before load
            properties = {"key1": 1, "key2": "value2", "key3": [1, 2, 3], }
            self.alter_database_properties(client, db_name=default_db, properties=properties)
            res1 = self.describe_database(client, db_name=default_db)[0]
            # assert res1.properties.items() >= properties.items()
            assert len(res1.keys()) == 4
            my_db = cf.gen_unique_str(prefix)
            self.create_database(client, my_db, properties=properties)
            res1 = self.describe_database(client, db_name=my_db)[0]
            # assert res1.properties.items() >= properties.items()
            assert len(res1.keys()) == 4
            properties = {"key1": 2, "key2": "value3", "key3": [1, 2, 3], 'key4': 0.123}
            self.alter_database_properties(client, db_name=my_db, properties=properties)
            res1 = self.describe_database(client, db_name=my_db)[0]
            # assert res1.properties.items() >= properties.items()
            assert len(res1.keys()) == 5

            # drop the default database properties
            self.drop_database_properties(client, db_name=default_db, property_keys=["key1", "key2"])
            res1 = self.describe_database(client, db_name=default_db)[0]
            assert len(res1.keys()) == 2
            self.drop_database_properties(client, db_name=default_db, property_keys=["key3", "key_non_exist"])
            res1 = self.describe_database(client, db_name=default_db)[0]
            assert len(res1.keys()) == 1
            # drop the user database
            self.drop_database(client, my_db)
