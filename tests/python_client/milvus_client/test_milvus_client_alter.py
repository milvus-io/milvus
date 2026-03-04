import pytest
import numbers
import time
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
default_warmup_dim = 128
default_warmup_nb = 2000


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
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_index_idempotent_with_field_warmup(self):
        """
        target: test create index idempotency when warmup is set at field level
        method: 1. create collection
                2. release collection
                3. alter field to set warmup=sync at field level (this adds warmup to field TypeParams)
                4. drop existing index
                5. create index (first time)
                6. create same index again (second time - should be idempotent)
        expected: both create_index calls should succeed (idempotent behavior)
        issue: https://github.com/milvus-io/milvus/issues/XXXXX
        note: This test verifies the fix for checkParams function in index_meta.go
              which was missing WarmupKey in DeleteParams, causing idempotency check to fail.
              When index is created, WarmupKey is removed from stored TypeParams.
              When checking idempotency, WarmupKey should also be filtered from request TypeParams.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. release collection before altering field properties
        self.release_collection(client, collection_name)
        # 3. alter field to set warmup=sync at field level
        # This adds warmup to field TypeParams, which will be included in CreateIndex request
        self.alter_collection_field(client, collection_name, field_name=default_vector_field_name,
                                    field_params={"warmup": "sync"})
        # 4. drop existing index
        self.drop_index(client, collection_name, default_vector_field_name)
        res = self.list_indexes(client, collection_name)[0]
        assert res == []
        # 5. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW",
                               metric_type="L2", params={"M": 8, "efConstruction": 200})
        # 6. create index (first time) - should succeed
        self.create_index(client, collection_name, index_params)
        idx_names, _ = self.list_indexes(client, collection_name, field_name=default_vector_field_name)
        assert len(idx_names) == 1
        # 7. create same index again (second time) - should be idempotent and succeed
        # Before fix: this would fail with "at most one distinct index is allowed per field"
        # because checkParams didn't filter WarmupKey from TypeParams comparison
        self.create_index(client, collection_name, index_params)
        idx_names_after, _ = self.list_indexes(client, collection_name, field_name=default_vector_field_name)
        assert len(idx_names_after) == 1
        assert idx_names == idx_names_after
        # 8. cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_index_idempotent_with_collection_warmup(self):
        """
        target: test create index idempotency when warmup is set at collection level
        method: 1. create collection
                2. release collection
                3. alter collection properties to set warmup.vectorField=sync
                4. drop existing index
                5. create index (first time)
                6. create same index again (second time - should be idempotent)
        expected: both create_index calls should succeed (idempotent behavior)
        note: This test verifies the fix for checkParams function in index_meta.go
              using collection-level warmup settings (warmup.vectorField)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. release collection before altering properties
        self.release_collection(client, collection_name)
        # 3. alter collection properties to set warmup.vectorField=sync
        self.alter_collection_properties(client, collection_name,
                                         properties={"warmup.vectorField": "sync"})
        # 4. drop existing index
        self.drop_index(client, collection_name, default_vector_field_name)
        res = self.list_indexes(client, collection_name)[0]
        assert res == []
        # 5. prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="HNSW",
                               metric_type="L2", params={"M": 8, "efConstruction": 200})
        # 6. create index (first time) - should succeed
        self.create_index(client, collection_name, index_params)
        idx_names, _ = self.list_indexes(client, collection_name, field_name=default_vector_field_name)
        assert len(idx_names) == 1
        # 7. create same index again (second time) - should be idempotent and succeed
        self.create_index(client, collection_name, index_params)
        idx_names_after, _ = self.list_indexes(client, collection_name, field_name=default_vector_field_name)
        assert len(idx_names_after) == 1
        assert idx_names == idx_names_after
        # 8. cleanup
        self.drop_collection(client, collection_name)


class TestMilvusClientAlterCollection(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_alter_collection_default(self):
        """
        target: test alter collection
        method:
            1. alter collection properties after load
            verify alter successfully if trying to altering mmap.enabled or collection.ttl.seconds
            2. alter collection properties after release
            verify alter successfully
            3. drop collection properties after load
            verify drop successfully
        expected: alter successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        error = {ct.err_code: 999,
                 ct.err_msg: "dynamic schema cannot supported to be disabled: invalid parameter"}
        self.alter_collection_properties(client, collection_name, properties={"dynamicfield.enabled": False},
                                         check_task=CheckTasks.err_res, check_items=error)
        error = {ct.err_code: 999,
                 ct.err_msg: "can not delete mmap properties if collection loaded"}
        self.drop_collection_properties(client, collection_name, property_keys=["mmap.enabled"],
                                        check_task=CheckTasks.err_res, check_items=error)
        # TODO                                
        error = {ct.err_code: 999,
                 ct.err_msg: "cannot delete key dynamicfield.enabled"}
        self.drop_collection_properties(client, collection_name, property_keys=["dynamicfield.enabled"],
                                        check_task=CheckTasks.err_res, check_items=error)
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
                                         properties={"collection.ttl.seconds": 100})
        res2 = self.describe_collection(client, collection_name)[0]
        assert {'mmap.enabled': 'True', 'collection.ttl.seconds': '100'}.items()  \
                <= res2.get('properties', {}).items()
        self.drop_collection_properties(client, collection_name,
                                        property_keys=["mmap.enabled", "collection.ttl.seconds"])
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
                     default_dynamic_field_name: {'a': {"b": i}}} for i in range(default_nb)]
        self.insert(client, collection_name, rows_new)
        # 6. create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_dynamic_field_name,
                               index_type="INVERTED",
                               params={"json_cast_type": "DOUBLE",
                                       "json_path": f"{default_dynamic_field_name}['a']['b']"})
        self.create_index(client, collection_name, index_params)
        index_name = "$meta/" + default_dynamic_field_name
        self.describe_index(client, collection_name, index_name + "/a/b",
                            check_task=CheckTasks.check_describe_index_property,
                            check_items={
                                "json_cast_type": "DOUBLE",
                                "json_path": f"{default_dynamic_field_name}['a']['b']",
                                "index_type": "INVERTED",
                                "field_name": default_dynamic_field_name,
                                "index_name": index_name + "/a/b"})
        # 7. query using filter with dynamic field and new field
        res = self.query(client, collection_name,
                         filter=f"{default_dynamic_field_name}['a']['b'] >= 0 and field_new < {default_value}",
                         output_fields=[default_dynamic_field_name],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: [{"id": item["id"],
                                                 default_dynamic_field_name: item[default_dynamic_field_name]}
                                                 for item in rows_new]})[0]
        assert set(res[0].keys()) == {default_dynamic_field_name, default_primary_key_field_name}
        # 8. search using filter with dynamic field and new field
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    filter=f"{default_dynamic_field_name}['a']['b'] >= 0 and field_new < {default_value}",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 9. add new field same as dynamic field name
        self.add_collection_field(client, collection_name, field_name=default_dynamic_field_name,
                                  data_type=DataType.INT64, nullable=True, default_value=default_value)
        # 10. query using filter with dynamic field and new field
        res = self.query(client, collection_name,
                         filter='$meta["{}"]["a"]["b"] >= 0 and {} == {}'.format(default_dynamic_field_name,
                                                                                 default_dynamic_field_name,
                                                                                 default_value),
                         output_fields=[default_dynamic_field_name, f'$meta["{default_dynamic_field_name}"]'],
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: [{"id": item["id"], default_dynamic_field_name: default_value}
                                                for item in rows_new]})[0]
        # dynamic field same as new field name, output_fields contain dynamic field, result do not contain dynamic field
        # https://github.com/milvus-io/milvus/issues/41702
        assert set(res[0].keys()) == {default_dynamic_field_name, default_primary_key_field_name}
        # 11. search using filter with dynamic field and new field
        self.search(client, collection_name, vectors_to_search,
                    filter='$meta["{}"]["a"]["b"] >= 0 and {} == {}'.format(default_dynamic_field_name,
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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("pk_field_type", [DataType.INT64, DataType.VARCHAR])
    def test_milvus_client_alter_allow_insert_auto_id(self, pk_field_type):
        """
        target: test alter collection allow insert auto id
        method: 
            1. create collection with auto_id=True
            2. try to insert data with primary key
            3. verify insert failed
            4. alter collection allow_insert_auto_id=True
            5. insert data with customized primary key
            6. verify insert successfully
            7. verify the new inserted data's primary keys are customized
            8. verify the collection info
            9. drop the collection properties allow_insert_auto_id
            10. alter collection allow_insert_auto_id=False
            11. verify the collection info
            12. alter collection allow_insert_auto_id=True with string value
            13. verify the collection info
            14. insert data with customized primary key
            15. verify insert successfully
        expected: insert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, pk_field_type, max_length=64, is_primary=True, auto_id=True)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. try to insert data with primary key
        rows_with_pk = [{
            default_primary_key_field_name: i,
            default_vector_field_name: cf.gen_vectors(1, dim, vector_data_type=DataType.FLOAT_VECTOR)[0]
        } for i in range(100)]
        if pk_field_type == DataType.VARCHAR:
            rows_with_pk = [{
                default_primary_key_field_name: f"id_{i}",
                default_vector_field_name: cf.gen_vectors(1, dim, vector_data_type=DataType.FLOAT_VECTOR)[0]
            } for i in range(100)]
        error = {ct.err_code: 999, ct.err_msg: f"more fieldData has pass in"}
        self.insert(client, collection_name, rows_with_pk, check_task=CheckTasks.err_res, check_items=error)

        rows_without_pk = cf.gen_row_data_by_schema(nb=100, schema=schema)
        self.insert(client, collection_name, rows_without_pk)
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == 100

        self.load_collection(client, collection_name)

        filter = f"{default_primary_key_field_name} in [10, 20,90]"
        if pk_field_type == DataType.VARCHAR:
            filter = f"{default_primary_key_field_name} in ['id_10', 'id_20', 'id_90']"
        res = self.query(client, collection_name, filter=filter,
                         output_fields=[default_primary_key_field_name])[0]
        assert (len(res)) == 0

        # 3. alter collection allow_insert_auto_id=True
        self.alter_collection_properties(client, collection_name, properties={"allow_insert_auto_id": True})
        # 4. insert data with customized primary key
        self.insert(client, collection_name, rows_with_pk)
        # 5. verify insert successfully
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == 100 * 2
        # 6. verify the new inserted data's primary keys are customized
        res = self.query(client, collection_name, filter=filter,
                         output_fields=[default_primary_key_field_name])[0]
        assert (len(res)) == 3

        # check the collection info
        res = self.describe_collection(client, collection_name)[0]
        assert res.get('properties').get('allow_insert_auto_id', None) == 'True'

        # drop the collection properties allow_insert_auto_id
        self.drop_collection_properties(client, collection_name, property_keys=["allow_insert_auto_id"])
        res = self.describe_collection(client, collection_name)[0]
        assert res.get('properties').get('allow_insert_auto_id', None) is None
        self.insert(client, collection_name, rows_with_pk, check_task=CheckTasks.err_res, check_items=error)

        # alter collection allow_insert_auto_id=False
        self.alter_collection_properties(client, collection_name, properties={"allow_insert_auto_id": False})
        res = self.describe_collection(client, collection_name)[0]
        assert res.get('properties').get('allow_insert_auto_id', None) == 'False'
        self.insert(client, collection_name, rows_with_pk, check_task=CheckTasks.err_res, check_items=error)

        # alter collection allow_insert_auto_id=True with string value
        self.alter_collection_properties(client, collection_name, properties={"allow_insert_auto_id": "True"})
        res = self.describe_collection(client, collection_name)[0]
        assert res.get('properties').get('allow_insert_auto_id', None) == 'True'
        rows_with_pk = [{
            default_primary_key_field_name: i,
            default_vector_field_name: cf.gen_vectors(1, dim, vector_data_type=DataType.FLOAT_VECTOR)[0]
        } for i in range(100, 200)]
        if pk_field_type == DataType.VARCHAR:
            rows_with_pk = [{
                default_primary_key_field_name: f"id_{i}",
                default_vector_field_name: cf.gen_vectors(1, dim, vector_data_type=DataType.FLOAT_VECTOR)[0]
            } for i in range(100, 200)]
        self.insert(client, collection_name, rows_with_pk)
        self.flush(client, collection_name)
        num_entities = self.get_collection_stats(client, collection_name)[0]
        assert num_entities.get("row_count", None) == 100 * 3
     

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
        collection_name = cf.gen_collection_name_by_testcase_name()
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_collection_field_nullable_field(self):
        """
        target: test alter collection field with nullable field
        method: create collection with nullable field and alter field
        expected: alter successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id_string", DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False)
        schema.add_field("embeddings_1", DataType.FLOAT_VECTOR, dim=dim, nullable=True)
        schema.add_field("embeddings_2", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("varchar_1", DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field("varchar_2", DataType.VARCHAR, max_length=64)
        self.create_collection(client, collection_name, dimension=dim, schema=schema)

        # try to alert nullable vector field to non-nullable field
        error = {ct.err_code: 999,
                 ct.err_msg: "nullable does not allow update in collection field param"}
        self.alter_collection_field(client, collection_name, field_name="embeddings_1",
                                    field_params={"nullable": False},
                                    check_task=CheckTasks.err_res, check_items=error)
        # try to alert non-nullable vector field to nullable field
        self.alter_collection_field(client, collection_name, field_name="embeddings_2",
                                    field_params={"nullable": True},
                                    check_task=CheckTasks.err_res, check_items=error)
        # try to alert nullable varchar field to non-nullable varchar field
        self.alter_collection_field(client, collection_name, field_name="varchar_1",
                                    field_params={"nullable": False},
                                    check_task=CheckTasks.err_res, check_items=error)
        # try to alert non-nullable varchar field to nullable varchar field 
        self.alter_collection_field(client, collection_name, field_name="varchar_2",
                                    field_params={"nullable": True},
                                    check_task=CheckTasks.err_res, check_items=error)

        # add a nullable vector field to the collection
        self.add_collection_field(client, collection_name, field_name="embeddings_3", 
                                  data_type=DataType.FLOAT_VECTOR, dim=dim, nullable=True)
        # try to alert the new added nullable vector field to non-nullable field
        self.alter_collection_field(client, collection_name, field_name="embeddings_3",
                                    field_params={"nullable": False},
                                    check_task=CheckTasks.err_res, check_items=error)
        # add a nullable varchar field to the collection
        self.add_collection_field(client, collection_name, field_name="varchar_3", 
                                  data_type=DataType.VARCHAR, max_length=64, nullable=True)
        # try to alert the new added nullable varchar field to non-nullable varchar field
        self.alter_collection_field(client, collection_name, field_name="varchar_3",
                                    field_params={"nullable": False},
                                    check_task=CheckTasks.err_res, check_items=error)

        # drop the collection
        self.drop_collection(client, collection_name)


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
        collection_name = cf.gen_collection_name_by_testcase_name()
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


class TestMilvusClientAlterCollectionFieldDescriptionValid(TestMilvusClientV2Base):
    """
    Positive tests for alter_collection_field() to change field description
    PR: https://github.com/milvus-io/milvus/pull/47057
    Issue: https://github.com/milvus-io/milvus/issues/46896
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_alter_field_description_basic(self):
        """
        target: test basic functionality of altering field description
        method: create collection, alter field description, verify change
        expected: field description updated successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection with default schema
        self.create_collection(client, collection_name, default_dim)

        # 2. Alter field description
        new_description = "This is a new description for vector field"
        self.alter_collection_field(
            client, collection_name,
            field_name=default_vector_field_name,
            field_params={"field.description": new_description}
        )

        # 3. Verify description changed
        desc_res = self.describe_collection(client, collection_name)[0]
        for field in desc_res.get("fields", []):
            if field.get("name") == default_vector_field_name:
                assert field.get("description") == new_description, \
                    f"Expected description '{new_description}', got '{field.get('description')}'"
                break

        # 4. Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_primary_key_field_description(self):
        """
        target: test altering primary key field description
        method: alter the description of primary key field
        expected: description updated successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection
        self.create_collection(client, collection_name, default_dim)

        # 2. Alter primary key field description
        new_description = "Primary key field for entity identification"
        self.alter_collection_field(
            client, collection_name,
            field_name=default_primary_key_field_name,
            field_params={"field.description": new_description}
        )

        # 3. Verify
        desc_res = self.describe_collection(client, collection_name)[0]
        for field in desc_res.get("fields", []):
            if field.get("name") == default_primary_key_field_name:
                assert field.get("description") == new_description
                break

        # 4. Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_vector_field_description(self):
        """
        target: test altering vector field description
        method: alter the description of vector field
        expected: description updated successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection with custom schema
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim,
                         description="original description")
        self.create_collection(client, collection_name, schema=schema)

        # 2. Alter vector field description
        new_description = "Dense embedding vector for similarity search"
        self.alter_collection_field(
            client, collection_name,
            field_name="embedding",
            field_params={"field.description": new_description}
        )

        # 3. Verify
        desc_res = self.describe_collection(client, collection_name)[0]
        for field in desc_res.get("fields", []):
            if field.get("name") == "embedding":
                assert field.get("description") == new_description
                break

        # 4. Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_scalar_field_description(self):
        """
        target: test altering scalar field description
        method: alter the description of a scalar field (VarChar)
        expected: description updated successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection with scalar field
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("title", DataType.VARCHAR, max_length=256,
                         description="original title description")
        self.create_collection(client, collection_name, schema=schema)

        # 2. Alter scalar field description
        new_description = "Title of the document"
        self.alter_collection_field(
            client, collection_name,
            field_name="title",
            field_params={"field.description": new_description}
        )

        # 3. Verify
        desc_res = self.describe_collection(client, collection_name)[0]
        for field in desc_res.get("fields", []):
            if field.get("name") == "title":
                assert field.get("description") == new_description
                break

        # 4. Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_field_description_multiple_times(self):
        """
        target: test altering field description multiple times
        method: alter the same field description consecutively
        expected: each alteration succeeds, idempotent behavior
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection
        self.create_collection(client, collection_name, default_dim)

        # 2. Alter description multiple times
        descriptions = [
            "First description",
            "Second description",
            "Third description",
            "Final description"
        ]

        for desc in descriptions:
            self.alter_collection_field(
                client, collection_name,
                field_name=default_vector_field_name,
                field_params={"field.description": desc}
            )

            # Verify each change
            desc_res = self.describe_collection(client, collection_name)[0]
            for field in desc_res.get("fields", []):
                if field.get("name") == default_vector_field_name:
                    assert field.get("description") == desc
                    break

        # 3. Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_alter_field_description_to_empty(self):
        """
        target: test altering field description to empty string
        method: set field description to empty string
        expected: description cleared successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection with field having description
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim,
                         description="This field has a description")
        self.create_collection(client, collection_name, schema=schema)

        # 2. Clear description
        self.alter_collection_field(
            client, collection_name,
            field_name=default_vector_field_name,
            field_params={"field.description": ""}
        )

        # 3. Verify description is empty
        desc_res = self.describe_collection(client, collection_name)[0]
        for field in desc_res.get("fields", []):
            if field.get("name") == default_vector_field_name:
                assert field.get("description") == ""
                break

        # 4. Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("description", [
        "中文描述测试",
        "Description with emoji 🚀🎉",
        "Line1\nLine2\nLine3",
        "Tab\tseparated\tvalues",
        "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?",
        " " * 100,  # Many spaces
    ])
    def test_alter_field_description_special_characters(self, description):
        """
        target: test altering field description with special characters
        method: set description containing unicode, emoji, newlines, etc.
        expected: description updated successfully with special characters preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection
        self.create_collection(client, collection_name, default_dim)

        # 2. Alter with special description
        self.alter_collection_field(
            client, collection_name,
            field_name=default_vector_field_name,
            field_params={"field.description": description}
        )

        # 3. Verify
        desc_res = self.describe_collection(client, collection_name)[0]
        for field in desc_res.get("fields", []):
            if field.get("name") == default_vector_field_name:
                assert field.get("description") == description
                break

        # 4. Cleanup
        self.drop_collection(client, collection_name)


class TestMilvusClientAlterCollectionFieldDescriptionInvalid(TestMilvusClientV2Base):
    """
    Negative tests for alter_collection_field() to change field description
    PR: https://github.com/milvus-io/milvus/pull/47057
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_field_description_nonexistent_collection(self):
        """
        target: test altering field description on non-existent collection
        method: call alter_collection_field with non-existent collection name
        expected: raise exception with error code 100
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Do not create collection, directly alter
        error = {ct.err_code: 100,
                 ct.err_msg: "collection not found"}

        self.alter_collection_field(
            client, collection_name,
            field_name=default_vector_field_name,
            field_params={"field.description": "new description"},
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_field_description_nonexistent_field(self):
        """
        target: test altering description of non-existent field
        method: call alter_collection_field with non-existent field name
        expected: raise exception indicating field not found
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection
        self.create_collection(client, collection_name, default_dim)

        # 2. Alter non-existent field
        error = {ct.err_code: 1100,
                 ct.err_msg: "does not exist in collection"}

        self.alter_collection_field(
            client, collection_name,
            field_name="nonexistent_field",
            field_params={"field.description": "new description"},
            check_task=CheckTasks.err_res,
            check_items=error
        )

        # 3. Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_field_description_empty_collection_name(self):
        """
        target: test altering field description with empty collection name
        method: call alter_collection_field with empty string collection name
        expected: raise exception with error code 1
        """
        client = self._client()
        collection_name = ""

        error = {ct.err_code: 1,
                 ct.err_msg: f"`collection_name` value {collection_name} is illegal"}

        self.alter_collection_field(
            client, collection_name,
            field_name=default_vector_field_name,
            field_params={"field.description": "new description"},
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_field_description_empty_field_name(self):
        """
        target: test altering field description with empty field name
        method: call alter_collection_field with empty string field name
        expected: raise exception indicating invalid field name
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. Create collection
        self.create_collection(client, collection_name, default_dim)

        # 2. Alter with empty field name
        error = {ct.err_code: 1100,
                 ct.err_msg: "does not exist in collection"}

        self.alter_collection_field(
            client, collection_name,
            field_name="",
            field_params={"field.description": "new description"},
            check_task=CheckTasks.err_res,
            check_items=error
        )

        # 3. Cleanup
        self.drop_collection(client, collection_name)



class TestMilvusClientWarmup(TestMilvusClientV2Base):
    """Warmup feature tests: field/collection/index level warmup configuration"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_schema_add_field_warmup_describe(self):
        """
        target: verify add_field(warmup=...) sets field warmup at schema creation, describe returns correct values
        method: create schema with warmup on multiple fields, describe_collection, insert/search/query
        expected: describe returns correct warmup per field, search/query work normally
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create schema with warmup on add_field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")
        schema.add_field("int64_field", DataType.INT64, nullable=True, warmup="disable")
        schema.add_field("float_field", DataType.FLOAT, nullable=True, warmup="sync")
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256, nullable=True, warmup="disable")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # 2. describe_collection verify field warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "int64_field") == "disable"
        assert cf.get_field_warmup(res, "float_field") == "sync"
        assert cf.get_field_warmup(res, "varchar_field") == "disable"
        assert cf.get_field_warmup(res, "pk") is None

        # 3. insert data & flush & load
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # 4. search & query
        vectors_to_search = cf.gen_vectors(1, default_warmup_dim)
        search_res = self.search(client, collection_name, vectors_to_search, limit=default_limit,
                                 output_fields=["int64_field", "varchar_field"])[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(client, collection_name, filter="pk >= 0", limit=default_limit,
                               output_fields=["pk", "int64_field", "float_field", "varchar_field"])[0]
        assert len(query_res) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_all_data_types_add_field_warmup(self):
        """
        target: verify all data types support add_field(warmup=...), describe returns correctly
        method: create schema with all data types + warmup="disable", describe, insert/search/query
        expected: all fields show warmup=disable in describe, search/query work normally
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec_float", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="disable")
        schema.add_field("vec_f16", DataType.FLOAT16_VECTOR, dim=default_warmup_dim, warmup="disable", nullable=True)
        schema.add_field("vec_bf16", DataType.BFLOAT16_VECTOR, dim=default_warmup_dim, warmup="disable", nullable=True)
        schema.add_field("vec_sparse", DataType.SPARSE_FLOAT_VECTOR, warmup="disable", nullable=True)
        schema.add_field("int64_1", DataType.INT64, warmup="disable", nullable=True)
        schema.add_field("float_1", DataType.FLOAT, warmup="disable", nullable=True)
        schema.add_field("double_1", DataType.DOUBLE, warmup="disable", nullable=True)
        schema.add_field("varchar_1", DataType.VARCHAR, max_length=256, warmup="disable", nullable=True)
        schema.add_field("bool_1", DataType.BOOL, warmup="disable", nullable=True)
        schema.add_field("array_int64_1", DataType.ARRAY, element_type=DataType.INT64,
                         max_capacity=10, warmup="disable", nullable=True)
        schema.add_field("array_float_1", DataType.ARRAY, element_type=DataType.FLOAT,
                         max_capacity=10, warmup="disable", nullable=True)
        schema.add_field("array_varchar_1", DataType.ARRAY, element_type=DataType.VARCHAR,
                         max_length=256, max_capacity=10, warmup="disable", nullable=True)
        schema.add_field("json_1", DataType.JSON, warmup="disable", nullable=True)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec_float", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        index_params.add_index(field_name="vec_f16", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        index_params.add_index(field_name="vec_bf16", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        index_params.add_index(field_name="vec_sparse", index_type="SPARSE_INVERTED_INDEX",
                               metric_type="IP")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # describe verify all fields
        res = self.describe_collection(client, collection_name)[0]
        warmup_fields = ["vec_float", "vec_f16", "vec_bf16", "vec_sparse",
                         "int64_1", "float_1", "double_1", "varchar_1", "bool_1",
                         "array_int64_1", "array_float_1", "array_varchar_1", "json_1"]
        for field_name in warmup_fields:
            actual = cf.get_field_warmup(res, field_name)
            assert actual == "disable", f"field {field_name}: expected warmup='disable', got '{actual}'"
        assert cf.get_field_warmup(res, "pk") is None

        # insert & load & search & query
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit,
                                 anns_field="vec_float",
                                 output_fields=["int64_1", "varchar_1", "json_1", "array_int64_1"])[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(client, collection_name, filter="pk >= 0", limit=5,
                               output_fields=["pk", "int64_1", "float_1", "double_1",
                                              "varchar_1", "bool_1", "json_1",
                                              "array_int64_1", "array_float_1", "array_varchar_1"])[0]
        assert len(query_res) == 5
        for row in query_res:
            assert "int64_1" in row
            assert "json_1" in row

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_schema_invalid_warmup_values(self):
        """
        target: verify add_field with invalid warmup values is rejected at create_collection
        method: create schema with invalid/empty/case-sensitive warmup values
        expected: create_collection fails with error
        """
        client = self._client()

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})

        # 3.1 invalid value
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="invalid_value")
        collection_name = cf.gen_collection_name_by_testcase_name()
        error = {ct.err_code: 1100, ct.err_msg: "invalid warmup policy"}
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               check_task=CheckTasks.err_res, check_items=error)

        # 3.2 empty string
        schema2 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema2.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema2.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="")
        collection_name2 = cf.gen_collection_name_by_testcase_name() + "_2"
        self.create_collection(client, collection_name2, schema=schema2, index_params=index_params,
                               check_task=CheckTasks.err_res, check_items=error)

        # 3.3 case-sensitive "Sync"
        schema3 = self.create_schema(client, enable_dynamic_field=False)[0]
        schema3.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema3.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="Sync")
        collection_name3 = cf.gen_collection_name_by_testcase_name() + "_3"
        self.create_collection(client, collection_name3, schema=schema3, index_params=index_params,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_collection_level_create_describe(self):
        """
        target: verify create_collection with collection-level warmup properties, describe returns correctly
        method: create collection with warmup.* properties, describe, insert/search/query
        expected: describe shows collection warmup in properties, fields have no warmup, search/query work
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("float_field", DataType.FLOAT)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})

        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={
                                   "warmup.scalarField": "sync",
                                   "warmup.scalarIndex": "disable",
                                   "warmup.vectorField": "sync",
                                   "warmup.vectorIndex": "disable"
                               })

        # describe verify collection properties
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "sync"
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # field params should NOT have warmup
        assert cf.get_field_warmup(res, "vec") is None
        assert cf.get_field_warmup(res, "float_field") is None
        assert cf.get_field_warmup(res, "varchar_field") is None

        # insert & load & search & query
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        vectors_to_search = cf.gen_vectors(1, default_warmup_dim)
        search_res = self.search(client, collection_name, vectors_to_search, limit=default_limit,
                                 output_fields=["pk", "float_field"])[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(client, collection_name, filter="pk >= 0", limit=default_limit,
                               output_fields=["pk", "float_field", "varchar_field"])[0]
        assert len(query_res) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_field_and_collection_coexist(self):
        """
        target: verify field + collection warmup can coexist, field level has higher priority
        method: create schema with field warmup + collection warmup, describe both
        expected: both levels visible in describe, field level overrides collection level
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")
        schema.add_field("int_field", DataType.INT64, warmup="sync", nullable=True)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256, nullable=True)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})

        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={
                                   "warmup.scalarField": "disable",
                                   "warmup.vectorField": "disable"
                               })

        # describe verify both levels
        res = self.describe_collection(client, collection_name)[0]
        # collection level
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"
        # field level (explicitly set)
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "int_field") == "sync"
        # not set at field level (inherits collection)
        assert cf.get_field_warmup(res, "varchar_field") is None
        assert cf.get_field_warmup(res, "pk") is None

        # insert & load & search & query
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit,
                                 output_fields=["int_field", "varchar_field"])[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(client, collection_name, filter="pk >= 0", limit=default_limit,
                               output_fields=["pk", "int_field", "varchar_field"])[0]
        assert len(query_res) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_alter_collection_field_warmup(self):
        """
        target: verify alter_collection_field modifies field warmup, describe reflects changes
        method: create collection with custom schema, alter field warmup step by step, describe each time
        expected: describe shows correct warmup after each alter, search works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # create collection with explicit schema (including varchar_field)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # initial state: no warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") is None
        assert cf.get_field_warmup(res, "varchar_field") is None

        # release → set vec warmup=sync
        self.release_collection(client, collection_name)
        self.alter_collection_field(client, collection_name,
                                    field_name="vec", field_params={"warmup": "sync"})

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "varchar_field") is None

        # set varchar_field warmup=disable
        self.alter_collection_field(client, collection_name,
                                    field_name="varchar_field", field_params={"warmup": "disable"})

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "varchar_field") == "disable"

        # modify vec warmup sync → disable
        self.alter_collection_field(client, collection_name,
                                    field_name="vec", field_params={"warmup": "disable"})

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"
        assert cf.get_field_warmup(res, "varchar_field") == "disable"

        # insert & load & search
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit,
                                 output_fields=["varchar_field"])[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_alter_drop_collection_warmup(self):
        """
        target: verify alter/drop collection warmup properties + describe correctness
        method: alter collection warmup, modify, drop partially, drop all, describe each step
        expected: describe reflects all changes correctly, search works at each stage
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # create collection & insert data
        self.create_collection(client, collection_name, default_warmup_dim, consistency_level="Strong")
        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_warmup_dim).astype(np.float32)),
                 "float": float(i), "varchar": str(i)} for i in range(default_warmup_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # initial: no warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") is None
        assert cf.get_collection_warmup(res, "warmup.scalarField") is None

        # release → alter 4 warmup properties
        self.release_collection(client, collection_name)
        self.alter_collection_properties(client, collection_name,
                                         properties={
                                             "warmup.vectorField": "sync",
                                             "warmup.scalarField": "disable",
                                             "warmup.vectorIndex": "disable",
                                             "warmup.scalarIndex": "sync"
                                         })

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "sync"
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") == "sync"

        # load & search
        self.load_collection(client, collection_name)
        vectors_to_search = cf.gen_vectors(1, default_warmup_dim)
        search_res = self.search(client, collection_name, vectors_to_search, limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → partial modify (swap 2 keys)
        self.release_collection(client, collection_name)
        self.alter_collection_properties(client, collection_name,
                                         properties={
                                             "warmup.vectorField": "disable",
                                             "warmup.scalarField": "sync"
                                         })

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") == "sync"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, vectors_to_search, limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → drop partial warmup properties
        self.release_collection(client, collection_name)
        self.drop_collection_properties(client, collection_name,
                                        property_keys=["warmup.vectorField", "warmup.scalarField"])

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") is None
        assert cf.get_collection_warmup(res, "warmup.scalarField") is None
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") == "sync"

        # drop remaining warmup properties
        self.drop_collection_properties(client, collection_name,
                                        property_keys=["warmup.vectorIndex", "warmup.scalarIndex"])

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") is None
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") is None

        # reload & search (fallback to cluster default)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, vectors_to_search, limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_warmup_index_level_full_lifecycle(self):
        """
        target: verify index level warmup create/alter/drop full lifecycle with describe_index
        method: create index with warmup, alter, drop, describe_index each step
        expected: describe_index reflects warmup state correctly, search works at each stage
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200, "warmup": "disable"})
        index_params.add_index(field_name="varchar_field", index_type="INVERTED")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # describe_index: vec should have warmup=disable
        vec_idx_res = self.describe_index(client, collection_name, "vec")[0]
        assert cf.get_index_warmup(vec_idx_res) == "disable"
        assert vec_idx_res["index_type"] == "HNSW"
        assert vec_idx_res["metric_type"] == "L2"

        # scalar index: no warmup
        scalar_idx_names = self.list_indexes(client, collection_name, field_name="varchar_field")[0]
        scalar_idx_res = self.describe_index(client, collection_name, scalar_idx_names[0])[0]
        assert cf.get_index_warmup(scalar_idx_res) is None

        # insert data & flush & load & search
        res = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → alter vector index warmup to sync
        self.release_collection(client, collection_name)
        vec_idx_names = self.list_indexes(client, collection_name, field_name="vec")[0]
        self.alter_index_properties(client, collection_name,
                                    index_name=vec_idx_names[0], properties={"warmup": "sync"})

        vec_idx_res = self.describe_index(client, collection_name, vec_idx_names[0])[0]
        assert cf.get_index_warmup(vec_idx_res) == "sync"
        assert vec_idx_res["index_type"] == "HNSW"
        assert vec_idx_res["M"] == "16"

        # alter scalar index warmup to disable
        self.alter_index_properties(client, collection_name,
                                    index_name=scalar_idx_names[0], properties={"warmup": "disable"})
        scalar_idx_res = self.describe_index(client, collection_name, scalar_idx_names[0])[0]
        assert cf.get_index_warmup(scalar_idx_res) == "disable"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → modify vector warmup sync → disable
        self.release_collection(client, collection_name)
        self.alter_index_properties(client, collection_name,
                                    index_name=vec_idx_names[0], properties={"warmup": "disable"})
        vec_idx_res = self.describe_index(client, collection_name, vec_idx_names[0])[0]
        assert cf.get_index_warmup(vec_idx_res) == "disable"

        # drop vector index warmup
        self.drop_index_properties(client, collection_name,
                                   index_name=vec_idx_names[0], property_keys=["warmup"])
        vec_idx_res = self.describe_index(client, collection_name, vec_idx_names[0])[0]
        assert cf.get_index_warmup(vec_idx_res) is None
        assert vec_idx_res["index_type"] == "HNSW"
        assert vec_idx_res["M"] == "16"

        # drop scalar index warmup
        self.drop_index_properties(client, collection_name,
                                   index_name=scalar_idx_names[0], property_keys=["warmup"])
        scalar_idx_res = self.describe_index(client, collection_name, scalar_idx_names[0])[0]
        assert cf.get_index_warmup(scalar_idx_res) is None

        # reload & search (fallback to upper level)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_three_level_priority(self):
        """
        target: verify Field > Collection > Cluster priority + drop fallback behavior
        method: set field + collection warmup, alter, drop collection warmup, describe each step
        expected: field level overrides collection, drop collection does not affect field level
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("int32_field", DataType.INT32, warmup="sync", nullable=True)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256, nullable=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={
                                   "warmup.scalarField": "disable",
                                   "warmup.vectorField": "disable"
                               })

        # describe initial state
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"
        assert cf.get_field_warmup(res, "int32_field") == "sync"  # field > collection
        assert cf.get_field_warmup(res, "vec") == "sync"           # field > collection
        assert cf.get_field_warmup(res, "varchar_field") is None   # inherits collection

        # insert & load & search & query
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit,
                                 output_fields=["int32_field", "varchar_field"])[0]
        assert len(search_res[0]) == default_limit

        query_res = self.query(client, collection_name, filter="pk >= 0", limit=default_limit,
                               output_fields=["pk", "int32_field", "varchar_field"])[0]
        assert len(query_res) == default_limit

        # release → alter field warmup
        self.release_collection(client, collection_name)
        self.alter_collection_field(client, collection_name,
                                    field_name="int32_field", field_params={"warmup": "disable"})

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "int32_field") == "disable"
        assert cf.get_field_warmup(res, "vec") == "sync"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → drop collection warmup
        self.release_collection(client, collection_name)
        self.drop_collection_properties(client, collection_name,
                                        property_keys=["warmup.scalarField", "warmup.vectorField"])

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.scalarField") is None
        assert cf.get_collection_warmup(res, "warmup.vectorField") is None
        # field level warmup unaffected
        assert cf.get_field_warmup(res, "int32_field") == "disable"
        assert cf.get_field_warmup(res, "vec") == "sync"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_index_overrides_collection(self):
        """
        target: verify Index > Collection priority + drop index warmup fallback
        method: set collection warmup, alter index warmup to override, drop index warmup
        expected: index level overrides collection, drop index warmup falls back to collection
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={"warmup.vectorIndex": "disable"})

        # describe: collection level set
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # describe_index: no index warmup
        idx_names = self.list_indexes(client, collection_name, field_name="vec")[0]
        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) is None

        # insert & flush
        rows = cf.gen_row_data_by_schema(nb=default_warmup_nb, schema=res)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # release → alter index warmup=sync (override collection disable)
        self.release_collection(client, collection_name)
        self.alter_index_properties(client, collection_name,
                                    index_name=idx_names[0], properties={"warmup": "sync"})

        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) == "sync"
        # collection level unaffected
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # load & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        # release → drop index warmup
        self.release_collection(client, collection_name)
        self.drop_index_properties(client, collection_name,
                                   index_name=idx_names[0], property_keys=["warmup"])

        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) is None
        # collection level still present
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # reload & search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_negative_cases(self):
        """
        target: verify all negative cases: invalid values, key mismatch, non-existent targets, loaded state
        method: test various invalid warmup operations
        expected: all return proper error codes/messages, no side effects on existing state
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_warmup_dim, consistency_level="Strong")
        self.release_collection(client, collection_name)

        invalid_warmup_error = {ct.err_code: 1100, ct.err_msg: "invalid warmup policy"}

        # 11.1 alter_collection_field invalid warmup values
        invalid_values = ["invalid", "Sync", "DISABLE", "true", "async"]
        for val in invalid_values:
            self.alter_collection_field(client, collection_name,
                                        field_name="vector", field_params={"warmup": val},
                                        check_task=CheckTasks.err_res, check_items=invalid_warmup_error)

        # 11.2 alter_collection_properties invalid values
        invalid_collection_error = {ct.err_code: 1100, ct.err_msg: "invalid warmup"}
        self.alter_collection_properties(client, collection_name,
                                         properties={"warmup.vectorField": "invalid"},
                                         check_task=CheckTasks.err_res, check_items=invalid_collection_error)
        self.alter_collection_properties(client, collection_name,
                                         properties={"warmup.scalarIndex": "SYNC"},
                                         check_task=CheckTasks.err_res, check_items=invalid_collection_error)

        # 11.3 key level mismatch: field key "warmup" at collection level
        self.alter_collection_properties(client, collection_name,
                                         properties={"warmup": "sync"},
                                         check_task=CheckTasks.err_res,
                                         check_items={ct.err_code: 1100, ct.err_msg: "warmup"})

        # 11.3 collection key at field level - server accepts arbitrary key names via
        # alter_collection_field, verify it doesn't affect the actual warmup setting
        self.alter_collection_field(client, collection_name,
                                    field_name="vector", field_params={"warmup.vectorField": "sync"})
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vector") is None  # warmup key not set via mismatched key

        # 11.4 non-existent field/index
        self.alter_collection_field(client, collection_name,
                                    field_name="not_exist_field", field_params={"warmup": "sync"},
                                    check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1100, ct.err_msg: "does not exist"})

        self.alter_index_properties(client, collection_name,
                                    index_name="not_exist_index", properties={"warmup": "sync"},
                                    check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1100, ct.err_msg: "not found"})

        # 11.5 alter warmup on loaded collection
        self.load_collection(client, collection_name)

        self.alter_collection_properties(client, collection_name,
                                         properties={"warmup.vectorField": "sync"},
                                         check_task=CheckTasks.err_res,
                                         check_items={ct.err_code: 1100,
                                                      ct.err_msg: "can not alter warmup properties if collection loaded"})

        self.alter_collection_field(client, collection_name,
                                    field_name="vector", field_params={"warmup": "sync"},
                                    check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1100,
                                                 ct.err_msg: "can not alter warmup if collection loaded"})

        # 11.6 create_collection with invalid warmup
        bad_col = cf.gen_collection_name_by_testcase_name() + "_bad"
        self.create_collection(client, bad_col, default_warmup_dim,
                               properties={"warmup.vectorIndex": "invalid"},
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 1100, ct.err_msg: "invalid warmup"})

        # 11.7 create_index with invalid warmup
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="COSINE",
                               params={"M": 16, "efConstruction": 200, "warmup": "bad_value"})
        self.create_index(client, collection_name, index_params,
                          check_task=CheckTasks.err_res,
                          check_items={ct.err_code: 1100, ct.err_msg: "invalid warmup policy"})

        # 11.8 alter_index_properties invalid warmup
        index_params2 = self.prepare_index_params(client)[0]
        index_params2.add_index(field_name="vector", index_type="HNSW", metric_type="COSINE",
                                params={"M": 16, "efConstruction": 200})
        self.create_index(client, collection_name, index_params2)
        idx_names = self.list_indexes(client, collection_name, field_name="vector")[0]
        self.alter_index_properties(client, collection_name,
                                    index_name=idx_names[0], properties={"warmup": "invalid"},
                                    check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1100, ct.err_msg: "invalid warmup policy"})

        # verify no side effect after errors
        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) is None
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vector") is None

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_sync_vs_disable_data_correctness(self):
        """
        target: verify sync and disable warmup produce identical search/query/retrieve results
        method: insert deterministic data, search/query under sync, switch to disable, compare
        expected: all results identical between sync and disable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("float_field", DataType.FLOAT, warmup="sync")
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256, warmup="sync")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={
                                   "warmup.scalarField": "sync", "warmup.scalarIndex": "sync",
                                   "warmup.vectorField": "sync", "warmup.vectorIndex": "sync"
                               })

        # insert deterministic data
        rng = np.random.default_rng(seed=42)
        rows = [{"pk": i, "float_field": float(i), "varchar_field": f"text_{i}",
                 "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(default_warmup_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # verify sync state
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "sync"

        # search + query under sync
        query_vec = [list(rng.random(default_warmup_dim).astype(np.float32))]
        search_sync = self.search(client, collection_name, query_vec, limit=20,
                                  output_fields=["pk", "float_field", "varchar_field"])[0]
        assert len(search_sync[0]) == 20

        query_sync = self.query(client, collection_name,
                                filter="float_field >= 100 and float_field < 200",
                                output_fields=["pk", "float_field", "varchar_field"])[0]
        sync_query_pks = sorted([r["pk"] for r in query_sync])

        retrieve_sync = self.query(client, collection_name, filter="pk >= 0 and pk < 5",
                                   output_fields=["pk", "vec"])[0]
        sync_vectors = {r["pk"]: r["vec"] for r in retrieve_sync}

        # release → switch to all disable
        self.release_collection(client, collection_name)
        for field_name in ["vec", "float_field", "varchar_field"]:
            self.alter_collection_field(client, collection_name,
                                        field_name=field_name, field_params={"warmup": "disable"})
        self.alter_collection_properties(client, collection_name,
                                         properties={
                                             "warmup.scalarField": "disable", "warmup.scalarIndex": "disable",
                                             "warmup.vectorField": "disable", "warmup.vectorIndex": "disable"
                                         })

        # verify disable state
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "disable"

        # reload & same operations under disable
        self.load_collection(client, collection_name)

        search_disable = self.search(client, collection_name, query_vec, limit=20,
                                     output_fields=["pk", "float_field", "varchar_field"])[0]
        assert len(search_disable[0]) == 20  # search works under disable

        query_disable = self.query(client, collection_name,
                                   filter="float_field >= 100 and float_field < 200",
                                   output_fields=["pk", "float_field", "varchar_field"])[0]
        disable_query_pks = sorted([r["pk"] for r in query_disable])

        retrieve_disable = self.query(client, collection_name, filter="pk >= 0 and pk < 5",
                                      output_fields=["pk", "vec"])[0]
        disable_vectors = {r["pk"]: r["vec"] for r in retrieve_disable}

        # deterministic operations (query/retrieve) should produce identical results
        assert sync_query_pks == disable_query_pks, "query results should be identical"
        assert sync_vectors == disable_vectors, "retrieved vectors should be identical"

        # disable mode: repeated search consistency (same load, should be deterministic)
        search_2nd = self.search(client, collection_name, query_vec, limit=20, output_fields=["pk"])[0]
        disable_pks = [hit["pk"] for hit in search_disable[0]]
        assert disable_pks == [hit["pk"] for hit in search_2nd[0]]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index_type", ["HNSW", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "FLAT",
                                            "SCANN", "DISKANN", "BIN_FLAT", "BIN_IVF_FLAT", "AUTOINDEX"])
    def test_warmup_vector_index_types(self, index_type):
        """
        target: verify various vector index types work with warmup
        method: parametrize index types, create with field warmup, alter index warmup, search
        expected: describe correct, search works under both warmup modes
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        if index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
            vec_type, dim, metric = DataType.BINARY_VECTOR, default_warmup_dim, "HAMMING"
        else:
            vec_type, dim, metric = DataType.FLOAT_VECTOR, default_warmup_dim, "L2"

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", vec_type, dim=dim, warmup="disable")

        index_params = self.prepare_index_params(client)[0]
        extra_params = {}
        if index_type == "IVF_FLAT":
            extra_params = {"nlist": 128}
        elif index_type == "IVF_SQ8":
            extra_params = {"nlist": 128}
        elif index_type == "IVF_PQ":
            extra_params = {"nlist": 128, "m": 16, "nbits": 8}
        elif index_type == "HNSW":
            extra_params = {"M": 16, "efConstruction": 200}
        elif index_type == "SCANN":
            extra_params = {"nlist": 128}
        elif index_type == "BIN_IVF_FLAT":
            extra_params = {"nlist": 128}
        index_params.add_index(field_name="vec", index_type=index_type,
                               metric_type=metric, params=extra_params)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # describe field warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"

        # insert & flush
        rows = [{"pk": i, "vec": cf.gen_vectors(1, dim, vec_type)[0]} for i in range(default_warmup_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # load & search (warmup=disable)
        self.load_collection(client, collection_name)
        query_vec = cf.gen_vectors(1, dim, vec_type)
        res_disable = self.search(client, collection_name, query_vec, limit=default_limit)[0]
        assert len(res_disable[0]) == default_limit

        # release → alter index warmup=sync → describe_index → reload & search
        self.release_collection(client, collection_name)
        idx_names = self.list_indexes(client, collection_name, field_name="vec")[0]
        self.alter_index_properties(client, collection_name,
                                    index_name=idx_names[0], properties={"warmup": "sync"})

        idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) == "sync"
        assert idx_res["index_type"] == index_type

        # field warmup unchanged
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"

        self.load_collection(client, collection_name)
        res_sync = self.search(client, collection_name, query_vec, limit=default_limit)[0]
        assert len(res_sync[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("scalar_index_type", ["STL_SORT", "TRIE", "BITMAP", "INVERTED", "AUTOINDEX"])
    def test_warmup_scalar_index_types(self, scalar_index_type):
        """
        target: verify various scalar index types work with warmup
        method: parametrize scalar index types, create with field warmup, alter index warmup, filtered search
        expected: describe correct, filtered search works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        if scalar_index_type == "TRIE":
            scalar_type = DataType.VARCHAR
            field_kwargs = {"max_length": 256}
        else:
            scalar_type = DataType.INT64
            field_kwargs = {}

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("scalar_field", scalar_type, warmup="disable", **field_kwargs)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        index_params.add_index(field_name="scalar_field", index_type=scalar_index_type)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # describe field warmup
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "scalar_field") == "disable"

        # insert data
        rng = np.random.default_rng(seed=19530)
        if scalar_type == DataType.VARCHAR:
            rows = [{"pk": i, "scalar_field": f"val_{i}",
                     "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(default_warmup_nb)]
            filter_expr = 'scalar_field like "val_1%"'
        else:
            rows = [{"pk": i, "scalar_field": i,
                     "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(default_warmup_nb)]
            filter_expr = "scalar_field >= 100 and scalar_field < 200"
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # load & filtered search (warmup=disable)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit,
                                 filter=filter_expr)[0]
        assert len(search_res[0]) > 0

        # release → alter scalar index warmup=sync → describe_index
        self.release_collection(client, collection_name)
        scalar_idx_names = self.list_indexes(client, collection_name, field_name="scalar_field")[0]
        self.alter_index_properties(client, collection_name,
                                    index_name=scalar_idx_names[0], properties={"warmup": "sync"})

        idx_res = self.describe_index(client, collection_name, scalar_idx_names[0])[0]
        assert cf.get_index_warmup(idx_res) == "sync"
        assert idx_res["index_type"] == scalar_index_type

        # field warmup unchanged
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "scalar_field") == "disable"

        # reload & filtered search
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit,
                                 filter=filter_expr)[0]
        assert len(search_res[0]) > 0

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_x_mmap(self):
        """
        target: verify warmup and mmap are orthogonal, all 4 combinations work identically
        method: iterate 4 warmup x mmap combinations, describe and search each
        expected: describe_index shows both warmup and mmap.enabled, results identical
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # insert data
        rng = np.random.default_rng(seed=19530)
        rows = [{"pk": i, "varchar_field": f"text_{i}",
                 "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(default_warmup_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        query_vec = cf.gen_vectors(1, default_warmup_dim)
        all_pks = []

        for warmup_val, mmap_val in [("sync", "true"), ("sync", "false"),
                                      ("disable", "true"), ("disable", "false")]:
            self.release_collection(client, collection_name)
            self.alter_collection_properties(client, collection_name,
                                             properties={"warmup.vectorField": warmup_val,
                                                         "warmup.scalarField": warmup_val})
            self.alter_collection_field(client, collection_name,
                                        field_name="vec", field_params={"mmap.enabled": mmap_val})
            self.alter_collection_field(client, collection_name,
                                        field_name="varchar_field", field_params={"mmap.enabled": mmap_val})

            idx_names = self.list_indexes(client, collection_name, field_name="vec")[0]
            self.alter_index_properties(client, collection_name,
                                        index_name=idx_names[0],
                                        properties={"mmap.enabled": True, "warmup": warmup_val})

            # describe_collection verify collection warmup
            res = self.describe_collection(client, collection_name)[0]
            assert cf.get_collection_warmup(res, "warmup.vectorField") == warmup_val
            assert cf.get_collection_warmup(res, "warmup.scalarField") == warmup_val

            # describe_index verify warmup and mmap coexist
            idx_res = self.describe_index(client, collection_name, idx_names[0])[0]
            assert cf.get_index_warmup(idx_res) == warmup_val
            assert idx_res.get("mmap.enabled") == "True"

            self.load_collection(client, collection_name)
            search_res = self.search(client, collection_name, query_vec, limit=default_limit,
                                     output_fields=["varchar_field"])[0]
            assert len(search_res[0]) == default_limit
            all_pks.append([hit["pk"] for hit in search_res[0]])

        # verify all combinations return identical results
        for pks in all_pks[1:]:
            assert pks == all_pks[0], "all warmup x mmap combinations should return identical results"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_x_partition(self):
        """
        target: verify warmup works with partitions
        method: create partitions, insert, load single partition, search, then load all
        expected: partition load respects warmup, search works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("category", DataType.VARCHAR, max_length=64, warmup="disable")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # describe verify
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_field_warmup(res, "category") == "disable"

        # create partitions & insert
        self.create_partition(client, collection_name, partition_name="part_a")
        self.create_partition(client, collection_name, partition_name="part_b")

        rng = np.random.default_rng(seed=19530)
        rows_a = [{"pk": i, "category": "cat_a",
                   "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(1000)]
        rows_b = [{"pk": i + 1000, "category": "cat_b",
                   "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(1000)]
        self.insert(client, collection_name, rows_a, partition_name="part_a")
        self.insert(client, collection_name, rows_b, partition_name="part_b")
        self.flush(client, collection_name)

        # load single partition → search
        self.load_partitions(client, collection_name, partition_names=["part_a"])
        query_vec = cf.gen_vectors(1, default_warmup_dim)
        search_res = self.search(client, collection_name, query_vec, limit=default_limit,
                                 partition_names=["part_a"])[0]
        assert len(search_res[0]) == default_limit
        for hit in search_res[0]:
            assert hit["pk"] < 1000

        # load second partition → search all
        self.load_partitions(client, collection_name, partition_names=["part_b"])
        search_all = self.search(client, collection_name, query_vec, limit=default_limit)[0]
        assert len(search_all[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_x_compaction(self):
        """
        target: verify compacted segments respect warmup, data consistent
        method: insert in batches, compact, compare search results before/after
        expected: search results identical, data count consistent
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={"warmup.vectorField": "sync", "warmup.vectorIndex": "sync"})

        # insert in batches to produce multiple segments
        rng = np.random.default_rng(seed=19530)
        for batch in range(5):
            rows = [{"pk": batch * 400 + i,
                     "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(400)]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

        # load & search before compact
        self.load_collection(client, collection_name)
        query_vec = cf.gen_vectors(1, default_warmup_dim)
        search_before = self.search(client, collection_name, query_vec, limit=20, output_fields=["pk"])[0]
        assert len(search_before[0]) == 20

        # compact & wait
        self.compact(client, collection_name)
        time.sleep(10)

        # release → reload → search after compact
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        search_after = self.search(client, collection_name, query_vec, limit=20, output_fields=["pk"])[0]
        assert len(search_after[0]) == 20  # search still works after compaction with warmup

        # query count - data integrity preserved after compaction
        count_res = self.query(client, collection_name, filter="pk >= 0",
                               output_fields=["count(*)"])[0]
        assert count_res[0]["count(*)"] == 2000

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.Loadbalance)
    def test_warmup_x_multi_replica(self):
        """
        target: verify warmup works with multi-replica load
        method: load with 2 replicas, search multiple times
        expected: consistent results across replicas
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={"warmup.vectorIndex": "sync"})

        # insert & flush
        rng = np.random.default_rng(seed=19530)
        rows = [{"pk": i, "vec": list(rng.random(default_warmup_dim).astype(np.float32))}
                for i in range(default_warmup_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # release first (collection is auto-loaded after create), then load with 2 replicas
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name, timeout=60, replica_number=2)

        # search multiple times for consistency
        query_vec = cf.gen_vectors(1, default_warmup_dim)
        results = []
        for _ in range(5):
            res = self.search(client, collection_name, query_vec, limit=default_limit)[0]
            results.append([hit["pk"] for hit in res[0]])
        for r in results[1:]:
            assert r == results[0]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_x_growing_segment(self):
        """
        target: verify growing segment not affected by warmup, newly inserted data searchable
        method: load, then insert new data (growing segment), query/search
        expected: new data queryable and searchable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("tag", DataType.VARCHAR, max_length=64, warmup="disable")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="disable")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               consistency_level="Strong",
                               properties={
                                   "warmup.scalarField": "disable", "warmup.scalarIndex": "disable",
                                   "warmup.vectorField": "disable", "warmup.vectorIndex": "disable"
                               })

        # describe verify all disable
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"
        assert cf.get_field_warmup(res, "tag") == "disable"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"

        # insert initial data & flush & load
        rng = np.random.default_rng(seed=19530)
        rows = [{"pk": i, "tag": "old", "vec": list(rng.random(default_warmup_dim).astype(np.float32))}
                for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # insert after load (growing segment)
        new_rows = [{"pk": 1000 + i, "tag": "new",
                     "vec": list(rng.random(default_warmup_dim).astype(np.float32))} for i in range(500)]
        self.insert(client, collection_name, new_rows)

        # query new data
        query_res = self.query(client, collection_name,
                               filter='tag == "new"', limit=100, output_fields=["pk", "tag"])[0]
        assert len(query_res) > 0

        # search includes old + new
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim),
                                 limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_multi_vector_fields(self):
        """
        target: verify multi vector fields with independent warmup + collection vectorField override
        method: create 3 vector fields with different warmup, collection vectorField=disable
        expected: each field's warmup correct in describe, all 3 vector search work
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec_a", DataType.FLOAT_VECTOR, dim=128, warmup="sync")
        schema.add_field("vec_b", DataType.FLOAT_VECTOR, dim=64, warmup="disable")
        schema.add_field("vec_c", DataType.FLOAT_VECTOR, dim=32)  # no warmup set

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec_a", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        index_params.add_index(field_name="vec_b", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        index_params.add_index(field_name="vec_c", index_type="IVF_FLAT", metric_type="L2",
                               params={"nlist": 128})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={"warmup.vectorField": "disable"})

        # describe verify 3-level state
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "disable"
        assert cf.get_field_warmup(res, "vec_a") == "sync"    # field > collection
        assert cf.get_field_warmup(res, "vec_b") == "disable"
        assert cf.get_field_warmup(res, "vec_c") is None       # inherits collection

        # insert & load
        rng = np.random.default_rng(seed=19530)
        rows = [{"pk": i,
                 "vec_a": list(rng.random(128).astype(np.float32)),
                 "vec_b": list(rng.random(64).astype(np.float32)),
                 "vec_c": list(rng.random(32).astype(np.float32))} for i in range(default_warmup_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # search each vector field
        res_a = self.search(client, collection_name, cf.gen_vectors(1, 128), limit=default_limit,
                            anns_field="vec_a")[0]
        assert len(res_a[0]) == default_limit

        res_b = self.search(client, collection_name, cf.gen_vectors(1, 64), limit=default_limit,
                            anns_field="vec_b")[0]
        assert len(res_b[0]) == default_limit

        res_c = self.search(client, collection_name, cf.gen_vectors(1, 32), limit=default_limit,
                            anns_field="vec_c")[0]
        assert len(res_c[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_warmup_create_index_idempotent(self):
        """
        target: verify field/collection/index warmup do not break create_index idempotency
        method: set warmup at each level, create index twice
        expected: 21.1/21.2 second create_index succeeds (idempotent);
                  21.3 second create_index fails because index params contain warmup (distinct index)
        """
        client = self._client()

        # 21.1 field level warmup - create_index idempotent
        col1 = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, col1, default_warmup_dim, consistency_level="Strong")
        self.release_collection(client, col1)
        self.alter_collection_field(client, col1, field_name="vector",
                                    field_params={"warmup": "sync"})
        self.drop_index(client, col1, "vector")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="COSINE",
                               params={"M": 8, "efConstruction": 200})
        self.create_index(client, col1, index_params)
        self.create_index(client, col1, index_params)  # idempotent
        assert len(self.list_indexes(client, col1, field_name="vector")[0]) == 1

        # 21.2 collection level warmup - create_index idempotent
        col2 = cf.gen_collection_name_by_testcase_name() + "_2"
        self.create_collection(client, col2, default_warmup_dim, consistency_level="Strong")
        self.release_collection(client, col2)
        self.alter_collection_properties(client, col2,
                                         properties={"warmup.vectorField": "sync"})
        self.drop_index(client, col2, "vector")
        index_params2 = self.prepare_index_params(client)[0]
        index_params2.add_index(field_name="vector", index_type="HNSW", metric_type="COSINE",
                                params={"M": 8, "efConstruction": 200})
        self.create_index(client, col2, index_params2)
        self.create_index(client, col2, index_params2)  # idempotent
        assert len(self.list_indexes(client, col2, field_name="vector")[0]) == 1

        # 21.3 index level warmup - second create_index should fail
        # because index params contain "warmup" making it a distinct index definition
        col3 = cf.gen_collection_name_by_testcase_name() + "_3"
        self.create_collection(client, col3, default_warmup_dim, consistency_level="Strong")
        self.release_collection(client, col3)
        self.drop_index(client, col3, "vector")
        index_params3 = self.prepare_index_params(client)[0]
        index_params3.add_index(field_name="vector", index_type="HNSW", metric_type="COSINE",
                                params={"M": 8, "efConstruction": 200, "warmup": "sync"})
        self.create_index(client, col3, index_params3)
        self.create_index(client, col3, index_params3,
                          check_task=CheckTasks.err_res,
                          check_items={ct.err_code: 65535,
                                       ct.err_msg: "at most one distinct index is allowed per field"})
        assert len(self.list_indexes(client, col3, field_name="vector")[0]) == 1

        self.drop_collection(client, col1)
        self.drop_collection(client, col2)
        self.drop_collection(client, col3)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_drop_recreate_no_residue(self):
        """
        target: verify drop/recreate collection leaves no warmup residue
        method: create with warmup, drop, recreate same name without warmup, describe
        expected: recreated collection has no warmup settings
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # create with all warmup settings
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")
        schema.add_field("int_field", DataType.INT64, warmup="disable", nullable=True)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={
                                   "warmup.vectorField": "sync", "warmup.scalarField": "disable",
                                   "warmup.vectorIndex": "disable", "warmup.scalarIndex": "sync"
                               })

        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "sync"

        # drop
        self.drop_collection(client, collection_name)

        # recreate without warmup
        self.create_collection(client, collection_name, default_warmup_dim, consistency_level="Strong")

        # verify no residue
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorField") is None
        assert cf.get_collection_warmup(res, "warmup.scalarField") is None
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") is None
        assert cf.get_collection_warmup(res, "warmup.scalarIndex") is None
        assert cf.get_field_warmup(res, "vector") is None

        # insert & load & search
        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_warmup_dim).astype(np.float32)),
                 "float": float(i), "varchar": str(i)} for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim),
                                 limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_x_dynamic_field(self):
        """
        target: verify warmup does not affect dynamic field access
        method: create collection with dynamic field + warmup, insert dynamic data, query/search
        expected: dynamic fields queryable and searchable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="disable")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               properties={"warmup.scalarField": "disable"})

        # describe verify
        res = self.describe_collection(client, collection_name)[0]
        assert cf.get_field_warmup(res, "vec") == "disable"
        assert cf.get_collection_warmup(res, "warmup.scalarField") == "disable"

        # insert with dynamic fields
        rng = np.random.default_rng(seed=19530)
        rows = [{"pk": i, "vec": list(rng.random(default_warmup_dim).astype(np.float32)),
                 "dynamic_str": f"dyn_{i}", "dynamic_int": i * 10} for i in range(default_warmup_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # query dynamic fields
        query_res = self.query(client, collection_name, filter="pk >= 0", limit=5,
                               output_fields=["pk", "dynamic_str", "dynamic_int"])[0]
        assert len(query_res) == 5
        assert "dynamic_str" in query_res[0]
        assert "dynamic_int" in query_res[0]

        # search + output dynamic fields
        search_res = self.search(client, collection_name, cf.gen_vectors(1, default_warmup_dim), limit=default_limit,
                                 output_fields=["dynamic_str"])[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_toggle_warmup_repeatedly(self):
        """
        target: verify repeated warmup toggling produces no state residue
        method: toggle warmup 5 rounds, describe + load + search each round
        expected: each round describe shows correct latest value, search works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self.create_collection(client, collection_name, default_warmup_dim, consistency_level="Strong")
        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_warmup_dim).astype(np.float32)),
                 "float": float(i), "varchar": str(i)} for i in range(default_warmup_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.release_collection(client, collection_name)

        query_vec = cf.gen_vectors(1, default_warmup_dim)
        for round_idx in range(5):
            policy = "sync" if round_idx % 2 == 0 else "disable"

            self.alter_collection_properties(client, collection_name,
                                             properties={"warmup.vectorField": policy,
                                                         "warmup.scalarField": policy})
            self.alter_collection_field(client, collection_name,
                                        field_name="vector", field_params={"warmup": policy})

            # describe verify
            res = self.describe_collection(client, collection_name)[0]
            assert cf.get_collection_warmup(res, "warmup.vectorField") == policy, \
                f"round {round_idx}: collection warmup expected {policy}"
            assert cf.get_field_warmup(res, "vector") == policy, \
                f"round {round_idx}: field warmup expected {policy}"

            # load & search
            self.load_collection(client, collection_name)
            search_res = self.search(client, collection_name, query_vec, limit=default_limit)[0]
            assert len(search_res[0]) == default_limit, f"round {round_idx}: search failed"

            self.release_collection(client, collection_name)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_warmup_empty_collection_and_boundary(self):
        """
        target: verify warmup with empty collection and drop/recreate index boundary cases
        method: 1) empty collection with warmup: load/search/query
                2) drop index, recreate, verify collection warmup persists
        expected: no error on empty collection, collection warmup survives index drop/recreate
        """
        client = self._client()

        # 25.1 empty collection + warmup
        col1 = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_warmup_dim, warmup="sync")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        self.create_collection(client, col1, schema=schema, index_params=index_params,
                               properties={"warmup.vectorField": "sync", "warmup.scalarField": "sync"})

        res = self.describe_collection(client, col1)[0]
        assert cf.get_field_warmup(res, "vec") == "sync"
        assert cf.get_collection_warmup(res, "warmup.vectorField") == "sync"

        # load empty collection (no error)
        self.load_collection(client, col1)

        # search returns empty
        search_res = self.search(client, col1, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == 0

        # query returns empty
        query_res = self.query(client, col1, filter="pk >= 0", limit=default_limit)[0]
        assert len(query_res) == 0

        self.drop_collection(client, col1)

        # 25.2 drop index, recreate, collection warmup persists
        col2 = cf.gen_collection_name_by_testcase_name() + "_2"
        self.create_collection(client, col2, default_warmup_dim, consistency_level="Strong")

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_warmup_dim).astype(np.float32)),
                 "float": float(i), "varchar": str(i)} for i in range(100)]
        self.insert(client, col2, rows)
        self.flush(client, col2)

        self.release_collection(client, col2)
        self.alter_collection_properties(client, col2,
                                         properties={"warmup.vectorIndex": "sync"})
        self.drop_index(client, col2, "vector")

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="HNSW", metric_type="COSINE",
                               params={"M": 8, "efConstruction": 200})
        self.create_index(client, col2, index_params)
        self.load_collection(client, col2)

        # collection warmup still present
        res = self.describe_collection(client, col2)[0]
        assert cf.get_collection_warmup(res, "warmup.vectorIndex") == "sync"

        # search works
        search_res = self.search(client, col2, cf.gen_vectors(1, default_warmup_dim), limit=default_limit)[0]
        assert len(search_res[0]) == default_limit

        self.drop_collection(client, col2)
