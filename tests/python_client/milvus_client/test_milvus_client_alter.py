import multiprocessing
import numbers
import random
import numpy
import threading
import pytest
import pandas as pd
import decimal
from decimal import Decimal, getcontext
from time import sleep
import heapq

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from common.constants import *
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_EVENTUALLY
from base.high_level_api_wrapper import HighLevelApiWrapper
client_w = HighLevelApiWrapper()

prefix = "milvus_client_api_index"
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
default_multiple_vector_field_name = "vector_new"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name


class TestMilvusClientAlterIndex(TestcaseBase):

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
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        idx_names, _ = client_w.list_indexes(client, collection_name, field_name=default_vector_field_name)
        client_w.load_collection(client, collection_name)
        res1 = client_w.describe_index(client, collection_name, index_name=idx_names[0])[0]
        assert res1.get('mmap.enabled', None) is None
        error = {ct.err_code: 999,
                 ct.err_msg: "can't alter index on loaded collection, please release the collection first"}
        # 1. alter index after load
        client_w.alter_index_properties(client, collection_name, idx_names[0], properties={"mmap.enabled": True},
                                        check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_index_properties(client, collection_name, idx_names[0], property_keys=["mmap.enabled"],
                                       check_task=CheckTasks.err_res, check_items=error)
        client_w.release_collection(client, collection_name)
        # 2. alter index after release
        client_w.alter_index_properties(client, collection_name, idx_names[0], properties={"mmap.enabled": True})
        res2 = client_w.describe_index(client, collection_name, index_name=idx_names[0])[0]
        assert res2.get('mmap.enabled', None) == 'True'
        client_w.drop_index_properties(client, collection_name, idx_names[0], property_keys=["mmap.enabled"])
        res3 = client_w.describe_index(client, collection_name, index_name=idx_names[0])[0]
        assert res3.get('mmap.enabled', None) is None

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_index_unsupported_properties(self):
        """
        target: test alter index with unsupported properties
        method: 1. alter index with unsupported properties
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        schema = client_w.create_schema(client, enable_dynamic_field=False)[0]
        dim = 32
        pk_field_name = 'id_string'
        vector_field_name = 'embeddings'
        str_field_name = 'title'
        max_length = 16
        schema.add_field(pk_field_name, DataType.VARCHAR, max_length=max_length, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, DataType.FLOAT_VECTOR, dim=dim, mmap_enabled=True)
        schema.add_field(str_field_name, DataType.VARCHAR, max_length=max_length, mmap_enabled=True)

        index_params = client_w.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name, metric_type="COSINE",
                               index_type="HNSW", params={"M": 16, "efConstruction": 100, "mmap.enabled": True})
        index_params.add_index(field_name=str_field_name)
        client_w.create_collection(client, collection_name, schema=schema, index_params=index_params)
        client_w.describe_collection(client, collection_name, check_task=CheckTasks.check_collection_fields_properties,
                                     check_items={str_field_name: {"max_length": max_length, "mmap_enabled": True},
                                                  vector_field_name: {"mmap_enabled": True}})
        client_w.release_collection(client, collection_name)
        properties = client_w.describe_index(client, collection_name, index_name=vector_field_name)[0]
        for p in properties.items():
            if p[0] not in ["mmap.enabled"]:
                log.debug(f"try to alter index property: {p[0]}")
                error = {ct.err_code: 1, ct.err_msg: f"{p[0]} is not a configable index proptery"}
                new_value = p[1] + 1 if isinstance(p[1], numbers.Number) else "new_value"
                client_w.alter_index_properties(client, collection_name, vector_field_name,
                                                properties={p[0]: new_value},
                                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_index_unsupported_value(self):
        """
        target: test alter index with unsupported properties
        method: 1. alter index with unsupported properties
        expected: raise exception
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        idx_names, _ = client_w.list_indexes(client, collection_name, field_name=default_vector_field_name)
        client_w.release_collection(client, collection_name)
        res1 = client_w.describe_index(client, collection_name, index_name=idx_names[0])[0]
        assert res1.get('mmap.enabled', None) is None
        unsupported_values = [None, [], '', 20, '  ', 0.01, "new_value"]
        for value in unsupported_values:
            error = {ct.err_code: 1, ct.err_msg: f"invalid mmap.enabled value: {value}, expected: true, false"}
            client_w.alter_index_properties(client, collection_name, idx_names[0],
                                            properties={"mmap.enabled": value},
                                            check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientAlterCollection(TestcaseBase):
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
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        client_w.load_collection(client, collection_name)
        res1 = client_w.describe_collection(client, collection_name)[0]
        assert res1.get('properties', None) == {}
        # 1. alter collection properties after load
        client_w.load_collection(client, collection_name)
        error = {ct.err_code: 999,
                 ct.err_msg: "can not alter mmap properties if collection loaded"}
        client_w.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True},
                                             check_task=CheckTasks.err_res, check_items=error)
        client_w.alter_collection_properties(client, collection_name, properties={"lazyload.enabled": True},
                                             check_task=CheckTasks.err_res, check_items=error)
        error = {ct.err_code: 999,
                 ct.err_msg: "can not delete mmap properties if collection loaded"}
        client_w.drop_collection_properties(client, collection_name, property_keys=["mmap.enabled"],
                                            check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection_properties(client, collection_name, property_keys=["lazyload.enabled"],
                                            check_task=CheckTasks.err_res, check_items=error)
        res3 = client_w.describe_collection(client, collection_name)[0]
        assert res3.get('properties', None) == {}
        client_w.drop_collection_properties(client, collection_name, property_keys=["collection.ttl.seconds"])
        assert res3.get('properties', None) == {}
        # 2. alter collection properties after release
        client_w.release_collection(client, collection_name)
        client_w.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        res2 = client_w.describe_collection(client, collection_name)[0]
        assert res2.get('properties', None) == {'mmap.enabled': 'True'}
        client_w.alter_collection_properties(client, collection_name,
                                             properties={"collection.ttl.seconds": 100, "lazyload.enabled": True})
        res2 = client_w.describe_collection(client, collection_name)[0]
        assert res2.get('properties', None) == {'mmap.enabled': 'True',
                                                'collection.ttl.seconds': '100', 'lazyload.enabled': 'True'}
        client_w.drop_collection_properties(client, collection_name,
                                            property_keys=["mmap.enabled", "lazyload.enabled",
                                                           "collection.ttl.seconds"])
        res3 = client_w.describe_collection(client, collection_name)[0]
        assert res3.get('properties', None) == {}


class TestMilvusClientAlterCollectionField(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_alter_collection_field_default(self):
        """
        target: test alter collection field before load
        method: alter varchar field max length
        expected: alter successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        schema = client_w.create_schema(client, enable_dynamic_field=False)[0]
        dim = 32
        pk_field_name = 'id_string'
        vector_field_name = 'embeddings'
        str_field_name = 'title'
        max_length = 16
        schema.add_field(pk_field_name, DataType.VARCHAR, max_length=max_length, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, DataType.FLOAT_VECTOR, dim=dim, mmap_enabled=True)
        schema.add_field(str_field_name, DataType.VARCHAR, max_length=max_length, mmap_enabled=True)

        index_params = client_w.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name, metric_type="COSINE",
                               index_type="IVF_FLAT", params={"nlist": 128})
        index_params.add_index(field_name=str_field_name)
        client_w.create_collection(client, collection_name, schema=schema, index_params=index_params)
        client_w.describe_collection(client, collection_name, check_task=CheckTasks.check_collection_fields_properties,
                                     check_items={str_field_name: {"max_length": max_length, "mmap_enabled": True},
                                                  vector_field_name: {"mmap_enabled": True}})

        rng = np.random.default_rng(seed=19530)
        rows = [{
            pk_field_name: f'id_{i}',
            vector_field_name: list(rng.random((1, dim))[0]),
            str_field_name: cf.gen_str_by_length(max_length)
        } for i in range(default_nb)]
        client_w.insert(client, collection_name, rows)

        # 1. alter collection field before load
        client_w.release_collection(client, collection_name)
        new_max_length = max_length//2
        # TODO: use one format of mmap_enabled after #38443 fixed
        client_w.alter_collection_field(client, collection_name,  field_name=str_field_name,
                                        field_params={"max_length": new_max_length, "mmap.enabled": False})
        client_w.alter_collection_field(client, collection_name, field_name=pk_field_name,
                                        field_params={"max_length": new_max_length})
        client_w.alter_collection_field(client, collection_name, field_name=vector_field_name,
                                        field_params={"mmap.enabled": False})
        error = {ct.err_code: 999, ct.err_msg: f"can not modify the maxlength for non-string types"}
        client_w.alter_collection_field(client, collection_name, field_name=vector_field_name,
                                        field_params={"max_length": new_max_length},
                                        check_task=CheckTasks.err_res, check_items=error)
        client_w.describe_collection(client, collection_name, check_task=CheckTasks.check_collection_fields_properties,
                                     check_items={str_field_name: {"max_length": new_max_length, "mmap_enabled": False},
                                                  vector_field_name: {"mmap_enabled": False}})

        # verify that cannot insert data with the old max_length
        for alter_field in [pk_field_name, str_field_name]:
            error = {ct.err_code: 999, ct.err_msg: f"length of varchar field {alter_field} exceeds max length"}
            rows = [{
                pk_field_name: cf.gen_str_by_length(max_length) if alter_field == pk_field_name else f'id_{i}',
                vector_field_name: list(rng.random((1, dim))[0]),
                str_field_name: cf.gen_str_by_length(max_length) if alter_field == str_field_name else f'title_{i}'
            } for i in range(default_nb, default_nb+10)]
            client_w.insert(client, collection_name, rows, check_task=CheckTasks.err_res, check_items=error)

        # verify that can insert data with the new max_length
        rows = [{
            pk_field_name: f"new_{cf.gen_str_by_length(new_max_length-4)}",
            vector_field_name: list(rng.random((1, dim))[0]),
            str_field_name: cf.gen_str_by_length(new_max_length)
        } for i in range(default_nb, default_nb+10)]
        client_w.insert(client, collection_name, rows)

        # 2. alter collection field after load
        client_w.load_collection(client, collection_name)
        error = {ct.err_code: 999,
                 ct.err_msg: "can not alter collection field properties if collection loaded"}
        client_w.alter_collection_field(client, collection_name, field_name=str_field_name,
                                        field_params={"max_length": max_length, "mmap.enabled": True},
                                        check_task=CheckTasks.err_res, check_items=error)
        client_w.alter_collection_field(client, collection_name, field_name=vector_field_name,
                                        field_params={"mmap.enabled": True},
                                        check_task=CheckTasks.err_res, check_items=error)
        client_w.alter_collection_field(client, collection_name, field_name=pk_field_name,
                                        field_params={"max_length": max_length})

        res = client_w.query(client, collection_name, filter=f"{pk_field_name} in ['id_10', 'id_20']",
                             output_fields=["*"])[0]
        assert (len(res)) == 2
        res = client_w.query(client, collection_name, filter=f"{pk_field_name} like 'new_%'",
                             output_fields=["*"])[0]
        assert(len(res)) == 10


class TestMilvusClientAlterDatabase(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    # @pytest.mark.skip("reason: need to fix #38469, #38471")
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
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        client_w.release_collection(client, collection_name)
        default_db = 'default'
        res1 = client_w.describe_database(client, db_name=default_db)[0]
        if len(res1.keys()) != 1:
            client_w.drop_database_properties(client, db_name=default_db, property_keys=res1.keys())
        assert len(client_w.describe_database(client, default_db)[0].keys()) == 1
        for need_load in [True, False]:
            if need_load:
                log.debug("alter database after load collection")
                client_w.load_collection(client, collection_name)

            # 1. alter default database properties before load
            properties = {"key1": 1, "key2": "value2", "key3": [1, 2, 3], }
            client_w.alter_database_properties(client, db_name=default_db, properties=properties)
            res1 = client_w.describe_database(client, db_name=default_db)[0]
            # assert res1.properties.items() >= properties.items()
            assert len(res1.keys()) == 4
            my_db = cf.gen_unique_str(prefix)
            client_w.create_database(client, my_db, properties=properties)
            res1 = client_w.describe_database(client, db_name=my_db)[0]
            # assert res1.properties.items() >= properties.items()
            assert len(res1.keys()) == 4
            properties = {"key1": 2, "key2": "value3", "key3": [1, 2, 3], 'key4': 0.123}
            client_w.alter_database_properties(client, db_name=my_db, properties=properties)
            res1 = client_w.describe_database(client, db_name=my_db)[0]
            # assert res1.properties.items() >= properties.items()
            assert len(res1.keys()) == 5

            # drop the default database properties
            client_w.drop_database_properties(client, db_name=default_db, property_keys=["key1", "key2"])
            res1 = client_w.describe_database(client, db_name=default_db)[0]
            assert len(res1.keys()) == 2
            client_w.drop_database_properties(client, db_name=default_db, property_keys=["key3", "key_non_exist"])
            res1 = client_w.describe_database(client, db_name=default_db)[0]
            assert len(res1.keys()) == 1
            # drop the user database
            client_w.drop_database(client, my_db)


