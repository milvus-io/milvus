from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel
from utils.util_log import test_log as log
import time
import pytest
import random
from pymilvus import (
    list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection, utility
)
import pandas as pd
import faker
fake = faker.Faker()


prefix = "par_key_isolation_"


@pytest.mark.tags(CaseLabel.L1)
class TestPartitionKeyIsolation(TestcaseBase):
    """ Test case of partition key isolation"""
    def test_par_key_isolation_with_valid_expr(self):
        # create
        self._connect()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        partition_key = "scalar_6"
        enable_isolation = "true"
        if collection_name in list_collections():
            log.info(f"collection {collection_name} exists, drop it")
            Collection(name=collection_name).drop()
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="scalar_3", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_3")),
            FieldSchema(name="scalar_6", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_6")),
            FieldSchema(name="scalar_9", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_9")),
            FieldSchema(name="scalar_12", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_12")),
            FieldSchema(name="scalar_5_linear", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_5_linear")),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim)
        ]
        schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True,
                                  num_partitions=1)
        collection = Collection(name=collection_name, schema=schema, num_partitions=1)

        collection.set_properties({"partitionkey.isolation": enable_isolation})
        log.info(f"collection {collection_name} created: {collection.describe()}")
        index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 30, "efConstruction": 360}}
        log.info(f"collection {collection_name} created")
        batch_size = 1000
        data_size = 10000
        epoch = data_size // batch_size
        remainder = data_size % batch_size
        all_data = []
        for i in range(epoch + 1):
            if i == epoch:
                if remainder == 0:
                    break
                batch_size = remainder
            start_idx = i * batch_size
            end_idx = (i + 1) * batch_size
            t0 = time.time()
            data = {
                "id": [i for i in range(start_idx, end_idx)],
                "scalar_3": [str(i % 3) for i in range(start_idx, end_idx)],
                "scalar_6": [str(i % 6) for i in range(start_idx, end_idx)],
                "scalar_9": [str(i % 9) for i in range(start_idx, end_idx)],
                "scalar_12": [str(i % 12) for i in range(start_idx, end_idx)],
                "scalar_5_linear": [str(i % 5) for i in range(start_idx, end_idx)],
                "emb": [[random.random() for _ in range(dim)] for _ in range(batch_size)]
            }
            df = pd.DataFrame(data)
            all_data.append(df)
            log.info(f"generate test data {batch_size} cost time {time.time() - t0}")
            collection.insert(df)
        num = collection.num_entities
        log.info(f"collection {collection_name} loaded, num_entities: {num}")
        all_df = pd.concat(all_data)
        collection.compact()
        collection.wait_for_compaction_completed()
        t0 = time.time()
        collection.create_index("emb", index_params=index_params)
        index_list = utility.list_indexes(collection_name=collection_name)
        for index_name in index_list:
            progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
            while progress["pending_index_rows"] > 0:
                time.sleep(30)
                progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
                log.info(f"collection {collection_name} index {index_name} progress: {progress}")
            log.info(f"collection {collection_name} index {index_name} progress: {progress}")
        tt = time.time() - t0
        log.info(f"create index cost time {tt}")
        collection.compact()
        collection.wait_for_compaction_completed()
        t0 = time.time()
        collection.load()
        log.info(f"load collection cost time {time.time() - t0}")

        valid_expressions = [
            "scalar_6 == '1' and scalar_12 == '1'",
            "scalar_6 == '1' and scalar_12 > '1'",
            "scalar_6 == '3' and (scalar_12 == '1' or scalar_3 != '1')",
            "scalar_6 == '2' and ('4' < scalar_12 < '6' or scalar_3 == '1')",
            "scalar_6 == '5' and scalar_12 in ['1', '3', '5']",
            "scalar_6 == '1'"
        ]
        for expr in valid_expressions:
            res = collection.search(
                data=[[random.random() for _ in range(dim)]],
                anns_field="emb",
                expr=expr,
                param={"metric_type": "L2", "params": {}},
                limit=10000,
                output_fields=["scalar_3", "scalar_6", "scalar_12"]
            )
            true_res = all_df.query(expr)
            assert len(res[0]) == len(true_res)

    def test_par_key_isolation_with_unsupported_expr(self):
        # create
        self._connect()
        collection_name = cf.gen_unique_str(prefix)
        partition_key = "scalar_6"
        enable_isolation = "true"
        if collection_name in list_collections():
            log.info(f"collection {collection_name} exists, drop it")
            Collection(name=collection_name).drop()
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="scalar_3", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_3")),
            FieldSchema(name="scalar_6", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_6")),
            FieldSchema(name="scalar_9", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_9")),
            FieldSchema(name="scalar_12", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_12")),
            FieldSchema(name="scalar_5_linear", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_5_linear")),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=768)
        ]
        schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True,
                                  num_partitions=1)
        collection = Collection(name=collection_name, schema=schema, num_partitions=1)

        collection.set_properties({"partitionkey.isolation": enable_isolation})
        log.info(f"collection {collection_name} created: {collection.describe()}")
        index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 30, "efConstruction": 360}}
        log.info(f"collection {collection_name} created")
        batch_size = 1000
        data_size = 10000
        epoch = data_size // batch_size
        remainder = data_size % batch_size
        for i in range(epoch + 1):
            if i == epoch:
                if remainder == 0:
                    break
                batch_size = remainder
            start_idx = i * batch_size
            end_idx = (i + 1) * batch_size
            t0 = time.time()
            data = {
                "id": [i for i in range(start_idx, end_idx)],
                "scalar_3": [str(i % 3) for i in range(start_idx, end_idx)],
                "scalar_6": [str(i % 6) for i in range(start_idx, end_idx)],
                "scalar_9": [str(i % 9) for i in range(start_idx, end_idx)],
                "scalar_12": [str(i % 12) for i in range(start_idx, end_idx)],
                "scalar_5_linear": [str(i % 5) for i in range(start_idx, end_idx)],
                "emb": [[random.random() for _ in range(768)] for _ in range(batch_size)]
            }
            df = pd.DataFrame(data)
            log.info(f"generate test data {batch_size} cost time {time.time() - t0}")
            collection.insert(df)
        collection.compact()
        collection.wait_for_compaction_completed()
        t0 = time.time()
        collection.create_index("emb", index_params=index_params)
        index_list = utility.list_indexes(collection_name=collection_name)
        for index_name in index_list:
            progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
            while progress["pending_index_rows"] > 0:
                time.sleep(30)
                progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
                log.info(f"collection {collection_name} index {index_name} progress: {progress}")
            log.info(f"collection {collection_name} index {index_name} progress: {progress}")
        tt = time.time() - t0
        log.info(f"create index cost time {tt}")
        collection.compact()
        collection.wait_for_compaction_completed()
        t0 = time.time()
        collection.load()
        log.info(f"load collection cost time {time.time() - t0}")
        num = collection.num_entities
        log.info(f"collection {collection_name} loaded, num_entities: {num}")

        invalid_expressions = [
            "scalar_6 in ['1', '2']",
            "scalar_6 not in ['1', '2']",
            "scalar_6 == '1' or scalar_3 == '1'",
            "scalar_6 != '1'",
            "scalar_6 > '1'",
            "'1' < scalar_6 < '3'",
            "scalar_3 == '1'"  # scalar_3 is not partition key
        ]
        false_result = []
        for expr in invalid_expressions:
            try:
                res = collection.search(
                    data=[[random.random() for _ in range(768)]],
                    anns_field="emb",
                    expr=expr,
                    param={"metric_type": "L2", "params": {"nprobe": 16}},
                    limit=10,
                    output_fields=["scalar_6"]
                )
                log.info(f"search with {expr} get res {res}")
                false_result.append(expr)
            except Exception as e:
                log.info(f"search with unsupported expr {expr} get {e}")
        if len(false_result) > 0:
            log.info(f"search with unsupported expr {false_result}, but not raise error\n")
            assert False

    def test_par_key_isolation_without_partition_key(self):
        # create
        self._connect()
        collection_name = cf.gen_unique_str(prefix)
        partition_key = "None"
        enable_isolation = "true"
        if collection_name in list_collections():
            log.info(f"collection {collection_name} exists, drop it")
            Collection(name=collection_name).drop()
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="scalar_3", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_3")),
            FieldSchema(name="scalar_6", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_6")),
            FieldSchema(name="scalar_9", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_9")),
            FieldSchema(name="scalar_12", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_12")),
            FieldSchema(name="scalar_5_linear", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_5_linear")),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=768)
        ]
        schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True,
                                  num_partitions=1)
        collection = Collection(name=collection_name, schema=schema)
        try:
            collection.set_properties({"partitionkey.isolation": enable_isolation})
            assert False
        except Exception as e:
            log.info(f"set_properties failed without partition key {e}")
            assert "partition key isolation mode is enabled but no partition key field is set" in str(e)

    def test_set_par_key_isolation_after_vector_indexed(self):
        # create
        self._connect()
        collection_name = cf.gen_unique_str(prefix)
        partition_key = "scalar_6"
        enable_isolation = "false"
        if collection_name in list_collections():
            log.info(f"collection {collection_name} exists, drop it")
            Collection(name=collection_name).drop()
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="scalar_3", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_3")),
            FieldSchema(name="scalar_6", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_6")),
            FieldSchema(name="scalar_9", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_9")),
            FieldSchema(name="scalar_12", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_12")),
            FieldSchema(name="scalar_5_linear", dtype=DataType.VARCHAR, max_length=1000,
                        is_partition_key=bool(partition_key == "scalar_5_linear")),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=768)
        ]
        schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True,
                                  num_partitions=1)
        collection = Collection(name=collection_name, schema=schema, num_partitions=1)

        collection.set_properties({"partitionkey.isolation": enable_isolation})
        log.info(f"collection {collection_name} created: {collection.describe()}")
        index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 30, "efConstruction": 360}}
        log.info(f"collection {collection_name} created")
        batch_size = 1000
        data_size = 10000
        epoch = data_size // batch_size
        remainder = data_size % batch_size
        for i in range(epoch + 1):
            if i == epoch:
                if remainder == 0:
                    break
                batch_size = remainder
            start_idx = i * batch_size
            end_idx = (i + 1) * batch_size
            t0 = time.time()
            data = {
                "id": [i for i in range(start_idx, end_idx)],
                "scalar_3": [str(i % 3) for i in range(start_idx, end_idx)],
                "scalar_6": [str(i % 6) for i in range(start_idx, end_idx)],
                "scalar_9": [str(i % 9) for i in range(start_idx, end_idx)],
                "scalar_12": [str(i % 12) for i in range(start_idx, end_idx)],
                "scalar_5_linear": [str(i % 5) for i in range(start_idx, end_idx)],
                "emb": [[random.random() for _ in range(768)] for _ in range(batch_size)]
            }
            df = pd.DataFrame(data)
            log.info(f"generate test data {batch_size} cost time {time.time() - t0}")
            collection.insert(df)
        collection.compact()
        collection.wait_for_compaction_completed()
        t0 = time.time()
        collection.create_index("emb", index_params=index_params)
        index_list = utility.list_indexes(collection_name=collection_name)
        for index_name in index_list:
            progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
            while progress["pending_index_rows"] > 0:
                time.sleep(30)
                progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
                log.info(f"collection {collection_name} index {index_name} progress: {progress}")
            log.info(f"collection {collection_name} index {index_name} progress: {progress}")
        tt = time.time() - t0
        log.info(f"create index cost time {tt}")
        result = True
        try:
            collection.set_properties({"partitionkey.isolation": "true"})

        except Exception as e:
            result = False
            log.info(f"set_properties after vector indexed {e}")
        assert result is False
        collection.drop_index()
        collection.set_properties({"partitionkey.isolation": "true"})
        collection.create_index("emb", index_params=index_params)
        collection.load()
        res = collection.search(
            data=[[random.random() for _ in range(768)]],
            anns_field="emb",
            expr="scalar_6 == '1' and scalar_3 == '1'",
            param={"metric_type": "L2", "params": {"nprobe": 16}},
            limit=10,
            output_fields=["scalar_6", "scalar_3"]
        )
        log.info(f"search res {res}")
        assert len(res[0]) > 0
