from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
import time
import pytest
import random
from pymilvus import DataType
import pandas as pd


@pytest.mark.tags(CaseLabel.L1)
class TestPartitionKeyIsolation(TestMilvusClientV2Base):
    """Test case of partition key isolation"""

    def test_par_key_isolation_with_valid_expr(self):
        # create
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        partition_key = "scalar_6"
        enable_isolation = "true"
        self.drop_collection(client, collection_name)
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(
            schema, "scalar_3", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_3")
        )
        self.add_field(
            schema, "scalar_6", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_6")
        )
        self.add_field(
            schema, "scalar_9", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_9")
        )
        self.add_field(
            schema, "scalar_12", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_12")
        )
        self.add_field(
            schema,
            "scalar_5_linear",
            DataType.VARCHAR,
            max_length=1000,
            is_partition_key=bool(partition_key == "scalar_5_linear"),
        )
        self.add_field(schema, "emb", DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema, num_partitions=1)

        self.alter_collection_properties(client, collection_name, {"partitionkey.isolation": enable_isolation})
        log.info(f"collection {collection_name} created")
        batch_size = 500
        data_size = 1000
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
            data = []
            for j in range(start_idx, end_idx):
                data.append(
                    {
                        "id": j,
                        "scalar_3": str(j % 3),
                        "scalar_6": str(j % 6),
                        "scalar_9": str(j % 9),
                        "scalar_12": str(j % 12),
                        "scalar_5_linear": str(j % 5),
                        "emb": [random.random() for _ in range(dim)],
                    }
                )
            # collect data as DataFrame for pandas verification later
            df_data = {
                "id": [j for j in range(start_idx, end_idx)],
                "scalar_3": [str(j % 3) for j in range(start_idx, end_idx)],
                "scalar_6": [str(j % 6) for j in range(start_idx, end_idx)],
                "scalar_9": [str(j % 9) for j in range(start_idx, end_idx)],
                "scalar_12": [str(j % 12) for j in range(start_idx, end_idx)],
                "scalar_5_linear": [str(j % 5) for j in range(start_idx, end_idx)],
            }
            all_data.append(pd.DataFrame(df_data))
            log.info(f"generate test data {len(data)} cost time {time.time() - t0}")
            self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        log.info(f"collection {collection_name} insert done")
        all_df = pd.concat(all_data)
        compact_id = self.compact(client, collection_name)[0]
        self.wait_for_compaction_ready(client, compact_id)
        t0 = time.time()
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="emb", metric_type="L2", index_type="HNSW", params={"M": 16, "efConstruction": 64}
        )
        self.create_index(client, collection_name, index_params, timeout=360)
        index_list = self.list_indexes(client, collection_name)[0]
        for index_name in index_list:
            self.wait_for_index_ready(client, collection_name, index_name)
        tt = time.time() - t0
        log.info(f"create index cost time {tt}")
        compact_id = self.compact(client, collection_name)[0]
        self.wait_for_compaction_ready(client, compact_id)
        t0 = time.time()
        self.load_collection(client, collection_name)
        log.info(f"load collection cost time {time.time() - t0}")

        valid_expressions = [
            "scalar_6 == '1' and scalar_12 == '1'",
            "scalar_6 == '1' and scalar_12 > '1'",
            "scalar_6 == '3' and (scalar_12 == '1' or scalar_3 != '1')",
            "scalar_6 == '2' and ('4' < scalar_12 < '6' or scalar_3 == '1')",
            "scalar_6 == '5' and scalar_12 in ['1', '3', '5']",
            "scalar_6 == '1'",
        ]
        for expr in valid_expressions:
            res = self.search(
                client,
                collection_name,
                data=[[random.random() for _ in range(dim)]],
                anns_field="emb",
                filter=expr,
                search_params={"metric_type": "L2", "params": {}},
                limit=1000,
                output_fields=["scalar_3", "scalar_6", "scalar_12"],
                consistency_level="Strong",
            )[0]
            true_res = all_df.query(expr)
            assert len(res[0]) == len(true_res)

    def test_par_key_isolation_with_unsupported_expr(self):
        # create
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_key = "scalar_6"
        enable_isolation = "true"
        self.drop_collection(client, collection_name)
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(
            schema, "scalar_3", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_3")
        )
        self.add_field(
            schema, "scalar_6", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_6")
        )
        self.add_field(
            schema, "scalar_9", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_9")
        )
        self.add_field(
            schema, "scalar_12", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_12")
        )
        self.add_field(
            schema,
            "scalar_5_linear",
            DataType.VARCHAR,
            max_length=1000,
            is_partition_key=bool(partition_key == "scalar_5_linear"),
        )
        self.add_field(schema, "emb", DataType.FLOAT_VECTOR, dim=128)
        self.create_collection(client, collection_name, schema=schema, num_partitions=1)

        self.alter_collection_properties(client, collection_name, {"partitionkey.isolation": enable_isolation})
        log.info(f"collection {collection_name} created")
        batch_size = 500
        data_size = 1000
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
            data = []
            for j in range(start_idx, end_idx):
                data.append(
                    {
                        "id": j,
                        "scalar_3": str(j % 3),
                        "scalar_6": str(j % 6),
                        "scalar_9": str(j % 9),
                        "scalar_12": str(j % 12),
                        "scalar_5_linear": str(j % 5),
                        "emb": [random.random() for _ in range(128)],
                    }
                )
            log.info(f"generate test data {len(data)} cost time {time.time() - t0}")
            self.insert(client, collection_name, data)
        compact_id = self.compact(client, collection_name)[0]
        self.wait_for_compaction_ready(client, compact_id)
        t0 = time.time()
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="emb", metric_type="L2", index_type="HNSW", params={"M": 16, "efConstruction": 64}
        )
        self.create_index(client, collection_name, index_params, timeout=360)
        index_list = self.list_indexes(client, collection_name)[0]
        for index_name in index_list:
            self.wait_for_index_ready(client, collection_name, index_name)
        tt = time.time() - t0
        log.info(f"create index cost time {tt}")
        compact_id = self.compact(client, collection_name)[0]
        self.wait_for_compaction_ready(client, compact_id)
        t0 = time.time()
        self.load_collection(client, collection_name)
        log.info(f"load collection cost time {time.time() - t0}")
        self.flush(client, collection_name)
        log.info(f"collection {collection_name} loaded")

        invalid_expressions = [
            "scalar_6 in ['1', '2']",
            "scalar_6 not in ['1', '2']",
            "scalar_6 == '1' or scalar_3 == '1'",
            "scalar_6 != '1'",
            "scalar_6 > '1'",
            "'1' < scalar_6 < '3'",
            "scalar_3 == '1'",  # scalar_3 is not partition key
        ]
        false_result = []
        for expr in invalid_expressions:
            # v2 api_request catches exceptions and returns (Error, False) instead of raising
            # use check_task=check_nothing to skip default assert_succ in ResponseChecker
            res, check = self.search(
                client,
                collection_name,
                data=[[random.random() for _ in range(128)]],
                anns_field="emb",
                filter=expr,
                search_params={"metric_type": "L2", "params": {"nprobe": 16}},
                limit=10,
                output_fields=["scalar_6"],
                consistency_level="Strong",
                check_task=CheckTasks.check_nothing,
            )
            if check is not False:
                log.info(f"search with {expr} get res {res}")
                false_result.append(expr)
            else:
                log.info(f"search with unsupported expr {expr} get {res}")
        if len(false_result) > 0:
            log.info(f"search with unsupported expr {false_result}, but not raise error\n")
            assert False

    def test_par_key_isolation_without_partition_key(self):
        # create
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_key = "None"
        enable_isolation = "true"
        self.drop_collection(client, collection_name)
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(
            schema, "scalar_3", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_3")
        )
        self.add_field(
            schema, "scalar_6", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_6")
        )
        self.add_field(
            schema, "scalar_9", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_9")
        )
        self.add_field(
            schema, "scalar_12", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_12")
        )
        self.add_field(
            schema,
            "scalar_5_linear",
            DataType.VARCHAR,
            max_length=1000,
            is_partition_key=bool(partition_key == "scalar_5_linear"),
        )
        self.add_field(schema, "emb", DataType.FLOAT_VECTOR, dim=128)
        self.create_collection(client, collection_name, schema=schema)
        err_msg = "partition key isolation mode is enabled but no partition key field is set"
        self.alter_collection_properties(
            client,
            collection_name,
            {"partitionkey.isolation": enable_isolation},
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": err_msg},
        )

    def test_set_par_key_isolation_after_vector_indexed(self):
        # create
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_key = "scalar_6"
        enable_isolation = "false"
        self.drop_collection(client, collection_name)
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(
            schema, "scalar_3", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_3")
        )
        self.add_field(
            schema, "scalar_6", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_6")
        )
        self.add_field(
            schema, "scalar_9", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_9")
        )
        self.add_field(
            schema, "scalar_12", DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_12")
        )
        self.add_field(
            schema,
            "scalar_5_linear",
            DataType.VARCHAR,
            max_length=1000,
            is_partition_key=bool(partition_key == "scalar_5_linear"),
        )
        self.add_field(schema, "emb", DataType.FLOAT_VECTOR, dim=128)
        self.create_collection(client, collection_name, schema=schema, num_partitions=1)

        self.alter_collection_properties(client, collection_name, {"partitionkey.isolation": enable_isolation})
        log.info(f"collection {collection_name} created")
        batch_size = 500
        data_size = 1000
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
            data = []
            for j in range(start_idx, end_idx):
                data.append(
                    {
                        "id": j,
                        "scalar_3": str(j % 3),
                        "scalar_6": str(j % 6),
                        "scalar_9": str(j % 9),
                        "scalar_12": str(j % 12),
                        "scalar_5_linear": str(j % 5),
                        "emb": [random.random() for _ in range(128)],
                    }
                )
            log.info(f"generate test data {len(data)} cost time {time.time() - t0}")
            self.insert(client, collection_name, data)
        compact_id = self.compact(client, collection_name)[0]
        self.wait_for_compaction_ready(client, compact_id)
        t0 = time.time()
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="emb", metric_type="L2", index_type="HNSW", params={"M": 16, "efConstruction": 64}
        )
        self.create_index(client, collection_name, index_params, timeout=360)
        index_list = self.list_indexes(client, collection_name)[0]
        for index_name in index_list:
            self.wait_for_index_ready(client, collection_name, index_name)
        tt = time.time() - t0
        log.info(f"create index cost time {tt}")
        # try set isolation=true after index exists → expect failure
        error = {
            ct.err_code: 702,
            ct.err_msg: "can not alter partition key isolation mode if the collection already has a vector index",
        }
        self.alter_collection_properties(
            client,
            collection_name,
            {"partitionkey.isolation": "true"},
            check_task=CheckTasks.err_res,
            check_items=error,
        )
        # drop index, then set isolation=true → expect success
        for index_name in index_list:
            self.drop_index(client, collection_name, index_name)
        self.alter_collection_properties(client, collection_name, {"partitionkey.isolation": "true"})
        # recreate index, load, search
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="emb", metric_type="L2", index_type="HNSW", params={"M": 16, "efConstruction": 64}
        )
        self.create_index(client, collection_name, index_params, timeout=360)
        self.load_collection(client, collection_name)
        res = self.search(
            client,
            collection_name,
            data=[[random.random() for _ in range(128)]],
            anns_field="emb",
            filter="scalar_6 == '1' and scalar_3 == '1'",
            search_params={"metric_type": "L2", "params": {"nprobe": 16}},
            limit=10,
            output_fields=["scalar_6", "scalar_3"],
            consistency_level="Strong",
        )[0]
        log.info(f"search res {res}")
        assert len(res[0]) > 0
