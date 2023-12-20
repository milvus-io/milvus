import logging
import time
import pytest
from pymilvus import DataType
import numpy as np
from pathlib import Path
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.milvus_sys import MilvusSys
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
from common.bulk_insert_data import (
    prepare_bulk_insert_json_files,
    prepare_bulk_insert_new_json_files,
    prepare_bulk_insert_numpy_files,
    prepare_bulk_insert_parquet_files,
    prepare_bulk_insert_csv_files,
    DataField as df,
)


default_vec_only_fields = [df.vec_field]
default_multi_fields = [
    df.vec_field,
    df.int_field,
    df.string_field,
    df.bool_field,
    df.float_field,
    df.array_int_field
]
default_vec_n_int_fields = [df.vec_field, df.int_field, df.array_int_field]


# milvus_ns = "chaos-testing"
base_dir = "/tmp/bulk_insert_data"


def entity_suffix(entities):
    if entities // 1000000 > 0:
        suffix = f"{entities // 1000000}m"
    elif entities // 1000 > 0:
        suffix = f"{entities // 1000}k"
    else:
        suffix = f"{entities}"
    return suffix


class TestcaseBaseBulkInsert(TestcaseBase):

    @pytest.fixture(scope="function", autouse=True)
    def init_minio_client(self, minio_host):
        Path("/tmp/bulk_insert_data").mkdir(parents=True, exist_ok=True)
        self._connect()
        self.milvus_sys = MilvusSys(alias='default')
        ms = MilvusSys()
        minio_port = "9000"
        self.minio_endpoint = f"{minio_host}:{minio_port}"
        self.bucket_name = ms.index_nodes[0]["infos"]["system_configurations"][
            "minio_bucket_name"
        ]


class TestBulkInsertPerf(TestcaseBaseBulkInsert):

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("file_size", [1, 10, 15])  # file size in GB
    @pytest.mark.parametrize("file_nums", [1])
    @pytest.mark.parametrize("array_len", [100])
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    def test_bulk_insert_all_field_with_parquet(self, auto_id, dim, file_size, file_nums, array_len, enable_dynamic_field):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_parquet_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=3000,
            dim=dim,
            data_fields=data_fields,
            file_size=file_size,
            file_nums=file_nums,
            array_length=array_len,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=1800
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("file_size", [1, 10, 15])  # file size in GB
    @pytest.mark.parametrize("file_nums", [1])
    @pytest.mark.parametrize("array_len", [100])
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    def test_bulk_insert_all_field_with_json(self, auto_id, dim, file_size, file_nums, array_len, enable_dynamic_field):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_new_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=3000,
            dim=dim,
            data_fields=data_fields,
            file_size=file_size,
            file_nums=file_nums,
            array_length=array_len,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=1800
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("file_size", [1, 10, 15])  # file size in GB
    @pytest.mark.parametrize("file_nums", [1])
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    def test_bulk_insert_all_field_with_numpy(self, auto_id, dim, file_size, file_nums, enable_dynamic_field):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=3000,
            dim=dim,
            data_fields=data_fields,
            file_size=file_size,
            file_nums=file_nums,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=1800
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success
