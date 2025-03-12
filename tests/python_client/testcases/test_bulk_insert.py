import logging
import random
import time
import pytest
from pymilvus import DataType, Function, FunctionType, FieldSchema, CollectionSchema
from pymilvus.bulk_writer import RemoteBulkWriter, BulkFileType
import numpy as np
from pathlib import Path
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_params import DefaultVectorIndexParams, DefaultVectorSearchParams
from common.milvus_sys import MilvusSys
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log
from common.bulk_insert_data import (
    prepare_bulk_insert_json_files,
    prepare_bulk_insert_new_json_files,
    prepare_bulk_insert_numpy_files,
    prepare_bulk_insert_parquet_files,
    DataField as df,
)
from faker import Faker
fake = Faker()
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
        self.bucket_name = ms.data_nodes[0]["infos"]["system_configurations"][
            "minio_bucket_name"
        ]


class TestBulkInsert(TestcaseBaseBulkInsert):

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 8, 128
    @pytest.mark.parametrize("entities", [100])  # 100, 1000
    def test_float_vector_only(self, is_row_based, auto_id, dim, entities):
        """
        collection: auto_id, customized_id
        collection schema: [pk, float_vector]
        Steps:
        1. create collection
        2. import data
        3. verify the data entities equal the import data
        4. load the collection
        5. verify search successfully
        6. verify query successfully
        """

        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_new_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=data_fields,
            force=True,
        )
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=None,
            files=files,
        )
        logging.info(f"bulk insert task id:{task_id}")
        success, _ = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert success

        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities

        # verify imported data is available for search
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.float_vec_field, index_params=index_params
        )
        time.sleep(2)
        self.utility_wrap.wait_for_index_building_complete(c_name, timeout=300)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        log.info(f"index building progress: {res}")
        self.collection_wrap.load()
        self.collection_wrap.load(_refresh=True)
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        log.info(
            f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
        )
        nq = 2
        topk = 2
        search_data = cf.gen_vectors(nq, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.float_vec_field,
            param=search_params,
            limit=topk,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": topk},
        )
        for hits in res:
            ids = hits.ids
            results, _ = self.collection_wrap.query(expr=f"{df.pk_field} in {ids}")
            assert len(results) == len(ids)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("dim", [128])  # 8
    @pytest.mark.parametrize("entities", [100])  # 100
    def test_str_pk_float_vector_only(self, is_row_based, dim, entities):
        """
        collection schema: [str_pk, float_vector]
        Steps:
        1. create collection
        2. import data
        3. verify the data entities equal the import data
        4. load the collection
        5. verify search successfully
        6. verify query successfully
        """
        auto_id = False  # no auto id for string_pk schema
        string_pk = True

        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_string_field(name=df.string_field, is_primary=True, auto_id=auto_id),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields)
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_new_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            str_pk=string_pk,
            data_fields=data_fields,
            schema=schema,
        )
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        completed, _ = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{completed} in {tt}")
        assert completed

        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities

        # verify imported data is available for search
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.float_vec_field, index_params=index_params
        )
        self.utility_wrap.wait_for_index_building_complete(c_name, timeout=300)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        log.info(f"index building progress: {res}")
        self.collection_wrap.load()
        self.collection_wrap.load(_refresh=True)
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        log.info(
            f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
        )
        nq = 3
        topk = 2
        search_data = cf.gen_vectors(nq, dim)
        search_params = ct.default_search_params
        time.sleep(2)
        res, _ = self.collection_wrap.search(
            search_data,
            df.float_vec_field,
            param=search_params,
            limit=topk,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": topk},
        )
        for hits in res:
            ids = hits.ids
            expr = f"{df.string_field} in {ids}"
            expr = expr.replace("'", '"')
            results, _ = self.collection_wrap.query(expr=expr)
            assert len(results) == len(ids)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("entities", [2000])
    def test_partition_float_vector_int_scalar(
        self, is_row_based, auto_id, dim, entities
    ):
        """
        collection: customized partitions
        collection schema: [pk, float_vectors, int_scalar]
        1. create collection and a partition
        2. build index and load partition
        3. import data into the partition
        4. verify num entities
        5. verify index status
        6. verify search and query
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_n_int_fields,
            file_nums=1,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT32),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # create a partition
        p_name = cf.gen_unique_str("bulk_insert")
        m_partition, _ = self.collection_wrap.create_partition(partition_name=p_name)
        # build index before bulk insert
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        # load before bulk insert
        self.collection_wrap.load(partition_names=[p_name])

        # import data into the partition
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=p_name,
            files=files,
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, state = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert success

        assert m_partition.num_entities == entities
        assert self.collection_wrap.num_entities == entities
        log.debug(state)
        time.sleep(2)
        self.utility_wrap.wait_for_index_building_complete(c_name, timeout=300)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        log.info(f"index building progress: {res}")
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
        time.sleep(2)
        log.info(
            f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
        )

        nq = 10
        topk = 5
        search_data = cf.gen_vectors(nq, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.vec_field,
            param=search_params,
            limit=topk,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": topk},
        )
        for hits in res:
            ids = hits.ids
            results, _ = self.collection_wrap.query(expr=f"{df.pk_field} in {ids}")
            assert len(results) == len(ids)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("entities", [2000])
    def test_binary_vector_json(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, binary_vector]
        Steps:
        1. create collection
        2. create index and load collection
        3. import data
        4. verify build status
        5. verify the data entities
        6. load collection
        7. verify search successfully
        6. verify query successfully
        """
        float_vec = False
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            float_vector=float_vec,
            data_fields=default_vec_only_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_binary_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index before bulk insert
        binary_index_params = {
            "index_type": "BIN_IVF_FLAT",
            "metric_type": "JACCARD",
            "params": {"nlist": 64},
        }
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=binary_index_params
        )
        # load collection
        self.collection_wrap.load()
        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(collection_name=c_name,
                                                      files=files)
        logging.info(f"bulk insert task ids:{task_id}")
        success, _ = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert success
        time.sleep(2)
        self.utility_wrap.wait_for_index_building_complete(c_name, timeout=300)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        log.info(f"index building progress: {res}")

        # verify num entities
        assert self.collection_wrap.num_entities == entities
        # verify search and query
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
        time.sleep(2)
        search_data = cf.gen_binary_vectors(1, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        res, _ = self.collection_wrap.search(
            search_data,
            df.vec_field,
            param=search_params,
            limit=1,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )
        for hits in res:
            ids = hits.ids
            results, _ = self.collection_wrap.query(expr=f"{df.pk_field} in {ids}")
            assert len(results) == len(ids)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("insert_before_bulk_insert", [True, False])
    def test_insert_before_or_after_bulk_insert(self, insert_before_bulk_insert):
        """
        collection schema: [pk, float_vector]
        Steps:
        1. create collection
        2. create index and insert data or not
        3. import data
        4. insert data or not
        5. verify the data entities
        6. verify search and query
        """
        bulk_insert_row = 500
        direct_insert_row = 3000
        dim = 128
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_field(name=df.float_field),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
        ]
        data = [
            [i for i in range(direct_insert_row)],
            [np.float32(i) for i in range(direct_insert_row)],
            cf.gen_vectors(direct_insert_row, dim=dim),

        ]
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.float_vec_field, index_params=index_params
        )
        # load collection
        self.collection_wrap.load()
        if insert_before_bulk_insert:
            # insert data
            self.collection_wrap.insert(data)
            self.collection_wrap.num_entities

        files = prepare_bulk_insert_new_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=True,
            rows=bulk_insert_row,
            dim=dim,
            data_fields=[df.pk_field, df.float_field, df.float_vec_field],
            force=True,
            schema=schema
        )
        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert success
        if not insert_before_bulk_insert:
            # insert data
            self.collection_wrap.insert(data)
            self.collection_wrap.num_entities

        num_entities = self.collection_wrap.num_entities
        log.info(f"collection entities: {num_entities}")
        assert num_entities == bulk_insert_row + direct_insert_row
        # verify index
        time.sleep(2)
        self.utility_wrap.wait_for_index_building_complete(c_name, timeout=300)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        log.info(f"index building progress: {res}")
        # verify search and query
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
        time.sleep(2)
        nq = 3
        topk = 10
        search_data = cf.gen_vectors(nq, dim=dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.float_vec_field,
            param=search_params,
            limit=topk,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": topk},
        )
        for hits in res:
            ids = hits.ids
            expr = f"{df.pk_field} in {ids}"
            expr = expr.replace("'", '"')
            results, _ = self.collection_wrap.query(expr=expr)
            assert len(results) == len(ids)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("create_index_before_bulk_insert", [True, False])
    @pytest.mark.parametrize("loaded_before_bulk_insert", [True, False])
    def test_load_before_or_after_bulk_insert(self, loaded_before_bulk_insert, create_index_before_bulk_insert):
        """
        collection schema: [pk, float_vector]
        Steps:
        1. create collection
        2. create index and load collection or not
        3. import data
        4. load collection or not
        5. verify the data entities
        5. verify the index status
        6. verify search and query
        """
        if loaded_before_bulk_insert and not create_index_before_bulk_insert:
            pytest.skip("can not load collection if index not created")
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=True,
            rows=500,
            dim=16,
            auto_id=True,
            data_fields=[df.vec_field],
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=16),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=True)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        if loaded_before_bulk_insert:
            # load collection
            self.collection_wrap.load()
        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert success
        if not loaded_before_bulk_insert:
            # load collection
            self.collection_wrap.load()

        num_entities = self.collection_wrap.num_entities
        log.info(f"collection entities: {num_entities}")
        assert num_entities == 500
        time.sleep(2)
        self.utility_wrap.wait_for_index_building_complete(c_name, timeout=300)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        log.info(f"index building progress: {res}")
        # verify search and query
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
        time.sleep(2)
        nq = 3
        topk = 10
        search_data = cf.gen_vectors(nq, 16)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.vec_field,
            param=search_params,
            limit=topk,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": topk},
        )
        for hits in res:
            ids = hits.ids
            expr = f"{df.pk_field} in {ids}"
            expr = expr.replace("'", '"')
            results, _ = self.collection_wrap.query(expr=expr)
            assert len(results) == len(ids)

    @pytest.mark.tags(CaseLabel.L3)
    def test_index_load_before_bulk_insert(self):
        """
        Steps:
        1. create collection
        2. create index and load collection
        3. import data
        4. verify
        """
        enable_dynamic_field = True
        auto_id = True
        dim = 128
        entities = 1000
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),

        ]
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_new_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
            schema=schema
        )
        # create index and load before bulk insert
        scalar_field_list = [df.int_field, df.float_field, df.double_field, df.string_field]
        scalar_fields = [f.name for f in fields if f.name in scalar_field_list]
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        binary_vec_fields = [f.name for f in fields if "vec" in f.name and "binary" in f.name]
        for f in scalar_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params={"index_type": "INVERTED"}
            )
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_index
            )
        for f in binary_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_binary_index
            )
        self.collection_wrap.load()

        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
        time.sleep(5)

        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        # query data
        for f in scalar_fields:
            if f == df.string_field:
                expr = f"{f} > '0'"
            else:
                expr = f"{f} > 0"
            res, result = self.collection_wrap.query(expr=expr, output_fields=["count(*)"])
            log.info(f"query result: {res}")
            assert result
        # search data
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        for field_name in float_vec_fields:
            res, _ = self.collection_wrap.search(
                search_data,
                field_name,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search

        _, search_data = cf.gen_binary_vectors(1, dim)
        search_params = ct.default_search_binary_params
        for field_name in binary_vec_fields:
            res, _ = self.collection_wrap.search(
                search_data,
                field_name,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [2000])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_bulk_insert_all_field_with_new_json_format(self, auto_id, dim, entities, enable_dynamic_field,
                                                        enable_partition_key, nullable):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.npy and uid.npy,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        if enable_partition_key is True and nullable is True:
            pytest.skip("partition key field not support nullable")
        float_vec_field_dim = dim
        binary_vec_field_dim = ((dim+random.randint(-16, 32)) // 8) * 8
        bf16_vec_field_dim = dim+random.randint(-16, 32)
        fp16_vec_field_dim = dim+random.randint(-16, 32)
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field, nullable=nullable),
            cf.gen_float_field(name=df.float_field, nullable=nullable),
            cf.gen_string_field(name=df.string_field, is_partition_key=enable_partition_key, nullable=nullable),
            cf.gen_string_field(name=df.text_field, enable_analyzer=True, enable_match=True, nullable=nullable),
            cf.gen_json_field(name=df.json_field, nullable=nullable),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64, nullable=nullable),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT, nullable=nullable),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=100, nullable=nullable),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL, nullable=nullable),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=float_vec_field_dim),
            cf.gen_binary_vec_field(name=df.binary_vec_field, dim=binary_vec_field_dim),
            cf.gen_bfloat16_vec_field(name=df.bf16_vec_field, dim=bf16_vec_field_dim),
            cf.gen_float16_vec_field(name=df.fp16_vec_field, dim=fp16_vec_field_dim)
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)

        files = prepare_bulk_insert_new_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
            schema=schema
        )
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        index_params = ct.default_index
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        binary_vec_fields = [f.name for f in fields if "vec" in f.name and "binary" in f.name]
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=index_params
            )
        for f in binary_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_binary_index
            )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")

        for f in [df.float_vec_field, df.bf16_vec_field, df.fp16_vec_field]:
            vector_data_type = "FLOAT_VECTOR"
            if f == df.float_vec_field:
                dim = float_vec_field_dim
                vector_data_type = "FLOAT_VECTOR"
            elif f == df.bf16_vec_field:
                dim = bf16_vec_field_dim
                vector_data_type = "BFLOAT16_VECTOR"
            else:
                dim = fp16_vec_field_dim
                vector_data_type = "FLOAT16_VECTOR"

            search_data = cf.gen_vectors(1, dim, vector_data_type=vector_data_type)
            search_params = ct.default_search_params
            res, _ = self.collection_wrap.search(
                search_data,
                f,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search

        _, search_data = cf.gen_binary_vectors(1, binary_vec_field_dim)
        search_params = ct.default_search_binary_params
        for field_name in binary_vec_fields:
            res, _ = self.collection_wrap.search(
                search_data,
                field_name,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search
        # query data
        if not nullable:
            expr_field = df.string_field
            expr = f"{expr_field} >= '0'"
        else:
            res, _ = self.collection_wrap.query(expr=f"{df.string_field} >= '0'", output_fields=[df.string_field, df.int_field])
            assert len(res) == 0
            expr_field = df.pk_field
            expr = f"{expr_field} >= 0"

        res, _ = self.collection_wrap.query(expr=f"{expr}", output_fields=[expr_field, df.int_field])
        assert len(res) == entities
        log.info(res)
        query_data = [r[expr_field] for r in res][:len(self.collection_wrap.partitions)]
        res, _ = self.collection_wrap.query(expr=f"{expr_field} in {query_data}", output_fields=[expr_field])
        assert len(res) == len(query_data)
        res, _ = self.collection_wrap.query(expr=f"text_match({df.text_field}, 'milvus')", output_fields=[df.text_field])
        if nullable is False:
            assert len(res) == entities
        else:
            assert 0 < len(res) < entities
        if enable_partition_key:
            assert len(self.collection_wrap.partitions) > 1

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [2000])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("include_meta", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_bulk_insert_all_field_with_numpy(self, auto_id, dim, entities, enable_dynamic_field, enable_partition_key, include_meta, nullable):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.npy and uid.npy,
        note: numpy file is not supported for array field
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        if enable_dynamic_field is False and include_meta is True:
            pytest.skip("include_meta only works with enable_dynamic_field")
        if nullable is True:
            pytest.skip("not support bulk insert numpy files in field which set nullable == true")
        float_vec_field_dim = dim
        binary_vec_field_dim = ((dim+random.randint(-16, 32)) // 8) * 8
        bf16_vec_field_dim = dim+random.randint(-16, 32)
        fp16_vec_field_dim = dim+random.randint(-16, 32)
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field, nullable=nullable),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field, is_partition_key=enable_partition_key),
            cf.gen_string_field(name=df.text_field, enable_analyzer=True, enable_match=True, nullable=nullable),
            cf.gen_json_field(name=df.json_field),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=float_vec_field_dim),
            cf.gen_binary_vec_field(name=df.binary_vec_field, dim=binary_vec_field_dim),
            cf.gen_bfloat16_vec_field(name=df.bf16_vec_field, dim=bf16_vec_field_dim),
            cf.gen_float16_vec_field(name=df.fp16_vec_field, dim=fp16_vec_field_dim)
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)

        files = prepare_bulk_insert_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
            schema=schema
        )
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        index_params = ct.default_index
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        binary_vec_fields = [f.name for f in fields if "vec" in f.name and "binary" in f.name]
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=index_params
            )
        for f in binary_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_binary_index
            )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")

        for f in [df.float_vec_field, df.bf16_vec_field, df.fp16_vec_field]:
            vector_data_type = "FLOAT_VECTOR"
            if f == df.float_vec_field:
                dim = float_vec_field_dim
                vector_data_type = "FLOAT_VECTOR"
            elif f == df.bf16_vec_field:
                dim = bf16_vec_field_dim
                vector_data_type = "BFLOAT16_VECTOR"
            else:
                dim = fp16_vec_field_dim
                vector_data_type = "FLOAT16_VECTOR"

            search_data = cf.gen_vectors(1, dim, vector_data_type=vector_data_type)
            search_params = ct.default_search_params
            res, _ = self.collection_wrap.search(
                search_data,
                f,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search

        _, search_data = cf.gen_binary_vectors(1, binary_vec_field_dim)
        search_params = ct.default_search_binary_params
        for field_name in binary_vec_fields:
            res, _ = self.collection_wrap.search(
                search_data,
                field_name,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search
        # query data
        res, _ = self.collection_wrap.query(expr=f"{df.string_field} >= '0'", output_fields=[df.string_field])
        assert len(res) == entities
        query_data = [r[df.string_field] for r in res][:len(self.collection_wrap.partitions)]
        res, _ = self.collection_wrap.query(expr=f"{df.string_field} in {query_data}", output_fields=[df.string_field])
        assert len(res) == len(query_data)
        res, _ = self.collection_wrap.query(expr=f"TEXT_MATCH({df.text_field}, 'milvus')", output_fields=[df.text_field])
        if nullable is False:
            assert len(res) == entities
        else:
            assert 0 < len(res) < entities
        if enable_partition_key:
            assert len(self.collection_wrap.partitions) > 1

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [2000])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("include_meta", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_bulk_insert_all_field_with_parquet(self, auto_id, dim, entities, enable_dynamic_field,
                                                enable_partition_key, include_meta, nullable):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        if enable_dynamic_field is False and include_meta is True:
            pytest.skip("include_meta only works with enable_dynamic_field")
        if enable_partition_key is True and nullable is True:
            pytest.skip("partition key field not support nullable")
        float_vec_field_dim = dim
        binary_vec_field_dim = ((dim+random.randint(-16, 32)) // 8) * 8
        bf16_vec_field_dim = dim+random.randint(-16, 32)
        fp16_vec_field_dim = dim+random.randint(-16, 32)
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field, nullable=nullable),
            cf.gen_float_field(name=df.float_field, nullable=nullable),
            cf.gen_string_field(name=df.string_field, is_partition_key=enable_partition_key, nullable=nullable),
            cf.gen_string_field(name=df.text_field, enable_analyzer=True, enable_match=True, nullable=nullable),
            cf.gen_json_field(name=df.json_field, nullable=nullable),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64, nullable=nullable),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT, nullable=nullable),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=100, nullable=nullable),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL, nullable=nullable),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=float_vec_field_dim),
            cf.gen_binary_vec_field(name=df.binary_vec_field, dim=binary_vec_field_dim),
            cf.gen_bfloat16_vec_field(name=df.bf16_vec_field, dim=bf16_vec_field_dim),
            cf.gen_float16_vec_field(name=df.fp16_vec_field, dim=fp16_vec_field_dim)
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)

        files = prepare_bulk_insert_parquet_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
            schema=schema
        )
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        index_params = ct.default_index
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        binary_vec_fields = [f.name for f in fields if "vec" in f.name and "binary" in f.name]
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=index_params
            )
        for f in binary_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_binary_index
            )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")

        for f in [df.float_vec_field, df.bf16_vec_field, df.fp16_vec_field]:
            vector_data_type = "FLOAT_VECTOR"
            if f == df.float_vec_field:
                dim = float_vec_field_dim
                vector_data_type = "FLOAT_VECTOR"
            elif f == df.bf16_vec_field:
                dim = bf16_vec_field_dim
                vector_data_type = "BFLOAT16_VECTOR"
            else:
                dim = fp16_vec_field_dim
                vector_data_type = "FLOAT16_VECTOR"

            search_data = cf.gen_vectors(1, dim, vector_data_type=vector_data_type)
            search_params = ct.default_search_params
            res, _ = self.collection_wrap.search(
                search_data,
                f,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search

        _, search_data = cf.gen_binary_vectors(1, binary_vec_field_dim)
        search_params = ct.default_search_binary_params
        for field_name in binary_vec_fields:
            res, _ = self.collection_wrap.search(
                search_data,
                field_name,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search
        # query data
        if not nullable:
            expr_field = df.string_field
            expr = f"{expr_field} >= '0'"
        else:
            res, _ = self.collection_wrap.query(expr=f"{df.string_field} >= '0'", output_fields=[df.string_field])
            assert len(res) == 0
            expr_field = df.pk_field
            expr = f"{expr_field} >= 0"

        res, _ = self.collection_wrap.query(expr=f"{expr}", output_fields=[df.string_field])
        assert len(res) == entities
        query_data = [r[expr_field] for r in res][:len(self.collection_wrap.partitions)]
        res, _ = self.collection_wrap.query(expr=f"{expr_field} in {query_data}", output_fields=[expr_field])
        assert len(res) == len(query_data)
        res, _ = self.collection_wrap.query(expr=f"TEXT_MATCH({df.text_field}, 'milvus')", output_fields=[df.text_field])
        if not nullable:
            assert len(res) == entities
        else:
            assert 0 < len(res) < entities

        if enable_partition_key:
            assert len(self.collection_wrap.partitions) > 1

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [2000])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("include_meta", [True, False])
    @pytest.mark.parametrize("sparse_format", ["doc", "coo"])
    def test_bulk_insert_sparse_vector_with_parquet(self, auto_id, dim, entities, enable_dynamic_field, include_meta, sparse_format):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        if enable_dynamic_field is False and include_meta is True:
            pytest.skip("include_meta only works with enable_dynamic_field")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=100),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
            cf.gen_sparse_vec_field(name=df.sparse_vec_field),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        files = prepare_bulk_insert_parquet_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
            include_meta=include_meta,
            sparse_format=sparse_format,
            schema=schema
        )

        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        index_params = ct.default_index
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        sparse_vec_fields = [f.name for f in fields if "vec" in f.name and "sparse" in f.name]
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=index_params
            )
        for f in sparse_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_sparse_inverted_index
            )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        for field_name in float_vec_fields:
            res, _ = self.collection_wrap.search(
                search_data,
                field_name,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field and include_meta:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search
        search_data = cf.gen_sparse_vectors(1, dim)
        search_params = ct.default_sparse_search_params
        for field_name in sparse_vec_fields:
            res, _ = self.collection_wrap.search(
                search_data,
                field_name,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field and include_meta:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [2000])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("include_meta", [True, False])
    @pytest.mark.parametrize("sparse_format", ["doc", "coo"])
    def test_bulk_insert_sparse_vector_with_json(self, auto_id, dim, entities, enable_dynamic_field, include_meta, sparse_format):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        if enable_dynamic_field is False and include_meta is True:
            pytest.skip("include_meta only works with enable_dynamic_field")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=100),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
            cf.gen_sparse_vec_field(name=df.sparse_vec_field),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        files = prepare_bulk_insert_new_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            enable_dynamic_field=enable_dynamic_field,
            force=True,
            include_meta=include_meta,
            sparse_format=sparse_format,
            schema=schema
        )
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt} with states:{states}")
        assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        index_params = ct.default_index
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        sparse_vec_fields = [f.name for f in fields if "vec" in f.name and "sparse" in f.name]
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=index_params
            )
        for f in sparse_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_sparse_inverted_index
            )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        for field_name in float_vec_fields:
            res, _ = self.collection_wrap.search(
                search_data,
                field_name,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field and include_meta:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search
        search_data = cf.gen_sparse_vectors(1, dim)
        search_params = ct.default_sparse_search_params
        for field_name in sparse_vec_fields:
            res, _ = self.collection_wrap.search(
                search_data,
                field_name,
                param=search_params,
                limit=1,
                output_fields=["*"],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
            for hit in res:
                for r in hit:
                    fields_from_search = r.fields.keys()
                    for f in fields:
                        assert f.name in fields_from_search
                    if enable_dynamic_field and include_meta:
                        assert "name" in fields_from_search
                        assert "address" in fields_from_search

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("sparse_format", ["doc", "coo"])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_with_all_field_json_with_bulk_writer(self, auto_id, dim, entities, enable_dynamic_field, sparse_format, nullable):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.npy and uid.npy,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        self._connect()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field, nullable=nullable),
            cf.gen_float_field(name=df.float_field, nullable=nullable),
            cf.gen_string_field(name=df.string_field, nullable=nullable),
            cf.gen_json_field(name=df.json_field, nullable=nullable),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64, nullable=nullable),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT, nullable=nullable),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=100, nullable=nullable),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL, nullable=nullable),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
            cf.gen_float16_vec_field(name=df.fp16_vec_field, dim=dim),
            cf.gen_bfloat16_vec_field(name=df.bf16_vec_field, dim=dim),
            cf.gen_sparse_vec_field(name=df.sparse_vec_field),
        ]
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)
        with RemoteBulkWriter(
            schema=schema,
            remote_path="bulk_data",
            connect_param=RemoteBulkWriter.ConnectParam(
                bucket_name=self.bucket_name,
                endpoint=self.minio_endpoint,
                access_key="minioadmin",
                secret_key="minioadmin",
            ),
            file_type=BulkFileType.JSON,
        ) as remote_writer:
            json_value = [
                # 1,
                # 1.0,
                # "1",
                # [1, 2, 3],
                # ["1", "2", "3"],
                # [1, 2, "3"],
                {"key": "value"},
            ]
            for i in range(entities):
                row = {
                    df.pk_field: i,
                    df.int_field: 1 if not (nullable and random.random() < 0.5) else None,
                    df.float_field: 1.0 if not (nullable and random.random() < 0.5) else None,
                    df.string_field: "string" if not (nullable and random.random() < 0.5) else None,
                    df.json_field: json_value[i%len(json_value)] if not (nullable and random.random() < 0.5) else None,
                    df.array_int_field: [1, 2] if not (nullable and random.random() < 0.5) else None,
                    df.array_float_field: [1.0, 2.0] if not (nullable and random.random() < 0.5) else None,
                    df.array_string_field: ["string1", "string2"] if not (nullable and random.random() < 0.5) else None,
                    df.array_bool_field: [True, False] if not (nullable and random.random() < 0.5) else None,
                    df.float_vec_field: cf.gen_vectors(1, dim)[0],
                    df.fp16_vec_field: cf.gen_vectors(1, dim, vector_data_type="FLOAT16_VECTOR")[0],
                    df.bf16_vec_field: cf.gen_vectors(1, dim, vector_data_type="BFLOAT16_VECTOR")[0],
                    df.sparse_vec_field: cf.gen_sparse_vectors(1, dim, sparse_format=sparse_format)[0]
                }
                if auto_id:
                    row.pop(df.pk_field)
                if enable_dynamic_field:
                    row["name"] = fake.name()
                    row["address"] = fake.address()
                remote_writer.append_row(row)
            remote_writer.commit()
            files = remote_writer.batch_files
        # import data
        for f in files:
            t0 = time.time()
            task_id, _ = self.utility_wrap.do_bulk_insert(
                collection_name=c_name, files=f
            )
            logging.info(f"bulk insert task ids:{task_id}")
            success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
                task_ids=[task_id], timeout=300
            )
            tt = time.time() - t0
            log.info(f"bulk insert state:{success} in {tt} with states:{states}")
            assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        index_params = ct.default_index
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        sparse_vec_fields = [f.name for f in fields if "vec" in f.name and "sparse" in f.name]
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=index_params
            )
        for f in sparse_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_sparse_inverted_index
            )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.float_vec_field,
            param=search_params,
            limit=1,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )
        for hit in res:
            for r in hit:
                fields_from_search = r.fields.keys()
                for f in fields:
                    assert f.name in fields_from_search
                if enable_dynamic_field:
                    assert "name" in fields_from_search
                    assert "address" in fields_from_search


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("entities", [1000])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    @pytest.mark.parametrize("sparse_format", ["doc"])
    @pytest.mark.parametrize("file_format", ["parquet", "json"])
    def test_with_all_field_and_bm25_function_with_bulk_writer(self, auto_id, dim, entities, enable_dynamic_field, sparse_format, file_format):
        """
        target: test bulk insert with all field and bm25 function
        method: create collection with all field and bm25 function, then import data with bulk writer
        expected: verify data imported correctly
        """
        self._connect()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_string_field(name=df.text_field, enable_analyzer=True, enable_match=True),
            cf.gen_json_field(name=df.json_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=100),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
            cf.gen_sparse_vec_field(name=df.sparse_vec_field),
            cf.gen_sparse_vec_field(name=df.bm25_sparse_vec_field),
        ]
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=[df.text_field],
            output_field_names=[df.bm25_sparse_vec_field],
            params={},
        )
        schema.add_function(bm25_function)
        self.collection_wrap.init_collection(c_name, schema=schema)
        documents = []
        if file_format == "parquet":
            ff = BulkFileType.PARQUET
        elif file_format == "json":
            ff = BulkFileType.JSON
        else:
            raise Exception(f"not support file format:{file_format}")
        with RemoteBulkWriter(
            schema=schema,
            remote_path="bulk_data",
            connect_param=RemoteBulkWriter.ConnectParam(
                bucket_name=self.bucket_name,
                endpoint=self.minio_endpoint,
                access_key="minioadmin",
                secret_key="minioadmin",
            ),
            file_type=ff,
        ) as remote_writer:
            json_value = [
                # 1,
                # 1.0,
                # "1",
                # [1, 2, 3],
                # ["1", "2", "3"],
                # [1, 2, "3"],
                {"key": "value"},
            ]
            for i in range(entities):
                row = {
                    df.pk_field: i,
                    df.int_field: 1,
                    df.float_field: 1.0,
                    df.string_field: "string",
                    df.text_field: fake.text(),
                    df.json_field: json_value[i%len(json_value)],
                    df.array_int_field: [1, 2],
                    df.array_float_field: [1.0, 2.0],
                    df.array_string_field: ["string1", "string2"],
                    df.array_bool_field: [True, False],
                    df.float_vec_field: cf.gen_vectors(1, dim)[0],
                    df.sparse_vec_field: cf.gen_sparse_vectors(1, dim, sparse_format=sparse_format)[0]
                }
                if auto_id:
                    row.pop(df.pk_field)
                if enable_dynamic_field:
                    row["name"] = fake.name()
                    row["address"] = fake.address()
                documents.append(row[df.text_field])
                remote_writer.append_row(row)
            remote_writer.commit()
            files = remote_writer.batch_files
        # import data
        for f in files:
            t0 = time.time()
            task_id, _ = self.utility_wrap.do_bulk_insert(
                collection_name=c_name, files=f
            )
            logging.info(f"bulk insert task ids:{task_id}")
            success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
                task_ids=[task_id], timeout=300
            )
            tt = time.time() - t0
            log.info(f"bulk insert state:{success} in {tt} with states:{states}")
            assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        index_params = ct.default_index
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        sparse_vec_fields = [f.name for f in fields if "vec" in f.name and "sparse" in f.name and "bm25" not in f.name]
        bm25_sparse_vec_fields = [f.name for f in fields if "vec" in f.name and "sparse" in f.name and "bm25" in f.name]
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=index_params
            )
        for f in sparse_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_sparse_inverted_index
            )
        for f in bm25_sparse_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_text_sparse_inverted_index
            )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.float_vec_field,
            param=search_params,
            limit=1,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )
        for hit in res:
            for r in hit:
                fields_from_search = r.fields.keys()
                for f in fields:
                    if f.name == df.bm25_sparse_vec_field:
                        continue
                    assert f.name in fields_from_search
                if enable_dynamic_field:
                    assert "name" in fields_from_search
                    assert "address" in fields_from_search

        # verify full text search
        word_freq = cf.analyze_documents(documents)
        token = word_freq.most_common(1)[0][0]

        search_data = [f" {token} " + fake.text()]
        search_params = ct.default_text_sparse_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.bm25_sparse_vec_field,
            param=search_params,
            limit=1,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_with_all_field_numpy_with_bulk_writer(self, auto_id, dim, entities, enable_dynamic_field, nullable):
        """
        """
        if nullable is True:
            pytest.skip("not support bulk writer numpy files in field(int_scalar) which has 'None' data")

        self._connect()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field, nullable=nullable),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_json_field(name=df.json_field),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
            cf.gen_float16_vec_field(name=df.fp16_vec_field, dim=dim),
            cf.gen_bfloat16_vec_field(name=df.bf16_vec_field, dim=dim),
        ]
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)
        with RemoteBulkWriter(
            schema=schema,
            remote_path="bulk_data",
            connect_param=RemoteBulkWriter.ConnectParam(
                bucket_name=self.bucket_name,
                endpoint=self.minio_endpoint,
                access_key="minioadmin",
                secret_key="minioadmin",
            ),
            file_type=BulkFileType.NUMPY,
        ) as remote_writer:
            json_value = [
                # 1,
                # 1.0,
                # "1",
                # [1, 2, 3],
                # ["1", "2", "3"],
                # [1, 2, "3"],
                {"key": "value"},
            ]
            for i in range(entities):
                row = {
                    df.pk_field: i,
                    df.int_field: 1 if not (nullable and random.random() < 0.5) else None,
                    df.float_field: 1.0,
                    df.string_field: "string",
                    df.json_field: json_value[i%len(json_value)],
                    df.float_vec_field: cf.gen_vectors(1, dim)[0],
                    df.fp16_vec_field: cf.gen_vectors(1, dim, vector_data_type="FLOAT16_VECTOR")[0],
                    df.bf16_vec_field: cf.gen_vectors(1, dim, vector_data_type="BFLOAT16_VECTOR")[0],
                }
                if auto_id:
                    row.pop(df.pk_field)
                if enable_dynamic_field:
                    row["name"] = fake.name()
                    row["address"] = fake.address()
                remote_writer.append_row(row)
            remote_writer.commit()
            files = remote_writer.batch_files
        # import data
        for f in files:
            t0 = time.time()
            task_id, _ = self.utility_wrap.do_bulk_insert(
                collection_name=c_name, files=f
            )
            logging.info(f"bulk insert task ids:{task_id}")
            success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
                task_ids=[task_id], timeout=300
            )
            tt = time.time() - t0
            log.info(f"bulk insert state:{success} in {tt} with states:{states}")
            assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        index_params = ct.default_index
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        sparse_vec_fields = [f.name for f in fields if "vec" in f.name and "sparse" in f.name]
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=index_params
            )
        for f in sparse_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_sparse_inverted_index
            )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.float_vec_field,
            param=search_params,
            limit=1,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )
        for hit in res:
            for r in hit:
                fields_from_search = r.fields.keys()
                for f in fields:
                    assert f.name in fields_from_search
                if enable_dynamic_field:
                    assert "name" in fields_from_search
                    assert "address" in fields_from_search

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("sparse_format", ["doc", "coo"])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_with_all_field_parquet_with_bulk_writer(self, auto_id, dim, entities, enable_dynamic_field, sparse_format, nullable):
        """
        """
        self._connect()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field, nullable=nullable),
            cf.gen_float_field(name=df.float_field, nullable=nullable),
            cf.gen_string_field(name=df.string_field, nullable=nullable),
            cf.gen_json_field(name=df.json_field, nullable=nullable),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64, nullable=nullable),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT, nullable=nullable),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=100, nullable=nullable),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL, nullable=nullable),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
            cf.gen_float16_vec_field(name=df.fp16_vec_field, dim=dim),
            cf.gen_bfloat16_vec_field(name=df.bf16_vec_field, dim=dim),
            cf.gen_sparse_vec_field(name=df.sparse_vec_field),
        ]
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)
        self.collection_wrap.init_collection(c_name, schema=schema)
        with RemoteBulkWriter(
            schema=schema,
            remote_path="bulk_data",
            connect_param=RemoteBulkWriter.ConnectParam(
                bucket_name=self.bucket_name,
                endpoint=self.minio_endpoint,
                access_key="minioadmin",
                secret_key="minioadmin",
            ),
            file_type=BulkFileType.JSON,
        ) as remote_writer:
            json_value = [
                # 1,
                # 1.0,
                # "1",
                # [1, 2, 3],
                # ["1", "2", "3"],
                # [1, 2, "3"],
                {"key": "value"},
            ]
            for i in range(entities):
                row = {
                    df.pk_field: i,
                    df.int_field: 1 if not (nullable and random.random() < 0.5) else None,
                    df.float_field: 1.0 if not (nullable and random.random() < 0.5) else None,
                    df.string_field: "string" if not (nullable and random.random() < 0.5) else None,
                    df.json_field: json_value[i%len(json_value)] if not (nullable and random.random() < 0.5) else None,
                    df.array_int_field: [1, 2] if not (nullable and random.random() < 0.5) else None,
                    df.array_float_field: [1.0, 2.0] if not (nullable and random.random() < 0.5) else None,
                    df.array_string_field: ["string1", "string2"] if not (nullable and random.random() < 0.5) else None,
                    df.array_bool_field: [True, False] if not (nullable and random.random() < 0.5) else None,
                    df.float_vec_field: cf.gen_vectors(1, dim)[0],
                    df.fp16_vec_field: cf.gen_vectors(1, dim, vector_data_type="FLOAT16_VECTOR")[0],
                    df.bf16_vec_field: cf.gen_vectors(1, dim, vector_data_type="BFLOAT16_VECTOR")[0],
                    df.sparse_vec_field: cf.gen_sparse_vectors(1, dim, sparse_format=sparse_format)[0]
                }
                if auto_id:
                    row.pop(df.pk_field)
                if enable_dynamic_field:
                    row["name"] = fake.name()
                    row["address"] = fake.address()
                remote_writer.append_row(row)
            remote_writer.commit()
            files = remote_writer.batch_files
        # import data
        for f in files:
            t0 = time.time()
            task_id, _ = self.utility_wrap.do_bulk_insert(
                collection_name=c_name, files=f
            )
            logging.info(f"bulk insert task ids:{task_id}")
            success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
                task_ids=[task_id], timeout=300
            )
            tt = time.time() - t0
            log.info(f"bulk insert state:{success} in {tt} with states:{states}")
            assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
        # verify imported data is available for search
        index_params = ct.default_index
        float_vec_fields = [f.name for f in fields if "vec" in f.name and "float" in f.name]
        sparse_vec_fields = [f.name for f in fields if "vec" in f.name and "sparse" in f.name]
        for f in float_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=index_params
            )
        for f in sparse_vec_fields:
            self.collection_wrap.create_index(
                field_name=f, index_params=ct.default_sparse_inverted_index
            )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.float_vec_field,
            param=search_params,
            limit=1,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )
        for hit in res:
            for r in hit:
                fields_from_search = r.fields.keys()
                for f in fields:
                    assert f.name in fields_from_search
                if enable_dynamic_field:
                    assert "name" in fields_from_search
                    assert "address" in fields_from_search


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    @pytest.mark.parametrize("file_nums", [0, 10])
    @pytest.mark.parametrize("array_len", [1])
    def test_with_wrong_parquet_file_num(self, auto_id, dim, entities, file_nums, array_len):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.parquet and uid.parquet,
        Steps:
        1. create collection
        2. import data
        3. verify failure, because only one file is allowed
        """
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64),
            cf.gen_array_field(name=df.array_float_field, element_type=DataType.FLOAT),
            cf.gen_array_field(name=df.array_string_field, element_type=DataType.VARCHAR, max_length=100),
            cf.gen_array_field(name=df.array_bool_field, element_type=DataType.BOOL),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_parquet_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            file_nums=file_nums,
            array_length=array_len,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        error = {}
        if file_nums == 0:
            error = {ct.err_code: 1100, ct.err_msg: "import request is empty"}
        if file_nums > 1:
            error = {ct.err_code: 65535, ct.err_msg: "for Parquet import, accepts only one file"}
        self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files,
            check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("entities", [2000])
    @pytest.mark.parametrize("file_nums", [5])
    def test_multi_numpy_files_from_diff_folders(
        self, auto_id, dim, entities, file_nums
    ):
        """
        collection schema 1: [pk, float_vector]
        data file: .npy files in different folders
        Steps:
        1. create collection, create index and load
        2. import data
        3. verify that import numpy files in a loop
        """
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        # load collection
        self.collection_wrap.load()
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        task_ids = []
        for i in range(file_nums):
            files = prepare_bulk_insert_numpy_files(
                minio_endpoint=self.minio_endpoint,
                bucket_name=self.bucket_name,
                rows=entities,
                dim=dim,
                data_fields=data_fields,
                file_nums=1,
                force=True,
            )
            task_id, _ = self.utility_wrap.do_bulk_insert(
                collection_name=c_name, files=files
            )
            task_ids.append(task_id)
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=task_ids, timeout=300
        )
        log.info(f"bulk insert state:{success}")

        assert success
        log.info(f" collection entities: {self.collection_wrap.num_entities}")
        assert self.collection_wrap.num_entities == entities * file_nums

        # verify search and query
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
        time.sleep(2)
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.vec_field,
            param=search_params,
            limit=1,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("par_key_field", [df.int_field, df.string_field])
    def test_partition_key_on_json_file(self, is_row_based, auto_id, par_key_field):
        """
        collection: auto_id, customized_id
        collection schema: [pk, int64, varchar, float_vector]
        Steps:
        1. create collection with partition key enabled
        2. import data
        3. verify the data entities equal the import data and distributed by values of partition key field
        4. load the collection
        5. verify search successfully
        6. verify query successfully
        """
        dim = 12
        entities = 200
        self._connect()
        c_name = cf.gen_unique_str("bulk_partition_key")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_float_vec_field(name=df.float_vec_field, dim=dim),
            cf.gen_int64_field(name=df.int_field, is_partition_key=(par_key_field == df.int_field)),
            cf.gen_string_field(name=df.string_field, is_partition_key=(par_key_field == df.string_field)),
            cf.gen_bool_field(name=df.bool_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_array_field(name=df.array_int_field, element_type=DataType.INT64)
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        files = prepare_bulk_insert_new_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=data_fields,
            force=True,
            schema=schema
        )
        self.collection_wrap.init_collection(c_name, schema=schema, num_partitions=10)
        assert len(self.collection_wrap.partitions) == 10

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=None,
            files=files,
        )
        logging.info(f"bulk insert task id:{task_id}")
        success, _ = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert success

        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities

        # verify imported data is available for search
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.float_vec_field, index_params=index_params
        )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(10)
        log.info(
            f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
        )
        nq = 2
        topk = 2
        search_data = cf.gen_vectors(nq, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.float_vec_field,
            param=search_params,
            limit=topk,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "limit": topk},
        )
        for hits in res:
            ids = hits.ids
            results, _ = self.collection_wrap.query(expr=f"{df.pk_field} in {ids}")
            assert len(results) == len(ids)

        # verify data was bulk inserted into different partitions
        num_entities = 0
        empty_partition_num = 0
        for p in self.collection_wrap.partitions:
            if p.num_entities == 0:
                empty_partition_num += 1
            num_entities += p.num_entities
        assert num_entities == entities

        # verify error when trying to bulk insert into a specific partition
        err_msg = "not allow to set partition name for collection with partition key"
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=self.collection_wrap.partitions[0].name,
            files=files,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 2100, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [13])
    @pytest.mark.parametrize("entities", [150])
    @pytest.mark.parametrize("file_nums", [10])
    def test_partition_key_on_multi_numpy_files(
            self, auto_id, dim, entities, file_nums
    ):
        """
        collection schema 1: [pk, int64, float_vector, double]
        data file: .npy files in different folders
        Steps:
        1. create collection with partition key enabled, create index and load
        2. import data
        3. verify that import numpy files in a loop
        """
        self._connect()
        c_name = cf.gen_unique_str("bulk_ins_parkey")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_int64_field(name=df.int_field, is_partition_key=True),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema, num_partitions=10)
        # build index
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        # load collection
        self.collection_wrap.load()
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        task_ids = []
        for i in range(file_nums):
            files = prepare_bulk_insert_numpy_files(
                minio_endpoint=self.minio_endpoint,
                bucket_name=self.bucket_name,
                rows=entities,
                dim=dim,
                data_fields=data_fields,
                file_nums=1,
                force=True,
            )
            task_id, _ = self.utility_wrap.do_bulk_insert(
                collection_name=c_name, files=files
            )
            task_ids.append(task_id)
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=task_ids, timeout=300
        )
        log.info(f"bulk insert state:{success}")

        assert success
        log.info(f" collection entities: {self.collection_wrap.num_entities}")
        assert self.collection_wrap.num_entities == entities * file_nums
        # verify imported data is indexed
        success = self.utility_wrap.wait_index_build_completed(c_name)
        assert success
        # verify search and query
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
        time.sleep(2)
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(
            search_data,
            df.vec_field,
            param=search_params,
            limit=1,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )

        # verify data was bulk inserted into different partitions
        num_entities = 0
        empty_partition_num = 0
        for p in self.collection_wrap.partitions:
            if p.num_entities == 0:
                empty_partition_num += 1
            num_entities += p.num_entities
        assert num_entities == entities * file_nums

    @pytest.mark.parametrize("pk_field", [df.pk_field, df.string_field])
    @pytest.mark.tags(CaseLabel.L3)
    def test_bulk_import_random_pk_stats_task(self, pk_field):
        # connect -> prepare json data
        self._connect()
        collection_name = cf.gen_unique_str("stats_task")
        nb = 3000
        fields = []
        files = ""

        # prepare data: int64_pk -> json data; varchar_pk -> numpy data
        if pk_field == df.pk_field:
            fields = [
                cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=False),
                cf.gen_float_vec_field(name=df.float_vec_field, dim=ct.default_dim),
            ]
            data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
            files = prepare_bulk_insert_new_json_files(
                minio_endpoint=self.minio_endpoint, bucket_name=self.bucket_name,
                is_row_based=True, rows=nb, dim=ct.default_dim, auto_id=False, data_fields=data_fields, force=True,
                shuffle=True
            )
        elif pk_field == df.string_field:
            fields = [
                cf.gen_string_field(name=df.string_field, is_primary=True, auto_id=False),
                cf.gen_float_vec_field(name=df.float_vec_field, dim=ct.default_dim),
            ]
            data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
            files = prepare_bulk_insert_numpy_files(
                minio_endpoint=self.minio_endpoint, bucket_name=self.bucket_name,
                rows=nb, dim=ct.default_dim, data_fields=data_fields, enable_dynamic_field=False, force=True,
                shuffle_pk=True
            )
        else:
            log.error(f"pk_field name {pk_field} not supported now, [{df.pk_field}, {df.string_field}] expected~")

        # create collection -> create vector index
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(collection_name, schema=schema)
        self.build_multi_index(index_params=DefaultVectorIndexParams.IVF_SQ8(df.float_vec_field))

        # bulk_insert data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=collection_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        completed, _ = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{completed} with latency {tt}")
        assert completed

        # load -> get_segment_info -> verify stats task
        self.collection_wrap.load()
        res_segment_info, _ = self.utility_wrap.get_query_segment_info(collection_name)
        assert len(res_segment_info) > 0  # maybe mix compaction to 1 segment
        cnt = 0
        for r in res_segment_info:
            log.info(f"segmentID {r.segmentID}: state: {r.state}; num_rows: {r.num_rows}; is_sorted: {r.is_sorted} ")
            cnt += r.num_rows
            assert r.is_sorted is True
        assert cnt == nb

        # verify search
        self.collection_wrap.search(
            data=cf.gen_vectors(ct.default_nq, ct.default_dim, vector_data_type=DataType.FLOAT_VECTOR.name),
            anns_field=df.float_vec_field, param=DefaultVectorSearchParams.IVF_SQ8(),
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": ct.default_nq,
                         "limit": ct.default_limit})

class TestImportWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test import with text embedding
    ******************************************************************
    """

    @pytest.mark.parametrize("file_format", ["json", "parquet", "numpy"])
    def test_import_without_embedding(self, tei_endpoint, minio_host, file_format):
        """
        target: test import data without embedding
        method: 1. create collection
                2. import data without embedding field
                3. verify embeddings are generated
        expected: embeddings should be generated after import
        """
        dim = 768
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)
        c_name = cf.gen_unique_str("import_without_embedding")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # prepare import data without embedding
        nb = 1000
        if file_format == "json":
            file_type = BulkFileType.JSON
        elif file_format == "numpy":
            file_type = BulkFileType.NUMPY
        else:
            file_type = BulkFileType.PARQUET
        with RemoteBulkWriter(
            schema=schema,
            remote_path="bulk_data",
            connect_param=RemoteBulkWriter.ConnectParam(
                bucket_name="milvus-bucket",
                endpoint=f"{minio_host}:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
            ),
            file_type=file_type,
        ) as remote_writer:
            for i in range(nb):
                row = {"id": i, "document": f"This is test document {i}"}
                remote_writer.append_row(row)
            remote_writer.commit()
            files = remote_writer.batch_files
        # import data
        for f in files:
            t0 = time.time()
            task_id, _ = self.utility_wrap.do_bulk_insert(
                collection_name=c_name, files=f
            )
            log.info(f"bulk insert task ids:{task_id}")
            success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
                task_ids=[task_id], timeout=300
            )
            tt = time.time() - t0
            log.info(f"bulk insert state:{success} in {tt} with states:{states}")
            assert success
        num_entities = collection_w.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == nb

        # create index and load
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()
        # verify embeddings are generated
        res, _ = collection_w.query(expr="id >= 0", output_fields=["dense"])
        assert len(res) == nb
        for r in res:
            assert "dense" in r
            assert len(r["dense"]) == dim
