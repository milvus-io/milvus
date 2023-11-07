import logging
import time
import pytest
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
    prepare_bulk_insert_numpy_files,
    DataField as df,
)


default_vec_only_fields = [df.vec_field]
default_multi_fields = [
    df.vec_field,
    df.int_field,
    df.string_field,
    df.bool_field,
    df.float_field,
]
default_vec_n_int_fields = [df.vec_field, df.int_field]


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
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_only_fields,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
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
            field_name=df.vec_field, index_params=index_params
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
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            str_pk=string_pk,
            data_fields=default_vec_only_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_string_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
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
            field_name=df.vec_field, index_params=index_params
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
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("entities", [3000])
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
    def test_binary_vector_only(self, is_row_based, auto_id, dim, entities):
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
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=True,
            rows=bulk_insert_row,
            dim=dim,
            data_fields=[df.pk_field, df.float_field, df.vec_field],
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_field(name=df.float_field),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
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
            field_name=df.vec_field, index_params=index_params
        )
        # load collection
        self.collection_wrap.load()
        if insert_before_bulk_insert:
            # insert data
            self.collection_wrap.insert(data)
            self.collection_wrap.num_entities
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
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    def test_with_all_field_numpy(self, auto_id, dim, entities):
        """
        collection schema 1: [pk, int64, float64, string float_vector]
        data file: vectors.npy and uid.npy,
        Steps:
        1. create collection
        2. import data
        3. verify
        """
        data_fields = [df.pk_field, df.int_field, df.float_field, df.double_field, df.vec_field]
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True, auto_id=auto_id),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        data_fields = [f.name for f in fields if not f.to_dict().get("auto_id", False)]
        files = prepare_bulk_insert_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
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
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        self.collection_wrap.load()
        log.info(f"wait for load finished and be ready for search")
        time.sleep(2)
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
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
            task_ids=[task_id], timeout=300
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
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_multi_fields,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_parkey")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int64_field(name=df.int_field, is_partition_key=(par_key_field == df.int_field)),
            cf.gen_string_field(name=df.string_field, is_partition_key=(par_key_field == df.string_field)),
            cf.gen_bool_field(name=df.bool_field),
            cf.gen_float_field(name=df.float_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        assert len(self.collection_wrap.partitions) == ct.default_partition_num

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
            field_name=df.vec_field, index_params=index_params
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

        # verify data was bulk inserted into different partitions
        num_entities = 0
        empty_partition_num = 0
        for p in self.collection_wrap.partitions:
            if p.num_entities == 0:
                empty_partition_num += 1
            num_entities += p.num_entities
        assert num_entities == entities

        # verify error when trying to bulk insert into a specific partition
        # TODO: enable the error msg assert after issue #25586 fixed
        err_msg = "not allow to set partition name for collection with partition key"
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=self.collection_wrap.partitions[0].name,
            files=files,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 99, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [13])
    @pytest.mark.parametrize("entities", [150])
    @pytest.mark.parametrize("file_nums", [10])
    @pytest.mark.xfail(reason="issue #28209")
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
        # verify imported data is indexed
        success = self.utility_wrap.wait_index_build_completed(c_name)
        assert success
        # verify search and query
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
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

