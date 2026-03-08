import logging
import time
import pytest
import random
import numpy as np
from pathlib import Path
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.milvus_sys import MilvusSys
from common.common_type import CaseLabel, CheckTasks
from utils.util_k8s import (
    get_pod_ip_name_pairs,
    get_milvus_instance_name,
    get_milvus_deploy_tool
)
from utils.util_log import test_log as log
from common.bulk_insert_data import (
    prepare_bulk_insert_json_files,
    prepare_bulk_insert_numpy_files,
    DataField as df,
    DataErrorType,
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
    def init_minio_client(self, host, milvus_ns):
        Path("/tmp/bulk_insert_data").mkdir(parents=True, exist_ok=True)
        self._connect()
        self.milvus_ns = milvus_ns
        self.milvus_sys = MilvusSys(alias='default')
        self.instance_name = get_milvus_instance_name(self.milvus_ns, host)
        self.deploy_tool = get_milvus_deploy_tool(self.milvus_ns, self.milvus_sys)
        minio_label = f"release={self.instance_name}, app=minio"
        if self.deploy_tool == "milvus-operator":
            minio_label = f"release={self.instance_name}-minio, app=minio"
        minio_ip_pod_pair = get_pod_ip_name_pairs(
            self.milvus_ns, minio_label
        )
        ms = MilvusSys()
        minio_ip = list(minio_ip_pod_pair.keys())[0]
        minio_port = "9000"
        self.minio_endpoint = f"{minio_ip}:{minio_port}"
        self.bucket_name = ms.data_nodes[0]["infos"]["system_configurations"][
            "minio_bucket_name"
        ]

    # def teardown_method(self, method):
    #     log.info(("*" * 35) + " teardown " + ("*" * 35))
    #     log.info("[teardown_method] Start teardown test case %s..." % method.__name__)


class TestBulkInsert(TestcaseBaseBulkInsert):

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])  # 8, 128
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
            task_ids=[task_id], timeout=90
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
        success = self.utility_wrap.wait_index_build_completed(c_name)
        assert success
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load()
        self.collection_wrap.load(_refresh=True)
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
    @pytest.mark.parametrize("dim", [8])  # 8
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
            force=True
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
            task_ids=[task_id], timeout=90
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
        success = self.utility_wrap.wait_index_build_completed(c_name)
        assert success
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load()
        self.collection_wrap.load(_reshard=True)
        log.info(
            f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
        )
        nq = 3
        topk = 2
        search_data = cf.gen_vectors(nq, dim)
        search_params = ct.default_search_params
        time.sleep(20)
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
    @pytest.mark.parametrize("dim", [4])
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
            task_ids=[task_id], timeout=120
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert success

        assert m_partition.num_entities == entities
        assert self.collection_wrap.num_entities == entities
        log.debug(state)
        success = self.utility_wrap.wait_index_build_completed(c_name)
        assert success
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(partition_names=[p_name], _refresh=True)
        time.sleep(10)
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
    @pytest.mark.parametrize("dim", [16])
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
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert success
        success = self.utility_wrap.wait_index_build_completed(c_name)
        assert success
        # verify num entities
        assert self.collection_wrap.num_entities == entities
        # verify search and query
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
        time.sleep(10)
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
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize(
        "fields_num_in_file", ["more", "less", "equal"]
    )  # "equal", "more", "less"
    @pytest.mark.parametrize("dim", [16])
    @pytest.mark.parametrize("entities", [500])
    def test_float_vector_multi_scalars(
        self, is_row_based, auto_id, fields_num_in_file, dim, entities
    ):
        """
        collection schema: [pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        Steps:
        1. create collection
        2. create index and load collection
        3. import data
        4. verify the data entities
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
            data_fields=default_multi_fields,
            force=True,
        )
        additional_field = "int_scalar_add"
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
            cf.gen_float_field(name=df.float_field),
        ]
        if fields_num_in_file == "more":
            fields.pop()
        elif fields_num_in_file == "less":
            fields.append(cf.gen_int32_field(name=additional_field))
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index before bulk insert
        # build index
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        # load collection
        self.collection_wrap.load()
        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        if fields_num_in_file in ["less", "more"]:
            assert not success
            if is_row_based:
                if fields_num_in_file == "less":
                    failed_reason = f"value of field '{additional_field}' is missed"
                else:
                    failed_reason = f"field '{df.float_field}' is not defined in collection schema"
            else:
                failed_reason = "is not equal to other fields"
            for state in states.values():
                assert state.state_name in ["Failed", "Failed and cleaned"]
                assert failed_reason in state.infos.get("failed_reason", "")
        else:
            assert success

            num_entities = self.collection_wrap.num_entities
            log.info(f" collection entities: {num_entities}")
            assert num_entities == entities

            # verify index status
            res, _ = self.collection_wrap.has_index()
            assert res is True
            success = self.utility_wrap.wait_index_build_completed(c_name)
            assert success
            # verify search and query
            log.info(f"wait for load finished and be ready for search")
            self.collection_wrap.load(_refresh=True)
            time.sleep(10)
            nq = 3
            topk = 10
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
                results, _ = self.collection_wrap.query(
                    expr=f"{df.pk_field} in {ids}",
                    output_fields=[df.pk_field, df.int_field],
                )
                assert len(results) == len(ids)
                if not auto_id:
                    for i in range(len(results)):
                        assert results[i].get(df.int_field, 0) == results[i].get(
                            df.pk_field, 1
                        )

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
        dim = 16
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
            task_ids=[task_id], timeout=90
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
        # verify index status
        success = self.utility_wrap.wait_index_build_completed(c_name)
        assert success
        # verify search and query
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
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
        if create_index_before_bulk_insert:
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
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert success
        if not create_index_before_bulk_insert:
            # build index
            index_params = ct.default_index
            self.collection_wrap.create_index(
                field_name=df.vec_field, index_params=index_params
            )
        if not loaded_before_bulk_insert:
            # load collection
            self.collection_wrap.load()

        num_entities = self.collection_wrap.num_entities
        log.info(f"collection entities: {num_entities}")
        assert num_entities == 500
        # verify no index
        success = self.utility_wrap.wait_index_build_completed(c_name)
        assert success
        # verify search and query
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load(_refresh=True)
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
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize(
        "fields_num_in_file", ["equal", "more", "less"]
    )  # "equal", "more", "less"
    @pytest.mark.parametrize("dim", [16])  # 1024
    @pytest.mark.parametrize("entities", [500])  # 5000
    def test_string_pk_float_vector_multi_scalars(
        self, is_row_based, fields_num_in_file, dim, entities
    ):
        """
        collection schema: [str_pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        Steps:
        1. create collection with string primary key
        2. create index and load collection
        3. import data
        4. verify the data entities
        5. verify index status
        6. verify search and query
        """
        string_pk = True
        auto_id = False
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            str_pk=string_pk,
            data_fields=default_multi_fields,
        )
        additional_field = "int_scalar_add"
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_string_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
            cf.gen_float_field(name=df.float_field),
        ]
        if fields_num_in_file == "more":
            fields.pop()
        elif fields_num_in_file == "less":
            fields.append(cf.gen_int32_field(name=additional_field))
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        # load collection
        self.collection_wrap.load()
        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        if fields_num_in_file in ["less", "more"]:
            assert not success  # TODO: check error msg
            if is_row_based:
                if fields_num_in_file == "less":
                    failed_reason = f"value of field '{additional_field}' is missed"
                else:
                    failed_reason = f"field '{df.float_field}' is not defined in collection schema"
            else:
                failed_reason = "is not equal to other fields"
            for state in states.values():
                assert state.state_name in ["Failed", "Failed and cleaned"]
                assert failed_reason in state.infos.get("failed_reason", "")
        else:
            assert success
            log.info(f" collection entities: {self.collection_wrap.num_entities}")
            assert self.collection_wrap.num_entities == entities
            # verify index
            success = self.utility_wrap.wait_index_build_completed(c_name)
            assert success
            # verify search and query
            log.info(f"wait for load finished and be ready for search")
            self.collection_wrap.load(_refresh=True)
            time.sleep(10)
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
            for hits in res:
                ids = hits.ids
                expr = f"{df.pk_field} in {ids}"
                expr = expr.replace("'", '"')
                results, _ = self.collection_wrap.query(expr=expr)
                assert len(results) == len(ids)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [15])
    @pytest.mark.parametrize("entities", [200])
    def test_float_vector_from_numpy_file(
        self, auto_id, dim, entities
    ):
        """
        collection schema 1: [pk, float_vector]
        schema 2: [pk, float_vector, int_scalar, string_scalar, float_scalar, bool_scalar]
        data file: .npy files
        Steps:
        1. create collection
        2. import data
        3. if is_row_based: verify import failed
        4. if column_based:
          4.1 verify the data entities equal the import data
          4.2 verify search and query successfully
        """
        if auto_id:
            data_fields = [df.vec_field, df.int_field, df.string_field, df.float_field, df.bool_field]
        else:
            data_fields = [df.pk_field, df.vec_field, df.int_field, df.string_field, df.float_field, df.bool_field]
        np_files = prepare_bulk_insert_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            force=True,
        )
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_bool_field(name=df.bool_field),
        ]
        files = np_files
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
            task_ids=[task_id], timeout=120
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")

        assert success
        log.info(f" collection entities: {self.collection_wrap.num_entities}")
        assert self.collection_wrap.num_entities == entities
        # create index and load
        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        result = self.utility_wrap.wait_index_build_completed(c_name)
        assert result is True
        self.collection_wrap.load()
        self.collection_wrap.load(_refresh=True)
        log.info(f"wait for load finished and be ready for search")
        log.info(
            f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
        )
        # verify imported data is available for search
        nq = 2
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
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_int_on_float_scalar(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        data files: json file that one of entities has int on float scalar
        Steps:
        1. create collection
        2. import data
        3. verify the data entities
        4. verify query successfully
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_multi_fields,
            err_type=DataErrorType.int_on_float_scalar,
            force=True,
        )

        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        log.info(f"bulk insert state:{success}")
        assert success
        assert self.collection_wrap.num_entities == entities

        index_params = ct.default_index
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        success = self.utility_wrap.wait_index_build_completed(c_name)
        assert success
        # verify imported data is available for search
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load()
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
        uids = res[0].ids
        res, _ = self.collection_wrap.query(
            expr=f"{df.pk_field} in {uids}", output_fields=[df.float_field]
        )
        assert isinstance(res[0].get(df.float_field, 1), np.float32)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
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
        data_fields = [df.int_field, df.float_field, df.double_field, df.vec_field]
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_int64_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_double_field(name=df.double_field),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
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
            task_ids=[task_id], timeout=90
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
        success = self.utility_wrap.wait_index_build_completed(c_name)
        log.info(f"wait for load finished and be ready for search")
        self.collection_wrap.load()
        self.collection_wrap.load(_refresh=True)
        log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
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
        for hits in res:
            ids = hits.ids
            results, _ = self.collection_wrap.query(expr=f"{df.pk_field} in {ids}")
            assert len(results) == len(ids)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [6])
    @pytest.mark.parametrize("entities", [2000])
    @pytest.mark.parametrize("file_nums", [10])
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
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_int64_field(name=df.int_field),
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
            task_ids=task_ids, timeout=90
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
            task_ids=[task_id], timeout=90
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
        segment_num = len(self.utility_wrap.get_query_segment_info(c_name)[0])
        num_entities = 0
        empty_partition_num = 0
        for p in self.collection_wrap.partitions:
            if p.num_entities == 0:
                empty_partition_num += 1
            num_entities += p.num_entities
        assert num_entities == entities
        # as there are not many vectors, one partition should only have one segment after bulk insert
        assert segment_num == (ct.default_partition_num - empty_partition_num)

        # verify error when tyring to bulk insert into a specific partition
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
    @pytest.mark.parametrize("entities", [300])
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
            task_ids=[task_id], timeout=90
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
        segment_num = len(self.utility_wrap.get_query_segment_info(c_name)[0])
        num_entities = 0
        empty_partition_num = 0
        for p in self.collection_wrap.partitions:
            if p.num_entities == 0:
                empty_partition_num += 1
            num_entities += p.num_entities
        assert num_entities == entities * file_nums


class TestBulkInsertInvalidParams(TestcaseBaseBulkInsert):

    @pytest.mark.tags(CaseLabel.L3)
    def test_non_existing_file(self):
        """
        collection: either auto_id or not
        collection schema: not existing file(s)
        Steps:
        1. create collection
        3. import data, but the data file(s) not exists
        4. verify import failed with errors
        """
        files = ["not_existing.json"]
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=ct.default_dim),
        ]
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=None,
            files=files,
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        assert not success
        failed_reason = f"failed to get file size of '{files[0]}'"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_empty_json_file(self, is_row_based, auto_id):
        """
        collection schema: [pk, float_vector]
        data file: empty file
        Steps:
        1. create collection
        2. import data, but the data file(s) is empty
        3. verify import fail if column based
        4. verify import successfully if row based
        """
        # set 0 entities
        entities = 0
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=ct.default_dim,
            auto_id=auto_id,
            data_fields=default_vec_only_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=ct.default_dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=None,
            files=files,
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        assert success
        # TODO: remove the assert below if issue #25685 was by design
        # assert not success
        # failed_reason = "row count is 0"
        # for state in states.values():
        #     assert state.state_name in ["Failed", "Failed and cleaned"]
        #     assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])  # True, False
    @pytest.mark.parametrize("auto_id", [True, False])  # True, False
    @pytest.mark.parametrize("dim", [16])  # 16
    @pytest.mark.parametrize("entities", [100])  # 3000
    @pytest.mark.parametrize("file_nums", [32])  # 10
    @pytest.mark.parametrize("multi_folder", [True, False])  # True, False
    def test_float_vector_from_multi_files(
        self, is_row_based, auto_id, dim, entities, file_nums, multi_folder
    ):
        """
        collection: auto_id
        collection schema: [pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        Steps:
        1. create collection
        2. build index and load collection
        3. import data from multiple files
        4. verify the data entities
        5. verify index status
        6. verify search successfully
        7. verify query successfully
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_multi_fields,
            file_nums=file_nums,
            multi_folder=multi_folder,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
            cf.gen_float_field(name=df.float_field)
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
        # import data
        t0 = time.time()
        err_msg = "row-based import, only allow one JSON file each time"
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files,
            check_task=CheckTasks.err_res, check_items={"err_code": 1, "err_msg": err_msg},
        )


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])  # 8
    @pytest.mark.parametrize("entities", [100])  # 100
    def test_wrong_file_type(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector]
        data files: wrong data type
        Steps:
        1. create collection
        2. import data
        3. verify import failed with errors
        """
        if is_row_based:
            if auto_id:
                file_type = ".npy"
            else:
                file_type = ""  # TODO
        else:
            if auto_id:
                file_type = ".csv"
            else:
                file_type = ".txt"
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_only_fields,
            file_type=file_type,
        )

        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        log.info(schema)
        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=None,
            files=files,
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert not success
        failed_reason = f"the file '{files[0]}' has no corresponding field in collection"
        failed_reason2 = "unsupported file type"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "") or \
                   failed_reason2 in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [100])
    def test_wrong_row_based_values(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector]
        data files: wrong row based values
        Steps:
        1. create collection
        3. import data with wrong row based value
        4. verify import failed with errors
        """
        # set the wrong row based params
        wrong_row_based = not is_row_based
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=wrong_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_only_fields,
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
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=None,
            files=files,
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        assert not success
        if is_row_based:
            value = df.vec_field  # if auto_id else df.pk_field
            failed_reason = f"invalid JSON format, the root key should be 'rows', but get '{value}'"
        else:
            failed_reason = "JSON parse: row count is 0"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])  # 8
    @pytest.mark.parametrize("entities", [100])  # 100
    def test_wrong_pk_field_name(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector]
        data files: wrong primary key field name
        Steps:
        1. create collection with a dismatch_uid as pk
        2. import data
        3. verify import data successfully if collection with auto_id
        4. verify import error if collection with auto_id=False
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_only_fields,
        )
        dismatch_pk_field = "dismatch_pk"
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=dismatch_pk_field, is_primary=True),
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
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        if auto_id:
            assert success
        else:
            assert not success
            if is_row_based:
                failed_reason = f"the field '{df.pk_field}' is not defined in collection schema"
            else:
                failed_reason = f"field {dismatch_pk_field} row count 0 is not equal to other fields row count"
            for state in states.values():
                assert state.state_name in ["Failed", "Failed and cleaned"]
                assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])  # 8
    @pytest.mark.parametrize("entities", [100])  # 100
    def test_wrong_vector_field_name(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector]
        Steps:
        1. create collection with a dismatch_uid as pk
        2. import data
        3. verify import data successfully if collection with auto_id
        4. verify import error if collection with auto_id=False
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_only_fields,
        )
        dismatch_vec_field = "dismatched_vectors"
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=dismatch_vec_field, dim=dim),
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
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")

        assert not success
        if is_row_based:
            failed_reason = f"the field '{df.vec_field}' is not defined in collection schema"
        else:
            if auto_id:
                failed_reason = f"JSON column consumer: row count is 0"
            else:
                failed_reason = f"field {dismatch_vec_field} row count 0 is not equal to other fields row count"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [200])
    def test_wrong_scalar_field_name(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vectors, int_scalar]
        data file: with dismatched int scalar
        1. create collection
        2. import data that one scalar field name is dismatched
        3. verify that import fails with errors
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_n_int_fields,
        )
        dismatch_scalar_field = "dismatched_scalar"
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=dismatch_scalar_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name="",
            files=files,
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert not success
        if is_row_based:
            failed_reason = f"field '{df.int_field}' is not defined in collection schema"
        else:
            failed_reason = f"field {dismatch_scalar_field} row count 0 is not equal to other fields row count"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [200])
    def test_wrong_dim_in_schema(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vectors, int_scalar]
        data file: with wrong dim of vectors
        1. import data the collection
        2. verify that import fails with errors
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_n_int_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        wrong_dim = dim + 1
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=wrong_dim),
            cf.gen_int32_field(name=df.int_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        log.info(f"bulk insert state:{success}")
        assert not success
        failed_reason = f"array size {dim} doesn't equal to vector dimension {wrong_dim} of field 'vectors'"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [200])
    def test_non_existing_collection(self, is_row_based, dim, entities):
        """
        collection: not create collection
        collection schema: [pk, float_vectors, int_scalar]
        1. import data into a non existing collection
        2. verify that import fails with errors
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            data_fields=default_vec_n_int_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        # import data into a non existing collection
        err_msg = f"can't find collection: {c_name}"
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            files=files,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [200])
    def test_non_existing_partition(self, is_row_based, dim, entities):
        """
        collection: create a collection
        collection schema: [pk, float_vectors, int_scalar]
        1. import data into a non existing partition
        2. verify that import fails with errors
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            data_fields=default_vec_n_int_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
        ]
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data into a non existing partition
        p_name = "non_existing"
        err_msg = f"partition ID not found for partition name '{p_name}'"
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name,
            partition_name=p_name,
            files=files,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 11, "err_msg": err_msg},
        )
        print(task_id)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [1000])
    @pytest.mark.parametrize("position", [0, 500, 999])  # the index of wrong dim entity
    def test_wrong_dim_in_one_entities_of_file(
        self, is_row_based, auto_id, dim, entities, position
    ):
        """
        collection schema: [pk, float_vectors, int_scalar]
        data file: one of entities has wrong dim data
        1. import data the collection
        2. verify that import fails with errors
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_n_int_fields,
            err_type=DataErrorType.one_entity_wrong_dim,
            wrong_position=position,
            force=True,
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
        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        log.info(f"bulk insert state:{success}")
        assert not success
        failed_reason = (
            f"doesn't equal to vector dimension {dim} of field 'vectors'"
        )
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [16])
    @pytest.mark.parametrize("entities", [300])
    @pytest.mark.parametrize("file_nums", [10])  # max task nums 32? need improve
    def test_float_vector_with_multi_json_files(
        self, is_row_based, auto_id, dim, entities, file_nums
    ):
        """
        collection schema: [pk, float_vectors, int_scalar], one of entities has wrong dim data
        data files: multi files,
        1. import data 10 files
        2. verify that import fails with errors and no data imported
        """
        multi_files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_multi_fields,
            file_nums=file_nums,
            force=True,
        )
        files = multi_files
        random.shuffle(files)  # mix up the file order

        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
            cf.gen_float_field(name=df.float_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1,
                         "err_msg": "row-based import, only allow one JSON file each time"}
        )
        assert self.collection_wrap.num_entities == 0


    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    def test_wrong_dim_in_numpy(self, auto_id, dim, entities):
        """
        collection schema 1: [pk, float_vector]
        data file: .npy file with wrong dim
        Steps:
        1. create collection
        2. import data
        3. verify failed with errors
        """
        data_fields = [df.vec_field]
        if not auto_id:
            data_fields.append(df.pk_field)
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
        wrong_dim = dim + 1
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=wrong_dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")

        assert not success
        failed_reason = f"illegal dimension {dim} of numpy file"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [False])
    @pytest.mark.parametrize("dim", [15])
    @pytest.mark.parametrize("entities", [100])
    def test_wrong_field_name_in_numpy(self, auto_id, dim, entities):
        """
        collection schema 1: [pk, float_vector]
        data file: .npy file
        Steps:
        1. create collection
        2. import data
        3. if is_row_based: verify import failed
        4. if column_based:
          4.1 verify the data entities equal the import data
          4.2 verify search and query successfully
        """
        data_fields = [df.vec_field]
        if not auto_id:
            data_fields.append(df.pk_field)
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
        wrong_vec_field = f"wrong_{df.vec_field}"
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=wrong_vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        log.info(schema)
        # import data
        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")

        assert not success
        failed_reason = f"file '{df.vec_field}.npy' has no corresponding field in collection"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [16])  # 128
    @pytest.mark.parametrize("entities", [100])  # 1000
    def test_duplicate_numpy_files(self, auto_id, dim, entities):
        """
        collection schema 1: [pk, float_vector]
        data file: .npy files
        Steps:
        1. create collection
        2. import data with duplicate npy files
        3. verify fail to import with errors
        """
        data_fields = [df.vec_field]
        if not auto_id:
            data_fields.append(df.pk_field)
        files = prepare_bulk_insert_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
        )
        files += files
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
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert not success
        failed_reason = "duplicate file"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    # @pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/21818")
    def test_data_type_string_on_int_pk(self, is_row_based, dim, entities):
        """
        collection schema: default multi scalars
        data file: json file with one of entities has string on int pk
        Steps:
        1. create collection
        2. import data with is_row_based=False
        3. verify import failed
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=False,
            data_fields=default_multi_fields,
            err_type=DataErrorType.str_on_int_pk,
            force=True,
        )

        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        # TODO: add string pk
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
            cf.gen_float_field(name=df.float_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=False)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        log.info(f"bulk insert state:{success}")
        assert not success
        failed_reason = f"illegal value"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_float_on_int_pk(self, is_row_based, dim, entities):
        """
        collection schema: [pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        data files: json file that one of entities has float on int pk
        Steps:
        1. create collection
        2. import data with wrong data type
        3. verify import failed
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=False,
            data_fields=default_multi_fields,
            err_type=DataErrorType.float_on_int_pk,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        # TODO: add string pk
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
            cf.gen_float_field(name=df.float_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=False)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        log.info(f"bulk insert state:{success}")
        assert not success
        failed_reason = f"failed to convert row value to entity"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_typo_on_bool(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        data files: json file that one of entities has typo on boolean field
        Steps:
        1. create collection
        2. import data
        3. verify import failed with errors
        """

        multi_fields = [
            df.vec_field,
            df.int_field,
            df.string_field,
            df.bool_field,
            df.float_field,
        ]
        if not auto_id:
            multi_fields.insert(0, df.pk_field)
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_multi_fields,
            err_type=DataErrorType.typo_on_bool,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        log.info(f"bulk insert state:{success}")
        assert not success
        failed_reason1 = "illegal value"
        failed_reason2 = "invalid character"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason1 in state.infos.get(
                "failed_reason", ""
            ) or failed_reason2 in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

        #
        # assert success
        # assert self.collection_wrap.num_entities == entities
        #
        # self.collection_wrap.load()
        #
        # # the pk value was automatically convert to int from float
        # res, _ = self.collection_wrap.query(expr=f"{float_field} in [1.0]", output_fields=[float_field])
        # assert res[0].get(float_field, 0) == 1.0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [6])
    @pytest.mark.parametrize("entities", [10])
    @pytest.mark.parametrize("file_nums", [2])
    def test_multi_numpy_files_from_diff_folders_in_one_request(
        self, auto_id, dim, entities, file_nums
    ):
        """
        collection schema 1: [pk, float_vector]
        data file: .npy files in different folders
        Steps:
        1. create collection
        2. import data
        3. fail to import data with errors
        """
        data_fields = [df.vec_field]
        if not auto_id:
            data_fields.append(df.pk_field)
        files = prepare_bulk_insert_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            file_nums=file_nums,
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

        t0 = time.time()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")

        assert not success
        failed_reason = "duplicate file"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [9])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_str_on_float_scalar(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        data files: json file that entities has string data on float scalars
        Steps:
        1. create collection
        2. import data
        3. verify import failed with errors
        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_multi_fields,
            err_type=DataErrorType.str_on_float_scalar,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        log.info(f"bulk insert state:{success}")
        assert not success
        failed_reason = "failed to convert row value to entity"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("float_vector", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [500])
    def test_data_type_str_on_vector_fields(
        self, is_row_based, auto_id, float_vector, dim, entities
    ):
        """
        collection schema: [pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        data files: json file that entities has string data on vectors
        Steps:
        1. create collection
        2. import data
        3. verify import failed with errors
        """
        wrong_position = entities // 2
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            float_vector=float_vector,
            data_fields=default_multi_fields,
            err_type=DataErrorType.str_on_vector_field,
            wrong_position=wrong_position,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [cf.gen_float_vec_field(name=df.vec_field, dim=dim)]
        if not float_vector:
            fields = [cf.gen_binary_vec_field(name=df.vec_field, dim=dim)]
        fields.extend([
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_float_field(name=df.float_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
        ])
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=c_name, files=files
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        log.info(f"bulk insert state:{success}")
        assert not success
        failed_reason1 = "failed to parse row value"
        failed_reason2 = "failed to convert row value to entity"
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]
            assert failed_reason1 in state.infos.get("failed_reason", "") or \
                   failed_reason2 in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0


@pytest.mark.skip()
class TestBulkInsertAdvanced(TestcaseBaseBulkInsert):

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize(
        "entities", [50000, 500000, 1000000]
    )  # 1m*3; 50k*20; 2m*3, 500k*4
    def test_float_vector_from_multi_numpy_files(self, auto_id, dim, entities):
        """
        collection schema 1: [pk, float_vector]
        data file: .npy files
        Steps:
        1. create collection
        2. import data
        3. if column_based:
          4.1 verify the data entities equal the import data
          4.2 verify search and query successfully
        """
        # NOTE: 128d_1m --> 977MB
        suffix = entity_suffix(entities)
        vec_field = f"vectors_{dim}d_{suffix}"
        self._connect()
        c_name = cf.gen_unique_str("bulk_insert")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        data_fields = [df.pk_field, vec_field]
        # import data
        file_nums = 3
        files = prepare_bulk_insert_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            file_nums=file_nums,
            force=True,
        )
        log.info(f"files:{files}")
        for i in range(file_nums):
            files = [
                f"{dim}d_{suffix}_{i}/{vec_field}.npy"
            ]  # npy file name shall be the vector field name
            if not auto_id:
                files.append(f"{dim}d_{suffix}_{i}/{df.pk_field}.npy")
            t0 = time.time()
            check_flag = True
            for file in files:
                file_size = Path(f"{base_dir}/{file}").stat().st_size / 1024 / 1024
                if file_size >= 1024:
                    check_flag = False
                    break

            task_id, _ = self.utility_wrap.do_bulk_insert(
                collection_name=c_name, files=files
            )
            logging.info(f"bulk insert task ids:{task_id}")
            success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
                task_ids=[task_id], timeout=180
            )
            tt = time.time() - t0
            log.info(
                f"auto_id:{auto_id}, bulk insert{suffix}-{i} state:{success} in {tt}"
            )
            assert success is check_flag

        # TODO: assert num entities
        if success:
            t0 = time.time()
            num_entities = self.collection_wrap.num_entities
            tt = time.time() - t0
            log.info(f" collection entities: {num_entities} in {tt}")
            assert num_entities == entities * file_nums

            # verify imported data is available for search
            index_params = ct.default_index
            self.collection_wrap.create_index(
                field_name=df.vec_field, index_params=index_params
            )
            self.collection_wrap.load()
            log.info(f"wait for load finished and be ready for search")
            time.sleep(20)
            loaded_segs = len(self.utility_wrap.get_query_segment_info(c_name)[0])
            log.info(f"query seg info: {loaded_segs} segs loaded.")
            search_data = cf.gen_vectors(1, dim)
            search_params = ct.default_search_params
            res, _ = self.collection_wrap.search(
                search_data,
                vec_field,
                param=search_params,
                limit=1,
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 1, "limit": 1},
            )
