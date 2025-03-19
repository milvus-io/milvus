import logging
import time
import pytest
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


milvus_ns = "chaos-testing"
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


class TestBulkInsertTaskClean(TestcaseBaseBulkInsert):

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])  # 8, 128
    @pytest.mark.parametrize("entities", [100])  # 100, 1000
    def test_success_task_not_cleaned(self, is_row_based, auto_id, dim, entities):
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
        7. wait for task clean triggered
        8. verify the task not cleaned
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
        logging.info(f"bulk insert task ids:{task_id}")
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
        time.sleep(5)
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
        log.info("wait for task clean triggered")
        time.sleep(6*60)  # wait for 6 minutes for task clean triggered
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities
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
    @pytest.mark.parametrize("dim", [8])  # 8, 128
    @pytest.mark.parametrize("entities", [100])  # 100, 1000
    def test_failed_task_was_cleaned(self, is_row_based, auto_id, dim, entities):
        """
        collection: auto_id, customized_id
        collection schema: [pk, float_vector]
        Steps:
        1. create collection
        2. import data with wrong dimension
        3. verify the data entities is 0 and task was failed
        4. wait for task clean triggered
        5. verify the task was cleaned

        """
        files = prepare_bulk_insert_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_only_fields,
            err_type=DataErrorType.one_entity_wrong_dim,
            wrong_position=entities // 2,
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
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk insert task ids:{task_id}")
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        tt = time.time() - t0
        log.info(f"bulk insert state:{success} in {tt}")
        assert not success
        for state in states.values():
            assert state.state_name in ["Failed", "Failed and cleaned"]

        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == 0
        log.info("wait for task clean triggered")
        time.sleep(6*60)  # wait for 6 minutes for task clean triggered
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == 0
        success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id], timeout=90
        )
        assert not success
        for state in states.values():
            assert state.state_name in ["Failed and cleaned"]
