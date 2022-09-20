import logging
import time
import pytest
import random
from pathlib import Path
from pymilvus import BulkLoadState
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.milvus_sys import MilvusSys
from common.common_type import CaseLabel, CheckTasks
from utils.util_k8s import (
    get_pod_ip_name_pairs,
    get_milvus_instance_name,
)
from utils.util_log import test_log as log
from bulk_load_data import (
    prepare_bulk_load_json_files,
    prepare_bulk_load_numpy_files,
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


def entity_suffix(entities):
    if entities // 1000000 > 0:
        suffix = f"{entities // 1000000}m"
    elif entities // 1000 > 0:
        suffix = f"{entities // 1000}k"
    else:
        suffix = f"{entities}"
    return suffix


class TestBulkLoad(TestcaseBase):
    @pytest.fixture(scope="function", autouse=True)
    def init_minio_client(self, host):
        Path("/tmp/bulk_load_data").mkdir(parents=True, exist_ok=True)
        self._connect()
        self.instance_name = get_milvus_instance_name(milvus_ns, host)
        minio_ip_pod_pair = get_pod_ip_name_pairs(
            milvus_ns, f"release={self.instance_name}, app=minio"
        )
        ms = MilvusSys()
        minio_ip = list(minio_ip_pod_pair.keys())[0]
        minio_port = "9000"
        self.minio_endpoint = f"{minio_ip}:{minio_port}"
        self.bucket_name = ms.index_nodes[0]["infos"]["system_configurations"][
            "minio_bucket_name"
        ]

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str("bulkload")
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name=None,
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, _ = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert success

        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities

        # verify imported data is available for search
        self.collection_wrap.load()
        log.info(
            f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
        )
        nq = 2
        topk = 2
        search_data = cf.gen_vectors(nq, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
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
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_string_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        completed, _ = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{completed} in {tt}")
        assert completed

        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities

        # verify imported data is available for search
        self.collection_wrap.load()
        log.info(
            f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
        )
        nq = 3
        topk = 2
        search_data = cf.gen_vectors(nq, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
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
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # create a partition
        p_name = cf.gen_unique_str()
        m_partition, _ = self.collection_wrap.create_partition(partition_name=p_name)
        # build index before bulk load
        index_params = {
            "index_type": "IVF_SQ8",
            "params": {"nlist": 128},
            "metric_type": "L2",
        }
        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=index_params
        )
        # load before bulk load
        self.collection_wrap.load(partition_names=[p_name])

        # import data into the partition
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name=p_name,
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, state = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, target_state=BulkLoadState.ImportCompleted, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert success

        assert m_partition.num_entities == entities
        assert self.collection_wrap.num_entities == entities
        # success, state = self.utility_wrap.\
        #     wait_for_bulk_load_tasks_completed(task_ids=task_ids,
        #                                        target_state=BulkLoadState.ImportCompleted,
        #                                        timeout=30)
        # assert success
        log.debug(state)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        exp_res = {"total_rows": entities, "indexed_rows": entities}
        assert res == exp_res
        time.sleep(10)
        log.info(
            f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
        )
        nq = 10
        topk = 5
        search_data = cf.gen_vectors(nq, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
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
    @pytest.mark.parametrize("is_row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [16])
    @pytest.mark.parametrize("entities", [2000])
    def test_binary_vector_only(self, is_row_based, auto_id, dim, entities):
        """
        collection schema: [pk, binary_vector]
        Steps:
        1. create collection
        2. build collection
        3. import data
        4. verify build status
        5. verify the data entities
        6. load collection
        7. verify search successfully
        6. verify query successfully
        """
        float_vec = False
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_binary_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index before bulk load
        binary_index_params = {
            "index_type": "BIN_IVF_FLAT",
            "metric_type": "JACCARD",
            "params": {"nlist": 64},
        }

        self.collection_wrap.create_index(
            field_name=df.vec_field, index_params=binary_index_params
        )

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        # TODO: Update to BulkLoadDataIndexed when issue #16889 fixed
        success, _ = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, target_state=BulkLoadState.ImportCompleted, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert success

        # TODO: verify build index after #16890 fixed
        # res, _ = self.utility_wrap.index_building_progress(c_name)
        # exp_res = {'total_rows': entities, 'indexed_rows': entities}
        # assert res == exp_res

        # verify num entities
        assert self.collection_wrap.num_entities == entities

        # load collection
        self.collection_wrap.load()

        # verify search and query
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
    @pytest.mark.parametrize("is_row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize(
        "fields_num_in_file", ["equal", "more", "less"]
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
        2. load collection
        3. import data
        4. verify the data entities
        5. verify index status
        6. verify search and query
        6. build index
        7. release collection and reload
        7. verify search successfully
        6. verify query successfully
        """
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
        ]
        if fields_num_in_file == "more":
            fields.pop()
        elif fields_num_in_file == "less":
            fields.append(cf.gen_int32_field(name=additional_field))
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # load collection
        self.collection_wrap.load()
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        if fields_num_in_file == "less":
            assert not success
            if is_row_based:
                failed_reason = (
                    f"JSON row validator: field {additional_field} missed at the row 0"
                )
            else:
                failed_reason = "is not equal to other fields"
            for state in states.values():
                assert state.state_name == "Failed"
                assert failed_reason in state.infos.get("failed_reason", "")
        else:
            assert success

            num_entities = self.collection_wrap.num_entities
            log.info(f" collection entities: {num_entities}")
            assert num_entities == entities

            # verify no index
            res, _ = self.collection_wrap.has_index()
            assert res is False
            # verify search and query
            nq = 3
            topk = 10
            search_data = cf.gen_vectors(nq, dim)
            search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
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

            # build index
            index_params = {
                "index_type": "HNSW",
                "params": {"M": 8, "efConstruction": 100},
                "metric_type": "IP",
            }
            self.collection_wrap.create_index(
                field_name=df.vec_field, index_params=index_params
            )

            # release collection and reload
            self.collection_wrap.release()
            self.collection_wrap.load()

            # verify index built
            res, _ = self.collection_wrap.has_index()
            assert res is True

            # search and query
            search_params = {"params": {"ef": 64}, "metric_type": "IP"}
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
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        2. load collection
        3. import data
        4. verify the data entities
        5. verify index status
        6. verify search and query
        6. build index
        7. release collection and reload
        7. verify search successfully
        6. verify query successfully
        """
        string_pk = True
        auto_id = False
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_string_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
        ]
        if fields_num_in_file == "more":
            fields.pop()
        elif fields_num_in_file == "less":
            fields.append(cf.gen_int32_field(name=additional_field))
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # load collection
        self.collection_wrap.load()
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        if fields_num_in_file == "less":
            assert not success  # TODO: check error msg
            if is_row_based:
                failed_reason = (
                    f"JSON row validator: field {additional_field} missed at the row 0"
                )
            else:
                failed_reason = "is not equal to other fields"
            for state in states.values():
                assert state.state_name == "Failed"
                assert failed_reason in state.infos.get("failed_reason", "")
        else:
            assert success

            # TODO: assert num entities
            log.info(f" collection entities: {self.collection_wrap.num_entities}")
            assert self.collection_wrap.num_entities == entities

            # verify no index
            res, _ = self.collection_wrap.has_index()
            assert res is False
            # verify search and query
            search_data = cf.gen_vectors(1, dim)
            search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
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

            # build index
            index_params = {
                "index_type": "HNSW",
                "params": {"M": 8, "efConstruction": 100},
                "metric_type": "IP",
            }
            self.collection_wrap.create_index(
                field_name=df.vec_field, index_params=index_params
            )

            # release collection and reload
            self.collection_wrap.release()
            self.collection_wrap.load()

            # verify index built
            res, _ = self.collection_wrap.has_index()
            assert res is True

            # search and query
            search_params = {"params": {"ef": 64}, "metric_type": "IP"}
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
    @pytest.mark.parametrize("is_row_based", [True, False])  # True, False
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
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
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
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        # TODO: update to BulkLoadDataIndexed after issue #16889 fixed
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        if not is_row_based:
            assert not success
            failed_reason = "is duplicated"  # "the field xxx is duplicated"
            for state in states.values():
                assert state.state_name == "Failed"
                assert failed_reason in state.infos.get("failed_reason", "")
        else:
            assert success
            num_entities = self.collection_wrap.num_entities
            log.info(f" collection entities: {num_entities}")
            assert num_entities == entities * file_nums

            # verify index built
            # res, _ = self.utility_wrap.index_building_progress(c_name)
            # exp_res = {'total_rows': entities * file_nums, 'indexed_rows': entities * file_nums}
            # assert res == exp_res

            # verify search and query
            nq = 5
            topk = 1
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
    @pytest.mark.parametrize("is_row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("multi_fields", [True, False])
    @pytest.mark.parametrize("dim", [15])
    @pytest.mark.parametrize("entities", [200])
    # TODO: string data shall be re-generated
    def test_float_vector_from_numpy_file(
        self, is_row_based, auto_id, multi_fields, dim, entities
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
        data_fields = [df.vec_field]
        np_files = prepare_bulk_load_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            force=True,
        )
        if not multi_fields:
            fields = [
                cf.gen_int64_field(name=df.pk_field, is_primary=True),
                cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            ]
            if not auto_id:
                scalar_fields = [df.pk_field]
            else:
                scalar_fields = None
        else:
            fields = [
                cf.gen_int64_field(name=df.pk_field, is_primary=True),
                cf.gen_float_vec_field(name=df.vec_field, dim=dim),
                cf.gen_int32_field(name=df.int_field),
                cf.gen_string_field(name=df.string_field),
                cf.gen_bool_field(name=df.bool_field),
            ]
            if not auto_id:
                scalar_fields = [
                    df.pk_field,
                    df.float_field,
                    df.int_field,
                    df.string_field,
                    df.bool_field,
                ]
            else:
                scalar_fields = [
                    df.int_field,
                    df.string_field,
                    df.bool_field,
                    df.float_field,
                ]

        files = np_files
        if scalar_fields is not None:
            json_files = prepare_bulk_load_json_files(
                is_row_based=is_row_based,
                dim=dim,
                auto_id=auto_id,
                rows=entities,
                data_fields=scalar_fields,
                force=True,
            )
            files = np_files + json_files

        self._connect()
        c_name = cf.gen_unique_str()
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        if is_row_based:
            assert not success
            failed_reason1 = "unsupported file type for row-based mode"
            failed_reason2 = (
                f"JSON row validator: field {df.vec_field} missed at the row 0"
            )
            for state in states.values():
                assert state.state_name == "Failed"
                assert failed_reason1 in state.infos.get(
                    "failed_reason", ""
                ) or failed_reason2 in state.infos.get("failed_reason", "")
        else:
            assert success
            log.info(f" collection entities: {self.collection_wrap.num_entities}")
            assert self.collection_wrap.num_entities == entities

            # verify imported data is available for search
            self.collection_wrap.load()
            log.info(
                f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}"
            )
            nq = 2
            topk = 5
            search_data = cf.gen_vectors(nq, dim)
            search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
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
    @pytest.mark.parametrize("is_row_based", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_float_on_int_pk(self, is_row_based, dim, entities):
        """
        collection schema: [pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        data files: json file that one of entities has float on int pk
        Steps:
        1. create collection
        2. import data
        3. verify the data entities
        4. verify query successfully
        """
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        # TODO: add string pk
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=False)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        log.info(f"bulk load state:{success}")
        assert success
        assert self.collection_wrap.num_entities == entities

        self.collection_wrap.load()

        # the pk value was automatically convert to int from float
        res, _ = self.collection_wrap.query(
            expr=f"{df.pk_field} in [3]", output_fields=[df.pk_field]
        )
        assert [{df.pk_field: 3}] == res

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        # TODO: add string pk
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
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        log.info(f"bulk load state:{success}")
        assert success
        assert self.collection_wrap.num_entities == entities

        self.collection_wrap.load()

        # it was automatically converted from int to float
        search_data = cf.gen_vectors(1, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
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
        assert isinstance(res[0].get(df.float_field, 1), float)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    @pytest.mark.parametrize("with_int_field", [True, False])
    def test_with_uid_n_int_numpy(self, auto_id, dim, entities, with_int_field):
        """
        collection schema 1: [pk, float_vector]
        data file: vectors.npy and uid.npy
        Steps:
        1. create collection
        2. import data
        3. verify failed with errors
        """
        data_fields = [df.vec_field]
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        if not auto_id:
            data_fields.append(df.pk_field)
        if with_int_field:
            data_fields.append(df.int_field)
            fields.append(cf.gen_int64_field(name=df.int_field))
        files = prepare_bulk_load_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str()
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=False, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert success
        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities

        # verify imported data is available for search
        self.collection_wrap.load()
        # log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
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
        1. create collection
        2. import data
        3. verify that import numpy files in a loop
        """
        is_row_based = False  # numpy files supports only column based

        self._connect()
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
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

        data_fields = [df.vec_field]
        if not auto_id:
            data_fields.append(df.pk_field)
        task_ids = []
        for i in range(file_nums):
            files = prepare_bulk_load_numpy_files(
                minio_endpoint=self.minio_endpoint,
                bucket_name=self.bucket_name,
                rows=entities,
                dim=dim,
                data_fields=data_fields,
                file_nums=1,
                force=True,
            )
            task_id, _ = self.utility_wrap.bulk_load(
                collection_name=c_name, is_row_based=is_row_based, files=files
            )
            task_ids.append(task_id[0])
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, target_state=BulkLoadState.ImportPersisted, timeout=30
        )
        log.info(f"bulk load state:{success}")

        assert success
        log.info(f" collection entities: {self.collection_wrap.num_entities}")
        assert self.collection_wrap.num_entities == entities * file_nums

        # verify search and query
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

    # TODO: not supported yet
    def test_from_customize_bucket(self):
        pass


class TestBulkLoadInvalidParams(TestcaseBase):
    @pytest.fixture(scope="function", autouse=True)
    def init_minio_client(self, host, port):
        Path("/tmp/bulk_load_data").mkdir(parents=True, exist_ok=True)
        self._connect()
        self.instance_name = get_milvus_instance_name(milvus_ns, host)
        minio_ip_pod_pair = get_pod_ip_name_pairs(
            milvus_ns, f"release={self.instance_name}, app=minio"
        )
        ms = MilvusSys()
        minio_ip = list(minio_ip_pod_pair.keys())[0]
        minio_port = "9000"
        self.minio_endpoint = f"{minio_ip}:{minio_port}"
        self.bucket_name = ms.index_nodes[0]["infos"]["system_configurations"][
            "minio_bucket_name"
        ]

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
    def test_non_existing_file(self, is_row_based):
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=ct.default_dim),
        ]
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name=None,
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        assert not success
        failed_reason = f"the file {files[0]} is empty"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=ct.default_dim,
            auto_id=auto_id,
            data_fields=default_vec_only_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=ct.default_dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name=None,
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        assert not success
        failed_reason = "JSON parse: row count is 0"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
                file_type = ""
        else:
            if auto_id:
                file_type = ".csv"
            else:
                file_type = ".txt"
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        log.info(schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name=None,
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert not success
        failed_reason = "unsupported file type"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=wrong_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_only_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name=None,
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        assert not success
        if is_row_based:
            value = df.vec_field  # if auto_id else df.pk_field
            failed_reason = f"JSON parse: invalid row-based JSON format, the key {value} is not found"
        else:
            failed_reason = "JSON parse: row count is 0"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=dismatch_pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name=None,
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        if auto_id:
            assert success
        else:
            assert not success
            if is_row_based:
                failed_reason = f"field {dismatch_pk_field} missed at the row 0"
            else:
                failed_reason = f"import error: field {dismatch_pk_field} row count 0 is not equal to other fields"
            for state in states.values():
                assert state.state_name == "Failed"
                assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=dismatch_vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name=None,
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        assert not success
        if is_row_based:
            failed_reason = f"field {dismatch_vec_field} missed at the row 0"
        else:
            if auto_id:
                failed_reason = f"JSON column consumer: row count is 0"
            else:
                failed_reason = f"import error: field {dismatch_vec_field} row count 0 is not equal to other fields"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=dismatch_scalar_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name="",
            is_row_based=is_row_based,
            files=files,
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert not success
        if is_row_based:
            failed_reason = f"field {dismatch_scalar_field} missed at the row 0"
        else:
            failed_reason = f"import error: field {dismatch_scalar_field} row count 0 is not equal to other fields"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_vec_n_int_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str()
        wrong_dim = dim + 1
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=wrong_dim),
            cf.gen_int32_field(name=df.int_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason = f"array size {dim} doesn't equal to vector dimension {wrong_dim} of field vectors at the row "
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [200])
    def test_non_existing_collection(self, is_row_based, dim, entities):
        """
        collection: not create collection
        collection schema: [pk, float_vectors, int_scalar]
        1. import data into a non existing collection
        2. verify that import fails with errors
        """
        files = prepare_bulk_load_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            data_fields=default_vec_n_int_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str()
        # import data into a non existing collection
        err_msg = f"can't find collection: {c_name}"
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            is_row_based=is_row_based,
            files=files,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [200])
    def test_non_existing_partition(self, is_row_based, dim, entities):
        """
        collection: create a collection
        collection schema: [pk, float_vectors, int_scalar]
        1. import data into a non existing partition
        2. verify that import fails with errors
        """
        files = prepare_bulk_load_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            data_fields=default_vec_n_int_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
        ]
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data into a non existing partition
        p_name = "non_existing"
        err_msg = f" partition {p_name} does not exist"
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name,
            partition_name=p_name,
            is_row_based=is_row_based,
            files=files,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 11, "err_msg": err_msg},
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason = (
            f"doesn't equal to vector dimension {dim} of field vectors at the row"
        )
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [16])
    @pytest.mark.parametrize("entities", [300])
    @pytest.mark.parametrize("file_nums", [10])  # max task nums 32? need improve
    def test_float_vector_one_of_files_fail(
        self, is_row_based, auto_id, dim, entities, file_nums
    ):
        """
        collection schema: [pk, float_vectors, int_scalar], one of entities has wrong dim data
        data files: multi files, and there are errors in one of files
        1. import data 11 files(10 correct and 1 with errors) into the collection
        2. verify that import fails with errors and no data imported
        """
        correct_files = prepare_bulk_load_json_files(
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

        # append a file that has errors
        dismatch_dim = dim + 1
        err_files = prepare_bulk_load_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dismatch_dim,
            auto_id=auto_id,
            data_fields=default_multi_fields,
            file_nums=1,
        )
        files = correct_files + err_files
        random.shuffle(files)  # mix up the file order

        self._connect()
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=300
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert not success
        if is_row_based:
            # all correct files shall be imported successfully
            assert self.collection_wrap.num_entities == entities * file_nums
        else:
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
        files = prepare_bulk_load_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str()
        wrong_dim = dim + 1
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=wrong_dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=False, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        assert not success
        failed_reason = f"Numpy parse: illegal row width {dim} for field {df.vec_field} dimension {wrong_dim}"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [False])
    @pytest.mark.parametrize("dim", [15])
    @pytest.mark.parametrize("entities", [100])
    @pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/18992")
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
        files = prepare_bulk_load_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str()
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
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=False, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        assert not success
        failed_reason = f"Numpy parse: the field {df.vec_field} doesn't exist"
        for state in states.values():
            assert state.state_name == "Failed"
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
        files = prepare_bulk_load_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
        )
        files += files
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=False, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert not success
        failed_reason = "duplicate file"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_string_on_int_pk(self, is_row_based, dim, entities):
        """
        collection schema: default multi scalars
        data file: json file with one of entities has string on int pk
        Steps:
        1. create collection
        2. import data with is_row_based=False
        3. verify import failed
        """
        files = prepare_bulk_load_json_files(
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
        c_name = cf.gen_unique_str()
        # TODO: add string pk
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
            cf.gen_int32_field(name=df.int_field),
            cf.gen_string_field(name=df.string_field),
            cf.gen_bool_field(name=df.bool_field),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=False)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason = f"illegal numeric value"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=False,
            data_fields=default_multi_fields,
            err_type=DataErrorType.typo_on_bool,
            scalars=default_multi_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str()
        # TODO: add string pk
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
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason1 = "illegal value"
        failed_reason2 = "invalid character"
        for state in states.values():
            assert state.state_name == "Failed"
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
        is_row_based = False  # numpy files supports only column based
        data_fields = [df.vec_field]
        if not auto_id:
            data_fields.append(df.pk_field)
        files = prepare_bulk_load_numpy_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            rows=entities,
            dim=dim,
            data_fields=data_fields,
            file_nums=file_nums,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=df.vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        assert not success
        failed_reason = "duplicate file"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            data_fields=default_multi_fields,
            err_type=DataErrorType.str_on_float_scalar,
            scalars=default_multi_fields,
        )
        self._connect()
        c_name = cf.gen_unique_str()
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
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason = "illegal numeric value"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("is_row_based", [True, False])
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
        files = prepare_bulk_load_json_files(
            minio_endpoint=self.minio_endpoint,
            bucket_name=self.bucket_name,
            is_row_based=is_row_based,
            rows=entities,
            dim=dim,
            auto_id=auto_id,
            float_vector=float_vector,
            data_fields=default_multi_fields,
            err_type=DataErrorType.str_on_vector_field,
            wrong_position=entities // 2,
            scalars=default_multi_fields,
            force=True,
        )
        self._connect()
        c_name = cf.gen_unique_str()
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
        task_ids, _ = self.utility_wrap.bulk_load(
            collection_name=c_name, is_row_based=is_row_based, files=files
        )
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids, timeout=30
        )
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason = "illegal numeric value"
        if not float_vector:
            failed_reason = f"doesn't equal to vector dimension {dim} of field vectors"
        for state in states.values():
            assert state.state_name == "Failed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0


@pytest.mark.skip()
class TestBulkLoadAdvanced(TestcaseBase):
    @pytest.fixture(scope="function", autouse=True)
    def init_minio_client(self, host, port):
        Path("/tmp/bulk_load_data").mkdir(parents=True, exist_ok=True)
        self._connect()
        self.instance_name = get_milvus_instance_name(milvus_ns, host)
        minio_ip_pod_pair = get_pod_ip_name_pairs(
            milvus_ns, f"release={self.instance_name}, app=minio"
        )
        ms = MilvusSys()
        minio_ip = list(minio_ip_pod_pair.keys())[0]
        minio_port = "9000"
        self.minio_endpoint = f"{minio_ip}:{minio_port}"
        self.bucket_name = ms.index_nodes[0]["infos"]["system_configurations"][
            "minio_bucket_name"
        ]

    def setup_class(self):
        log.info("[setup_import] Start setup class...")
        log.info("copy data files to minio")

    def teardown_class(self):
        log.info("[teardown_import] Start teardown class...")
        log.info("clean up data files in minio")

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
        suffix = entity_suffix(entities)
        vec_field = f"vectors_{dim}d_{suffix}"
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [
            cf.gen_int64_field(name=df.pk_field, is_primary=True),
            cf.gen_float_vec_field(name=vec_field, dim=dim),
        ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        file_nums = 3
        for i in range(file_nums):
            files = [
                f"{dim}d_{suffix}_{i}/{vec_field}.npy"
            ]  # npy file name shall be the vector field name
            if not auto_id:
                files.append(f"{dim}d_{suffix}_{i}/{df.pk_field}.npy")
            t0 = time.time()
            task_ids, _ = self.utility_wrap.bulk_load(
                collection_name=c_name, is_row_based=False, files=files
            )
            logging.info(f"bulk load task ids:{task_ids}")
            success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
                task_ids=task_ids, timeout=180
            )
            tt = time.time() - t0
            log.info(
                f"auto_id:{auto_id}, bulk load{suffix}-{i} state:{success} in {tt}"
            )
            assert success

        # TODO: assert num entities
        t0 = time.time()
        num_entities = self.collection_wrap.num_entities
        tt = time.time() - t0
        log.info(f" collection entities: {num_entities} in {tt}")
        assert num_entities == entities * file_nums

        # verify imported data is available for search
        self.collection_wrap.load()
        loaded_segs = len(self.utility_wrap.get_query_segment_info(c_name)[0])
        log.info(f"query seg info: {loaded_segs} segs loaded.")
        search_data = cf.gen_vectors(1, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
        res, _ = self.collection_wrap.search(
            search_data,
            vec_field,
            param=search_params,
            limit=1,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": 1, "limit": 1},
        )
        # self.collection_wrap.query(expr=f"id in {ids}")

    """Validate data consistency and availability during import"""
