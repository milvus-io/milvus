import logging
import time
from time import sleep
import pytest
import random
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks, BulkLoadStates
from utils.util_log import test_log as log
from bulk_load_data import parpar_bulk_load_data, gen_json_file_name


vec_field = "vectors"
pk_field = "uid"
float_field = "float_scalar"
int_field = "int_scalar"
bool_field = "bool_scalar"
string_field = "string_scalar"


def entity_suffix(entities):
    if entities // 1000000 > 0:
        suffix = f"{entities // 1000000}m"
    elif entities // 1000 > 0:
        suffix = f"{entities // 1000}k"
    else:
        suffix = f"{entities}"
    return suffix


def gen_file_prefix(row_based=True, auto_id=True, prefix=""):
    if row_based:
        if auto_id:
            return f"{prefix}row_auto"
        else:
            return f"{prefix}row_cust"
    else:
        if auto_id:
            return f"{prefix}col_auto"
        else:
            return f"{prefix}col_cust"


class TestImport(TestcaseBase):

    def setup_class(self):
        log.info("[setup_import] Start setup class...")
        # TODO: copy data files to minio
        log.info("copy data files to minio")

    def teardown_class(self):
        log.info("[teardown_import] Start teardown class...")
        # TODO: clean up data or not is a question
        log.info("clean up data files in minio")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])     # 8, 128
    @pytest.mark.parametrize("entities", [100])    # 100, 1000
    def test_float_vector_only(self, row_based, auto_id, dim, entities):
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
        parpar_bulk_load_data(json_file=True, row_based=row_based, rows=entities, dim=dim,
                              auto_id=auto_id, str_pk=False, float_vector=True,
                              multi_scalars=False, file_nums=1)
        # TODO: file names shall be return by gen_json_files
        file_name = gen_json_file_name(row_based=row_based, rows=entities,
                                       dim=dim, auto_id=auto_id, str_pk=False,
                                       float_vector=True, multi_scalars=False, file_num=0)
        files = [file_name]
        self._connect()
        c_name = cf.gen_unique_str("bulkload")
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name='',
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        completed, _ = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                            timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{completed} in {tt}")
        assert completed

        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities

        # verify imported data is available for search
        self.collection_wrap.load()
        log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
        res, _ = self.collection_wrap.search(search_data, vec_field,
                                             param=search_params, limit=1,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "limit": 1})
        # self.collection_wrap.query(expr=f"id in {ids}")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("dim", [8])  # 8
    @pytest.mark.parametrize("entities", [100])  # 100
    @pytest.mark.xfail(reason="milvus crash issue #16858")
    def test_str_pk_float_vector_only(self, row_based, dim, entities):
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
        auto_id = False      # no auto id for string_pk schema
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id)
        files = [f"{prefix}_str_pk_float_vectors_only_{dim}d_{entities}.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_string_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        completed, _ = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                            timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{completed} in {tt}")
        assert completed

        num_entities = self.collection_wrap.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == entities

        # verify imported data is available for search
        self.collection_wrap.load()
        log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
        res, _ = self.collection_wrap.search(search_data, vec_field,
                                             param=search_params, limit=1,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "limit": 1})
        # self.collection_wrap.query(expr=f"id in {ids}")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [10000])
    def test_partition_float_vector_int_scalar(self, row_based, auto_id, dim, entities):
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
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id)
        files = [f"{prefix}_float_vectors_int_scalar_{dim}d_{entities}.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name="int_scalar")]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # create a partition
        p_name = cf.gen_unique_str()
        m_partition, _ = self.collection_wrap.create_partition(partition_name=p_name)
        # build index before bulk load
        index_params = {"index_type": "IVF_SQ8", "params": {"nlist": 128}, "metric_type": "L2"}
        self.collection_wrap.create_index(field_name=vec_field, index_params=index_params)
        # load before bulk load
        self.collection_wrap.load(partition_names=[p_name])

        # import data into the partition
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name=p_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, _ = self.utility_wrap.\
            wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                               target_state=BulkLoadStates.BulkLoadDataQueryable,
                                               timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert success

        assert m_partition.num_entities == entities
        assert self.collection_wrap.num_entities == entities

        log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")

        search_data = cf.gen_vectors(1, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        res, _ = self.collection_wrap.search(search_data, vec_field,
                                             param=search_params, limit=1,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "limit": 1})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [16])
    @pytest.mark.parametrize("entities", [10])
    def test_binary_vector_only(self, row_based, auto_id, dim, entities):
        """
        collection: auto_id
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
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id)
        files = [f"{prefix}_binary_vectors_only_{dim}d_{entities}.json"]
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_binary_vec_field(name=vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index before bulk load
        binary_index_params = {"index_type": "BIN_IVF_FLAT", "metric_type": "JACCARD", "params": {"nlist": 64}}

        self.collection_wrap.create_index(field_name=vec_field, index_params=binary_index_params)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, _ = self.utility_wrap.wait_for_bulk_load_tasks_completed(
                                    task_ids=task_ids,
                                    target_state=BulkLoadStates.BulkLoadDataIndexed,
                                    timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert success

        # verify build index status
        # sleep(3)
        # TODO: verify build index after index_building_progress() refactor
        res, _ = self.utility_wrap.index_building_progress(c_name)
        exp_res = {'total_rows': entities, 'indexed_rows': entities}
        assert res == exp_res

        # TODO: verify num entities
        assert self.collection_wrap.num_entities == entities

        # load collection
        self.collection_wrap.load()

        # verify search and query
        search_data = cf.gen_binary_vectors(1, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        res, _ = self.collection_wrap.search(search_data, vec_field,
                                             param=search_params, limit=1,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "limit": 1})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("fields_num_in_file", ["equal", "more", "less"])   # "equal", "more", "less"
    @pytest.mark.parametrize("dim", [1024])    # 1024
    @pytest.mark.parametrize("entities", [5000])    # 5000
    def test_float_vector_multi_scalars(self, row_based, auto_id, fields_num_in_file, dim, entities):
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
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id)
        files = [f"{prefix}_float_vectors_multi_scalars_{dim}d_{entities}.json"]
        additional_field = "int_scalar_add"
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field),
                  cf.gen_string_field(name=string_field),
                  cf.gen_bool_field(name=bool_field)]
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
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
                                                task_ids=task_ids,
                                                target_state=BulkLoadStates.BulkLoadDataQueryable,
                                                timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        if fields_num_in_file == "less":
            assert not success    # TODO: check error msg
            if row_based:
                failed_reason = f"JSON row validator: field {additional_field} missed at the row 0"
            else:
                failed_reason = "is not equal to other fields"
            for state in states.values():
                assert state.state_name == "BulkLoadFailed"
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
            res, _ = self.collection_wrap.search(search_data, vec_field,
                                                 param=search_params, limit=1,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "limit": 1})

            # self.collection_wrap.query(expr=f"id in {ids}")

            # build index
            index_params = {"index_type": "HNSW", "params": {"M": 8, "efConstruction": 100}, "metric_type": "IP"}
            self.collection_wrap.create_index(field_name=vec_field, index_params=index_params)

            # release collection and reload
            self.collection_wrap.release()
            self.collection_wrap.load()

            # verify index built
            res, _ = self.collection_wrap.has_index()
            assert res is True

            # search and query
            search_params = {"params": {"ef": 64}, "metric_type": "IP"}
            res, _ = self.collection_wrap.search(search_data, vec_field,
                                                 param=search_params, limit=1,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "limit": 1})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("fields_num_in_file", ["equal"])  # "equal", "more", "less"
    @pytest.mark.parametrize("dim", [4])  # 1024
    @pytest.mark.parametrize("entities", [10])  # 5000
    @pytest.mark.xfail(reason="milvus crash issue #16858")
    def test_string_pk_float_vector_multi_scalars(self, row_based, fields_num_in_file, dim, entities):
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
        prefix = gen_file_prefix(row_based=row_based, auto_id=False)
        files = [f"{prefix}_str_pk_float_vectors_multi_scalars_{dim}d_{entities}.json"]
        additional_field = "int_scalar_add"
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_string_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field),
                  cf.gen_string_field(name=string_field),
                  cf.gen_bool_field(name=bool_field)]
        if fields_num_in_file == "more":
            fields.pop()
        elif fields_num_in_file == "less":
            fields.append(cf.gen_int32_field(name=additional_field))
        schema = cf.gen_collection_schema(fields=fields, auto_id=False)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # load collection
        self.collection_wrap.load()
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
                                                task_ids=task_ids,
                                                target_state=BulkLoadStates.BulkLoadDataQueryable,
                                                timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        if fields_num_in_file == "less":
            assert not success  # TODO: check error msg
            if row_based:
                failed_reason = f"JSON row validator: field {additional_field} missed at the row 0"
            else:
                failed_reason = "is not equal to other fields"
            for state in states.values():
                assert state.state_name == "BulkLoadFailed"
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
            res, _ = self.collection_wrap.search(search_data, vec_field,
                                                 param=search_params, limit=1,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "limit": 1})

            # self.collection_wrap.query(expr=f"id in {ids}")

            # build index
            index_params = {"index_type": "HNSW", "params": {"M": 8, "efConstruction": 100}, "metric_type": "IP"}
            self.collection_wrap.create_index(field_name=vec_field, index_params=index_params)

            # release collection and reload
            self.collection_wrap.release()
            self.collection_wrap.load()

            # verify index built
            res, _ = self.collection_wrap.has_index()
            assert res is True

            # search and query
            search_params = {"params": {"ef": 64}, "metric_type": "IP"}
            res, _ = self.collection_wrap.search(search_data, vec_field,
                                                 param=search_params, limit=1,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "limit": 1})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True])      # True, False
    @pytest.mark.parametrize("auto_id", [True])        # True, False
    @pytest.mark.parametrize("dim", [16])    # 16
    @pytest.mark.parametrize("entities", [3000])    # 3000
    @pytest.mark.parametrize("file_nums", [10])    # 10, max task nums 32? need improve
    @pytest.mark.parametrize("multi_folder", [False])    # True, False
    @pytest.mark.xfail(reason="BulkloadIndexed cannot be reached for issue ##16848")
    def test_float_vector_from_multi_files(self, row_based, auto_id, dim, entities, file_nums, multi_folder):
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
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id)
        files = []
        if not multi_folder:
            for i in range(file_nums):
                files.append(f"{prefix}_float_vectors_multi_scalars_{dim}d_{entities}_{i}.json")
        else:
            # sub_folder index 20 to 29
            for i in range(20, 30):
                files.append(f"/sub{i}/{prefix}_float_vectors_multi_scalars_{dim}d_{entities}_{i}.json")
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field),
                  cf.gen_string_field(name=string_field),
                  cf.gen_bool_field(name=bool_field)
                  ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index
        index_params = ct.default_index
        self.collection_wrap.create_index(field_name=vec_field, index_params=index_params)
        # load collection
        self.collection_wrap.load()
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name='',
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
                                    task_ids=task_ids,
                                    target_state=BulkLoadStates.BulkLoadDataIndexed,
                                    timeout=300)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        if not row_based:
            assert not success
            failed_reason = "is duplicated"  # "the field xxx is duplicated"
            for state in states.values():
                assert state.state_name == "BulkLoadFailed"
                assert failed_reason in state.infos.get("failed_reason", "")
        else:
            assert success
            log.info(f" collection entities: {self.collection_wrap.num_entities}")
            assert self.collection_wrap.num_entities == entities * file_nums

            # verify index built
            res, _ = self.utility_wrap.index_building_progress(c_name)
            exp_res = {'total_rows': entities * file_nums, 'indexed_rows': entities * file_nums}
            assert res == exp_res

            # verify search and query
            search_data = cf.gen_vectors(1, dim)
            search_params = ct.default_search_params
            res, _ = self.collection_wrap.search(search_data, vec_field,
                                                 param=search_params, limit=1,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "limit": 1})

            # self.collection_wrap.query(expr=f"id in {ids}")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("multi_fields", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    # TODO: string data shall be re-generated
    def test_float_vector_from_npy_file(self, row_based, auto_id, multi_fields, dim, entities):
        """
        collection schema 1: [pk, float_vector]
        schema 2: [pk, float_vector, int_scalar, string_scalar, float_scalar, bool_scalar]
        data file: .npy files
        Steps:
        1. create collection
        2. import data
        3. if row_based: verify import failed
        4. if column_based:
          4.1 verify the data entities equal the import data
          4.2 verify search and query successfully
        """
        vec_field = f"vectors_{dim}d_{entities}"
        self._connect()
        c_name = cf.gen_unique_str()
        if not multi_fields:
            fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                      cf.gen_float_vec_field(name=vec_field, dim=dim)]
        else:
            fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                      cf.gen_float_vec_field(name=vec_field, dim=dim),
                      cf.gen_int32_field(name=int_field),
                      cf.gen_string_field(name=string_field),
                      cf.gen_bool_field(name=bool_field)
                      ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        files = [f"{vec_field}.npy"]      # npy file name shall be the vector field name
        if not multi_fields:
            if not auto_id:
                files.append(f"col_uid_only_{dim}d_{entities}.json")
                files.reverse()
        else:
            if not auto_id:
                files.append(f"col_uid_multi_scalars_{dim}d_{entities}.json")
            else:
                files.append(f"col_multi_scalars_{dim}d_{entities}.json")
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                            timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        if row_based:
            assert not success
            failed_reason1 = "unsupported file type for row-based mode"
            if auto_id:
                failed_reason2 = f"invalid row-based JSON format, the key {int_field} is not found"
            else:
                failed_reason2 = f"invalid row-based JSON format, the key {pk_field} is not found"
            for state in states.values():
                assert state.state_name == "BulkLoadFailed"
                assert failed_reason1 in state.infos.get("failed_reason", "") or \
                       failed_reason2 in state.infos.get("failed_reason", "")
        else:
            assert success
            # TODO: assert num entities
            log.info(f" collection entities: {self.collection_wrap.num_entities}")
            assert self.collection_wrap.num_entities == entities

            # verify imported data is available for search
            self.collection_wrap.load()
            log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
            search_data = cf.gen_vectors(1, dim)
            search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
            res, _ = self.collection_wrap.search(search_data, vec_field,
                                                 param=search_params, limit=1,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "limit": 1})
            # self.collection_wrap.query(expr=f"id in {ids}")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_float_on_int_pk(self, row_based, dim, entities):
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
        prefix = gen_file_prefix(row_based=row_based, auto_id=False, prefix="float_on_int_pk_")
        files = [f"{prefix}_float_vectors_multi_scalars_{dim}d_{entities}_0.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        # TODO: add string pk
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field),
                  cf.gen_string_field(name=string_field),
                  cf.gen_bool_field(name=bool_field)
                  ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=False)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids,
            timeout=30)
        log.info(f"bulk load state:{success}")
        assert success
        assert self.collection_wrap.num_entities == entities

        self.collection_wrap.load()

        # the pk value was automatically convert to int from float
        res, _ = self.collection_wrap.query(expr=f"{pk_field} in [3]", output_fields=[pk_field])
        assert [{pk_field: 3}] == res

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_int_on_float_scalar(self, row_based, auto_id, dim, entities):
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
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id, prefix="int_on_float_scalar_")
        files = [f"{prefix}_float_vectors_multi_scalars_{dim}d_{entities}_0.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        # TODO: add string pk
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field),
                  cf.gen_float_field(name=float_field),
                  cf.gen_string_field(name=string_field),
                  cf.gen_bool_field(name=bool_field)
                  ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids,
            timeout=30)
        log.info(f"bulk load state:{success}")
        assert success
        assert self.collection_wrap.num_entities == entities

        self.collection_wrap.load()

        # the pk value was automatically convert to int from float
        res, _ = self.collection_wrap.query(expr=f"{float_field} in [1.0]", output_fields=[float_field])
        assert res[0].get(float_field, 0) == 1.0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [16])  # 128
    @pytest.mark.parametrize("entities", [100])  # 1000
    @pytest.mark.parametrize("file_nums", [32])    # 32, max task nums 32? need improve
    @pytest.mark.skip(season="redesign after issue #16698 fixed")
    def test_multi_numpy_files_from_multi_folders(self, auto_id, dim, entities, file_nums):
        """
        collection schema 1: [pk, float_vector]
        data file: .npy files
        Steps:
        1. create collection
        2. import data
        3. if row_based: verify import failed
        4. if column_based:
          4.1 verify the data entities equal the import data
          4.2 verify search and query successfully
        """
        vec_field = f"vectors_{dim}d_{entities}"
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # build index
        index_params = ct.default_index
        self.collection_wrap.create_index(field_name=vec_field, index_params=index_params)
        # load collection
        self.collection_wrap.load()
        # import data
        for i in range(file_nums):
            files = [f"/{i}/{vec_field}.npy"]  # npy file name shall be the vector field name
            if not auto_id:
                files.append(f"/{i}/{pk_field}.npy")
            t0 = time.time()
            task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                      row_based=False,
                                                      files=files)
            logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                               timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        assert success
        log.info(f" collection entities: {self.collection_wrap.num_entities}")
        assert self.collection_wrap.num_entities == entities * file_nums

        # verify search and query
        sleep(10)
        search_data = cf.gen_vectors(1, dim)
        search_params = ct.default_search_params
        res, _ = self.collection_wrap.search(search_data, vec_field,
                                             param=search_params, limit=1,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "limit": 1})

    # TODO: not supported yet
    def test_from_customize_bucket(self):
        pass

#     @pytest.mark.tags(CaseLabel.L3)
#     @pytest.mark.parametrize("row_based", [True, False])
#     @pytest.mark.parametrize("auto_id", [True, False])
#     def test_auto_id_binary_vector_string_scalar(self, row_based, auto_id):
#         """
#         collection:
#         collection schema: [pk, binary_vector, string_scalar]
#         1. create collection
#         2. insert some data
#         3. import data
#         4. verify data entities
#         5. build index
#         6. load collection
#         7. verify search and query
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_float_vector_string_primary(self):
#         """
#         collection: custom_id
#         collection schema: float vectors and string primary key
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_float_partition_vector_string_primary(self):
#         """
#         collection: custom_id and custom partition
#         collection schema: float vectors and string primary key
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_binary_vector_int_primary_from_bucket(self):
#         """
#         collection: custom_id
#         collection schema: binary vectors and int primary key
#         import from a particular bucket
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_binary_vector_string_primary_multi_scalars_twice(self):
#         """
#         collection: custom_id
#         collection schema: binary vectors, string primary key and multiple scalars
#         import twice
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_float_vector_int_primary_multi_scalars_twice(self):
#         """
#         collection: custom_id
#         collection schema: float vectors, int primary key and multiple scalars
#         import twice
#         """
#         pass
#
#
# class TestColumnBasedImport(TestcaseBase):
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_auto_id_float_vector(self):
#         """
#         collection: auto_id
#         collection schema: [auto_id, float vector]
#         Steps:
#         1. create collection
#         2. import column based data file
#         3. verify the data entities equal the import data
#         4. load the collection
#         5. verify search successfully
#         6. verify query successfully
#         """
#         pass


class TestImportInvalidParams(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    def test_non_existing_file(self, row_based):
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
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=ct.default_dim)]
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name='',
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                               timeout=30)
        assert not success
        failed_reason = "minio file manage cannot be found"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_empty_json_file(self, row_based, auto_id):
        """
        collection: either auto_id or not
        collection schema: [pk, float_vector]
        Steps:
        1. create collection
        2. import data, but the data file(s) is empty
        3. verify import fail if column based
        4. verify import successfully if row based
        """
        # set the wrong row based params
        files = ["empty.json"]
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=ct.default_dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name='',
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                               timeout=30)
        if row_based:
            assert success
        else:
            assert not success
            failed_reason = "JSON column consumer: row count is 0"
            for state in states.values():
                assert state.state_name == "BulkLoadFailed"
                assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 8
    @pytest.mark.parametrize("entities", [100])  # 100
    def test_wrong_file_type(self, row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector]
        data files: wrong data type
        Steps:
        1. create collection
        2. import data
        3. verify import failed with errors
        """
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id, prefix="err_file_type_")
        if row_based:
            if auto_id:
                data_type = ".csv"
            else:
                data_type = ""
        else:
            if auto_id:
                data_type = ".npy"
            else:
                data_type = ".txt"
        files = [f"{prefix}_float_vectors_only_{dim}d_{entities}{data_type}"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name='',
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                            timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert not success
        failed_reason = "unsupported file type"
        if not row_based and auto_id:
            failed_reason = "Numpy parse: npy: not a valid NumPy file format"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [100])
    def test_wrong_row_based_values(self, row_based, auto_id, dim, entities):
        """
        collection: either auto_id or not
        import data: not existing file(s)
        Steps:
        1. create collection
        3. import data, but the data file(s) not exists
        4. verify import failed with errors
        """
        # set the wrong row based params
        prefix = gen_file_prefix(row_based=not row_based)
        files = [f"{prefix}_float_vectors_only_{dim}d_{entities}.json"]
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name='',
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                               timeout=30)
        assert not success
        if row_based:
            failed_reason = "invalid row-based JSON format, the key vectors is not found"
        else:
            failed_reason = "JSON column consumer: row count is 0"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])     # 8
    @pytest.mark.parametrize("entities", [100])    # 100
    def test_wrong_pk_field_name(self, row_based, auto_id, dim, entities):
        """
        collection: auto_id, customized_id
        import data: [pk, float_vector]
        Steps:
        1. create collection with a dismatch_uid as pk
        2. import data
        3. verify import data successfully if collection with auto_id
        4. verify import error if collection with auto_id=False
        """
        prefix = gen_file_prefix(row_based, auto_id)
        files = [f"{prefix}_float_vectors_only_{dim}d_{entities}.json"]
        pk_field = "dismatch_pk"
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name='',
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                               timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        if auto_id:
            assert success
        else:
            assert not success
            if row_based:
                failed_reason = f"field {pk_field} missed at the row 0"
            else:
                # TODO: improve the failed msg: issue #16722
                failed_reason = f"import error: field {pk_field} row count 0 is not equal to other fields"
            for state in states.values():
                assert state.state_name == "BulkLoadFailed"
                assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])     # 8
    @pytest.mark.parametrize("entities", [100])    # 100
    def test_wrong_vector_field_name(self, row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector]
        Steps:
        1. create collection with a dismatch_uid as pk
        2. import data
        3. verify import data successfully if collection with auto_id
        4. verify import error if collection with auto_id=False
        """
        prefix = gen_file_prefix(row_based, auto_id)
        files = [f"{prefix}_float_vectors_only_{dim}d_{entities}.json"]
        vec_field = "dismatched_vectors"
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name='',
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                               timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        assert not success
        if row_based:
            failed_reason = f"field {vec_field} missed at the row 0"
        else:
            if auto_id:
                failed_reason = f"JSON column consumer: row count is 0"
            else:
                # TODO: improve the failed msg: issue #16722
                failed_reason = f"import error: field {vec_field} row count 0 is not equal to other fields 100"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [10000])
    def test_wrong_scalar_field_name(self, row_based, auto_id, dim, entities):
        """
        collection: customized partitions
        collection schema: [pk, float_vectors, int_scalar]
        1. create collection
        2. import data that one scalar field name is dismatched
        3. verify that import fails with errors
        """
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id)
        files = [f"{prefix}_float_vectors_int_scalar_{dim}d_{entities}.json"]
        scalar_field = "dismatched_scalar"
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=scalar_field)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name="",
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
                            task_ids=task_ids,
                            timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert not success
        if row_based:
            failed_reason = f"field {scalar_field} missed at the row 0"
        else:
            # TODO: improve the failed msg: issue #16722
            failed_reason = f"import error: field {scalar_field} row count 0 is not equal to other fields 100"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [10000])
    def test_wrong_dim_in_schema(self, row_based, auto_id, dim, entities):
        """
        collection: create a collection with a dim that dismatch with json file
        collection schema: [pk, float_vectors, int_scalar]
        1. import data the collection
        2. verify that import fails with errors
        """
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id)
        files = [f"{prefix}_float_vectors_int_scalar_{dim}d_{entities}.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        wrong_dim = dim + 1
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=wrong_dim),
                  cf.gen_int32_field(name=int_field)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids,
            timeout=30)
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason = f"array size {dim} doesn't equal to vector dimension {wrong_dim} of field vectors at the row "
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [10000])
    def test_non_existing_collection(self, row_based, dim, entities):
        """
        collection: not create collection
        collection schema: [pk, float_vectors, int_scalar]
        1. import data into a non existing collection
        2. verify that import fails with errors
        """
        prefix = gen_file_prefix(row_based=row_based)
        files = [f"{prefix}_float_vectors_int_scalar_{dim}d_{entities}.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        # import data into a non existing collection
        err_msg = f"can't find collection: {c_name}"
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files,
                                                  check_task=CheckTasks.err_res,
                                                  check_items={"err_code": 1,
                                                               "err_msg": err_msg}
                                                  )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [10000])
    def test_non_existing_partition(self, row_based, dim, entities):
        """
        collection: create a collection
        collection schema: [pk, float_vectors, int_scalar]
        1. import data into a non existing partition
        2. verify that import fails with errors
        """
        prefix = gen_file_prefix(row_based=row_based)
        files = [f"{prefix}_float_vectors_int_scalar_{dim}d_{entities}.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field)]
        schema = cf.gen_collection_schema(fields=fields)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data into a non existing partition
        p_name = "non_existing"
        err_msg = f" partition {p_name} does not exist"
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name=p_name,
                                                  row_based=row_based,
                                                  files=files,
                                                  check_task=CheckTasks.err_res,
                                                  check_items={"err_code": 11,
                                                               "err_msg": err_msg}
                                                  )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [4])
    @pytest.mark.parametrize("entities", [10000])
    @pytest.mark.parametrize("position", ["first", "middle", "end"])
    def test_wrong_dim_in_one_entities_of_file(self, row_based, auto_id, dim, entities, position):
        """
        collection: create a collection
        collection schema: [pk, float_vectors, int_scalar], one of entities has wrong dim data
        1. import data the collection
        2. verify that import fails with errors
        """
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id, prefix=f"err_{position}_dim_")
        files = [f"{prefix}_float_vectors_int_scalar_{dim}d_{entities}.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids,
            timeout=30)
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason = f"doesn't equal to vector dimension {dim} of field vectors at the row"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [16])  # 16
    @pytest.mark.parametrize("entities", [3000])  # 3000
    @pytest.mark.parametrize("file_nums", [10])  # max task nums 32? need improve
    def test_float_vector_one_of_files_fail(self, row_based, auto_id, dim, entities, file_nums):
        """
        collection schema: [pk, float_vectors, int_scalar], one of entities has wrong dim data
        data files: multi files, and there are errors in one of files
        1. import data 11 files(10 correct and 1 with errors) into the collection
        2. verify that import fails with errors and no data imported
        """
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id)
        files = []
        for i in range(file_nums):
            files.append(f"{prefix}_float_vectors_multi_scalars_{dim}d_{entities}_{i}.json")
        # append a file that has errors
        files.append(f"err_{prefix}_float_vectors_multi_scalars_{dim}d_{entities}_101.json")
        random.shuffle(files)     # mix up the file order

        self._connect()
        c_name = cf.gen_unique_str(prefix)
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field),
                  cf.gen_string_field(name=string_field),
                  cf.gen_bool_field(name=bool_field)
                  ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  partition_name='',
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                               timeout=300)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")
        assert not success
        if row_based:
            # all correct files shall be imported successfully
            assert self.collection_wrap.num_entities == entities * file_nums
        else:
            # TODO: Update assert after #16707 fixed
            assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("same_field", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    @pytest.mark.xfail(reason="issue #16698")
    def test_float_vector_from_multi_npy_files(self, auto_id, same_field, dim, entities):
        """
        collection schema 1: [pk, float_vector]
        data file: .npy files
        Steps:
        1. create collection
        2. import data with row_based=False from multiple .npy files
        3. verify import failed with errors
        """
        vec_field = f"vectors_{dim}d_{entities}_0"
        self._connect()
        c_name = cf.gen_unique_str()
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim)]
        if not same_field:
            fields.append(cf.gen_float_field(name=f"vectors_{dim}d_{entities}_1"))
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        files = [f"{vec_field}.npy", f"{vec_field}.npy"]
        if not same_field:
            files = [f"{vec_field}.npy", f"vectors_{dim}d_{entities}_1.npy"]
        if not auto_id:
            files.append(f"col_uid_only_{dim}d_{entities}.json")

        # import data
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=False,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids,
            timeout=30)
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason = f"Numpy parse: illegal data type"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")
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
        vec_field = f"vectors_{dim}d_{entities}"
        self._connect()
        c_name = cf.gen_unique_str()
        wrong_dim = dim + 1
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=wrong_dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        files = [f"{vec_field}.npy"]  # npy file name shall be the vector field name
        if not auto_id:
            files.append(f"col_uid_only_{dim}d_{entities}.json")
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=False,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                               timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        assert not success
        failed_reason = f"Numpy parse: illegal row width {dim} for field {vec_field} dimension {wrong_dim}"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [1000])  # 1000
    def test_wrong_field_name_in_numpy(self, auto_id, dim, entities):
        """
        collection schema 1: [pk, float_vector]
        data file: .npy file
        Steps:
        1. create collection
        2. import data
        3. if row_based: verify import failed
        4. if column_based:
          4.1 verify the data entities equal the import data
          4.2 verify search and query successfully
        """
        vec_field = f"vectors_{dim}d_{entities}"
        self._connect()
        c_name = cf.gen_unique_str()
        wrong_vec_field = f"wrong_{vec_field}"
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=wrong_vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        files = [f"{vec_field}.npy"]  # npy file name shall be the vector field name
        if not auto_id:
            files.append(f"col_uid_only_{dim}d_{entities}.json")
        t0 = time.time()
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=False,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                               timeout=30)
        tt = time.time() - t0
        log.info(f"bulk load state:{success} in {tt}")

        assert not success
        failed_reason = f"Numpy parse: the field {vec_field} doesn't exist"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_string_on_int_pk(self, row_based, dim, entities):
        """
        collection schema: [pk, float_vectors, int_scalar], one of entities has wrong dim data
        data file: json file with one of entities has string on int pk
        Steps:
        1. create collection
        2. import data with row_based=False
        3. verify import failed
        """
        err_string_on_pk = "iamstring"
        prefix = gen_file_prefix(row_based=row_based, auto_id=False, prefix="err_str_on_int_pk_")
        files = [f"{prefix}_float_vectors_multi_scalars_{dim}d_{entities}_0.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        # TODO: add string pk
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field),
                  cf.gen_string_field(name=string_field),
                  cf.gen_bool_field(name=bool_field)
                  ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=False)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids,
            timeout=30)
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason = f"illegal numeric value {err_string_on_pk} at the row"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason in state.infos.get("failed_reason", "")
        assert self.collection_wrap.num_entities == 0

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("row_based", [True, False])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("dim", [8])
    @pytest.mark.parametrize("entities", [10])
    def test_data_type_int_on_float_scalar(self, row_based, auto_id, dim, entities):
        """
        collection schema: [pk, float_vector,
                        float_scalar, int_scalar, string_scalar, bool_scalar]
        data files: json file that one of entities has typo on boolean field
        Steps:
        1. create collection
        2. import data
        3. verify import failed with errors
        """
        prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id, prefix="err_typo_on_bool_")
        files = [f"{prefix}_float_vectors_multi_scalars_{dim}d_{entities}_0.json"]
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        # TODO: add string pk
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim),
                  cf.gen_int32_field(name=int_field),
                  cf.gen_float_field(name=float_field),
                  cf.gen_string_field(name=string_field),
                  cf.gen_bool_field(name=bool_field)
                  ]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)
        # import data
        task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                  row_based=row_based,
                                                  files=files)
        logging.info(f"bulk load task ids:{task_ids}")
        success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(
            task_ids=task_ids,
            timeout=30)
        log.info(f"bulk load state:{success}")
        assert not success
        failed_reason1 = "illegal value"
        failed_reason2 = "invalid character"
        for state in states.values():
            assert state.state_name == "BulkLoadFailed"
            assert failed_reason1 in state.infos.get("failed_reason", "") or \
                   failed_reason2 in state.infos.get("failed_reason", "")
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


    # TODO: string data on float field


class TestImportAdvanced(TestcaseBase):

    def setup_class(self):
        log.info("[setup_import] Start setup class...")
        log.info("copy data files to minio")

    def teardown_class(self):
        log.info("[teardown_import] Start teardown class...")
        log.info("clean up data files in minio")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("dim", [128])  # 128
    @pytest.mark.parametrize("entities", [50000])  # 1m*3; 50k*20; 2m*3, 500k*4
    @pytest.mark.xfail(reason="search fail for issue #16784")
    def test_float_vector_from_multi_npy_files(self, auto_id, dim, entities):
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
        fields = [cf.gen_int64_field(name=pk_field, is_primary=True),
                  cf.gen_float_vec_field(name=vec_field, dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, auto_id=auto_id)
        self.collection_wrap.init_collection(c_name, schema=schema)

        # import data
        file_nums = 3
        for i in range(file_nums):
            files = [f"{dim}d_{suffix}_{i}/{vec_field}.npy"]  # npy file name shall be the vector field name
            if not auto_id:
                files.append(f"{dim}d_{suffix}_{i}/{pk_field}.npy")
            t0 = time.time()
            task_ids, _ = self.utility_wrap.bulk_load(collection_name=c_name,
                                                      row_based=False,
                                                      files=files)
            logging.info(f"bulk load task ids:{task_ids}")
            success, states = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids,
                                                                                   timeout=180)
            tt = time.time() - t0
            log.info(f"auto_id:{auto_id}, bulk load{suffix}-{i} state:{success} in {tt}")
            assert success

        # TODO: assert num entities
        t0 = time.time()
        num_entities = self.collection_wrap.num_entities
        tt = time.time() - t0
        log.info(f" collection entities: {num_entities} in {tt}")
        assert num_entities == entities * file_nums

        # verify imported data is available for search
        self.collection_wrap.load()
        log.info(f"query seg info: {self.utility_wrap.get_query_segment_info(c_name)[0]}")
        search_data = cf.gen_vectors(1, dim)
        search_params = {"metric_type": "L2", "params": {"nprobe": 2}}
        res, _ = self.collection_wrap.search(search_data, vec_field,
                                             param=search_params, limit=1,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "limit": 1})
        # self.collection_wrap.query(expr=f"id in {ids}")

    """Validate data consistency and availability during import"""
    @pytest.mark.tags(CaseLabel.L3)
    def test_default(self):
        pass


