from enum import Enum
from random import randint
import time
import random
from time import sleep
from delayed_assert import expect
from base.collection_wrapper import ApiCollectionWrapper
from base.utility_wrapper import ApiUtilityWrapper
from common import common_func as cf
from common import common_type as ct
from chaos import constants

from common.common_type import CheckTasks
from utils.util_log import test_log as log


class Op(Enum):
    create = "create"
    insert = "insert"
    flush = "flush"
    index = "index"
    search = "search"
    query = "query"
    delete = "delete"
    compact = "compact"
    drop = "drop"
    load_balance = "load_balance"
    bulk_load = "bulk_load"

    unknown = "unknown"


timeout = 20
enable_traceback = False


class Checker:
    """
    A base class of milvus operation checker to
       a. check whether milvus is servicing
       b. count operations and success rate
    """

    def __init__(self, collection_name=None, shards_num=2):
        self._succ = 0
        self._fail = 0
        self.rsp_times = []
        self.average_time = 0
        self.c_wrap = ApiCollectionWrapper()
        c_name = collection_name if collection_name is not None else cf.gen_unique_str("Checker_")
        self.c_wrap.init_collection(
            name=c_name,
            schema=cf.gen_default_collection_schema(),
            shards_num=shards_num,
            timeout=timeout,
            enable_traceback=enable_traceback,
        )
        self.c_wrap.insert(
            data=cf.gen_default_list_data(nb=constants.ENTITIES_FOR_SEARCH),
            timeout=timeout,
            enable_traceback=enable_traceback,
        )
        self.initial_entities = self.c_wrap.num_entities  # do as a flush
        self.c_wrap.release()

    def total(self):
        return self._succ + self._fail

    def succ_rate(self):
        return self._succ / self.total() if self.total() != 0 else 0

    def check_result(self):
        succ_rate = self.succ_rate()
        total = self.total()
        rsp_times = self.rsp_times
        average_time = 0 if len(rsp_times) == 0 else sum(rsp_times) / len(rsp_times)
        max_time = 0 if len(rsp_times) == 0 else max(rsp_times)
        min_time = 0 if len(rsp_times) == 0 else min(rsp_times)
        checkers_result = f"succ_rate: {succ_rate:.2f}, total: {total:03d}, average_time: {average_time:.4f}, max_time: {max_time:.4f}, min_time: {min_time:.4f}"
        return checkers_result

    def reset(self):
        self._succ = 0
        self._fail = 0
        self.rsp_times = []
        self.average_time = 0


class SearchChecker(Checker):
    """check search operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1):
        if collection_name is None:
            collection_name = cf.gen_unique_str("SearchChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num)
        self.c_wrap.load(replica_number=replica_number)  # do load before search

    def keep_running(self):
        while True:
            search_vec = cf.gen_vectors(5, ct.default_dim)
            t0 = time.time()
            _, result = self.c_wrap.search(
                data=search_vec,
                anns_field=ct.default_float_vec_field_name,
                param={"nprobe": 32},
                limit=1,
                timeout=timeout,
                enable_traceback=enable_traceback,
                check_task=CheckTasks.check_nothing,
            )
            t1 = time.time()
            if result:
                self.rsp_times.append(t1 - t0)
                self.average_time = ((t1 - t0) + self.average_time * self._succ) / (
                    self._succ + 1
                )
                self._succ += 1
                log.debug(
                    f"search success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}"
                )
            else:
                self._fail += 1
            sleep(constants.WAIT_PER_OP / 10)


class InsertFlushChecker(Checker):
    """check Insert and flush operations in a dependent thread"""

    def __init__(self, collection_name=None, flush=False, shards_num=2):
        if collection_name is None:
            if flush:
                collection_name = cf.gen_unique_str("FlushChecker_")
            else:
                collection_name = cf.gen_unique_str("InsertChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num)
        self._flush = flush
        self.initial_entities = self.c_wrap.num_entities

    def keep_running(self):
        while True:
            t0 = time.time()
            _, insert_result = self.c_wrap.insert(
                data=cf.gen_default_list_data(nb=constants.DELTA_PER_INS),
                timeout=timeout,
                enable_traceback=enable_traceback,
                check_task=CheckTasks.check_nothing,
            )
            t1 = time.time()
            if not self._flush:
                if insert_result:
                    self.rsp_times.append(t1 - t0)
                    self.average_time = ((t1 - t0) + self.average_time * self._succ) / (
                        self._succ + 1
                    )
                    self._succ += 1
                    log.debug(
                        f"insert success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}"
                    )
                else:
                    self._fail += 1
                sleep(constants.WAIT_PER_OP / 10)
            else:
                # call flush in property num_entities
                t0 = time.time()
                num_entities = self.c_wrap.num_entities
                t1 = time.time()
                if num_entities == (self.initial_entities + constants.DELTA_PER_INS):
                    self.rsp_times.append(t1 - t0)
                    self.average_time = ((t1 - t0) + self.average_time * self._succ) / (
                        self._succ + 1
                    )
                    self._succ += 1
                    log.debug(
                        f"flush success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}"
                    )
                    self.initial_entities += constants.DELTA_PER_INS
                else:
                    self._fail += 1


class CreateChecker(Checker):
    """check create operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("CreateChecker_")
        super().__init__(collection_name=collection_name)

    def keep_running(self):
        while True:
            t0 = time.time()
            _, result = self.c_wrap.init_collection(
                name=cf.gen_unique_str("CreateChecker_"),
                schema=cf.gen_default_collection_schema(),
                timeout=timeout,
                enable_traceback=enable_traceback,
                check_task=CheckTasks.check_nothing,
            )
            t1 = time.time()
            if result:
                self.rsp_times.append(t1 - t0)
                self.average_time = ((t1 - t0) + self.average_time * self._succ) / (
                    self._succ + 1
                )
                self._succ += 1
                log.debug(
                    f"create success, time: {t1 - t0:.4f}, average_time: {self.average_time:4f}"
                )
                self.c_wrap.drop(timeout=timeout)

            else:
                self._fail += 1
            sleep(constants.WAIT_PER_OP / 10)


class IndexChecker(Checker):
    """check Insert operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("IndexChecker_")
        super().__init__(collection_name=collection_name)
        self.c_wrap.insert(
            data=cf.gen_default_list_data(nb=5 * constants.ENTITIES_FOR_SEARCH),
            timeout=timeout,
            enable_traceback=enable_traceback,
        )
        log.debug(
            f"Index ready entities: {self.c_wrap.num_entities}"
        )  # do as a flush before indexing

    def keep_running(self):
        while True:
            t0 = time.time()
            _, result = self.c_wrap.create_index(
                ct.default_float_vec_field_name,
                constants.DEFAULT_INDEX_PARAM,
                name=cf.gen_unique_str("index_"),
                timeout=timeout,
                enable_traceback=enable_traceback,
                check_task=CheckTasks.check_nothing,
            )
            t1 = time.time()
            if result:
                self.rsp_times.append(t1 - t0)
                self.average_time = ((t1 - t0) + self.average_time * self._succ) / (
                    self._succ + 1
                )
                self._succ += 1
                log.debug(
                    f"index success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}"
                )
                self.c_wrap.drop_index(timeout=timeout)
            else:
                self._fail += 1


class QueryChecker(Checker):
    """check query operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1):
        if collection_name is None:
            collection_name = cf.gen_unique_str("QueryChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num)
        self.c_wrap.load(replica_number=replica_number)  # do load before query

    def keep_running(self):
        while True:
            int_values = []
            for _ in range(5):
                int_values.append(randint(0, constants.ENTITIES_FOR_SEARCH))
            term_expr = f"{ct.default_int64_field_name} in {int_values}"
            t0 = time.time()
            _, result = self.c_wrap.query(
                term_expr,
                timeout=timeout,
                enable_traceback=enable_traceback,
                check_task=CheckTasks.check_nothing,
            )
            t1 = time.time()
            if result:
                self.rsp_times.append(t1 - t0)
                self.average_time = ((t1 - t0) + self.average_time * self._succ) / (
                    self._succ + 1
                )
                self._succ += 1
                log.debug(
                    f"query success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}"
                )
            else:
                self._fail += 1
            sleep(constants.WAIT_PER_OP / 10)


class DeleteChecker(Checker):
    """check delete operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DeleteChecker_")
        super().__init__(collection_name=collection_name)
        self.c_wrap.load()  # load before query

    def keep_running(self):
        while True:
            term_expr = f"{ct.default_int64_field_name} > 0"
            res, _ = self.c_wrap.query(
                term_expr, output_fields=[ct.default_int64_field_name]
            )
            ids = [r[ct.default_int64_field_name] for r in res]
            delete_ids = random.sample(ids, 2)
            expr = f"{ct.default_int64_field_name} in {delete_ids}"
            t0 = time.time()
            _, result = self.c_wrap.delete(expr=expr, timeout=timeout)
            tt = time.time() - t0
            if result:
                self.rsp_times.append(tt)
                self.average_time = (tt + self.average_time * self._succ) / (
                    self._succ + 1
                )
                self._succ += 1
                log.debug(
                    f"delete success, time: {tt:.4f}, average_time: {self.average_time:.4f}"
                )
            else:
                self._fail += 1
            sleep(constants.WAIT_PER_OP / 10)


class CompactChecker(Checker):
    """check compact operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("CompactChecker_")
        super().__init__(collection_name=collection_name)
        self.ut = ApiUtilityWrapper()
        self.c_wrap.load(enable_traceback=enable_traceback)  # load before compact

    def keep_running(self):
        while True:
            seg_info = self.ut.get_query_segment_info(self.c_wrap.name)
            t0 = time.time()
            res, result = self.c_wrap.compact(timeout=timeout)
            print(f"compact done: res {res}")
            self.c_wrap.wait_for_compaction_completed()
            self.c_wrap.get_compaction_plans()
            t1 = time.time()
            if result:
                self.rsp_times.append(t1 - t0)
                self.average_time = ((t1 - t0) + self.average_time * self._succ) / (
                    self._succ + 1
                )
                self._succ += 1
                log.debug(
                    f"compact success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}"
                )
            else:
                self._fail += 1
            sleep(constants.WAIT_PER_OP / 10)


class DropChecker(Checker):
    """check drop operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DropChecker_")
        super().__init__(collection_name=collection_name)
        # self.c_wrap.load(enable_traceback=enable_traceback)  # load before compact

    def keep_running(self):
        while True:
            t0 = time.time()
            _, result = self.c_wrap.drop()
            t1 = time.time()
            if result:
                self.rsp_times.append(t1 - t0)
                self.average_time = ((t1 - t0) + self.average_time * self._succ) / (
                    self._succ + 1
                )
                self._succ += 1
                log.debug(
                    f"drop success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}"
                )
            else:
                self._fail += 1
            sleep(constants.WAIT_PER_OP / 10)


class LoadBalanceChecker(Checker):
    """check loadbalance operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("LoadBalanceChecker_")
        super().__init__(collection_name=collection_name)
        self.utility_wrap = ApiUtilityWrapper()
        self.c_wrap.load(enable_traceback=enable_traceback)

    def keep_running(self):
        while True:
            c_name = self.c_wrap.name
            res, _ = self.c_wrap.get_replicas()
            # prepare load balance params
            # find a group which has multi nodes
            group_nodes = []
            for g in res.groups:
                if len(g.group_nodes) >= 2:
                    group_nodes = list(g.group_nodes)
                    break
            src_node_id = group_nodes[0]
            dst_node_ids = group_nodes[1:]
            res, _ = self.utility_wrap.get_query_segment_info(c_name)
            segment_distribution = cf.get_segment_distribution(res)
            sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
            # load balance
            t0 = time.time()
            _, result = self.utility_wrap.load_balance(
                c_name, src_node_id, dst_node_ids, sealed_segment_ids
            )
            t1 = time.time()
            # get segments distribution after load balance
            time.sleep(3)
            res, _ = self.utility_wrap.get_query_segment_info(c_name)
            segment_distribution = cf.get_segment_distribution(res)
            sealed_segment_ids_after_load_banalce = segment_distribution[src_node_id][
                "sealed"
            ]
            check_1 = (
                len(
                    set(sealed_segment_ids) & set(sealed_segment_ids_after_load_banalce)
                )
                == 0
            )
            des_sealed_segment_ids = []
            for des_node_id in dst_node_ids:
                des_sealed_segment_ids += segment_distribution[des_node_id]["sealed"]
            # assert sealed_segment_ids is subset of des_sealed_segment_ids
            check_2 = set(sealed_segment_ids).issubset(set(des_sealed_segment_ids))

            if result and (check_1 and check_2):
                self.rsp_times.append(t1 - t0)
                self.average_time = ((t1 - t0) + self.average_time * self._succ) / (
                    self._succ + 1
                )
                self._succ += 1
                log.debug(
                    f"load balance success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}"
                )
            else:
                self._fail += 1
            sleep(10)


class BulkLoadChecker(Checker):
    """check bulk load operations in a dependent thread"""

    def __init__(self, collection_name=None, flush=False):
        if collection_name is None:
            collection_name = cf.gen_unique_str("BulkLoadChecker_")
        super().__init__(collection_name=collection_name)
        self.utility_wrap = ApiUtilityWrapper()
        self.schema = cf.gen_default_collection_schema()
        self.flush = flush
        self.files = ["bulk_load_data_source.json"]
        self.row_based = True
        self.recheck_failed_task = False
        self.failed_tasks = []

    def update(self, files=None, schema=None, row_based=None):
        if files is not None:
            self.files = files
        if schema is not None:
            self.schema = schema
        if row_based is not None:
            self.row_based = row_based

    def keep_running(self):
        while True:
            if self.recheck_failed_task and self.failed_tasks:
                c_name = self.failed_tasks.pop(0)
                log.info(f"check failed task: {c_name}")
            else:
                c_name = cf.gen_unique_str("BulkLoadChecker_")
            self.c_wrap.init_collection(name=c_name, schema=self.schema)
            if self.flush:
                t0 = time.time()
                pre_entities_num = self.c_wrap.num_entities
                tt = time.time() - t0
                log.info(f"flush before bulk load, cost time: {tt:.4f}")
            # import data
            t0 = time.time()
            task_ids, res_1 = self.utility_wrap.bulk_load(
                collection_name=c_name, row_based=self.row_based, files=self.files
            )
            log.info(f"bulk load task ids:{task_ids}")
            completed, res_2 = self.utility_wrap.wait_for_bulk_load_tasks_completed(
                task_ids=task_ids, timeout=30
            )
            tt = time.time() - t0
            # added_num = sum(res_2[task_id].row_count for task_id in task_ids)
            if completed:
                self.rsp_times.append(tt)
                self.average_time = (tt + self.average_time * self._succ) / (
                    self._succ + 1
                )
                self._succ += 1
                log.info(
                    f"bulk load success for collection {c_name}, time: {tt:.4f}, average_time: {self.average_time:4f}"
                )
                if self.flush:
                    t0 = time.time()
                    cur_entities_num = self.c_wrap.num_entities
                    tt = time.time() - t0
                    log.info(f"flush after bulk load, cost time: {tt:.4f}")
            else:
                self._fail += 1
                # if the task failed, store the failed collection name for further checking after chaos
                self.failed_tasks.append(c_name)
                log.info(
                    f"bulk load failed for collection {c_name} time: {tt:.4f}, average_time: {self.average_time:4f}"
                )
                sleep(constants.WAIT_PER_OP / 10)


def assert_statistic(checkers, expectations={}):
    for k in checkers.keys():
        # expect succ if no expectations
        succ_rate = checkers[k].succ_rate()
        total = checkers[k].total()
        checker_result = k.check_result()

        if expectations.get(k, "") == constants.FAIL:
            log.info(f"Expect Fail: {str(k)} {checker_result}")
            expect(
                succ_rate < 0.49 or total < 2, f"Expect Fail: {str(k)} {checker_result}"
            )
        else:
            log.info(f"Expect Succ: {str(k)} {checker_result}")
            expect(
                succ_rate > 0.90 or total > 2, f"Expect Succ: {str(k)} {checker_result}"
            )

