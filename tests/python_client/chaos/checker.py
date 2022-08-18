from enum import Enum
from random import randint
import time
from datetime import datetime
import functools
from time import sleep
from base.collection_wrapper import ApiCollectionWrapper
from base.utility_wrapper import ApiUtilityWrapper
from common import common_func as cf
from common import common_type as ct
from chaos import constants

from common.common_type import CheckTasks
from utils.util_log import test_log as log
from utils.api_request import Error


class Op(Enum):
    create = 'create'
    insert = 'insert'
    flush = 'flush'
    index = 'index'
    search = 'search'
    query = 'query'
    delete = 'delete'
    compact = 'compact'
    drop = 'drop'
    load_balance = 'load_balance'
    bulk_load = 'bulk_load'
    unknown = 'unknown'


timeout = 40
enable_traceback = False
DEFAULT_FMT = '[start time:{start_time}][time cost:{elapsed:0.8f}s][operation_name:{operation_name}][collection name:{collection_name}] -> {result!r}'


def trace(fmt=DEFAULT_FMT, prefix='chaos-test', flag=True):
    def decorate(func):
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            start_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            t0 = time.perf_counter()
            res, result = func(self, *args, **kwargs)
            elapsed = time.perf_counter() - t0
            end_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            if flag:
                collection_name = self.c_wrap.name
                operation_name = func.__name__
                log_str = f"[{prefix}]" + fmt.format(**locals())
                # TODO: add report function in this place, like uploading to influxdb
                # it is better a async way to do this, in case of blocking the request processing
                log.debug(log_str)
            if result:
                self.rsp_times.append(elapsed)
                self.average_time = (
                    elapsed + self.average_time * self._succ) / (self._succ + 1)
                self._succ += 1
            else:
                self._fail += 1
            return res, result
        return inner_wrapper
    return decorate


def exception_handler():
    def wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            try:
                res, result = func(self, *args, **kwargs)
                return res, result
            except Exception as e:
                log_row_length = 300
                e_str = str(e)
                log_e = e_str[0:log_row_length] + \
                    '......' if len(e_str) > log_row_length else e_str
                log.error(log_e)
                return Error(e), False
        return inner_wrapper
    return wrapper


class Checker:
    """
    A base class of milvus operation checker to
       a. check whether milvus is servicing
       b. count operations and success rate
    """

    def __init__(self, collection_name=None, shards_num=2):
        self._succ = 0
        self._fail = 0
        self._keep_running = True
        self.rsp_times = []
        self.average_time = 0
        self.c_wrap = ApiCollectionWrapper()
        c_name = collection_name if collection_name is not None else cf.gen_unique_str(
            'Checker_')
        self.c_wrap.init_collection(name=c_name,
                                    schema=cf.gen_default_collection_schema(),
                                    shards_num=shards_num,
                                    timeout=timeout,
                                    enable_traceback=enable_traceback)
        self.c_wrap.insert(data=cf.gen_default_list_data(nb=constants.ENTITIES_FOR_SEARCH),
                           timeout=timeout,
                           enable_traceback=enable_traceback)
        self.initial_entities = self.c_wrap.num_entities  # do as a flush

    def total(self):
        return self._succ + self._fail

    def succ_rate(self):
        return self._succ / self.total() if self.total() != 0 else 0

    def check_result(self):
        succ_rate = self.succ_rate()
        total = self.total()
        rsp_times = self.rsp_times
        average_time = 0 if len(rsp_times) == 0 else sum(
            rsp_times) / len(rsp_times)
        max_time = 0 if len(rsp_times) == 0 else max(rsp_times)
        min_time = 0 if len(rsp_times) == 0 else min(rsp_times)
        checker_name = self.__class__.__name__
        checkers_result = f"{checker_name}, succ_rate: {succ_rate:.2f}, total: {total:03d}, average_time: {average_time:.4f}, max_time: {max_time:.4f}, min_time: {min_time:.4f}"
        log.info(checkers_result)
        return checkers_result

    def terminate(self):
        self._keep_running = False
        self.reset()

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
        # do load before search
        self.c_wrap.load(replica_number=replica_number)

    @trace()
    def search(self):
        res, result = self.c_wrap.search(
            data=cf.gen_vectors(5, ct.default_dim),
            anns_field=ct.default_float_vec_field_name,
            param={"nprobe": 32},
            limit=1,
            timeout=timeout,
            check_task=CheckTasks.check_nothing
        )
        return res, result

    @exception_handler()
    def run_task(self):
        res, result = self.search()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class InsertFlushChecker(Checker):
    """check Insert and flush operations in a dependent thread"""

    def __init__(self, collection_name=None, flush=False, shards_num=2):
        super().__init__(collection_name=collection_name, shards_num=shards_num)
        self._flush = flush
        self.initial_entities = self.c_wrap.num_entities

    def keep_running(self):
        while True:
            t0 = time.time()
            _, insert_result = \
                self.c_wrap.insert(data=cf.gen_default_list_data(nb=constants.DELTA_PER_INS),
                                   timeout=timeout,
                                   enable_traceback=enable_traceback,
                                   check_task=CheckTasks.check_nothing)
            t1 = time.time()
            if not self._flush:
                if insert_result:
                    self.rsp_times.append(t1 - t0)
                    self.average_time = ((t1 - t0) + self.average_time * self._succ) / (self._succ + 1)
                    self._succ += 1
                    log.debug(f"insert success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}")
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
                    self.average_time = ((t1 - t0) + self.average_time * self._succ) / (self._succ + 1)
                    self._succ += 1
                    log.debug(f"flush success, time: {t1 - t0:.4f}, average_time: {self.average_time:.4f}")
                    self.initial_entities += constants.DELTA_PER_INS
                else:
                    self._fail += 1


class FlushChecker(Checker):
    """check flush operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2):
        if collection_name is None:
            collection_name = cf.gen_unique_str("FlushChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num)
        self.initial_entities = self.c_wrap.num_entities

    @trace()
    def flush(self):
        num_entities = self.c_wrap.num_entities
        if num_entities >= (self.initial_entities + constants.DELTA_PER_INS):
            result = True
            self.initial_entities += constants.DELTA_PER_INS
        else:
            result = False
        return num_entities, result

    @exception_handler()
    def run_task(self):
        _, result = self.c_wrap.insert(data=cf.gen_default_list_data(nb=constants.DELTA_PER_INS),
                                       timeout=timeout,
                                       enable_traceback=enable_traceback,
                                       check_task=CheckTasks.check_nothing)
        res, result = self.flush()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class InsertChecker(Checker):
    """check flush operations in a dependent thread"""

    def __init__(self, collection_name=None, flush=False, shards_num=2):
        if collection_name is None:
            collection_name = cf.gen_unique_str("InsertChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num)
        self._flush = flush
        self.initial_entities = self.c_wrap.num_entities

    @trace()
    def insert(self):
        res, result = self.c_wrap.insert(data=cf.gen_default_list_data(nb=constants.DELTA_PER_INS),
                                         timeout=timeout,
                                         enable_traceback=enable_traceback,
                                         check_task=CheckTasks.check_nothing)
        return res, result

    @exception_handler()
    def run_task(self):
        res, result = self.insert()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class CreateChecker(Checker):
    """check create operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("CreateChecker_")
        super().__init__(collection_name=collection_name)

    @trace()
    def init_collection(self):
        res, result = self.c_wrap.init_collection(
            name=cf.gen_unique_str("CreateChecker_"),
            schema=cf.gen_default_collection_schema(),
            timeout=timeout,
            enable_traceback=enable_traceback,
            check_task=CheckTasks.check_nothing)
        return res, result

    @exception_handler()
    def run_task(self):
        res, result = self.init_collection()
        if result:
            self.c_wrap.drop(timeout=timeout)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class IndexChecker(Checker):
    """check Insert operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("IndexChecker_")
        super().__init__(collection_name=collection_name)
        self.c_wrap.insert(data=cf.gen_default_list_data(nb=5 * constants.ENTITIES_FOR_SEARCH),
                           timeout=timeout, enable_traceback=enable_traceback)
        # do as a flush before indexing
        log.debug(f"Index ready entities: {self.c_wrap.num_entities}")

    @trace()
    def create_index(self):
        res, result = self.c_wrap.create_index(ct.default_float_vec_field_name,
                                               constants.DEFAULT_INDEX_PARAM,
                                               name=cf.gen_unique_str(
                                                   'index_'),
                                               timeout=timeout,
                                               enable_traceback=enable_traceback,
                                               check_task=CheckTasks.check_nothing)
        return res, result

    @exception_handler()
    def run_task(self):
        res, result = self.create_index()
        if result:
            self.c_wrap.drop_index(timeout=timeout)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class QueryChecker(Checker):
    """check query operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1):
        if collection_name is None:
            collection_name = cf.gen_unique_str("QueryChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num)
        self.c_wrap.load(replica_number=replica_number)  # do load before query
        self.term_expr = None

    @trace()
    def query(self):
        res, result = self.c_wrap.query(self.term_expr, timeout=timeout,
                                        check_task=CheckTasks.check_nothing)
        return res, result

    @exception_handler()
    def run_task(self):
        int_values = []
        for _ in range(5):
            int_values.append(randint(0, constants.ENTITIES_FOR_SEARCH))
        self.term_expr = f'{ct.default_int64_field_name} in {int_values}'
        res, result= self.query()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class LoadChecker(Checker):
    """check load operations in a dependent thread"""

    def __init__(self, collection_name=None, replica_number=1):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DeleteChecker_")
        super().__init__(collection_name=collection_name)
        self.replica_number = replica_number

    @trace()
    def load(self):
        res, result = self.c_wrap.load(replica_number=self.replica_number, timeout=timeout)
        return res, result

    @exception_handler()
    def run_task(self):
        res, result = self.load()
        if result:
            self.c_wrap.release()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class DeleteChecker(Checker):
    """check delete operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DeleteChecker_")
        super().__init__(collection_name=collection_name)
        self.c_wrap.load()  # load before query
        term_expr = f'{ct.default_int64_field_name} > 0'
        res, _ = self.c_wrap.query(term_expr, output_fields=[
                                   ct.default_int64_field_name])
        self.ids = [r[ct.default_int64_field_name] for r in res]
        self.expr = None

    @trace()
    def delete(self):
        res, result = self.c_wrap.delete(expr=self.expr, timeout=timeout)
        return res, result

    @exception_handler()
    def run_task(self):
        delete_ids = self.ids.pop()
        self.expr = f'{ct.default_int64_field_name} in {[delete_ids]}'
        res, result = self.delete()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class CompactChecker(Checker):
    """check compact operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("CompactChecker_")
        super().__init__(collection_name=collection_name)
        self.ut = ApiUtilityWrapper()
        self.c_wrap.load()  # load before compact

    @trace()
    def compact(self):
        res, result = self.c_wrap.compact(timeout=timeout)
        self.c_wrap.wait_for_compaction_completed()
        self.c_wrap.get_compaction_plans()
        return res, result

    @exception_handler()
    def run_task(self):
        res, result = self.compact()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class DropChecker(Checker):
    """check drop operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DropChecker_")
        super().__init__(collection_name=collection_name)

    @trace()
    def drop(self):
        res, result = self.c_wrap.drop()
        return res, result

    def run_task(self):
        res, result = self.drop()
        return res, result

    def keep_running(self):
        while self._keep_running:
            res, result = self.run_task()
            if result:
                self.c_wrap.init_collection(
                    name=cf.gen_unique_str("CreateChecker_"),
                    schema=cf.gen_default_collection_schema(),
                    timeout=timeout,
                    check_task=CheckTasks.check_nothing)
            sleep(constants.WAIT_PER_OP / 10)


class LoadBalanceChecker(Checker):
    """check loadbalance operations in a dependent thread"""

    def __init__(self, collection_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("LoadBalanceChecker_")
        super().__init__(collection_name=collection_name)
        self.utility_wrap = ApiUtilityWrapper()
        self.c_wrap.load()
        self.sealed_segment_ids = None
        self.dst_node_ids = None
        self.src_node_id = None

    @trace()
    def load_balance(self):
        res, result = self.utility_wrap.load_balance(
            self.c_wrap.name, self.src_node_id, self.dst_node_ids, self.sealed_segment_ids)
        return res, result

    def prepare(self):
        """prepare load balance params"""
        res, _ = self.c_wrap.get_replicas()
        # find a group which has multi nodes
        group_nodes = []
        for g in res.groups:
            if len(g.group_nodes) >= 2:
                group_nodes = list(g.group_nodes)
                break
        self.src_node_id = group_nodes[0]
        self.dst_node_ids = group_nodes[1:]
        res, _ = self.utility_wrap.get_query_segment_info(self.c_wrap.name)
        segment_distribution = cf.get_segment_distribution(res)
        self.sealed_segment_ids = segment_distribution[self.src_node_id]["sealed"]

    @exception_handler()
    def run_task(self):
        self.prepare()
        res, result = self.load_balance()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class BulkLoadChecker(Checker):
    """check bulk load operations in a dependent thread"""

    def __init__(self, collection_name=None, files=[]):
        if collection_name is None:
            collection_name = cf.gen_unique_str("BulkLoadChecker_")
        super().__init__(collection_name=collection_name)
        self.utility_wrap = ApiUtilityWrapper()
        self.schema = cf.gen_default_collection_schema()
        self.files = files
        self.row_based = True
        self.recheck_failed_task = False
        self.failed_tasks = []
        self.c_name = None

    def update(self, files=None, schema=None, row_based=None):
        if files is not None:
            self.files = files
        if schema is not None:
            self.schema = schema
        if row_based is not None:
            self.row_based = row_based

    @trace()
    def bulk_load(self):
        task_ids, result = self.utility_wrap.bulk_load(collection_name=self.c_name,
                                                       row_based=self.row_based,
                                                       files=self.files)
        completed, result = self.utility_wrap.wait_for_bulk_load_tasks_completed(task_ids=task_ids, timeout=30)
        return task_ids, completed

    @exception_handler()
    def run_task(self):
        if self.recheck_failed_task and self.failed_tasks:
            self.c_name = self.failed_tasks.pop(0)
            log.debug(f"check failed task: {self.c_name}")
        else:
            self.c_name = cf.gen_unique_str("BulkLoadChecker_")
        self.c_wrap.init_collection(name=self.c_name, schema=self.schema)
        # import data
        task_ids, completed = self.bulk_load()
        if not completed:
            self.failed_tasks.append(self.c_name)
        return task_ids, completed

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)