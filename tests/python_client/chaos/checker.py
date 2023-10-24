import pytest
import unittest
from enum import Enum
from random import randint
import time
import threading
import os
import uuid
import json
import pandas as pd
from datetime import datetime
from prettytable import PrettyTable
import functools
from time import sleep
from base.collection_wrapper import ApiCollectionWrapper
from base.utility_wrapper import ApiUtilityWrapper
from common import common_func as cf
from common import common_type as ct
from common.milvus_sys import MilvusSys
from chaos import constants

from common.common_type import CheckTasks
from utils.util_log import test_log as log
from utils.api_request import Error

event_lock = threading.Lock()
request_lock = threading.Lock()


def get_chaos_info():
    try:
        with open(constants.CHAOS_INFO_SAVE_PATH, 'r') as f:
            chaos_info = json.load(f)
    except Exception as e:
        log.error(f"get_chaos_info error: {e}")
        return None
    return chaos_info


class Singleton(type):
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]


class EventRecords(metaclass=Singleton):

    def __init__(self):
        self.file_name = f"/tmp/ci_logs/event_records_{uuid.uuid4()}.parquet"
        self.created_file = False

    def insert(self, event_name, event_status, ts=None):
        log.info(f"insert event: {event_name}, {event_status}")
        insert_ts = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f') if ts is None else ts
        data = {
            "event_name": [event_name],
            "event_status": [event_status],
            "event_ts": [insert_ts]
        }
        df = pd.DataFrame(data)
        if not self.created_file:
            with event_lock:
                df.to_parquet(self.file_name, engine='fastparquet')
                self.created_file = True
        else:
            with event_lock:
                df.to_parquet(self.file_name, engine='fastparquet', append=True)

    def get_records_df(self):
        df = pd.read_parquet(self.file_name)
        return df


class RequestRecords(metaclass=Singleton):

    def __init__(self):
        self.file_name = f"/tmp/ci_logs/request_records_{uuid.uuid4()}.parquet"
        self.buffer = []
        self.created_file = False

    def insert(self, operation_name, collection_name, start_time, time_cost, result):
        data = {
            "operation_name": operation_name,
            "collection_name": collection_name,
            "start_time": start_time,
            "time_cost": time_cost,
            "result": result
        }
        self.buffer.append(data)
        if len(self.buffer) > 100:
            df = pd.DataFrame(self.buffer)
            if not self.created_file:
                with request_lock:
                    df.to_parquet(self.file_name, engine='fastparquet')
                    self.created_file = True
            else:
                with request_lock:
                    df.to_parquet(self.file_name, engine='fastparquet', append=True)
            self.buffer = []

    def sink(self):
        if len(self.buffer) == 0:
            return
        df = pd.DataFrame(self.buffer)
        if not self.created_file:
            with request_lock:
                df.to_parquet(self.file_name, engine='fastparquet')
                self.created_file = True
        else:
            with request_lock:
                df.to_parquet(self.file_name, engine='fastparquet', append=True)

    def get_records_df(self):
        self.sink()
        df = pd.read_parquet(self.file_name)
        return df


class ResultAnalyzer:

    def __init__(self):
        rr = RequestRecords()
        df = rr.get_records_df()
        df["start_time"] = pd.to_datetime(df["start_time"])
        df = df.sort_values(by='start_time')
        self.df = df
        self.chaos_info = get_chaos_info()
        self.chaos_start_time = self.chaos_info['create_time'] if self.chaos_info is not None else None
        self.chaos_end_time = self.chaos_info['delete_time'] if self.chaos_info is not None else None
        self.recovery_time = self.chaos_info['recovery_time'] if self.chaos_info is not None else None

    def get_stage_success_rate(self):
        df = self.df
        window = pd.offsets.Milli(1000)

        result = df.groupby([pd.Grouper(key='start_time', freq=window), 'operation_name']).apply(lambda x: pd.Series({
            'success_count': x[x['result'] == 'True'].shape[0],
            'failed_count': x[x['result'] == 'False'].shape[0]
        }))
        data = result.reset_index()
        data['success_rate'] = data['success_count'] / (data['success_count'] + data['failed_count']).replace(0, 1)
        grouped_data = data.groupby('operation_name')
        if self.chaos_info is None:
            chaos_start_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
            chaos_end_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
            recovery_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            chaos_start_time = self.chaos_info['create_time']
            chaos_end_time = self.chaos_info['delete_time']
            recovery_time = self.chaos_info['recovery_time']
        stage_success_rate = {}
        for name, group in grouped_data:
            log.info(f"operation_name: {name}")
            # spilt data to 3 parts by chaos start time and chaos end time and aggregate the success rate
            data_before_chaos = group[group['start_time'] < chaos_start_time].agg(
                {'success_rate': 'mean', 'failed_count': 'sum', 'success_count': 'sum'})
            data_during_chaos = group[
                (group['start_time'] >= chaos_start_time) & (group['start_time'] <= chaos_end_time)].agg(
                {'success_rate': 'mean', 'failed_count': 'sum', 'success_count': 'sum'})
            data_after_chaos = group[group['start_time'] > recovery_time].agg(
                {'success_rate': 'mean', 'failed_count': 'sum', 'success_count': 'sum'})
            stage_success_rate[name] = {
                'before_chaos': f"{data_before_chaos['success_rate']}({data_before_chaos['success_count']}/{data_before_chaos['success_count'] + data_before_chaos['failed_count']})" if not data_before_chaos.empty else "no data",
                'during_chaos': f"{data_during_chaos['success_rate']}({data_during_chaos['success_count']}/{data_during_chaos['success_count'] + data_during_chaos['failed_count']})" if not data_during_chaos.empty else "no data",
                'after_chaos': f"{data_after_chaos['success_rate']}({data_after_chaos['success_count']}/{data_after_chaos['success_count'] + data_after_chaos['failed_count']})" if not data_after_chaos.empty else "no data",
            }
        log.info(f"stage_success_rate: {stage_success_rate}")
        return stage_success_rate

    def get_realtime_success_rate(self, interval=10):
        df = self.df
        window = pd.offsets.Second(interval)
        result = df.groupby([pd.Grouper(key='start_time', freq=window), 'operation_name']).apply(lambda x: pd.Series({
            'success_count': x[x['result'] == 'True'].shape[0],
            'failed_count': x[x['result'] == 'False'].shape[0]
        }))
        data = result.reset_index()
        data['success_rate'] = data['success_count'] / (data['success_count'] + data['failed_count']).replace(0, 1)
        grouped_data = data.groupby('operation_name')
        return grouped_data

    def show_result_table(self):
        table = PrettyTable()
        table.field_names = ['operation_name', 'before_chaos',
                             f'during_chaos\n{self.chaos_start_time}~{self.recovery_time}',
                             'after_chaos']
        data = self.get_stage_success_rate()
        for operation, values in data.items():
            row = [operation, values['before_chaos'], values['during_chaos'], values['after_chaos']]
            table.add_row(row)
        log.info(f"succ rate for operations in different stage\n{table}")


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
    bulk_insert = 'bulk_insert'
    unknown = 'unknown'


timeout = 120
search_timeout = 10
query_timeout = 10

enable_traceback = False
DEFAULT_FMT = '[start time:{start_time}][time cost:{elapsed:0.8f}s][operation_name:{operation_name}][collection name:{collection_name}] -> {result!r}'

request_records = RequestRecords()


def trace(fmt=DEFAULT_FMT, prefix='test', flag=True):
    def decorate(func):
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            start_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
            start_time_ts = time.time()
            t0 = time.perf_counter()
            res, result = func(self, *args, **kwargs)
            elapsed = time.perf_counter() - t0
            end_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
            operation_name = func.__name__
            if flag:
                collection_name = self.c_wrap.name
                log_str = f"[{prefix}]" + fmt.format(**locals())
                # TODO: add report function in this place, like uploading to influxdb
                try:
                    t0 = time.perf_counter()
                    request_records.insert(operation_name, collection_name, start_time, elapsed, str(result))
                    tt = time.perf_counter() - t0
                    log.debug(f"insert request record cost {tt}s")
                except Exception as e:
                    log.error(e)
                log.debug(log_str)
            if result:
                self.rsp_times.append(elapsed)
                self.average_time = (
                                            elapsed + self.average_time * self._succ) / (self._succ + 1)
                self._succ += 1
                # add first success record if there is no success record before
                if len(self.fail_records) > 0 and self.fail_records[-1][0] == "failure" and \
                        self._succ + self._fail == self.fail_records[-1][1] + 1:
                    self.fail_records.append(("success", self._succ + self._fail, start_time, start_time_ts))
            else:
                self._fail += 1
                self.fail_records.append(("failure", self._succ + self._fail, start_time, start_time_ts))
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

    def __init__(self, collection_name=None, shards_num=2, dim=ct.default_dim, insert_data=True, schema=None):
        self.recovery_time = 0
        self._succ = 0
        self._fail = 0
        self.fail_records = []
        self._keep_running = True
        self.rsp_times = []
        self.average_time = 0
        self.files = []
        self.ms = MilvusSys()
        self.bucket_name = self.ms.index_nodes[0]["infos"]["system_configurations"]["minio_bucket_name"]
        self.c_wrap = ApiCollectionWrapper()
        self.utility_wrap = ApiUtilityWrapper()
        c_name = collection_name if collection_name is not None else cf.gen_unique_str(
            'Checker_')
        self.c_name = c_name
        schema = cf.gen_default_collection_schema(dim=dim) if schema is None else schema
        self.schema = schema
        self.dim = cf.get_dim_by_schema(schema=schema)
        self.int64_field_name = cf.get_int64_field_name(schema=schema)
        self.float_vector_field_name = cf.get_float_vec_field_name(schema=schema)
        self.c_wrap.init_collection(name=c_name,
                                    schema=schema,
                                    shards_num=shards_num,
                                    timeout=timeout,
                                    enable_traceback=enable_traceback)
        if insert_data:
            log.info(f"collection {c_name} created, start to insert data")
            t0 = time.perf_counter()
            self.c_wrap.insert(
                data=cf.get_column_data_by_schema(nb=constants.ENTITIES_FOR_SEARCH, schema=schema, start=0),
                timeout=timeout,
                enable_traceback=enable_traceback)
            log.info(f"insert data for collection {c_name} cost {time.perf_counter() - t0}s")

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
        log.debug(f"{checker_name} rsp times: {self.rsp_times}")
        if len(self.fail_records) > 0:
            log.info(f"{checker_name} failed at {self.fail_records}")
        return checkers_result

    def terminate(self):
        self._keep_running = False
        self.reset()

    def pause(self):
        self._keep_running = False
        time.sleep(10)

    def resume(self):
        self._keep_running = True
        time.sleep(10)

    def reset(self):
        self._succ = 0
        self._fail = 0
        self.rsp_times = []
        self.fail_records = []
        self.average_time = 0

    def get_rto(self):
        if len(self.fail_records) == 0:
            return 0
        end = self.fail_records[-1][3]
        start = self.fail_records[0][3]
        recovery_time = end - start  # second
        self.recovery_time = recovery_time
        checker_name = self.__class__.__name__
        log.info(f"{checker_name} recovery time is {self.recovery_time}, start at {self.fail_records[0][2]}, "
                 f"end at {self.fail_records[-1][2]}")
        return recovery_time

    def prepare_bulk_insert_data(self,
                                 nb=constants.ENTITIES_FOR_BULKINSERT,
                                 file_type="npy",
                                 minio_endpoint="127.0.0.1:9000",
                                 bucket_name=None):
        schema = self.schema
        bucket_name = self.bucket_name if bucket_name is None else bucket_name
        log.info(f"prepare data for bulk insert")
        try:
            files = cf.prepare_bulk_insert_data(schema=schema,
                                                nb=nb,
                                                file_type=file_type,
                                                minio_endpoint=minio_endpoint,
                                                bucket_name=bucket_name)
            self.files = files
            return files, True
        except Exception as e:
            log.error(f"prepare data for bulk insert failed with error {e}")
            return [], False

    def do_bulk_insert(self):
        log.info(f"bulk insert collection name: {self.c_name}")
        task_ids, result = self.utility_wrap.do_bulk_insert(collection_name=self.c_name,
                                                            files=self.files)
        log.info(f"task ids {task_ids}")
        completed, result = self.utility_wrap.wait_for_bulk_insert_tasks_completed(task_ids=[task_ids], timeout=720)
        return task_ids, completed


class SearchChecker(Checker):
    """check search operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None, ):
        if collection_name is None:
            collection_name = cf.gen_unique_str("SearchChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self.c_wrap.create_index(self.float_vector_field_name,
                                 constants.DEFAULT_INDEX_PARAM,
                                 index_name=cf.gen_unique_str('index_'),
                                 timeout=timeout,
                                 enable_traceback=enable_traceback,
                                 check_task=CheckTasks.check_nothing)
        # do load before search
        self.c_wrap.load(replica_number=replica_number)

    @trace()
    def search(self):
        res, result = self.c_wrap.search(
            data=cf.gen_vectors(5, self.dim),
            anns_field=self.float_vector_field_name,
            param=constants.DEFAULT_SEARCH_PARAM,
            limit=1,
            timeout=search_timeout,
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

    def __init__(self, collection_name=None, flush=False, shards_num=2, schema=None):
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self._flush = flush
        self.initial_entities = self.c_wrap.num_entities

    def keep_running(self):
        while True:
            t0 = time.time()
            _, insert_result = \
                self.c_wrap.insert(
                    data=cf.get_column_data_by_schema(nb=constants.ENTITIES_FOR_SEARCH, schema=self.schema),
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
                sleep(constants.WAIT_PER_OP * 6)


class FlushChecker(Checker):
    """check flush operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("FlushChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
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
        _, result = self.c_wrap.insert(
            data=cf.get_column_data_by_schema(nb=constants.ENTITIES_FOR_SEARCH, schema=self.schema),
            timeout=timeout,
            enable_traceback=enable_traceback,
            check_task=CheckTasks.check_nothing)
        res, result = self.flush()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP * 6)


class InsertChecker(Checker):
    """check flush operations in a dependent thread"""

    def __init__(self, collection_name=None, flush=False, shards_num=2, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("InsertChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self._flush = flush
        self.initial_entities = self.c_wrap.num_entities
        self.inserted_data = []
        self.scale = 1 * 10 ** 6
        self.start_time_stamp = int(time.time() * self.scale)  # us
        self.term_expr = f'{self.int64_field_name} >= {self.start_time_stamp}'
        self.file_name = f"/tmp/ci_logs/insert_data_{uuid.uuid4()}.parquet"

    @trace()
    def insert(self):
        data = cf.get_column_data_by_schema(nb=constants.DELTA_PER_INS, schema=self.schema)
        ts_data = []
        for i in range(constants.DELTA_PER_INS):
            time.sleep(0.001)
            offset_ts = int(time.time() * self.scale)
            ts_data.append(offset_ts)

        data[0] = ts_data  # set timestamp (ms) as int64
        log.debug(f"insert data: {ts_data}")
        res, result = self.c_wrap.insert(data=data,
                                         timeout=timeout,
                                         enable_traceback=enable_traceback,
                                         check_task=CheckTasks.check_nothing)
        if result:
            # TODO: persist data to file
            self.inserted_data.extend(ts_data)
        return res, result

    @exception_handler()
    def run_task(self):
        res, result = self.insert()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)

    def verify_data_completeness(self):
        try:
            self.c_wrap.create_index(self.float_vector_field_name,
                                     constants.DEFAULT_INDEX_PARAM,
                                     index_name=cf.gen_unique_str('index_'),
                                     timeout=timeout,
                                     enable_traceback=enable_traceback,
                                     check_task=CheckTasks.check_nothing)
        except Exception as e:
            log.error(f"create index error: {e}")
        self.c_wrap.load()
        end_time_stamp = int(time.time() * self.scale)
        self.term_expr = f'{self.int64_field_name} >= {self.start_time_stamp} and ' \
                         f'{self.int64_field_name} <= {end_time_stamp}'
        data_in_client = []
        for d in self.inserted_data:
            if self.start_time_stamp <= d <= end_time_stamp:
                data_in_client.append(d)
        res, result = self.c_wrap.query(self.term_expr, timeout=timeout,
                                        output_fields=[f'{self.int64_field_name}'],
                                        limit=len(data_in_client) * 2,
                                        check_task=CheckTasks.check_nothing)

        data_in_server = []
        for r in res:
            d = r[f"{ct.default_int64_field_name}"]
            data_in_server.append(d)
        pytest.assume(set(data_in_server) == set(data_in_client))


class CreateChecker(Checker):
    """check create operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("CreateChecker_")
        super().__init__(collection_name=collection_name, schema=schema)

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
            sleep(constants.WAIT_PER_OP)


class IndexChecker(Checker):
    """check Insert operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("IndexChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.index_name = cf.gen_unique_str('index_')
        for i in range(5):
            self.c_wrap.insert(data=cf.get_column_data_by_schema(nb=constants.ENTITIES_FOR_SEARCH, schema=self.schema),
                               timeout=timeout, enable_traceback=enable_traceback)
        # do as a flush before indexing
        log.debug(f"Index ready entities: {self.c_wrap.num_entities}")

    @trace()
    def create_index(self):
        res, result = self.c_wrap.create_index(self.float_vector_field_name,
                                               constants.DEFAULT_INDEX_PARAM,
                                               index_name=self.index_name,
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
            sleep(constants.WAIT_PER_OP * 6)


class QueryChecker(Checker):
    """check query operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("QueryChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        res, result = self.c_wrap.create_index(self.float_vector_field_name,
                                               constants.DEFAULT_INDEX_PARAM,
                                               index_name=cf.gen_unique_str(
                                                   'index_'),
                                               timeout=timeout,
                                               enable_traceback=enable_traceback,
                                               check_task=CheckTasks.check_nothing)
        self.c_wrap.load(replica_number=replica_number)  # do load before query
        self.term_expr = None

    @trace()
    def query(self):
        res, result = self.c_wrap.query(self.term_expr, timeout=query_timeout,
                                        check_task=CheckTasks.check_nothing)
        return res, result

    @exception_handler()
    def run_task(self):
        int_values = []
        for _ in range(5):
            int_values.append(randint(0, constants.ENTITIES_FOR_SEARCH))
        self.term_expr = f'{self.int64_field_name} in {int_values}'
        res, result = self.query()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class LoadChecker(Checker):
    """check load operations in a dependent thread"""

    def __init__(self, collection_name=None, replica_number=1, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("LoadChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.replica_number = replica_number
        res, result = self.c_wrap.create_index(self.float_vector_field_name,
                                               constants.DEFAULT_INDEX_PARAM,
                                               index_name=cf.gen_unique_str(
                                                   'index_'),
                                               timeout=timeout,
                                               enable_traceback=enable_traceback,
                                               check_task=CheckTasks.check_nothing)

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

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DeleteChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        res, result = self.c_wrap.create_index(self.float_vector_field_name,
                                               constants.DEFAULT_INDEX_PARAM,
                                               index_name=cf.gen_unique_str(
                                                   'index_'),
                                               timeout=timeout,
                                               enable_traceback=enable_traceback,
                                               check_task=CheckTasks.check_nothing)
        self.c_wrap.load()  # load before query
        term_expr = f'{self.int64_field_name} > 0'
        res, _ = self.c_wrap.query(term_expr, output_fields=[
            self.int64_field_name])
        self.ids = [r[self.int64_field_name] for r in res]
        self.expr = None

    @trace()
    def delete(self):
        res, result = self.c_wrap.delete(expr=self.expr, timeout=timeout)
        return res, result

    @exception_handler()
    def run_task(self):
        delete_ids = self.ids.pop()
        self.expr = f'{self.int64_field_name} in {[delete_ids]}'
        res, result = self.delete()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class CompactChecker(Checker):
    """check compact operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("CompactChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.ut = ApiUtilityWrapper()
        res, result = self.c_wrap.create_index(self.float_vector_field_name,
                                               constants.DEFAULT_INDEX_PARAM,
                                               index_name=cf.gen_unique_str(
                                                   'index_'),
                                               timeout=timeout,
                                               enable_traceback=enable_traceback,
                                               check_task=CheckTasks.check_nothing)
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

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DropChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.collection_pool = []
        self.gen_collection_pool(schema=self.schema)

    def gen_collection_pool(self, pool_size=50, schema=None):
        for i in range(pool_size):
            collection_name = cf.gen_unique_str("DropChecker_")
            res, result = self.c_wrap.init_collection(name=collection_name, schema=schema)
            if result:
                self.collection_pool.append(collection_name)

    @trace()
    def drop(self):
        res, result = self.c_wrap.drop()
        if result:
            self.collection_pool.remove(self.c_wrap.name)
        return res, result

    @exception_handler()
    def run_task(self):
        res, result = self.drop()
        return res, result

    def keep_running(self):
        while self._keep_running:
            res, result = self.run_task()
            if result:
                try:
                    if len(self.collection_pool) <= 10:
                        self.gen_collection_pool(schema=self.schema)
                except Exception as e:
                    log.error(f"Failed to generate collection pool: {e}")
                try:
                    c_name = self.collection_pool[0]
                    self.c_wrap.init_collection(name=c_name)
                except Exception as e:
                    log.error(f"Failed to init new collection: {e}")
            sleep(constants.WAIT_PER_OP)


class LoadBalanceChecker(Checker):
    """check loadbalance operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("LoadBalanceChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.utility_wrap = ApiUtilityWrapper()
        res, result = self.c_wrap.create_index(self.float_vector_field_name,
                                               constants.DEFAULT_INDEX_PARAM,
                                               index_name=cf.gen_unique_str(
                                                   'index_'),
                                               timeout=timeout,
                                               enable_traceback=enable_traceback,
                                               check_task=CheckTasks.check_nothing)
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


class BulkInsertChecker(Checker):
    """check bulk load operations in a dependent thread"""

    def __init__(self, collection_name=None, files=[], use_one_collection=False, dim=ct.default_dim,
                 schema=None, insert_data=False):
        if collection_name is None:
            collection_name = cf.gen_unique_str("BulkInsertChecker_")
        super().__init__(collection_name=collection_name, dim=dim, schema=schema, insert_data=insert_data)
        self.utility_wrap = ApiUtilityWrapper()
        self.schema = cf.gen_bulk_insert_collection_schema() if schema is None else schema
        self.files = files
        self.recheck_failed_task = False
        self.failed_tasks = []
        self.use_one_collection = use_one_collection  # if True, all tasks will use one collection to bulk insert
        self.c_name = collection_name

    def update(self, files=None, schema=None):
        if files is not None:
            self.files = files
        if schema is not None:
            self.schema = schema

    @trace()
    def bulk_insert(self):
        log.info(f"bulk insert collection name: {self.c_name}")
        task_ids, result = self.utility_wrap.do_bulk_insert(collection_name=self.c_name,
                                                            files=self.files)
        log.info(f"task ids {task_ids}")
        completed, result = self.utility_wrap.wait_for_bulk_insert_tasks_completed(task_ids=[task_ids], timeout=720)
        return task_ids, completed

    @exception_handler()
    def run_task(self):
        if not self.use_one_collection:
            if self.recheck_failed_task and self.failed_tasks:
                self.c_name = self.failed_tasks.pop(0)
                log.debug(f"check failed task: {self.c_name}")
            else:
                self.c_name = cf.gen_unique_str("BulkInsertChecker_")
        self.c_wrap.init_collection(name=self.c_name, schema=self.schema)
        log.info(f"collection schema: {self.c_wrap.schema}")
        # bulk insert data
        num_entities = self.c_wrap.num_entities
        log.info(f"before bulk insert, collection {self.c_name} has num entities {num_entities}")
        task_ids, completed = self.bulk_insert()
        num_entities = self.c_wrap.num_entities
        log.info(f"after bulk insert, collection {self.c_name} has num entities {num_entities}")
        if not completed:
            self.failed_tasks.append(self.c_name)
        return task_ids, completed

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class TestResultAnalyzer(unittest.TestCase):
    def test_get_stage_success_rate(self):
        ra = ResultAnalyzer()
        res = ra.get_stage_success_rate()
        print(res)


if __name__ == '__main__':
    unittest.main()
