import pytest
import unittest
from enum import Enum
import random
import re
import time
import threading
import uuid
import json
import pandas as pd
from datetime import datetime
from prettytable import PrettyTable
import functools
from collections import Counter
from time import sleep
from pymilvus import AnnSearchRequest, RRFRanker, MilvusClient, DataType, CollectionSchema, connections, LexicalHighlighter
from pymilvus.milvus_client.index import IndexParams
from pymilvus.bulk_writer import RemoteBulkWriter, BulkFileType
from pymilvus.client.embedding_list import EmbeddingList
from common import common_func as cf
from common import common_type as ct
from common.milvus_sys import MilvusSys
from chaos import constants
from faker import Faker

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
        log.warning(f"get_chaos_info error: {e}")
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
        self.file_name = f"/tmp/ci_logs/event_records_{uuid.uuid4()}.jsonl"

    def insert(self, event_name, event_status, ts=None):
        log.info(f"insert event: {event_name}, {event_status}")
        insert_ts = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f') if ts is None else ts
        data = {
            "event_name": event_name,
            "event_status": event_status,
            "event_ts": insert_ts
        }
        with event_lock:
            with open(self.file_name, 'a') as f:
                f.write(json.dumps(data) + '\n')

    def get_records_df(self):
        with event_lock:
            try:
                records = []
                with open(self.file_name, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            records.append(json.loads(line))
                if not records:
                    return pd.DataFrame(columns=["event_name", "event_status", "event_ts"])
                return pd.DataFrame(records)
            except FileNotFoundError:
                return pd.DataFrame(columns=["event_name", "event_status", "event_ts"])
            except Exception as e:
                log.warning(f"EventRecords read error: {e}")
                return pd.DataFrame(columns=["event_name", "event_status", "event_ts"])


class RequestRecords(metaclass=Singleton):

    def __init__(self):
        self.file_name = f"/tmp/ci_logs/request_records_{uuid.uuid4()}.jsonl"
        self.buffer = []

    def insert(self, operation_name, collection_name, start_time, time_cost, result):
        data = {
            "operation_name": operation_name,
            "collection_name": collection_name,
            "start_time": start_time,
            "time_cost": time_cost,
            "result": result
        }
        with request_lock:
            self.buffer.append(data)
            if len(self.buffer) >= 100:
                self._flush_buffer()

    def _flush_buffer(self):
        """将 buffer 写入文件（调用时需持有 request_lock）"""
        if not self.buffer:
            return
        try:
            with open(self.file_name, 'a') as f:
                for record in self.buffer:
                    f.write(json.dumps(record) + '\n')
            self.buffer = []
        except Exception as e:
            log.error(f"RequestRecords flush error: {e}")

    def sink(self):
        with request_lock:
            self._flush_buffer()

    def get_records_df(self):
        self.sink()
        with request_lock:
            try:
                records = []
                with open(self.file_name, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            records.append(json.loads(line))
                if not records:
                    return pd.DataFrame(columns=["operation_name", "collection_name", "start_time", "time_cost", "result"])
                return pd.DataFrame(records)
            except FileNotFoundError:
                return pd.DataFrame(columns=["operation_name", "collection_name", "start_time", "time_cost", "result"])
            except Exception as e:
                log.warning(f"RequestRecords read error: {e}")
                return pd.DataFrame(columns=["operation_name", "collection_name", "start_time", "time_cost", "result"])


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
                             f'during_chaos: {self.chaos_start_time}~{self.recovery_time}',
                             'after_chaos']
        data = self.get_stage_success_rate()
        for operation, values in data.items():
            row = [operation, values['before_chaos'], values['during_chaos'], values['after_chaos']]
            table.add_row(row)
        log.info(f"succ rate for operations in different stage\n{table}")


class Op(Enum):
    create = 'create'  # short name for create collection
    create_db = 'create_db'
    create_collection = 'create_collection'
    create_partition = 'create_partition'
    insert = 'insert'
    insert_freshness = 'insert_freshness'
    upsert = 'upsert'
    upsert_freshness = 'upsert_freshness'
    partial_update = 'partial_update'
    flush = 'flush'
    index = 'index'
    create_index = 'create_index'
    drop_index = 'drop_index'
    load = 'load'
    load_collection = 'load_collection'
    load_partition = 'load_partition'
    release = 'release'
    release_collection = 'release_collection'
    release_partition = 'release_partition'
    search = 'search'
    tensor_search = 'tensor_search'
    full_text_search = 'full_text_search'
    hybrid_search = 'hybrid_search'
    query = 'query'
    text_match = 'text_match'
    phrase_match = 'phrase_match'
    json_query = 'json_query'
    geo_query = 'geo_query'
    delete = 'delete'
    delete_freshness = 'delete_freshness'
    compact = 'compact'
    drop = 'drop'  # short name for drop collection
    drop_db = 'drop_db'
    drop_collection = 'drop_collection'
    drop_partition = 'drop_partition'
    load_balance = 'load_balance'
    bulk_insert = 'bulk_insert'
    alter_collection = 'alter_collection'
    add_field = 'add_field'
    rename_collection = 'rename_collection'
    snapshot = 'snapshot'
    restore_snapshot = 'restore_snapshot'
    unknown = 'unknown'


timeout = 120
search_timeout = 10
query_timeout = 10

enable_traceback = False
DEFAULT_FMT = '[start time:{start_time}][time cost:{elapsed:0.8f}s][operation_name:{operation_name}][collection name:{collection_name}] -> {result!r}'

request_records = RequestRecords()


def create_index_params_from_dict(field_name: str, index_param_dict: dict) -> IndexParams:
    """Helper function to convert dict-style index params to IndexParams object"""
    index_params = IndexParams()
    params_copy = index_param_dict.copy()
    index_type = params_copy.pop("index_type", "")
    index_params.add_index(field_name=field_name, index_type=index_type, **params_copy)
    return index_params


def normalize_error_message(error_msg):
    """
    Normalize error message by extracting text from message= fields.
    Only keep letter content from message values to group similar errors.
    """
    msg = str(error_msg)
    # Extract all message= content
    messages = re.findall(r'message[=:]\s*["\']?([^"\'>,\)]+)', msg, re.IGNORECASE)
    if messages:
        # Combine all message content and keep only letters and spaces
        combined = ' '.join(messages)
        combined = re.sub(r'[^a-zA-Z\s]', ' ', combined)
        combined = re.sub(r'\s+', ' ', combined).strip()
        return combined
    # Fallback: extract text from details= if no message found
    details = re.findall(r'details\s*=\s*"([^"]+)"', msg)
    if details:
        combined = ' '.join(details)
        combined = re.sub(r'[^a-zA-Z\s]', ' ', combined)
        combined = re.sub(r'\s+', ' ', combined).strip()
        return combined
    # Last fallback: keep only letters from entire message
    msg = re.sub(r'[^a-zA-Z\s]', ' ', msg)
    msg = re.sub(r'\s+', ' ', msg).strip()
    return msg


def trace(fmt=DEFAULT_FMT, prefix='test', flag=True):
    def decorate(func):
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            start_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
            start_time_ts = time.time()
            t0 = time.perf_counter()
            res, result = func(self, *args, **kwargs)
            elapsed = time.perf_counter() - t0
            operation_name = func.__name__
            if flag:
                collection_name = self.c_name
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
                # Collect unique error messages (normalized to group similar errors)
                if hasattr(res, 'message'):
                    normalized_msg = normalize_error_message(res.message)
                elif res is not None:
                    normalized_msg = normalize_error_message(str(res))
                else:
                    normalized_msg = "Unknown error"
                self.error_messages.add(normalized_msg)
            return res, result

        return inner_wrapper

    return decorate


def exception_handler():
    def wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            class_name = None
            function_name = None
            try:
                function_name = func.__name__
                class_name = getattr(self, '__class__', None).__name__ if self else None
                res, result = func(self, *args, **kwargs)
                return res, result
            except Exception as e:
                log_row_length = 300
                e_str = str(e)
                log_e = e_str[0:log_row_length] + '......' if len(e_str) > log_row_length else e_str
                if class_name:
                    log_message = f"Error in {class_name}.{function_name}: {log_e}"
                else:
                    log_message = f"Error in {function_name}: {log_e}"
                log.exception(log_message)
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

    def __init__(self, collection_name=None, partition_name=None, shards_num=2, dim=8, insert_data=True,
                 schema=None, replica_number=1, **kwargs):
        self.recovery_time = 0
        self._succ = 0
        self._fail = 0
        self.fail_records = []
        self.error_messages = set()  # Store unique error messages
        self._keep_running = True
        self.rsp_times = []
        self.average_time = 0
        self.scale = 1 * 10 ** 6
        self.files = []
        self.word_freq = Counter()
        self.ms = MilvusSys()
        self.bucket_name = cf.param_info.param_bucket_name

        # Initialize MilvusClient - prioritize uri and token
        if cf.param_info.param_uri:
            uri = cf.param_info.param_uri
        else:
            uri = "http://" + cf.param_info.param_host + ":" + str(cf.param_info.param_port)

        if cf.param_info.param_token:
            token = cf.param_info.param_token
        else:
            token = f"{cf.param_info.param_user}:{cf.param_info.param_password}"
        self.milvus_client = MilvusClient(uri=uri, token=token)

        # Also create a connection for low-level APIs that MilvusClient doesn't support
        self.alias = cf.gen_unique_str("checker_alias_")
        connections.connect(
            alias=self.alias,
            uri=uri,
            token=token
        )
        c_name = collection_name if collection_name is not None else cf.gen_unique_str(
            'Checker_')
        self.c_name = c_name
        p_name = partition_name if partition_name is not None else "_default"
        self.p_name = p_name
        self.p_names = [self.p_name] if partition_name is not None else None

        # Get or create schema
        if self.milvus_client.has_collection(c_name):
            collection_info = self.milvus_client.describe_collection(c_name)
            schema = CollectionSchema.construct_from_dict(collection_info)
        else:
            enable_struct_array_field = kwargs.get("enable_struct_array_field", True)
            enable_dynamic_field = kwargs.get("enable_dynamic_field", True)
            schema = cf.gen_all_datatype_collection_schema(dim=dim, enable_struct_array_field=enable_struct_array_field, enable_dynamic_field=enable_dynamic_field) if schema is None else schema

        log.debug(f"schema: {schema}")
        self.schema = schema
        self.dim = cf.get_dim_by_schema(schema=schema)
        self.int64_field_name = cf.get_int64_field_name(schema=schema)
        self.text_field_name = cf.get_text_field_name(schema=schema)
        self.text_match_field_name_list = cf.get_text_match_field_name(schema=schema)
        self.float_vector_field_name = cf.get_float_vec_field_name(schema=schema)

        # Create collection if not exists
        if not self.milvus_client.has_collection(c_name):
            self.milvus_client.create_collection(
                collection_name=c_name,
                schema=schema,
                shards_num=shards_num,
                timeout=timeout
            )
        self.scalar_field_names = cf.get_scalar_field_name_list(schema=schema)
        self.json_field_names = cf.get_json_field_name_list(schema=schema)
        self.geometry_field_names = cf.get_geometry_field_name_list(schema=schema)
        self.float_vector_field_names = cf.get_float_vec_field_name_list(schema=schema)
        self.binary_vector_field_names = cf.get_binary_vec_field_name_list(schema=schema)
        self.int8_vector_field_names = cf.get_int8_vec_field_name_list(schema=schema)
        self.bm25_sparse_field_names = cf.get_bm25_vec_field_name_list(schema=schema)
        self.emb_list_field_names = cf.get_emb_list_field_name_list(schema=schema)

        # Get existing indexes and their fields
        indexed_fields = set()
        try:
            index_names = self.milvus_client.list_indexes(c_name)
            for idx_name in index_names:
                try:
                    idx_info = self.milvus_client.describe_index(c_name, idx_name)
                    if 'field_name' in idx_info:
                        indexed_fields.add(idx_info['field_name'])
                except Exception as e:
                    log.debug(f"Failed to describe index {idx_name}: {e}")
        except Exception as e:
            log.debug(f"Failed to list indexes: {e}")

        log.debug(f"Already indexed fields: {indexed_fields}")

        # create index for scalar fields
        for f in self.scalar_field_names:
            if f in indexed_fields:
                continue
            try:
                index_params = IndexParams()
                index_params.add_index(field_name=f, index_type="INVERTED")
                self.milvus_client.create_index(
                    collection_name=c_name,
                    index_params=index_params,
                    timeout=timeout
                )
            except Exception as e:
                log.debug(f"Failed to create index for {f}: {e}")

        # create index for json fields
        for f in self.json_field_names:
            if f in indexed_fields:
                continue
            for json_path, json_cast in [("name", "varchar"), ("address", "varchar"), ("count", "double")]:
                try:
                    index_params = IndexParams()
                    index_params.add_index(
                        field_name=f,
                        index_type="INVERTED",
                        params={"json_path": f"{f}['{json_path}']", "json_cast_type": json_cast}
                    )
                    self.milvus_client.create_index(
                        collection_name=c_name,
                        index_params=index_params,
                        timeout=timeout
                    )
                except Exception as e:
                    log.debug(f"Failed to create json index for {f}['{json_path}']: {e}")

        # create index for geometry fields
        for f in self.geometry_field_names:
            if f in indexed_fields:
                continue
            try:
                index_params = IndexParams()
                index_params.add_index(field_name=f, index_type="RTREE")
                self.milvus_client.create_index(
                    collection_name=c_name,
                    index_params=index_params,
                    timeout=timeout
                )
            except Exception as e:
                log.debug(f"Failed to create index for {f}: {e}")

        # create index for float vector fields
        vector_index_created = False
        for f in self.float_vector_field_names:
            if f in indexed_fields:
                vector_index_created = True
                log.debug(f"Float vector field {f} already has index")
                continue
            try:
                index_params = create_index_params_from_dict(f, constants.DEFAULT_INDEX_PARAM)
                self.milvus_client.create_index(
                    collection_name=c_name,
                    index_params=index_params,
                    timeout=timeout
                )
                log.debug(f"Created index for float vector field {f}")
                indexed_fields.add(f)
                vector_index_created = True
            except Exception as e:
                log.warning(f"Failed to create index for {f}: {e}")

        # create index for int8 vector fields
        for f in self.int8_vector_field_names:
            if f in indexed_fields:
                vector_index_created = True
                log.debug(f"Int8 vector field {f} already has index")
                continue
            try:
                index_params = create_index_params_from_dict(f, constants.DEFAULT_INT8_INDEX_PARAM)
                self.milvus_client.create_index(
                    collection_name=c_name,
                    index_params=index_params,
                    timeout=timeout
                )
                log.debug(f"Created index for int8 vector field {f}")
                indexed_fields.add(f)
                vector_index_created = True
            except Exception as e:
                log.warning(f"Failed to create index for {f}: {e}")

        # create index for binary vector fields
        for f in self.binary_vector_field_names:
            if f in indexed_fields:
                vector_index_created = True
                log.debug(f"Binary vector field {f} already has index")
                continue
            try:
                index_params = create_index_params_from_dict(f, constants.DEFAULT_BINARY_INDEX_PARAM)
                self.milvus_client.create_index(
                    collection_name=c_name,
                    index_params=index_params,
                    timeout=timeout
                )
                log.debug(f"Created index for binary vector field {f}")
                indexed_fields.add(f)
                vector_index_created = True
            except Exception as e:
                log.warning(f"Failed to create index for {f}: {e}")

        # create index for bm25 sparse fields
        for f in self.bm25_sparse_field_names:
            if f in indexed_fields:
                continue
            try:
                index_params = create_index_params_from_dict(f, constants.DEFAULT_BM25_INDEX_PARAM)
                self.milvus_client.create_index(
                    collection_name=c_name,
                    index_params=index_params,
                    timeout=timeout
                )
                log.debug(f"Created index for bm25 sparse field {f}")
            except Exception as e:
                log.warning(f"Failed to create index for {f}: {e}")

        # create index for emb list fields
        for f in self.emb_list_field_names:
            if f in indexed_fields:
                continue
            try:
                index_params = create_index_params_from_dict(f, constants.DEFAULT_EMB_LIST_INDEX_PARAM)
                self.milvus_client.create_index(
                    collection_name=c_name,
                    index_params=index_params,
                    timeout=timeout
                )
                log.debug(f"Created index for emb list field {f}")
            except Exception as e:
                log.warning(f"Failed to create index for {f}: {e}")

        # Load collection - only if at least one vector field has an index
        self.replica_number = replica_number
        if vector_index_created:
            try:
                self.milvus_client.load_collection(collection_name=c_name, replica_number=self.replica_number)
                log.debug(f"Loaded collection {c_name} with replica_number={self.replica_number}")
            except Exception as e:
                log.warning(f"Failed to load collection {c_name}: {e}. Collection may need to be loaded manually.")
        else:
            log.warning(f"No vector index created for collection {c_name}, skipping load. You may need to create indexes and load manually.")

        # Create partition if specified
        if p_name != "_default" and not self.milvus_client.has_partition(c_name, p_name):
            self.milvus_client.create_partition(collection_name=c_name, partition_name=p_name)

        # Insert initial data if needed
        num_entities = self.milvus_client.get_collection_stats(c_name).get("row_count", 0)
        if insert_data and num_entities == 0:
            log.info(f"collection {c_name} created, start to insert data")
            t0 = time.perf_counter()
            self.insert_data(nb=constants.ENTITIES_FOR_SEARCH, partition_name=self.p_name)
            log.info(f"insert data for collection {c_name} cost {time.perf_counter() - t0}s")

        self.initial_entities = self.milvus_client.get_collection_stats(c_name).get("row_count", 0)
        self.scale = 100000  # timestamp scale to make time.time() as int64

    def get_schema(self):
        collection_info = self.milvus_client.describe_collection(self.c_name)
        return collection_info

    def insert_data(self, nb=constants.DELTA_PER_INS, partition_name=None):
        partition_name = self.p_name if partition_name is None else partition_name
        data = cf.gen_row_data_by_schema(nb=nb, schema=self.get_schema())
        ts_data = []
        for i in range(nb):
            time.sleep(0.001)
            offset_ts = int(time.time() * self.scale)
            ts_data.append(offset_ts)
        for i in range(nb):
            data[i][self.int64_field_name] = ts_data[i]
        df = pd.DataFrame(data)
        for text_field in self.text_match_field_name_list:
            if text_field in df.columns:
                texts = df[text_field].tolist()
                wf = cf.analyze_documents(texts)
                self.word_freq.update(wf)

        # Debug: Check if struct array fields are present in generated data before insert
        if data and len(data) > 0:
            log.debug(f"[insert_data] First row keys: {list(data[0].keys())}")
            # Check for struct array fields (common names: struct_array, metadata, etc.)
            for key, value in data[0].items():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    log.debug(f"[insert_data] Found potential struct array field '{key}': {len(value)} items, first item: {value[0]}")

        try:
            res = self.milvus_client.insert(
                                             collection_name=self.c_name,
                                             data=data,
                                             partition_name=partition_name,
                                             timeout=timeout,
                                             enable_traceback=enable_traceback,
                                             check_task=CheckTasks.check_nothing)
            return res, True
        except Exception as e:
            return str(e), False

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
        self.error_messages = set()
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
        log.info("prepare data for bulk insert")
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
        from pymilvus import utility
        task_ids = utility.do_bulk_insert(collection_name=self.c_name, files=self.files, using=self.alias)
        log.info(f"task ids {task_ids}")
        completed = utility.wait_for_bulk_insert_tasks_completed(task_ids=[task_ids], timeout=720, using=self.alias)
        return task_ids, completed


class CollectionLoadChecker(Checker):
    """check collection load operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None, ):
        self.replica_number = replica_number
        if collection_name is None:
            collection_name = cf.gen_unique_str("CollectionLoadChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)

    @trace()
    def load_collection(self):
        try:
            self.milvus_client.load_collection(collection_name=self.c_name, replica_number=self.replica_number)
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.load_collection()
        if result:
            self.milvus_client.release_collection(collection_name=self.c_name)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class CollectionReleaseChecker(Checker):
    """check collection release operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None, ):
        self.replica_number = replica_number
        if collection_name is None:
            collection_name = cf.gen_unique_str("CollectionReleaseChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self.milvus_client.load_collection(collection_name=self.c_name, replica_number=self.replica_number)

    @trace()
    def release_collection(self):
        try:
            self.milvus_client.release_collection(collection_name=self.c_name)
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.release_collection()
        if result:
            self.milvus_client.load_collection(collection_name=self.c_name, replica_number=self.replica_number)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)



class CollectionRenameChecker(Checker):
    """check collection rename operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None, ):
        self.replica_number = replica_number
        if collection_name is None:
            collection_name = cf.gen_unique_str("CollectionRenameChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)

    @trace()
    def rename_collection(self, old_collection_name, new_collection_name):
        try:
            self.milvus_client.rename_collection(old_name=old_collection_name, new_name=new_collection_name)
            return None, True
        except Exception as e:
            log.info(f"rename collection failed with error {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        new_collection_name = "CollectionRenameChecker_" + cf.gen_unique_str("new_")
        res, result = self.rename_collection(self.c_name, new_collection_name)
        if result:
            result = self.milvus_client.has_collection(collection_name=new_collection_name)
            if result:
                self.c_name = new_collection_name
                data = cf.gen_row_data_by_schema(nb=1, schema=self.get_schema())
                self.milvus_client.insert(collection_name=new_collection_name, data=data)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class PartitionLoadChecker(Checker):
    """check partition load operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None, ):
        self.replica_number = replica_number
        if collection_name is None:
            collection_name = cf.gen_unique_str("PartitionLoadChecker_")
        p_name = cf.gen_unique_str("PartitionLoadChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema, partition_name=p_name)
        self.milvus_client.release_collection(collection_name=self.c_name)

    @trace()
    def load_partition(self):
        try:
            self.milvus_client.load_partitions(collection_name=self.c_name, partition_names=[self.p_name], replica_number=self.replica_number)
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.load_partition()
        if result:
            self.milvus_client.release_partitions(collection_name=self.c_name, partition_names=[self.p_name])
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class PartitionReleaseChecker(Checker):
    """check partition release operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None, ):
        self.replica_number = replica_number
        if collection_name is None:
            collection_name = cf.gen_unique_str("PartitionReleaseChecker_")
        p_name = cf.gen_unique_str("PartitionReleaseChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema, partition_name=p_name)
        self.milvus_client.release_collection(collection_name=self.c_name)
        self.milvus_client.load_partitions(collection_name=self.c_name, partition_names=[self.p_name], replica_number=self.replica_number)

    @trace()
    def release_partition(self):
        try:
            self.milvus_client.release_partitions(collection_name=self.c_name, partition_names=[self.p_name])
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.release_partition()
        if result:
            self.milvus_client.load_partitions(collection_name=self.c_name, partition_names=[self.p_name], replica_number=self.replica_number)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class SearchChecker(Checker):
    """check search operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("SearchChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self.insert_data()
        self.dense_anns_field_name_list = cf.get_dense_anns_field_name_list(self.schema)
        self.data = None
        self.anns_field_name = None
        self.search_param = None

    @trace()
    def search(self):
        try:
            res = self.milvus_client.search(
                collection_name=self.c_name,
                data=self.data,
                anns_field=self.anns_field_name,
                search_params=self.search_param,
                limit=5,
                partition_names=self.p_names,
                timeout=search_timeout
            )
            return res, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        anns_field_item = random.choice(self.dense_anns_field_name_list)
        self.anns_field_name = anns_field_item["name"]
        dim = anns_field_item["dim"]
        self.data = cf.gen_vectors(5, dim, vector_data_type=anns_field_item["dtype"])
        if anns_field_item["dtype"] in [DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR]:
            self.search_param = constants.DEFAULT_SEARCH_PARAM
        elif anns_field_item["dtype"] == DataType.INT8_VECTOR:
            self.search_param = constants.DEFAULT_INT8_SEARCH_PARAM

        res, result = self.search()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)

class TensorSearchChecker(Checker):
    """check search operations for struct array vector fields in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("TensorSearchChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self.insert_data()
        # Only get struct array vector fields
        self.struct_array_vector_field_list = cf.get_struct_array_vector_field_list(self.schema)
        self.data = None
        self.anns_field_name = None
        self.search_param = None

    @staticmethod
    def _create_embedding_list(dim, num_vectors, dtype):
        """Create EmbeddingList for struct array vector search"""
        embedding_list = EmbeddingList()
        vectors = cf.gen_vectors(num_vectors, dim, vector_data_type=dtype)
        for vector in vectors:
            embedding_list.add(vector)
        return embedding_list

    @trace()
    def search(self):
        try:
            res = self.milvus_client.search(
                collection_name=self.c_name,
                data=self.data,
                anns_field=self.anns_field_name,
                search_params=self.search_param,
                limit=5,
                partition_names=self.p_names,
                timeout=search_timeout
            )
            return res, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        if not self.struct_array_vector_field_list:
            log.warning("No struct array vector fields available for search")
            return None, False

        # Randomly select a struct array vector field
        anns_field_item = random.choice(self.struct_array_vector_field_list)
        dim = anns_field_item["dim"]
        dtype = anns_field_item["dtype"]

        # Use the anns_field format: struct_field[vector_field]
        self.anns_field_name = anns_field_item["anns_field"]

        # Create EmbeddingList with random number of vectors (1-5)
        num_vectors = random.randint(1, 5)
        self.data = [self._create_embedding_list(dim, num_vectors, dtype)]

        # Use MAX_SIM_COSINE for struct array vector search
        self.search_param = {"metric_type": "MAX_SIM_COSINE"}

        res, result = self.search()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class FullTextSearchChecker(Checker):
    """check full text search operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None, ):
        if collection_name is None:
            collection_name = cf.gen_unique_str("FullTextSearchChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self.insert_data()

    @trace()
    def full_text_search(self):
        bm25_anns_field = random.choice(self.bm25_sparse_field_names)
        # Create highlighter for full text search results
        highlighter = LexicalHighlighter(
            pre_tags=["<em>"],
            post_tags=["</em>"],
            highlight_search_text=True,
            fragment_offset=10,
            fragment_size=50
        )
        try:
            res = self.milvus_client.search(
                collection_name=self.c_name,
                data=cf.gen_vectors(5, self.dim, vector_data_type="TEXT_SPARSE_VECTOR"),
                anns_field=bm25_anns_field,
                search_params=constants.DEFAULT_BM25_SEARCH_PARAM,
                limit=5,
                partition_names=self.p_names,
                timeout=search_timeout,
                highlighter=highlighter
            )
            return res, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.full_text_search()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class HybridSearchChecker(Checker):
    """check hybrid search operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None, ):
        if collection_name is None:
            collection_name = cf.gen_unique_str("HybridSearchChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        # do load before search
        self.milvus_client.load_collection(collection_name=self.c_name, replica_number=replica_number)
        self.insert_data()

    def gen_hybrid_search_request(self):
        res = []
        dim = self.dim
        for vec_field_name in self.float_vector_field_names:
            search_param = {
                "data": cf.gen_vectors(1, dim),
                "anns_field": vec_field_name,
                "param": constants.DEFAULT_SEARCH_PARAM,
                "limit": 10,
                "expr": f"{self.int64_field_name} > 0",
            }
            req = AnnSearchRequest(**search_param)
            res.append(req)
        return res

    @trace()
    def hybrid_search(self):
        try:
            res = self.milvus_client.hybrid_search(
                collection_name=self.c_name,
                reqs=self.gen_hybrid_search_request(),
                ranker=RRFRanker(),
                limit=10,
                partition_names=self.p_names,
                timeout=search_timeout
            )
            return res, True
        except Exception as e:
            log.info(f"hybrid search failed with error {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.hybrid_search()
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
        stats = self.milvus_client.get_collection_stats(collection_name=self.c_name)
        self.initial_entities = stats.get("row_count", 0)

    def keep_running(self):
        while True:
            t0 = time.time()
            try:
                self.milvus_client.insert(
                    collection_name=self.c_name,
                    data=cf.gen_row_data_by_schema(nb=constants.ENTITIES_FOR_SEARCH, schema=self.get_schema()),
                    timeout=timeout
                )
                insert_result = True
            except Exception:
                insert_result = False
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
                # call flush to get num_entities
                t0 = time.time()
                self.milvus_client.flush(collection_names=[self.c_name])
                stats = self.milvus_client.get_collection_stats(collection_name=self.c_name)
                num_entities = stats.get("row_count", 0)
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
        stats = self.milvus_client.get_collection_stats(collection_name=self.c_name)
        self.initial_entities = stats.get("row_count", 0)

    @trace()
    def flush(self):
        try:
            self.milvus_client.flush(collection_name=self.c_name)
            return None, True
        except Exception as e:
            log.info(f"flush error: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        try:
            self.milvus_client.insert(
                collection_name=self.c_name,
                data=cf.gen_row_data_by_schema(nb=constants.ENTITIES_FOR_SEARCH, schema=self.get_schema()),
                timeout=timeout
            )
            res, result = self.flush()
            return res, result
        except Exception as e:
            log.error(f"run task error: {e}")
            return str(e), False

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP * 6)


class AddFieldChecker(Checker):
    """check add field operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("AddFieldChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        stats = self.milvus_client.get_collection_stats(collection_name=self.c_name)
        self.initial_entities = stats.get("row_count", 0)

    @trace()
    def add_field(self):
        try:
            new_field_name = cf.gen_unique_str("new_field_")
            self.milvus_client.add_collection_field(collection_name=self.c_name,
                                                    field_name=new_field_name,
                                                    data_type=DataType.INT64,
                                                    nullable=True)
            log.debug(f"add field {new_field_name} to collection {self.c_name}")
            time.sleep(1)
            _, result = self.insert_data()
            res = self.milvus_client.query(collection_name=self.c_name,
                                           filter=f"{new_field_name} >= 0",
                                           output_fields=[new_field_name])
            result = True
            if result:
                log.debug(f"query with field {new_field_name} success")
            return None, result
        except Exception as e:
            log.error(e)
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.add_field()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP * 6)



class InsertChecker(Checker):
    """check insert operations in a dependent thread"""

    def __init__(self, collection_name=None, flush=False, shards_num=2, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("InsertChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self._flush = flush
        stats = self.milvus_client.get_collection_stats(collection_name=self.c_name)
        self.initial_entities = stats.get("row_count", 0)
        self.inserted_data = []
        self.scale = 1 * 10 ** 6
        self.start_time_stamp = int(time.time() * self.scale)  # us
        self.term_expr = f'{self.int64_field_name} >= {self.start_time_stamp}'

    @trace()
    def insert_entities(self):
        data = cf.gen_row_data_by_schema(nb=constants.DELTA_PER_INS, schema=self.get_schema())
        rows = len(data)
        ts_data = []
        for i in range(constants.DELTA_PER_INS):
            time.sleep(0.001)
            offset_ts = int(time.time() * self.scale)
            ts_data.append(offset_ts)

        for i in range(rows):
            data[i][self.int64_field_name] = ts_data[i]

        log.debug(f"insert data: {rows}")
        # Debug: Check if struct array fields are present in generated data
        if data and len(data) > 0:
            log.debug(f"[InsertChecker] First row keys: {list(data[0].keys())}")
            # Check for struct array fields (common names: struct_array, metadata, etc.)
            for key, value in data[0].items():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    log.debug(f"[InsertChecker] Found potential struct array field '{key}': {len(value)} items, first item: {value[0]}")

        try:
            res = self.milvus_client.insert(collection_name=self.c_name,
                                           data=data,
                                           partition_name=self.p_names[0] if self.p_names else None,
                                           timeout=timeout)
            return res, True
        except Exception as e:
            log.info(f"insert error: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):

        res, result = self.insert_entities()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)

    def verify_data_completeness(self):
        # deprecated
        try:
            index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
            self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        except Exception as e:
            log.error(f"create index error: {e}")
        self.milvus_client.load_collection(collection_name=self.c_name)
        end_time_stamp = int(time.time() * self.scale)
        self.term_expr = f'{self.int64_field_name} >= {self.start_time_stamp} and ' \
                         f'{self.int64_field_name} <= {end_time_stamp}'
        data_in_client = []
        for d in self.inserted_data:
            if self.start_time_stamp <= d <= end_time_stamp:
                data_in_client.append(d)
        res = self.milvus_client.query(collection_name=self.c_name, filter=self.term_expr,
                                      output_fields=[f'{self.int64_field_name}'],
                                      limit=len(data_in_client) * 2, timeout=timeout)
        result = True

        data_in_server = []
        for r in res:
            d = r[f"{ct.default_int64_field_name}"]
            data_in_server.append(d)
        pytest.assume(set(data_in_server) == set(data_in_client))


class InsertFreshnessChecker(Checker):
    """check insert freshness operations in a dependent thread"""

    def __init__(self, collection_name=None, flush=False, shards_num=2, schema=None):
        self.latest_data = None
        if collection_name is None:
            collection_name = cf.gen_unique_str("InsertChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self._flush = flush
        stats = self.milvus_client.get_collection_stats(collection_name=self.c_name)
        self.initial_entities = stats.get("row_count", 0)
        self.inserted_data = []
        self.scale = 1 * 10 ** 6
        self.start_time_stamp = int(time.time() * self.scale)  # us
        self.term_expr = f'{self.int64_field_name} >= {self.start_time_stamp}'

    def insert_entities(self):
        data = cf.gen_row_data_by_schema(nb=constants.DELTA_PER_INS, schema=self.get_schema())
        ts_data = []
        for i in range(constants.DELTA_PER_INS):
            time.sleep(0.001)
            offset_ts = int(time.time() * self.scale)
            ts_data.append(offset_ts)

        data[0] = ts_data  # set timestamp (ms) as int64
        log.debug(f"insert data: {len(ts_data)}")
        try:
            res = self.milvus_client.insert(collection_name=self.c_name,
                                           data=data,
                                           partition_name=self.p_names[0] if self.p_names else None,
                                           timeout=timeout)
            result = True
        except Exception as e:
            res = str(e)
            result = False
        self.latest_data = ts_data[-1]
        self.term_expr = f'{self.int64_field_name} == {self.latest_data}'
        return res, result

    @trace()
    def insert_freshness(self):
        while True:
            try:
                res = self.milvus_client.query(collection_name=self.c_name,
                                              filter=self.term_expr,
                                              output_fields=[f'{self.int64_field_name}'],
                                              timeout=timeout)
                result = True
            except Exception as e:
                res = str(e)
                result = False
                break
            if len(res) == 1 and res[0][f"{self.int64_field_name}"] == self.latest_data:
                break
        return res, result

    @exception_handler()
    def run_task(self):
        res, result = self.insert_entities()
        res, result = self.insert_freshness()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class UpsertChecker(Checker):
    """check upsert operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("UpsertChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self.data = cf.gen_row_data_by_schema(nb=constants.DELTA_PER_INS, schema=self.get_schema())

    @trace()
    def upsert_entities(self):
        try:
            res = self.milvus_client.upsert(collection_name=self.c_name,
                                           data=self.data,
                                           timeout=timeout)
            return res, True
        except Exception as e:
            log.info(f"upsert failed: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        # half of the data is upsert, the other half is insert
        rows = len(self.data)
        pk_old = [d[self.int64_field_name] for d in self.data[:rows // 2]]
        self.data = cf.gen_row_data_by_schema(nb=constants.DELTA_PER_INS, schema=self.get_schema())
        pk_new = [d[self.int64_field_name] for d in self.data[rows // 2:]]
        pk_update = pk_old + pk_new
        for i in range(rows):
            self.data[i][self.int64_field_name] = pk_update[i]
        res, result = self.upsert_entities()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP * 6)


class UpsertFreshnessChecker(Checker):
    """check upsert freshness operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, schema=None):
        self.term_expr = None
        self.latest_data = None
        if collection_name is None:
            collection_name = cf.gen_unique_str("UpsertChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        self.data = cf.gen_row_data_by_schema(nb=constants.DELTA_PER_INS, schema=self.get_schema())

    def upsert_entities(self):
        try:
            res = self.milvus_client.upsert(collection_name=self.c_name,
                                           data=self.data,
                                           timeout=timeout)
            return res, True
        except Exception as e:
            return str(e), False

    @trace()
    def upsert_freshness(self):
        while True:
            try:
                res = self.milvus_client.query(collection_name=self.c_name,
                                              filter=self.term_expr,
                                              output_fields=[f'{self.int64_field_name}'],
                                              timeout=timeout)
                result = True
            except Exception as e:
                res = str(e)
                result = False
                break
            if len(res) == 1 and res[0][f"{self.int64_field_name}"] == self.latest_data:
                break
        return res, result

    @exception_handler()
    def run_task(self):
        # half of the data is upsert, the other half is insert
        rows = len(self.data[0])
        pk_old = self.data[0][:rows // 2]
        self.data = cf.gen_row_data_by_schema(nb=constants.DELTA_PER_INS, schema=self.get_schema())
        pk_new = self.data[0][rows // 2:]
        pk_update = pk_old + pk_new
        self.data[0] = pk_update
        self.latest_data = self.data[0][-1]
        self.term_expr = f'{self.int64_field_name} == {self.latest_data}'
        res, result = self.upsert_entities()
        res, result = self.upsert_freshness()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP * 6)
    
class PartialUpdateChecker(Checker):
    """check partial update operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("PartialUpdateChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema, enable_struct_array_field=False)
        self.data = cf.gen_row_data_by_schema(nb=constants.DELTA_PER_INS, schema=self.get_schema())

    @trace()
    def partial_update_entities(self):
        try:

            res = self.milvus_client.upsert(collection_name=self.c_name,
                                           data=self.data,
                                           partial_update=True,
                                           timeout=timeout)
            return res, True
        except Exception as e:
            log.info(f"error {e}")
            return str(e), False

    @exception_handler()
    def run_task(self, count=0):

        schema = self.get_schema()
        pk_field_name = self.int64_field_name
        rows = len(self.data)

        # if count is even, use partial update; if count is odd, use full insert
        if count % 2 == 0:
            # Generate a fresh full batch (used for inserts and as a source of values)
            full_rows = cf.gen_row_data_by_schema(nb=rows, schema=schema)
            self.data = full_rows
        else:
            num_fields = len(schema["fields"])
            # Choose subset fields to update: always include PK + one non-PK field if available
            num = count % num_fields
            desired_fields = [pk_field_name, schema["fields"][num if num != 0 else 1]["name"]]
            partial_rows = cf.gen_row_data_by_schema(nb=rows, schema=schema,
                                                    desired_field_names=desired_fields)
            self.data = partial_rows
        res, result = self.partial_update_entities()
        return res, result

    def keep_running(self):
        count = 0
        while self._keep_running:
            self.run_task(count)
            count += 1
            sleep(constants.WAIT_PER_OP * 6)


class CollectionCreateChecker(Checker):
    """check collection create operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("CreateChecker_")
        super().__init__(collection_name=collection_name, schema=schema)

    @trace()
    def init_collection(self):
        try:
            collection_name = cf.gen_unique_str("CreateChecker_")
            schema = cf.gen_default_collection_schema()
            self.milvus_client.create_collection(collection_name=collection_name,
                                                schema=schema)
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.init_collection()
        # if result:
        #     # 50% chance to drop collection
        #     if random.randint(0, 1) == 0:
        #         self.c_wrap.drop(timeout=timeout)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class CollectionDropChecker(Checker):
    """check collection drop operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DropChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.collection_pool = []
        self.gen_collection_pool(schema=self.schema)

    def gen_collection_pool(self, pool_size=50, schema=None):
        for i in range(pool_size):
            collection_name = cf.gen_unique_str("DropChecker_")
            try:
                self.milvus_client.create_collection(collection_name=collection_name, schema=schema)
                self.collection_pool.append(collection_name)
            except Exception as e:
                log.error(f"Failed to create collection {collection_name}: {e}")

    @trace()
    def drop_collection(self):
        try:
            self.milvus_client.drop_collection(collection_name=self.c_name)
            if self.c_name in self.collection_pool:
                self.collection_pool.remove(self.c_name)
            return None, True
        except Exception as e:
            log.info(f"error while dropping collection {self.c_name}: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.drop_collection()
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
                    # Update current collection name to use from pool
                    self.c_name = c_name
                except Exception as e:
                    log.error(f"Failed to init new collection: {e}")
            sleep(constants.WAIT_PER_OP)


class PartitionCreateChecker(Checker):
    """check partition create operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None, partition_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("PartitionCreateChecker_")
        super().__init__(collection_name=collection_name, schema=schema, partition_name=partition_name)
        c_name = cf.gen_unique_str("PartitionDropChecker_")
        self.milvus_client.create_collection(collection_name=c_name, schema=self.schema)
        self.c_name = c_name
        log.info(f"collection {c_name} created")
        p_name = cf.gen_unique_str("PartitionDropChecker_")
        self.milvus_client.create_partition(collection_name=self.c_name, partition_name=p_name)
        self.p_name = p_name
        log.info(f"partition: {self.p_name}")

    @trace()
    def create_partition(self):
        try:
            partition_name = cf.gen_unique_str("PartitionCreateChecker_")
            self.milvus_client.create_partition(collection_name=self.c_name,
                                               partition_name=partition_name)
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.create_partition()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class PartitionDropChecker(Checker):
    """check partition drop operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None, partition_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("PartitionDropChecker_")
        super().__init__(collection_name=collection_name, schema=schema, partition_name=partition_name)
        c_name = cf.gen_unique_str("PartitionDropChecker_")
        self.milvus_client.create_collection(collection_name=c_name, schema=self.schema)
        self.c_name = c_name
        log.info(f"collection {c_name} created")
        p_name = cf.gen_unique_str("PartitionDropChecker_")
        self.milvus_client.create_partition(collection_name=self.c_name, partition_name=p_name)
        self.p_name = p_name
        log.info(f"partition: {self.p_name}")

    @trace()
    def drop_partition(self):
        try:
            self.milvus_client.drop_partition(collection_name=self.c_name, partition_name=self.p_name)
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.drop_partition()
        if result:
            # create two partition then drop one
            for i in range(2):
                p_name = cf.gen_unique_str("PartitionDropChecker_")
                self.milvus_client.create_partition(collection_name=self.c_name, partition_name=p_name)
                if i == 1:  # Keep track of the last partition to drop next time
                    self.p_name = p_name
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class DatabaseCreateChecker(Checker):
    """check create database operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DatabaseChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.db_name = None

    @trace()
    def init_db(self):
        db_name = cf.gen_unique_str("db_")
        try:
            self.milvus_client.create_database(db_name=db_name)
            self.db_name = db_name
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.init_db()
        if result:
            self.milvus_client.drop_database(db_name=self.db_name)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class DatabaseDropChecker(Checker):
    """check drop database operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DatabaseChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.db_name = cf.gen_unique_str("db_")
        self.milvus_client.create_database(db_name=self.db_name)

    @trace()
    def drop_db(self):
        try:
            self.milvus_client.drop_database(db_name=self.db_name)
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.drop_db()
        if result:
            self.db_name = cf.gen_unique_str("db_")
            self.milvus_client.create_database(db_name=self.db_name)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class IndexCreateChecker(Checker):
    """check index create operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("IndexChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        for i in range(5):
            self.milvus_client.insert(collection_name=self.c_name,
                                     data=cf.gen_row_data_by_schema(nb=constants.ENTITIES_FOR_SEARCH, schema=self.get_schema()),
                                     timeout=timeout)
        # do as a flush before indexing
        stats = self.milvus_client.get_collection_stats(collection_name=self.c_name)
        log.debug(f"Index ready entities: {stats.get('row_count', 0)}")

    @trace()
    def create_index(self):
        try:
            index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
            self.milvus_client.create_index(collection_name=self.c_name,
                                           index_params=index_params)
            return None, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        c_name = cf.gen_unique_str("IndexCreateChecker_")
        self.milvus_client.create_collection(collection_name=c_name, schema=self.schema)
        self.c_name = c_name
        res, result = self.create_index()
        if result:
            self.milvus_client.drop_index(collection_name=self.c_name, index_name="")
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP * 6)


class IndexDropChecker(Checker):
    """check index drop operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("IndexChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        for i in range(5):
            self.milvus_client.insert(collection_name=self.c_name, data=cf.gen_row_data_by_schema(nb=constants.ENTITIES_FOR_SEARCH, schema=self.get_schema()),
                               timeout=timeout)
        # do as a flush before indexing
        stats = self.milvus_client.get_collection_stats(collection_name=self.c_name)
        log.debug(f"Index ready entities: {stats.get('row_count', 0)}")

    @trace()
    def drop_index(self):
        try:
            res = self.milvus_client.drop_index(collection_name=self.c_name, index_name="")
            return res, True
        except Exception as e:
            log.info(f"drop_index error: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.drop_index()
        if result:
            self.milvus_client.create_collection(collection_name=cf.gen_unique_str("IndexDropChecker_"), schema=self.schema)
            index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
            self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.milvus_client.create_collection(collection_name=cf.gen_unique_str("IndexDropChecker_"), schema=self.schema)
            index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
            self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
            self.run_task()
            sleep(constants.WAIT_PER_OP * 6)


class QueryChecker(Checker):
    """check query operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("QueryChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
        self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        self.milvus_client.load_collection(collection_name=self.c_name, replica_number=replica_number)  # do load before query
        self.insert_data()
        self.term_expr = f"{self.int64_field_name} > 0"

    @trace()
    def query(self):
        try:
            res = self.milvus_client.query(collection_name=self.c_name, filter=self.term_expr, limit=5, timeout=query_timeout)
            return res, True
        except Exception as e:
            log.info(f"query error: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.query()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class TextMatchChecker(Checker):
    """check text match search operations with highlighter in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("TextMatchChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
        self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        self.milvus_client.load_collection(collection_name=self.c_name, replica_number=replica_number)
        self.insert_data()
        key_word = self.word_freq.most_common(1)[0][0]
        self.text_match_field_name = random.choice(self.text_match_field_name_list)
        self.key_word = key_word
        self.term_expr = f"TEXT_MATCH({self.text_match_field_name}, '{key_word}')"

    @trace()
    def text_match(self):
        # Create highlighter with query for text match
        highlighter = LexicalHighlighter(
            pre_tags=["<em>"],
            post_tags=["</em>"],
            highlight_search_text=False,
            highlight_query=[{"type": "TextMatch", "field": self.text_match_field_name, "text": self.key_word}]
        )
        try:
            res = self.milvus_client.search(
                collection_name=self.c_name,
                data=cf.gen_vectors(1, self.dim),
                anns_field=self.float_vector_field_name,
                search_params=constants.DEFAULT_SEARCH_PARAM,
                filter=self.term_expr,
                limit=5,
                output_fields=[self.text_match_field_name],
                timeout=search_timeout,
                highlighter=highlighter
            )
            return res, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        key_word = self.word_freq.most_common(1)[0][0]
        self.text_match_field_name = random.choice(self.text_match_field_name_list)
        self.key_word = key_word
        self.term_expr = f"TEXT_MATCH({self.text_match_field_name}, '{key_word}')"
        res, result = self.text_match()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class PhraseMatchChecker(Checker):
    """check phrase match query operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("PhraseMatchChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
        self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        self.milvus_client.load_collection(collection_name=self.c_name, replica_number=replica_number)  # do load before query
        self.insert_data()
        key_word_1 = self.word_freq.most_common(2)[0][0]
        key_word_2 = self.word_freq.most_common(2)[1][0]
        slop=5
        text_match_field_name = random.choice(self.text_match_field_name_list)
        self.term_expr = f"PHRASE_MATCH({text_match_field_name}, '{key_word_1} {key_word_2}', {slop})"

    @trace()
    def phrase_match(self):
        try:
            res = self.milvus_client.query(collection_name=self.c_name, filter=self.term_expr, limit=5, timeout=query_timeout)
            return res, True
        except Exception as e:
            log.info(f"phrase_match error: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        key_word_1 = self.word_freq.most_common(2)[0][0]
        key_word_2 = self.word_freq.most_common(2)[1][0]
        slop=5
        text_match_field_name = random.choice(self.text_match_field_name_list)
        self.term_expr = f"PHRASE_MATCH({text_match_field_name}, '{key_word_1} {key_word_2}', {slop})"
        res, result = self.phrase_match()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class JsonQueryChecker(Checker):
    """check json query operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("JsonQueryChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
        self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        self.milvus_client.load_collection(collection_name=self.c_name, replica_number=replica_number)  # do load before query
        self.insert_data()
        self.term_expr = self.get_term_expr()

    def get_term_expr(self):
        json_field_name = random.choice(self.json_field_names)
        fake = Faker()
        address_list = [fake.address() for _ in range(10)]
        name_list = [fake.name() for _ in range(10)]
        number_list = [random.randint(0, 100) for _ in range(10)]
        path = random.choice([ "name", "count"])
        path_value = {
            "address": address_list, # TODO not used in json query because of issue
            "name": name_list,
            "count": number_list
        }
        return f"{json_field_name}['{path}'] <= '{path_value[path][random.randint(0, len(path_value[path]) - 1)]}'"
        

    @trace()
    def json_query(self):
        try:
            res = self.milvus_client.query(collection_name=self.c_name, filter=self.term_expr, limit=5, timeout=query_timeout)
            return res, True
        except Exception as e:
            log.info(f"json_query error: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        self.term_expr = self.get_term_expr()
        res, result = self.json_query()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)

class GeoQueryChecker(Checker):
    """check geometry query operations in a dependent thread"""

    def __init__(self, collection_name=None, shards_num=2, replica_number=1, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("GeoQueryChecker_")
        super().__init__(collection_name=collection_name, shards_num=shards_num, schema=schema)
        index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
        self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        self.milvus_client.load_collection(collection_name=self.c_name, replica_number=replica_number)  # do load before query
        self.insert_data()
        self.term_expr = self.get_term_expr()

    def get_term_expr(self):
        geometry_field_name = random.choice(self.geometry_field_names)
        query_polygon = "POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))"
        return f"ST_WITHIN({geometry_field_name}, '{query_polygon}')"
        

    @trace()
    def geo_query(self):
        try:
            res = self.milvus_client.query(collection_name=self.c_name, filter=self.term_expr, limit=5, timeout=query_timeout)
            return res, True
        except Exception as e:
            log.info(f"geo_query error: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        self.term_expr = self.get_term_expr()
        res, result = self.geo_query()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class DeleteChecker(Checker):
    """check delete operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None, shards_num=2):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DeleteChecker_")
        super().__init__(collection_name=collection_name, schema=schema, shards_num=shards_num)
        index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
        self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        self.milvus_client.load_collection(collection_name=self.c_name)  # load before query
        self.insert_data()
        query_expr = f'{self.int64_field_name} > 0'
        res = self.milvus_client.query(collection_name=self.c_name, filter=query_expr, output_fields=[self.int64_field_name], partition_name=self.p_name)
        self.ids = [r[self.int64_field_name] for r in res]
        self.query_expr = query_expr
        delete_ids = self.ids[:len(self.ids) // 2]  # delete half of ids
        self.delete_expr = f'{self.int64_field_name} in {delete_ids}'

    def update_delete_expr(self):
        res = self.milvus_client.query(collection_name=self.c_name, filter=self.query_expr, output_fields=[self.int64_field_name], partition_name=self.p_name)
        all_ids = [r[self.int64_field_name] for r in res]
        if len(all_ids) < 100:
            # insert data to make sure there are enough ids to delete
            self.insert_data(nb=10000)
            res = self.milvus_client.query(collection_name=self.c_name, filter=self.query_expr, output_fields=[self.int64_field_name], partition_name=self.p_name)
            all_ids = [r[self.int64_field_name] for r in res]
        delete_ids = all_ids[:3000]  # delete 3000 ids
        self.delete_expr = f'{self.int64_field_name} in {delete_ids}'

    @trace()
    def delete_entities(self):
        try:
            res = self.milvus_client.delete(collection_name=self.c_name, filter=self.delete_expr, timeout=timeout, partition_name=self.p_name)
            return res, True
        except Exception as e:
            log.info(f"delete_entities error: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        self.update_delete_expr()
        res, result = self.delete_entities()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class DeleteFreshnessChecker(Checker):
    """check delete freshness operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("DeleteChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
        self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        self.milvus_client.load_collection(collection_name=self.c_name)  # load before query
        self.insert_data()
        query_expr = f'{self.int64_field_name} > 0'
        res = self.milvus_client.query(collection_name=self.c_name, filter=query_expr, output_fields=[self.int64_field_name], partition_name=self.p_name)
        self.ids = [r[self.int64_field_name] for r in res]
        self.query_expr = query_expr
        delete_ids = self.ids[:len(self.ids) // 2]  # delete half of ids
        self.delete_expr = f'{self.int64_field_name} in {delete_ids}'

    def update_delete_expr(self):
        res = self.milvus_client.query(collection_name=self.c_name, filter=self.query_expr, output_fields=[self.int64_field_name], partition_name=self.p_name)
        all_ids = [r[self.int64_field_name] for r in res]
        if len(all_ids) < 100:
            # insert data to make sure there are enough ids to delete
            self.insert_data(nb=10000)
            res = self.milvus_client.query(collection_name=self.c_name, filter=self.query_expr, output_fields=[self.int64_field_name], partition_name=self.p_name)
            all_ids = [r[self.int64_field_name] for r in res]
        delete_ids = all_ids[:len(all_ids) // 2]  # delete half of ids
        self.delete_expr = f'{self.int64_field_name} in {delete_ids}'

    def delete_entities(self):
        try:
            res = self.milvus_client.delete(collection_name=self.c_name, filter=self.delete_expr, timeout=timeout, partition_name=self.p_name)
            return res, True
        except Exception as e:
            log.info(f"delete_entities error: {e}")
            return str(e), False

    @trace()
    def delete_freshness(self):
        try:
            while True:
                res = self.milvus_client.query(collection_name=self.c_name, filter=self.delete_expr, output_fields=[f'{self.int64_field_name}'], timeout=timeout)
                if len(res) == 0:
                    break
            return res, True
        except Exception as e:
            log.info(f"delete_freshness error: {e}")
            return str(e), False

    @exception_handler()
    def run_task(self):
        self.update_delete_expr()
        res, result = self.delete_entities()
        res, result = self.delete_freshness()

        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class CompactChecker(Checker):
    """check compact operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("CompactChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
        self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        self.milvus_client.load_collection(collection_name=self.c_name)  # load before compact

    @trace()
    def compact(self):
        from pymilvus import Collection
        collection = Collection(name=self.c_name, using=self.alias)
        res = collection.compact(timeout=timeout)
        collection.wait_for_compaction_completed()
        collection.get_compaction_plans()
        return res, True

    @exception_handler()
    def run_task(self):
        res, result = self.compact()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class LoadBalanceChecker(Checker):
    """check load balance operations in a dependent thread"""

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("LoadBalanceChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        index_params = create_index_params_from_dict(self.float_vector_field_name, constants.DEFAULT_INDEX_PARAM)
        self.milvus_client.create_index(collection_name=self.c_name, index_params=index_params)
        self.milvus_client.load_collection(collection_name=self.c_name)
        self.sealed_segment_ids = None
        self.dst_node_ids = None
        self.src_node_id = None

    @trace()
    def load_balance(self):
        from pymilvus import utility
        res = utility.load_balance(collection_name=self.c_name, src_node_id=self.src_node_id, dst_node_ids=self.dst_node_ids, sealed_segment_ids=self.sealed_segment_ids, using=self.alias)
        return res, True

    def prepare(self):
        """prepare load balance params"""
        from pymilvus import Collection
        collection = Collection(name=self.c_name, using=self.alias)
        res = collection.get_replicas()
        # find a group which has multi nodes
        group_nodes = []
        for g in res.groups:
            if len(g.group_nodes) >= 2:
                group_nodes = list(g.group_nodes)
                break
        self.src_node_id = group_nodes[0]
        self.dst_node_ids = group_nodes[1:]
        from pymilvus import utility
        res = utility.get_query_segment_info(self.c_name, using=self.alias)
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
    """check bulk insert operations in a dependent thread"""

    def __init__(self, collection_name=None, files=[], use_one_collection=False, dim=ct.default_dim,
                 schema=None, insert_data=False, minio_endpoint=None, bucket_name=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("BulkInsertChecker_")
        super().__init__(collection_name=collection_name, dim=dim, schema=schema, insert_data=insert_data)
        self.schema = cf.gen_bulk_insert_collection_schema() if schema is None else schema
        self.files = files
        self.recheck_failed_task = False
        self.failed_tasks = []
        self.failed_tasks_id = []
        self.use_one_collection = use_one_collection  # if True, all tasks will use one collection to bulk insert
        self.c_name = collection_name
        self.minio_endpoint = minio_endpoint
        self.bucket_name = bucket_name

    def prepare(self, data_size=100000):
        with RemoteBulkWriter(
                schema=self.schema,
                file_type=BulkFileType.NUMPY,
                remote_path="bulk_data",
                connect_param=RemoteBulkWriter.ConnectParam(
                    endpoint=self.minio_endpoint,
                    access_key="minioadmin",
                    secret_key="minioadmin",
                    bucket_name=self.bucket_name
                )
        ) as remote_writer:

            for _ in range(data_size):
                row = cf.gen_row_data_by_schema(nb=1, schema=self.get_schema())[0]
                remote_writer.append_row(row)
            remote_writer.commit()
            batch_files = remote_writer.batch_files
            log.info(f"batch files: {batch_files}")
            self.files = batch_files[0]

    def update(self, files=None, schema=None):
        if files is not None:
            self.files = files
        if schema is not None:
            self.schema = schema

    def get_bulk_insert_task_state(self):
        from pymilvus import utility
        state_map = {}
        for task_id in self.failed_tasks_id:
            state = utility.get_bulk_insert_state(task_id=task_id, using=self.alias)
            state_map[task_id] = state
        return state_map

    @trace()
    def bulk_insert(self):
        log.info(f"bulk insert collection name: {self.c_name}")
        from pymilvus import utility
        task_ids = utility.do_bulk_insert(collection_name=self.c_name, files=self.files, using=self.alias)
        log.info(f"task ids {task_ids}")
        completed = utility.wait_for_bulk_insert_tasks_completed(task_ids=[task_ids], timeout=720, using=self.alias)
        return task_ids, completed

    @exception_handler()
    def run_task(self):
        if not self.use_one_collection:
            if self.recheck_failed_task and self.failed_tasks:
                self.c_name = self.failed_tasks.pop(0)
                log.debug(f"check failed task: {self.c_name}")
            else:
                self.c_name = cf.gen_unique_str("BulkInsertChecker_")
        self.milvus_client.create_collection(collection_name=self.c_name, schema=self.schema)
        log.info(f"collection schema: {self.milvus_client.describe_collection(self.c_name)}")
        # bulk insert data
        num_entities = self.milvus_client.get_collection_stats(collection_name=self.c_name).get("row_count", 0)
        log.info(f"before bulk insert, collection {self.c_name} has num entities {num_entities}")
        task_ids, completed = self.bulk_insert()
        num_entities = self.milvus_client.get_collection_stats(collection_name=self.c_name).get("row_count", 0)
        log.info(f"after bulk insert, collection {self.c_name} has num entities {num_entities}")
        if not completed:
            self.failed_tasks.append(self.c_name)
            self.failed_tasks_id.append(task_ids)
        return task_ids, completed

    def keep_running(self):
        self.prepare()
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP / 10)


class AlterCollectionChecker(Checker):
    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("AlterCollectionChecker")
        super().__init__(collection_name=collection_name, schema=schema, enable_dynamic_field=False)
        self.milvus_client.release_collection(collection_name=self.c_name)
        res = self.milvus_client.describe_collection(collection_name=self.c_name)
        log.info(f"before alter collection {self.c_name} schema: {res}")
        # alter collection attributes
        self.milvus_client.alter_collection_properties(collection_name=self.c_name,
                                                       properties={"mmap.enabled": True})
        self.milvus_client.alter_collection_properties(collection_name=self.c_name,
                                                       properties={"collection.ttl.seconds": 3600})
        self.milvus_client.alter_collection_properties(collection_name=self.c_name, 
                                                       properties={"dynamicfield.enabled": True})
        res = self.milvus_client.describe_collection(collection_name=self.c_name)
        log.info(f"after alter collection {self.c_name} schema: {res}")
        
    @trace()
    def alter_check(self):
        try:
            res = self.milvus_client.describe_collection(collection_name=self.c_name)
            properties = res.get("properties", {})
            if properties.get("mmap.enabled") != "True":
                return res, False
            if properties.get("collection.ttl.seconds") != "3600":
                return res, False
            if res["enable_dynamic_field"] != True:
                return res, False
            return res, True
        except Exception as e:
            return str(e), False

    @exception_handler()
    def run_task(self):
        res, result = self.alter_check()
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP)


class SnapshotChecker(Checker):
    """Check snapshot create/restore operations succeed on a shared collection.

    This is a lightweight checker that only verifies snapshot operations complete
    successfully, without checking data correctness. It can safely share a
    collection with other checkers since it does not depend on data consistency.

    Each cycle: create snapshot -> restore to new collection -> wait for completion -> cleanup
    """

    def __init__(self, collection_name=None, schema=None):
        if collection_name is None:
            collection_name = cf.gen_unique_str("SnapshotChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.snapshot_name = None
        self.restored_collection = None

    @trace()
    def snapshot(self):
        try:
            # 1. Create snapshot
            self.snapshot_name = cf.gen_unique_str("snapshot_")
            self.milvus_client.create_snapshot(self.c_name, self.snapshot_name)
            log.info(f"[SnapshotChecker] Created snapshot {self.snapshot_name} for {self.c_name}")

            # 2. Restore to new collection
            self.restored_collection = cf.gen_unique_str("restored_")
            job_id = self.milvus_client.restore_snapshot(
                self.snapshot_name, self.restored_collection
            )
            log.info(f"[SnapshotChecker] Started restore job {job_id}")

            # 3. Wait for restore completion
            start_time = time.time()
            restore_timeout = 300
            while time.time() - start_time < restore_timeout:
                state = self.milvus_client.get_restore_snapshot_state(job_id)
                log.debug(f"[SnapshotChecker] Restore state: {state.state}")
                if state.state == "RestoreSnapshotCompleted":
                    log.info(f"[SnapshotChecker] Restore completed in {time.time()-start_time:.1f}s")
                    return None, True
                if state.state == "RestoreSnapshotFailed":
                    return f"Restore failed: {state.reason}", False
                time.sleep(2)

            return f"Restore timeout after {restore_timeout}s", False

        except Exception as e:
            log.error(f"[SnapshotChecker] Snapshot failed: {e}")
            return str(e), False
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            if self.restored_collection:
                self.milvus_client.drop_collection(self.restored_collection)
                log.debug(f"[SnapshotChecker] Dropped restored collection {self.restored_collection}")
                self.restored_collection = None
        except Exception as e:
            log.warning(f"[SnapshotChecker] Failed to drop restored collection: {e}")
        try:
            if self.snapshot_name:
                self.milvus_client.drop_snapshot(self.snapshot_name)
                log.debug(f"[SnapshotChecker] Dropped snapshot {self.snapshot_name}")
                self.snapshot_name = None
        except Exception as e:
            log.warning(f"[SnapshotChecker] Failed to drop snapshot: {e}")

    @exception_handler()
    def run_task(self):
        return self.snapshot()

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP * 3)


class SnapshotRestoreChecker(Checker):
    """Check snapshot restore with data verification using an independent collection.

    This checker uses its own dedicated collection (not shared with other checkers)
    and performs all DML operations internally, so no external locking is needed.

    Each cycle:
    1. Performs DML operations (insert/upsert/delete) on its own collection
    2. Flushes and captures state
    3. Creates snapshot and restores to a new collection
    4. Verifies data correctness after restore
    5. Cleans up snapshot and restored collection
    """

    def __init__(self, collection_name=None, schema=None):
        # Always use a dedicated collection for snapshot testing
        if collection_name is None:
            collection_name = cf.gen_unique_str("SnapshotRestoreChecker_")
        super().__init__(collection_name=collection_name, schema=schema)
        self.snapshot_name = None
        self.restored_collection = None
        self.snapshot_row_count = 0
        self.snapshot_sample_pks = []

    def _do_insert(self, nb=100):
        """Insert rows into the checker's own collection."""
        data = cf.gen_row_data_by_schema(nb=nb, schema=self.get_schema())
        for i, d in enumerate(data):
            pk = int(time.time() * 1000000) + i
            d[self.int64_field_name] = pk
        self.milvus_client.insert(self.c_name, data)
        log.debug(f"[SnapshotRestoreChecker] Inserted {nb} rows")

    def _do_upsert(self, nb=10):
        """Upsert rows in the checker's own collection."""
        res = self.milvus_client.query(
            collection_name=self.c_name,
            filter=f"{self.int64_field_name} >= 0",
            output_fields=[self.int64_field_name],
            limit=nb
        )
        if not res:
            return
        pks = [r[self.int64_field_name] for r in res]
        data = cf.gen_row_data_by_schema(nb=len(pks), schema=self.get_schema())
        for i, d in enumerate(data):
            d[self.int64_field_name] = pks[i]
        self.milvus_client.upsert(self.c_name, data)
        log.debug(f"[SnapshotRestoreChecker] Upserted {len(pks)} rows")

    def _do_delete(self, nb=5):
        """Delete rows from the checker's own collection, keeping at least 100 rows."""
        count_res = self.milvus_client.query(
            collection_name=self.c_name,
            filter="",
            output_fields=["count(*)"]
        )
        row_count = count_res[0]["count(*)"] if count_res else 0
        if row_count <= 100:
            return
        res = self.milvus_client.query(
            collection_name=self.c_name,
            filter=f"{self.int64_field_name} >= 0",
            output_fields=[self.int64_field_name],
            limit=nb
        )
        if not res:
            return
        pks_to_delete = [r[self.int64_field_name] for r in res]
        filter_expr = f"{self.int64_field_name} in {pks_to_delete}"
        self.milvus_client.delete(self.c_name, filter=filter_expr)
        log.debug(f"[SnapshotRestoreChecker] Deleted {len(pks_to_delete)} rows")

    def _do_dml_operations(self):
        """Execute a random DML operation on the checker's own collection."""
        op = random.choice(['insert', 'upsert', 'delete'])
        try:
            if op == 'insert':
                self._do_insert(nb=10)
            elif op == 'upsert':
                self._do_upsert(nb=5)
            elif op == 'delete':
                self._do_delete(nb=5)
        except Exception as e:
            log.warning(f"[SnapshotRestoreChecker] DML operation {op} failed: {e}")

    def _capture_snapshot_state(self):
        """Capture current collection state after flush."""
        try:
            res = self.milvus_client.query(
                collection_name=self.c_name,
                filter="",
                output_fields=["count(*)"],
                consistency_level="Strong"
            )
            self.snapshot_row_count = res[0]["count(*)"] if res else 0

            if self.snapshot_row_count > 0:
                sample_size = min(50, self.snapshot_row_count)
                res = self.milvus_client.query(
                    collection_name=self.c_name,
                    filter=f"{self.int64_field_name} >= 0",
                    output_fields=[self.int64_field_name],
                    limit=sample_size,
                    consistency_level="Strong"
                )
                self.snapshot_sample_pks = [r[self.int64_field_name] for r in res]
            else:
                self.snapshot_sample_pks = []

            log.info(f"[SnapshotRestoreChecker] Captured snapshot state: row_count={self.snapshot_row_count}, sample_pks={len(self.snapshot_sample_pks)}")
        except Exception as e:
            log.warning(f"Failed to capture snapshot state: {e}")
            self.snapshot_row_count = 0
            self.snapshot_sample_pks = []

    def _verify_restored_data(self, restored_name):
        """Verify data correctness after restore."""
        try:
            self.milvus_client.load_collection(restored_name)
        except Exception as e:
            log.warning(f"Failed to load restored collection: {e}")
            return False, f"Failed to load restored collection: {e}"

        try:
            res = self.milvus_client.query(
                collection_name=restored_name,
                filter="",
                output_fields=["count(*)"],
                consistency_level="Strong"
            )
            actual_count = res[0]["count(*)"] if res else 0

            log.info(f"[SnapshotRestoreChecker] Verify restored data: expected={self.snapshot_row_count}, actual={actual_count}")

            if actual_count != self.snapshot_row_count:
                return False, f"Row count mismatch: expected {self.snapshot_row_count}, got {actual_count}"

            if self.snapshot_sample_pks:
                filter_expr = f"{self.int64_field_name} in {self.snapshot_sample_pks}"
                res = self.milvus_client.query(
                    collection_name=restored_name,
                    filter=filter_expr,
                    output_fields=[self.int64_field_name],
                    consistency_level="Strong"
                )
                found_pks = {r[self.int64_field_name] for r in res}
                expected_pks = set(self.snapshot_sample_pks)

                if found_pks != expected_pks:
                    missing = expected_pks - found_pks
                    return False, f"Missing PKs after restore: {missing}"

            return True, f"Data verified: row_count={actual_count}, sample_pks={len(self.snapshot_sample_pks)}"
        except Exception as e:
            return False, f"Verification failed: {e}"

    @trace()
    def restore_snapshot(self):
        try:
            # 1. Execute DML operations to modify collection state
            for _ in range(3):
                self._do_dml_operations()
                time.sleep(0.1)

            # 2. Flush and capture state (no lock needed - this is our own collection)
            self.milvus_client.flush(collection_name=self.c_name)
            log.debug(f"Flushed collection {self.c_name}")
            time.sleep(1)

            self._capture_snapshot_state()
            row_count_before = self.snapshot_row_count
            log.info(f"State before snapshot: row_count={row_count_before}, sample_pks={len(self.snapshot_sample_pks)}")

            # 3. Create snapshot
            self.snapshot_name = cf.gen_unique_str("snapshot_")
            self.milvus_client.create_snapshot(self.c_name, self.snapshot_name)
            log.info(f"Created snapshot {self.snapshot_name} for collection {self.c_name}")

            # 4. Restore to new collection
            self.restored_collection = cf.gen_unique_str("restored_")
            job_id = self.milvus_client.restore_snapshot(
                self.snapshot_name, self.restored_collection
            )
            log.info(f"Started restore job {job_id} to collection {self.restored_collection}")

            # 5. Wait for restore completion
            start_time = time.time()
            restore_timeout = 300
            while time.time() - start_time < restore_timeout:
                state = self.milvus_client.get_restore_snapshot_state(job_id)
                log.debug(f"Restore state: {state.state}")
                if state.state == "RestoreSnapshotCompleted":
                    log.info(f"Restore job {job_id} completed in {time.time()-start_time:.1f}s")
                    break
                if state.state == "RestoreSnapshotFailed":
                    return f"Restore failed: {state.reason}", False
                time.sleep(2)
            else:
                return f"Restore timeout after {restore_timeout}s", False

            # 6. Verify data correctness
            verified, msg = self._verify_restored_data(self.restored_collection)
            if not verified:
                return msg, False

            log.info(f"Snapshot restore verified successfully: {msg}")
            return None, True

        except Exception as e:
            log.error(f"Snapshot restore failed: {e}")
            return str(e), False
        finally:
            self._cleanup()

    def _cleanup(self):
        """Cleanup snapshot and restored collection."""
        try:
            if self.restored_collection:
                self.milvus_client.drop_collection(self.restored_collection)
                log.debug(f"Dropped restored collection {self.restored_collection}")
                self.restored_collection = None
        except Exception as e:
            log.warning(f"Failed to drop restored collection: {e}")

        try:
            if self.snapshot_name:
                self.milvus_client.drop_snapshot(self.snapshot_name)
                log.debug(f"Dropped snapshot {self.snapshot_name}")
                self.snapshot_name = None
        except Exception as e:
            log.warning(f"Failed to drop snapshot: {e}")

    @exception_handler()
    def run_task(self):
        return self.restore_snapshot()

    def keep_running(self):
        while self._keep_running:
            self.run_task()
            sleep(constants.WAIT_PER_OP * 3)


class TestResultAnalyzer(unittest.TestCase):
    def test_get_stage_success_rate(self):
        ra = ResultAnalyzer()
        res = ra.get_stage_success_rate()
        print(res)


if __name__ == '__main__':
    unittest.main()
