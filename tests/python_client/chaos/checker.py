from enum import Enum
from random import randint

from time import sleep
from base.collection_wrapper import ApiCollectionWrapper
from common import common_func as cf
from common import common_type as ct
import constants

from common.common_type import CheckTasks
from utils.util_log import test_log as log


class Op(Enum):
    create = 'create'
    insert = 'insert'
    flush = 'flush'
    index = 'index'
    search = 'search'
    query = 'query'

    unknown = 'unknown'


timeout = 20


class Checker:
    def __init__(self):
        self._succ = 0
        self._fail = 0
        self._running = True
        self.c_wrap = ApiCollectionWrapper()
        self.c_wrap.init_collection(name=cf.gen_unique_str('Checker_'),
                                    schema=cf.gen_default_collection_schema(),
                                    timeout=timeout)
        self.c_wrap.insert(data=cf.gen_default_list_data(nb=constants.ENTITIES_FOR_SEARCH),
                           timeout=timeout)
        self.initial_entities = self.c_wrap.num_entities    # do as a flush

    def total(self):
        return self._succ + self._fail

    def succ_rate(self):
        return self._succ / self.total() if self.total() != 0 else 0

    def terminate(self):
        self._running = False

    def reset(self):
        self._succ = 0
        self._fail = 0


class SearchChecker(Checker):
    def __init__(self):
        super().__init__()
        self.c_wrap.load()   # do load before search

    def keep_running(self):
        while self._running is True:
            search_vec = cf.gen_vectors(5, ct.default_dim)
            _, result = self.c_wrap.search(
                                data=search_vec,
                                anns_field=ct.default_float_vec_field_name,
                                param={"nprobe": 32},
                                limit=1, timeout=timeout, check_task=CheckTasks.check_nothing
                            )
            if result:
                self._succ += 1
            else:
                self._fail += 1
            sleep(constants.WAIT_PER_OP / 10)


class InsertFlushChecker(Checker):
    def __init__(self, flush=False):
        super().__init__()
        self._flush = flush
        self.initial_entities = self.c_wrap.num_entities

    def keep_running(self):
        while self._running:
            _, insert_result = \
                self.c_wrap.insert(data=cf.gen_default_list_data(nb=constants.DELTA_PER_INS),
                                   timeout=timeout, check_task=CheckTasks.check_nothing)
            if not self._flush:
                if insert_result:
                    self._succ += 1
                else:
                    self._fail += 1
                sleep(constants.WAIT_PER_OP / 10)
            else:
                if self.c_wrap.num_entities == (self.initial_entities + constants.DELTA_PER_INS):
                    self._succ += 1
                    self.initial_entities += constants.DELTA_PER_INS
                else:
                    self._fail += 1


class CreateChecker(Checker):
    def __init__(self):
        super().__init__()

    def keep_running(self):
        while self._running is True:
            _, result = self.c_wrap.init_collection(
                                    name=cf.gen_unique_str("CreateChecker_"),
                                    schema=cf.gen_default_collection_schema(),
                                    timeout=timeout, check_task=CheckTasks.check_nothing
                                )
            if result:
                self._succ += 1
                self.c_wrap.drop(timeout=timeout)
            else:
                self._fail += 1
            sleep(constants.WAIT_PER_OP / 10)


class IndexChecker(Checker):
    def __init__(self):
        super().__init__()
        self.c_wrap.insert(data=cf.gen_default_list_data(nb=5*constants.ENTITIES_FOR_SEARCH),
                           timeout=timeout)
        log.debug(f"Index ready entities: {self.c_wrap.num_entities }")  # do as a flush before indexing

    def keep_running(self):
        while self._running:
            _, result = self.c_wrap.create_index(ct.default_float_vec_field_name,
                                                 constants.DEFAULT_INDEX_PARAM,
                                                 name=cf.gen_unique_str('index_'),
                                                 timeout=timeout, check_task=CheckTasks.check_nothing)
            if result:
                self._succ += 1
                self.c_wrap.drop_index(timeout=timeout)
            else:
                self._fail += 1


class QueryChecker(Checker):
    def __init__(self):
        super().__init__()
        self.c_wrap.load()      # load before query

    def keep_running(self):
        while self._running:
            int_values = []
            for _ in range(5):
                int_values.append(randint(0, constants.ENTITIES_FOR_SEARCH))
            term_expr = f'{ct.default_int64_field_name} in {int_values}'
            _, result = self.c_wrap.query(term_expr, timeout=timeout, check_task=CheckTasks.check_nothing)
            if result:
                self._succ += 1
            else:
                self._fail += 1
            sleep(constants.WAIT_PER_OP / 10)
