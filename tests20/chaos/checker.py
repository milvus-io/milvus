import sys
import threading
from enum import Enum

from time import sleep
from base.collection_wrapper import ApiCollectionWrapper
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log


class Op(Enum):
    create = 'create'
    insert_n_flush = 'insert_n_flush'
    index = 'index'
    search = 'search'
    query = 'query'

    unknown = 'unknown'


class Checker:
    def __init__(self):
        self._succ = 0
        self._fail = 0
        self._running = True

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
    def __init__(self, collection_wrap):
        super().__init__()
        self.c_wrap = collection_wrap

    def keep_running(self):
        while self._running is True:
            search_vec = cf.gen_vectors(5, ct.default_dim)
            _, result = self.c_wrap.search(
                                data=search_vec,
                                params={"nprobe": 32},
                                limit=1,
                                check_task="nothing"
                            )
            if result is True:
                self._succ += 1
            else:
                self._fail += 1


class InsertAndFlushChecker(Checker):
    def __init__(self, connection, collection_wrap):
        super().__init__()
        self._flush_succ = 0
        self._flush_fail = 0
        self.conn = connection
        self.c_wrap = collection_wrap

    def insert_succ_rate(self):
        return self._succ / self.total() if self.total() != 0 else 0

    def flush_succ_rate(self):
        return self._flush_succ / self.flush_total() if self.flush_total() != 0 else 0

    def flush_total(self):
        return self._flush_succ + self._flush_fail

    def reset(self):
        self._succ = 0
        self._fail = 0
        self._flush_succ = 0
        self._flush_fail = 0

    def keep_running(self):
        while self._running is True:
            _, insert_result = self.c_wrap.insert(
                                    data=cf.gen_default_dataframe_data(nb=1)
                                    )
            if insert_result is True:
                self._succ += 1
                entities_1 = self.c_wrap.num_entities
                self.conn.flush([self.c_wrap.name])
                entities_2 = self.c_wrap.num_entities
                log.debug("Before flush: %d, After flush %d" % (entities_1, entities_2))
                if entities_2 == (entities_1 + 1):
                    self._flush_succ += 1
                    log.debug("flush succ")
                else:
                    self._flush_fail += 1
                    log.debug("flush fail")
            else:
                self._fail += 1
                self._flush_fail += 1


class CreateChecker(Checker):
    def __init__(self):
        super().__init__()
        self.c_wrapper = ApiCollectionWrapper()

    def keep_running(self):
        while self._running is True:
            sleep(2)
            collection, result = self.c_wrapper.init_collection(
                                    name=cf.gen_unique_str("CreateChecker_"),
                                    schema=cf.gen_default_collection_schema(),
                                    check_task="check_nothing"
                                )
            if result is True:
                self._succ += 1
                self.c_wrapper.drop(check_task="check_nothing")
            else:
                self._fail += 1


class IndexChecker(Checker):
    def __init__(self):
        super().__init__()

    def keep_running(self):
        pass


class QueryChecker(Checker):
    def __init__(self):
        super().__init__()

    def keep_running(self):
        pass


class FlushChecker(Checker):
    def __init__(self):
        super().__init__()

    def keep_running(self):
        pass

