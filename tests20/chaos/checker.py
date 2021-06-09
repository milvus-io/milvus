import sys
import threading

from time import sleep
from common import common_func as cf
from common import common_type as ct

nums = 0


class Checker:
    def __init__(self):
        self._succ = 0
        self._fail = 0
        self._running = True

    def total(self):
        return self._succ + self._fail

    def statics(self):
        return self._succ / self.total() if self.total() != 0 else 0

    def terminate(self):
        self._running = False

    def reset(self):
        self._succ = 0
        self._fail = 0


class SearchChecker(Checker):
    def __init__(self, collection_wrapper):
        super().__init__()
        self.c_wrapper = collection_wrapper

    def keep_running(self):
        while self._running is True:
            search_vec = cf.gen_vectors(5, ct.default_dim)
            _, result = self.c_wrapper.search(
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
    def __init__(self, collection_wrapper):
        super().__init__()
        self._flush_succ = 0
        self._flush_fail = 0
        self.c_wrapper = collection_wrapper

    def keep_running(self):
        while self._running is True:
            sleep(1)
            _, insert_result = self.c_wrapper.insert(
                                    data=cf.gen_default_list_data(nb=ct.default_nb),
                                    check_task="nothing")

            if insert_result is True:
                self._succ += 1
                num_entities = self.c_wrapper.num_entities
                self.connection.flush([self.c_wrapper.collection.name])
                if self.c_wrapper.num_entities == (num_entities + ct.default_nb):
                    self._flush_succ += 1
                else:
                    self._flush_fail += 1
            else:
                self._fail += 1
                self._flush_fail += 1

    def insert_statics(self):
        return self.statics()

    def flush_statics(self):
        return self._flush_succ / self.total() if self.total() != 0 else 0

    def reset(self):
        self._succ = 0
        self._fail = 0
        self._flush_succ = 0
        self._flush_fail = 0


class CreateChecker(Checker):
    def __init__(self, collection_wrapper):
        super().__init__()
        self.c_wrapper = collection_wrapper
        self.num = 0

    def keep_running(self):
        while self._running is True:
            collection, result = self.c_wrapper.init_collection(
                                    name=cf.gen_unique_str(),
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

