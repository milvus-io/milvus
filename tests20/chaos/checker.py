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

    def keep_searching(self):
        while self._running is True:
            search_vec = cf.gen_vectors(5, ct.default_dim)
            _, result = self.c_wrapper.search(
                data=search_vec,
                params={"nprobe": 32},
                limit=1,
                check_res="nothing"
            )
            if result is True:
                self._succ += 1
            else:
                self._fail += 1


class InsertChecker(Checker):
    def __init__(self, collection_wrapper):
        super().__init__()
        self.c_wrapper = collection_wrapper

    def keep_inserting(self):
        while self._running is True:
            sleep(1)
            _, result = self.c_wrapper.insert(data=cf.gen_default_list_data(),
                                              check_res="nothing")
            if result is True:
                self._succ += 1
            else:
                self._fail += 1


class CreateChecker(Checker):
    def __init__(self, collection_wrapper):
        super().__init__()
        self.c_wrapper = collection_wrapper
        self.num = 0

    def keep_creating(self):
        while self._running is True:
            collection, result = self.c_wrapper.collection_init(name=cf.gen_unique_str(),
                                                       schema=cf.gen_default_collection_schema(),
                                                       check_res="check_nothing")
            if result is True:
                self._succ += 1
                self.c_wrapper.drop(check_res="check_nothing")
            else:
                self._fail += 1


class IndexChecker(Checker):
    def __init__(self):
        super().__init__()

    def keep_indexing(self):
        pass


class DropChecker(Checker):
    def __init__(self):
        super().__init__()

    def keep_dropping(self):
        pass


class FlushChecker(Checker):
    def __init__(self):
        super().__init__()

    def keep_flushing(self):
        pass

