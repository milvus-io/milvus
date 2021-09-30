from pymilvus import utility
import sys

sys.path.append("..")
from check.func_check import ResponseChecker
from utils.api_request import api_request


TIMEOUT = 20


class ApiUtilityWrapper:
    """ Method of encapsulating utility files """

    ut = utility

    def loading_progress(self, collection_name, partition_names=None,
                         using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.loading_progress, collection_name, partition_names, using])
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, is_succ, collection_name=collection_name,
                                       partition_names=partition_names, using=using).run()
        return res, check_result

    def wait_for_loading_complete(self, collection_name, partition_names=None, timeout=None, using="default",
                                  check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.wait_for_loading_complete, collection_name,
                                    partition_names, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, partition_names=partition_names,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def index_building_progress(self, collection_name, index_name="", using="default",
                                check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.index_building_progress, collection_name, index_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, index_name=index_name,
                                       using=using).run()
        return res, check_result

    def wait_for_index_building_complete(self, collection_name, index_name="", timeout=None, using="default",
                                         check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.wait_for_index_building_complete, collection_name,
                                    index_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, index_name=index_name,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def has_collection(self, collection_name, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.has_collection, collection_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, using=using).run()
        return res, check_result

    def has_partition(self, collection_name, partition_name, using="default",
                      check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.has_partition, collection_name, partition_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name,
                                       partition_name=partition_name, using=using).run()
        return res, check_result

    def drop_collection(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.drop_collection, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def list_collections(self, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.list_collections, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def calc_distance(self, vectors_left, vectors_right, params=None, timeout=None,
                      using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.calc_distance, vectors_left, vectors_right,
                                    params, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result
