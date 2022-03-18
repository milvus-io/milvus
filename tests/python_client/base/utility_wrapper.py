from datetime import datetime

from pymilvus import utility
import sys

sys.path.append("..")
from check.func_check import ResponseChecker
from utils.api_request import api_request


TIMEOUT = 20


class ApiUtilityWrapper:
    """ Method of encapsulating utility files """

    ut = utility

    # def import_data(self, collection_name, files, partition_name=None, options=None, using="default",
    #                 check_task=None, check_items=None):
    #     func_name = sys._getframe().f_code.co_name
    #     res, is_succ = api_request([self.ut.import_data, collection_name, files,
    #                                 partition_name, options, using])
    #     check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
    #                                    collection_name=collection_name, using=using).run()
    #     return res, check_result
    #
    # def get_import_state(self, task_id, using="default", check_task=None, check_items=None):
    #     func_name = sys._getframe().f_code.co_name
    #     res, is_succ = api_request([self.ut.get_import_state, task_id, using])
    #     check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
    #                                    task_id=task_id, using=using).run()
    #     return res, check_result

    def get_query_segment_info(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.get_query_segment_info, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, timeout=timeout, using=using).run()
        return res, check_result

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

    def load_balance(self, src_node_id, dst_node_ids, sealed_segment_ids, timeout=None,
                     using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.load_balance, src_node_id, dst_node_ids,
                                    sealed_segment_ids, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def create_alias(self, collection_name, alias, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.create_alias, collection_name, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def drop_alias(self, alias, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.drop_alias, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def alter_alias(self, collection_name, alias, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.alter_alias, collection_name, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def list_aliases(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.list_aliases, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def mkts_from_datetime(self, d_time=None, milliseconds=0., delta=None):
        d_time = datetime.now() if d_time is None else d_time
        res, _ = api_request([self.ut.mkts_from_datetime, d_time, milliseconds, delta])
        return res

    def mkts_from_hybridts(self, hybridts, milliseconds=0., delta=None):
        res, _ = api_request([self.ut.mkts_from_hybridts, hybridts, milliseconds, delta])
        return res
