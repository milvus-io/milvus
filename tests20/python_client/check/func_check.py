from utils.util_log import test_log as log
from common.common_type import *
from common.code_mapping import ErrorCode, ErrorMessage
from pymilvus_orm import Collection, Partition
from utils.api_request import Error
from check.param_check import *


class ResponseChecker:
    def __init__(self, response, func_name, check_task, check_items, is_succ=True, **kwargs):
        self.response = response            # response of api request
        self.func_name = func_name          # api function name
        self.check_task = check_task        # task to check response of the api request
        self.check_items = check_items    # check items and expectations that to be checked in check task
        self.succ = is_succ                 # api responses successful or not

        self.kwargs_dict = {}       # not used for now, just for extension
        for key, value in kwargs.items():
            self.kwargs_dict[key] = value
        self.keys = self.kwargs_dict.keys()

    def run(self):
        """
        Method: start response checking for milvus API call
        """
        result = True
        if self.check_task is None:
            result = self.assert_succ(self.succ, True)

        elif self.check_task == CheckTasks.err_res:
            result = self.assert_exception(self.response, self.succ, self.check_items)

        elif self.check_task == CheckTasks.check_normal:
            result = self.check_value_equal(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_collection_property:
            result = self.check_collection_property(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_partition_property:
            result = self.check_partition_property(self.response, self.func_name, self.check_items)

        # Add check_items here if something new need verify

        return result

    @staticmethod
    def assert_succ(actual, expect):
        assert actual is expect
        return True

    @staticmethod
    def assert_exception(res, actual=True, error_dict=None):
        assert actual is False
        assert len(error_dict) > 0
        if isinstance(res, Error):
            # err_code = error_dict["err_code"]
            # assert res.code == err_code or ErrorMessage[err_code] in res.message
            assert res.code == error_dict["err_code"] or error_dict["err_msg"] in res.message
        else:
            log.error("[CheckFunc] Response of API is not an error: %s" % str(res))
            assert False
        return True

    @staticmethod
    def check_value_equal(res, func_name, params):
        """ check response of connection interface that result is normal """

        if func_name == "list_connections":
            if not isinstance(res, list):
                log.error("[CheckFunc] Response of list_connections is not a list: %s" % str(res))
                assert False

            list_content = params.get("list_content", None)
            if not isinstance(list_content, list):
                log.error("[CheckFunc] Check param of list_content is not a list: %s" % str(list_content))
                assert False

            new_res = get_connect_object_name(res)
            assert list_equal_check(new_res, list_content)

        if func_name == "get_connection_addr":
            dict_content = params.get("dict_content", None)
            assert dict_equal_check(res, dict_content)

        if func_name == "connect":
            class_obj = Connect_Object_Name
            res_obj = type(res).__name__
            assert res_obj == class_obj

        if func_name == "get_connection":
            value_content = params.get("value_content", None)
            res_obj = type(res).__name__ if res is not None else None
            assert res_obj == value_content

        return True

    @staticmethod
    def check_collection_property(collection, func_name, check_items):
        exp_func_name = "collection_init"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if not isinstance(collection, Collection):
            raise Exception("The result to check isn't collection type object")
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("name", None):
            assert collection.name == check_items["name"]
        if check_items.get("schema", None):
            assert collection.description == check_items["schema"].description
            assert collection.schema == check_items["schema"]
        return True

    @staticmethod
    def check_partition_property(partition, func_name, check_items):
        exp_func_name = "_init_partition"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if not isinstance(partition, Partition):
            raise Exception("The result to check isn't partition type object")
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("name", None):
            assert partition.name == check_items["name"]
        if check_items.get("description", None):
            assert partition.description == check_items["description"]
        if check_items.get("is_empty", None):
            assert partition.is_empty == check_items["is_empty"]
        if check_items.get("num_entities", None):
            assert partition.num_entities == check_items["num_entities"]
        return True

