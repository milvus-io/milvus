from utils.util_log import test_log as log
from common.common_type import *
from pymilvus_orm import Collection, Partition


class ResponseChecker:
    def __init__(self, response, func_name, check_items, check_params, expect_succ=True, **kwargs):
        self.response = response  # response of api request
        self.func_name = func_name
        self.check_items = check_items
        self.check_params = check_params
        self.expect_succ = expect_succ

        # parse **kwargs. not used for now
        self.params = {}
        for key, value in kwargs.items():
            self.params[key] = value
        self.keys = self.params.keys()

    def run(self):
        """
        Method: start response checking for milvus API call
        """
        result = True
        if self.check_items is None:
            result = self.assert_expectation(self.expect_succ, True)

        elif self.check_items == CheckParams.err_res:
            result = self.assert_expectation(self.expect_succ, False)

        elif self.check_items == CheckParams.list_count and self.check_params is not None:
            result = self.check_list_count(self.response, self.func_name, self.check_params)

        elif self.check_items == CheckParams.collection_property_check:
            result = self.req_collection_property_check(self.response, self.func_name, self.params)

        elif self.check_items == CheckParams.partition_property_check:
            result = self.partition_property_check(self.response, self.func_name, self.params)

        # Add check_items here if something new need verify

        return result

    @staticmethod
    def assert_expectation(expect, actual):
        assert actual == expect
        return True

    @staticmethod
    def check_list_count(res, func_name, params):
        if not isinstance(res, list):
            log.error("[CheckFunc] Response of API is not a list: %s" % str(res))
            assert False

        if func_name == "list_connections":
            list_count = params.get("list_count", None)
            if not str(list_count).isdigit():
                log.error("[CheckFunc] Check param of list_count is not a number: %s" % str(list_count))
                assert False

            assert len(res) == int(list_count)

        return True

    @staticmethod
    def req_collection_property_check(collection, func_name, params):
        '''
        :param collection
        :return:
        '''
        exp_func_name = "collection_init"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if not isinstance(collection, Collection):
            raise Exception("The result to check isn't collection type object")
        assert collection.name == params["name"]
        assert collection.description == params["schema"].description
        assert collection.schema == params["schema"]
        return True

    @staticmethod
    def partition_property_check(partition, func_name, params):
        exp_func_name = "partition_init"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if not isinstance(partition, Partition):
            raise Exception("The result to check isn't collection type object")
        assert partition.name == params["name"]
        assert partition.description == params["description"]
        assert partition.is_empty == params["is_empty"]
        assert partition.num_entities == params["num_entities"]
        return True

