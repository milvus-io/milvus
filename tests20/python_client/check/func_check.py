from utils.util_log import test_log as log
from common.common_type import *
from pymilvus_orm import Collection, Partition


class CheckFunc:
    def __init__(self, res, func_name, check_res, check_params, check_res_result=True, **kwargs):
        self.res = res  # response of api request
        self.func_name = func_name
        self.check_res = check_res
        self.check_params = check_params
        self.check_res_result = check_res_result
        self.params = {}

        for key, value in kwargs.items():
            self.params[key] = value

        self.keys = self.params.keys()

    def run(self):
        # log.debug("[Run CheckFunc] Start checking res...")
        check_result = True

        if self.check_res is None:
            check_result = self.check_response(self.check_res_result, True)

        elif self.check_res == CheckParams.err_res:
            check_result = self.check_response(self.check_res_result, False)

        elif self.check_res == CheckParams.list_count and self.check_params is not None:
            check_result = self.check_list_count(self.res, self.func_name, self.check_params)

        elif self.check_res == CheckParams.collection_property_check:
            check_result = self.req_collection_property_check(self.res, self.func_name, self.params)

        elif self.check_res == CheckParams.partition_property_check:
            check_result = self.partition_property_check(self.res, self.func_name, self.params)

        return check_result

    @staticmethod
    def check_response(check_res_result, expect_result):
        assert check_res_result == expect_result
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

