from utils.util_log import test_log as log
from common import common_type as ct
from common import common_func as cf
from common.common_type import CheckTasks, Connect_Object_Name
# from common.code_mapping import ErrorCode, ErrorMessage
from pymilvus import Collection, Partition
from utils.api_request import Error
import check.param_check as pc


class ResponseChecker:
    def __init__(self, response, func_name, check_task, check_items, is_succ=True, **kwargs):
        self.response = response  # response of api request
        self.func_name = func_name  # api function name
        self.check_task = check_task  # task to check response of the api request
        self.check_items = check_items  # check items and expectations that to be checked in check task
        self.succ = is_succ  # api responses successful or not

        self.kwargs_dict = {}  # not used for now, just for extension
        for key, value in kwargs.items():
            self.kwargs_dict[key] = value
        self.keys = self.kwargs_dict.keys()

    def run(self):
        """
        Method: start response checking for milvus API call
        """
        result = True
        if self.check_task is None:
            # Interface normal return check
            result = self.assert_succ(self.succ, True)

        elif self.check_task == CheckTasks.err_res:
            # Interface return error code and error message check
            result = self.assert_exception(self.response, self.succ, self.check_items)

        elif self.check_task == CheckTasks.check_nothing:
            return self.succ

        elif self.check_task == CheckTasks.ccr:
            # Connection interface response check
            result = self.check_value_equal(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_collection_property:
            # Collection interface response check
            result = self.check_collection_property(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_partition_property:
            # Partition interface response check
            result = self.check_partition_property(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_search_results:
            # Search interface of collection and partition that response check
            result = self.check_search_results(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_query_results:
            # Query interface of collection and partition that response check
            result = self.check_query_results(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_distance:
            # Calculate distance interface that response check
            result = self.check_distance(self.response, self.func_name, self.check_items)

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
            error_code = error_dict[ct.err_code]
            assert res.code == error_code or error_dict[ct.err_msg] in res.message
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

            list_content = params.get(ct.list_content, None)
            if not isinstance(list_content, list):
                log.error("[CheckFunc] Check param of list_content is not a list: %s" % str(list_content))
                assert False

            new_res = pc.get_connect_object_name(res)
            assert pc.list_equal_check(new_res, list_content)

        if func_name == "get_connection_addr":
            dict_content = params.get(ct.dict_content, None)
            assert pc.dict_equal_check(res, dict_content)

        if func_name == "connect":
            class_obj = Connect_Object_Name
            res_obj = type(res).__name__
            assert res_obj == class_obj

        if func_name == "get_connection":
            value_content = params.get(ct.value_content, None)
            res_obj = type(res).__name__ if res is not None else None
            assert res_obj == value_content

        return True

    @staticmethod
    def check_collection_property(res, func_name, check_items):
        """
        According to the check_items to check collection properties of res, which return from func_name
        :param res: actual response of init collection
        :type res: Collection

        :param func_name: init collection API
        :type func_name: str

        :param check_items: which items expected to be checked, including name, schema, num_entities, primary
        :type check_items: dict, {check_key: expected_value}
        """
        exp_func_name = "init_collection"
        exp_func_name_2 = "construct_from_dataframe"
        if func_name != exp_func_name and func_name != exp_func_name_2:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if isinstance(res, Collection):
            collection = res
        elif isinstance(res, tuple):
            collection = res[0]
            log.debug(collection.schema)
        else:
            raise Exception("The result to check isn't collection type object")
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("name", None):
            assert collection.name == check_items.get("name")
        if check_items.get("schema", None):
            assert collection.schema == check_items.get("schema")
        if check_items.get("num_entities", None):
            if check_items.get("num_entities") == 0:
                assert collection.is_empty
            assert collection.num_entities == check_items.get("num_entities")
        if check_items.get("primary", None):
            assert collection.primary_field.name == check_items.get("primary")
        return True

    @staticmethod
    def check_partition_property(partition, func_name, check_items):
        exp_func_name = "init_partition"
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

    @staticmethod
    def check_search_results(search_res, func_name, check_items):
        """
        target: check the search results
        method: 1. check the query number
                2. check the limit(topK) and ids
                3. check the distance
        expected: check the search is ok
        """
        log.info("search_results_check: checking the searching results")
        if func_name != 'search':
            log.warning("The function name is {} rather than {}".format(func_name, "search"))
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("_async", None):
            if check_items["_async"]:
                search_res.done()
                search_res = search_res.result()
        if len(search_res) != check_items["nq"]:
            log.error("search_results_check: Numbers of query searched (%d) "
                      "is not equal with expected (%d)"
                      % (len(search_res), check_items["nq"]))
            assert len(search_res) == check_items["nq"]
        else:
            log.info("search_results_check: Numbers of query searched is correct")
        for hits in search_res:
            if (len(hits) != check_items["limit"]) \
                    or (len(hits.ids) != check_items["limit"]):
                log.error("search_results_check: limit(topK) searched (%d) "
                          "is not equal with expected (%d)"
                          % (len(hits), check_items["limit"]))
                assert len(hits) == check_items["limit"]
                assert len(hits.ids) == check_items["limit"]
            else:
                ids_match = pc.list_contain_check(list(hits.ids),
                                                  list(check_items["ids"]))
                if not ids_match:
                    log.error("search_results_check: ids searched not match")
                    assert ids_match
        log.info("search_results_check: limit (topK) and "
                 "ids searched for %d queries are correct" % len(search_res))
        return True

    @staticmethod
    def check_query_results(query_res, func_name, check_items):
        """
        According to the check_items to check actual query result, which return from func_name.

        :param: query_res: A list that contains all results
        :type: list

        :param func_name: Query API name
        :type func_name: str

        :param check_items: The items expected to be checked, including exp_res, with_vec
                            The type of exp_res value is as same as query_res
                            The type of with_vec value is bool, True value means check vector field, False otherwise
        :type check_items: dict
        """
        if func_name != 'query':
            log.warning("The function name is {} rather than {}".format(func_name, "query"))
        if not isinstance(query_res, list):
            raise Exception("The query result to check isn't list type object")
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        exp_res = check_items.get("exp_res", None)
        with_vec = check_items.get("with_vec", False)
        if exp_res and isinstance(query_res, list):
            assert pc.equal_entities_list(exp=exp_res, actual=query_res, with_vec=with_vec)
            # assert len(exp_res) == len(query_res)
            # for i in range(len(exp_res)):
            #     assert_entity_equal(exp=exp_res[i], actual=query_res[i])

    @staticmethod
    def check_distance(distance_res, func_name, check_items):
        if func_name != 'calc_distance':
            log.warning("The function name is {} rather than {}".format(func_name, "calc_distance"))
        if not isinstance(distance_res, list):
            raise Exception("The distance result to check isn't list type object")
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        vectors_l = check_items["vectors_l"]
        vectors_r = check_items["vectors_r"]
        metric = check_items.get("metric", "L2")
        sqrt = check_items.get("sqrt", False)
        cf.compare_distance_2d_vector(vectors_l, vectors_r,
                                      distance_res,
                                      metric, sqrt)

        return True
