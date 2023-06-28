from pymilvus.client.types import CompactionPlans
from pymilvus import Role

from utils.util_log import test_log as log
from common import common_type as ct
from common import common_func as cf
from common.common_type import CheckTasks, Connect_Object_Name
# from common.code_mapping import ErrorCode, ErrorMessage
from pymilvus import Collection, Partition, ResourceGroupInfo
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

        elif self.check_task == CheckTasks.check_query_empty:
            result = self.check_query_empty(self.response, self.func_name)

        elif self.check_task == CheckTasks.check_query_empty:
            result = self.check_query_not_empty(self.response, self.func_name)

        elif self.check_task == CheckTasks.check_distance:
            # Calculate distance interface that response check
            result = self.check_distance(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_delete_compact:
            result = self.check_delete_compact_plan(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_merge_compact:
            result = self.check_merge_compact_plan(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_role_property:
            # Collection interface response check
            result = self.check_role_property(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_permission_deny:
            # Collection interface response check
            result = self.check_permission_deny(self.response, self.succ)
            
        elif self.check_task == CheckTasks.check_rg_property:
            # describe resource group interface response check
            result = self.check_rg_property(self.response, self.func_name, self.check_items)
            
        elif self.check_task == CheckTasks.check_describe_collection_property:
            # describe collection interface(high level api) response check
            result = self.check_describe_collection_property(self.response, self.func_name, self.check_items)

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
            type_name = type(res).__name__
            assert not type_name.lower().__contains__("error")

        if func_name == "has_connection":
            value_content = params.get(ct.value_content, False)
            res_obj = res if res is not None else False
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
    def check_describe_collection_property(res, func_name, check_items):
        """
        According to the check_items to check collection properties of res, which return from func_name
        :param res: actual response of init collection
        :type res: Collection

        :param func_name: init collection API
        :type func_name: str

        :param check_items: which items expected to be checked, including name, schema, num_entities, primary
        :type check_items: dict, {check_key: expected_value}
        """
        exp_func_name = "describe_collection"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("collection_name", None) is not None:
            assert res["collection_name"] == check_items.get("collection_name")
        if check_items.get("auto_id", False):
            assert res["auto_id"] == check_items.get("auto_id")
        if check_items.get("num_shards", 1):
            assert res["num_shards"] == check_items.get("num_shards", 1)
        if check_items.get("consistency_level", 2):
            assert res["consistency_level"] == check_items.get("consistency_level", 2)
        if check_items.get("enable_dynamic_field", True):
            assert res["enable_dynamic_field"] == check_items.get("enable_dynamic_field", True)
        if check_items.get("num_partitions", 1):
            assert res["num_partitions"] == check_items.get("num_partitions", 1)
        if check_items.get("id_name", "id"):
            assert res["fields"][0]["name"] == check_items.get("id_name", "id")
        if check_items.get("vector_name", "vector"):
            assert res["fields"][1]["name"] == check_items.get("vector_name", "vector")
        if check_items.get("dim", None) is not None:
            assert res["fields"][1]["params"]["dim"] == check_items.get("dim")
        assert res["fields"][0]["is_primary"] is True
        assert res["fields"][0]["field_id"] == 100 and res["fields"][0]["type"] == 5
        assert res["fields"][1]["field_id"] == 101 and res["fields"][1]["type"] == 101

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
    def check_rg_property(rg, func_name, check_items):
        exp_func_name = "describe_resource_group"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if not isinstance(rg, ResourceGroupInfo):
            raise Exception("The result to check isn't ResourceGroupInfo type object")
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("name", None):
            assert rg.name == check_items["name"]
        if check_items.get("capacity", None):
            assert rg.capacity == check_items["capacity"]
        if check_items.get("num_available_node", None):
            assert rg.num_available_node == check_items["num_available_node"]
        if check_items.get("num_loaded_replica", None):
            assert dict(rg.num_loaded_replica).items() >= check_items["num_loaded_replica"].items()
        if check_items.get("num_outgoing_node", None):
            assert dict(rg.num_outgoing_node).items() >= check_items["num_outgoing_node"].items()
        if check_items.get("num_incoming_node", None):
            assert dict(rg.num_incoming_node).items() >= check_items["num_incoming_node"].items()
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
        if check_items.get("output_fields", None):
            assert set(search_res[0][0].entity.fields) == set(check_items["output_fields"])
            log.info('search_results_check: Output fields of query searched is correct')
        if len(search_res) != check_items["nq"]:
            log.error("search_results_check: Numbers of query searched (%d) "
                      "is not equal with expected (%d)"
                      % (len(search_res), check_items["nq"]))
            assert len(search_res) == check_items["nq"]
        else:
            log.info("search_results_check: Numbers of query searched is correct")
        enable_high_level_api = check_items.get("enable_high_level_api", False)
        log.debug(search_res)
        for hits in search_res:
            searched_original_vectors = []
            ids = []
            if enable_high_level_api:
                for hit in hits:
                    ids.append(hit['id'])
            else:
                ids = list(hits.ids)
            if (len(hits) != check_items["limit"]) \
                    or (len(ids) != check_items["limit"]):
                log.error("search_results_check: limit(topK) searched (%d) "
                          "is not equal with expected (%d)"
                          % (len(hits), check_items["limit"]))
                assert len(hits) == check_items["limit"]
                assert len(ids) == check_items["limit"]
            else:
                if check_items.get("ids", None) is not None:
                    ids_match = pc.list_contain_check(ids,
                                                      list(check_items["ids"]))
                    if not ids_match:
                        log.error("search_results_check: ids searched not match")
                        assert ids_match
                elif check_items.get("metric", None) is not None:
                    if check_items.get("vector_nq") is None:
                        raise Exception("vector for searched (nq) is needed for distance check")
                    if check_items.get("original_vectors") is None:
                        raise Exception("inserted vectors are needed for distance check")
                    for id in hits.ids:
                        searched_original_vectors.append(check_items["original_vectors"][id])
                    cf.compare_distance_vector_and_vector_list(check_items["vector_nq"][i],
                                                               searched_original_vectors,
                                                               check_items["metric"], hits.distances)
                    log.info("search_results_check: Checked the distances for one nq: OK")
                else:
                    pass    # just check nq and topk, not specific ids need check
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
        primary_field = check_items.get("primary_field", None)
        if exp_res is not None:
            if isinstance(query_res, list):
                assert pc.equal_entities_list(exp=exp_res, actual=query_res, primary_field=primary_field, 
                                              with_vec=with_vec)
                return True
            else:
                log.error(f"Query result {query_res} is not list")
                return False
        log.warning(f'Expected query result is {exp_res}')

    @staticmethod
    def check_query_empty(query_res, func_name):
        """
        Verify that the query result is empty

        :param: query_res: A list that contains all results
        :type: list

        :param func_name: Query API name
        :type func_name: str
        """
        if func_name != 'query':
            log.warning("The function name is {} rather than {}".format(func_name, "query"))
        if not isinstance(query_res, list):
            raise Exception("The query result to check isn't list type object")
        assert len(query_res) == 0, "Query result is not empty"

    @staticmethod
    def check_query_not_empty(query_res, func_name):
        """
        Verify that the query result is not empty

        :param: query_res: A list that contains all results
        :type: list

        :param func_name: Query API name
        :type func_name: str
        """
        if func_name != 'query':
            log.warning("The function name is {} rather than {}".format(func_name, "query"))
        if not isinstance(query_res, list):
            raise Exception("The query result to check isn't list type object")
        assert len(query_res) > 0

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

    @staticmethod
    def check_delete_compact_plan(compaction_plans, func_name, check_items):
        """
        Verify that the delete type compaction plan

        :param: compaction_plans: A compaction plan
        :type: CompactionPlans

        :param func_name: get_compaction_plans API name
        :type func_name: str

        :param check_items: which items you wish to check
                            plans_num represent the delete compact plans number
        :type: dict
        """
        to_check_func = 'get_compaction_plans'
        if func_name != to_check_func:
            log.warning("The function name is {} rather than {}".format(func_name, to_check_func))
        if not isinstance(compaction_plans, CompactionPlans):
            raise Exception("The compaction_plans result to check isn't CompactionPlans type object")

        plans_num = check_items.get("plans_num", 1)
        assert len(compaction_plans.plans) == plans_num
        for plan in compaction_plans.plans:
            assert len(plan.sources) == 1
            assert plan.sources[0] != plan.target

    @staticmethod
    def check_merge_compact_plan(compaction_plans, func_name, check_items):
        """
        Verify that the merge type compaction plan

        :param: compaction_plans: A compaction plan
        :type: CompactionPlans

        :param func_name: get_compaction_plans API name
        :type func_name: str

        :param check_items: which items you wish to check
                            segment_num represent how many segments are expected to be merged, default is 2
        :type: dict
        """
        to_check_func = 'get_compaction_plans'
        if func_name != to_check_func:
            log.warning("The function name is {} rather than {}".format(func_name, to_check_func))
        if not isinstance(compaction_plans, CompactionPlans):
            raise Exception("The compaction_plans result to check isn't CompactionPlans type object")

        segment_num = check_items.get("segment_num", 2)
        assert len(compaction_plans.plans) == 1
        assert len(compaction_plans.plans[0].sources) == segment_num
        assert compaction_plans.plans[0].target not in compaction_plans.plans[0].sources

    @staticmethod
    def check_role_property(role, func_name, check_items):
        exp_func_name = "create_role"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if not isinstance(role, Role):
            raise Exception("The result to check isn't role type object")
        if check_items is None:
            raise Exception("No expect values found in the check task")
        if check_items.get("name", None):
            assert role.name == check_items["name"]
        return True

    @staticmethod
    def check_permission_deny(res, actual=True):
        assert actual is False
        if isinstance(res, Error):
            assert "permission deny" in res.message
        else:
            log.error("[CheckFunc] Response of API is not an error: %s" % str(res))
            assert False
        return True
