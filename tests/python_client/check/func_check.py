import pandas.core.frame
from pymilvus.client.types import CompactionPlans
from pymilvus import Role

from utils.util_log import test_log as log
from common import common_type as ct
from common import common_func as cf
from common.common_type import CheckTasks, Connect_Object_Name
# from common.code_mapping import ErrorCode, ErrorMessage
from pymilvus import Collection, Partition, ResourceGroupInfo
import check.param_check as pc


class Error:
    def __init__(self, error):
        self.code = getattr(error, 'code', -1)
        self.message = getattr(error, 'message', str(error))

    def __str__(self):
        return f"Error(code={self.code}, message={self.message})"

    def __repr__(self):
        return f"Error(code={self.code}, message={self.message})"


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

        elif self.check_task == CheckTasks.check_search_iterator:
            # Search iterator interface of collection and partition that response check
            result = self.check_search_iterator(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_query_results:
            # Query interface of collection and partition that response check
            result = self.check_query_results(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_query_iterator:
            # query iterator interface of collection and partition that response check
            result = self.check_query_iterator(self.response, self.func_name, self.check_items)

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

        elif self.check_task == CheckTasks.check_auth_failure:
            # connection interface response check
            result = self.check_auth_failure(self.response, self.succ)

        elif self.check_task == CheckTasks.check_rg_property:
            # describe resource group interface response check
            result = self.check_rg_property(self.response, self.func_name, self.check_items)

        elif self.check_task == CheckTasks.check_describe_collection_property:
            # describe collection interface(high level api) response check
            result = self.check_describe_collection_property(self.response, self.func_name, self.check_items)
        elif self.check_task == CheckTasks.check_collection_fields_properties:
            # check field properties in describe collection response
            result = self.check_collection_fields_properties(self.response, self.func_name, self.check_items)
        elif self.check_task == CheckTasks.check_describe_database_property:
            # describe database interface(high level api) response check
            result = self.check_describe_database_property(self.response, self.func_name, self.check_items)
        elif self.check_task == CheckTasks.check_insert_result:
            # check `insert` interface response
            result = self.check_insert_response(check_items=self.check_items)
        elif self.check_task == CheckTasks.check_describe_index_property:
            # describe collection interface(high level api) response check
            result = self.check_describe_index_property(self.response, self.func_name, self.check_items)

        # Add check_items here if something new need verify

        return result

    def assert_succ(self, actual, expect):
        assert actual is expect, f"Response of API {self.func_name} expect {expect}, but got {actual}"
        return True

    def assert_exception(self, res, actual=True, error_dict=None):
        assert actual is False, f"Response of API {self.func_name} expect get error, but success"
        assert len(error_dict) > 0
        if isinstance(res, Error):
            error_code = error_dict[ct.err_code]
            # assert res.code == error_code or error_dict[ct.err_msg] in res.message, (
            #     f"Response of API {self.func_name} "
            #     f"expect get error code {error_dict[ct.err_code]} or error message {error_dict[ct.err_code]}, "
            #     f"but got {res.code} {res.message}")
            assert error_dict[ct.err_msg] in res.message, (
                f"Response of API {self.func_name} "
                f"expect get error message {error_dict[ct.err_code]}, "
                f"but got {res.code} {res.message}")

        else:
            log.error("[CheckFunc] Response of API is not an error: %s" % str(res))
            assert False, (f"Response of API expect get error code {error_dict[ct.err_code]} or "
                           f"error message {error_dict[ct.err_code]}"
                           f"but success")
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
        if check_items.get("nullable_fields", None) is not None:
            nullable_fields = check_items.get("nullable_fields")
            if not isinstance(nullable_fields, list):
                log.error("nullable_fields should be a list including all the nullable fields name")
                assert False
            for field in res["fields"]:
                if field["name"] in nullable_fields:
                    assert field["nullable"] is True
        assert res["fields"][0]["is_primary"] is True
        assert res["fields"][0]["field_id"] == 100 and (res["fields"][0]["type"] == 5 or 21)
        assert res["fields"][1]["field_id"] == 101 and res["fields"][1]["type"] == 101

        return True

    @staticmethod
    def check_collection_fields_properties(res, func_name, check_items):
        """
        According to the check_items to check collection field properties of res, which return from func_name
        :param res: actual response of client.describe_collection()
        :type res: Collection

        :param func_name: describe_collection
        :type func_name: str

        :param check_items: which field properties expected to be checked, like max_length etc.
        :type check_items: dict, {field_name: {field_properties}, ...}
        """
        exp_func_name = "describe_collection"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("collection_name", None) is not None:
            assert res["collection_name"] == check_items.get("collection_name")
        for key in check_items.keys():
            for field in res["fields"]:
                if field["name"] == key:
                    assert field['params'].items() >= check_items[key].items()
        return True

    @staticmethod
    def check_describe_database_property(res, func_name, check_items):
        """
        According to the check_items to check database properties of res, which return from func_name
        :param res: actual response of init database
        :type res: Database

        :param func_name: init database API
        :type func_name: str

        :param check_items: which items expected to be checked
        :type check_items: dict, {check_key: expected_value}
        """
        exp_func_name = "describe_database"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("db_name", None) is not None:
            assert res["name"] == check_items.get("db_name")
        if check_items.get("database.force.deny.writing", None) is not None:
            if check_items.get("database.force.deny.writing") == "Missing":
                assert "database.force.deny.writing" not in res
            else:
                assert res["database.force.deny.writing"] == check_items.get("database.force.deny.writing")
        if check_items.get("database.force.deny.reading", None) is not None:
            if check_items.get("database.force.deny.reading") == "Missing":
                assert "database.force.deny.reading" not in res
            else:
                assert res["database.force.deny.reading"] == check_items.get("database.force.deny.reading")
        if check_items.get("database.replica.number", None) is not None:
            if check_items.get("database.replica.number") == "Missing":
                assert "database.replica.number" not in res
            else:
                assert res["database.replica.number"] == check_items.get("database.replica.number")
        if check_items.get("properties_length", None) is not None:
            assert len(res) == check_items.get("properties_length")

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
        enable_milvus_client_api = check_items.get("enable_milvus_client_api", False)
        pk_name = check_items.get("pk_name", ct.default_primary_field_name)

        if func_name != 'search' and func_name != 'hybrid_search':
            log.warning("The function name is {} rather than {} or {}".format(func_name, "search", "hybrid_search"))
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("_async", None):
            if check_items["_async"]:
                search_res.done()
                search_res = search_res.result()
        if check_items.get("output_fields", None):
            assert set(search_res[0][0].entity.fields.keys()) == set(check_items["output_fields"])
            if check_items.get("original_entities", None):
                original_entities = check_items["original_entities"]
                if not isinstance(original_entities, pandas.core.frame.DataFrame):
                    original_entities = pandas.DataFrame(original_entities)
                pc.output_field_value_check(search_res, original_entities, pk_name=pk_name)
        if len(search_res) != check_items["nq"]:
            log.error("search_results_check: Numbers of query searched (%d) "
                      "is not equal with expected (%d)"
                      % (len(search_res), check_items["nq"]))
            assert len(search_res) == check_items["nq"]
        else:
            log.info("search_results_check: Numbers of query searched is correct")
        # log.debug(search_res)
        nq_i = 0
        for hits in search_res:
            ids = []
            distances = []
            if enable_milvus_client_api:
                for hit in hits:
                    ids.append(hit[pk_name])
                    distances.append(hit['distance'])
            else:
                ids = list(hits.ids)
                distances = list(hits.distances)
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
                    # verify the distances are already sorted
                    if check_items.get("metric").upper() in ["IP", "COSINE", "BM25"]:
                        assert distances == sorted(distances, reverse=True)
                    else:
                        assert distances == sorted(distances, reverse=False)
                    if check_items.get("vector_nq") is None or check_items.get("original_vectors") is None:
                        log.debug("skip distance check for knowhere does not return the precise distances")
                    else:
                        pass
                else:
                    pass  # just check nq and topk, not specific ids need check
            nq_i += 1
        log.info("search_results_check: limit (topK) and "
                 "ids searched for %d queries are correct" % len(search_res))

        return True

    @staticmethod
    def check_search_iterator(search_res, func_name, check_items):
        """
        target: check the search iterator results
        method: 1. check the iterator number
                2. check the limit(topK) and ids
                3. check the distance
        expected: check the search is ok
        """
        log.info("search_iterator_results_check: checking the searching results")
        if func_name != 'search_iterator':
            log.warning("The function name is {} rather than {}".format(func_name, "search_iterator"))
        search_iterator = search_res
        expected_batch_size = check_items.get("batch_size", None)
        expected_iterate_times = check_items.get("iterate_times", None)
        pk_list = []
        iterate_times = 0
        while True:
            try:
                res = search_iterator.next()
                iterate_times += 1
                if not res:
                    log.info("search iteration finished, close")
                    search_iterator.close()
                    break
                if expected_batch_size is not None:
                    assert len(res) <= expected_batch_size
                if check_items.get("radius", None):
                    for distance in res.distances():
                        if check_items["metric_type"] == "L2":
                            assert distance < check_items["radius"]
                        else:
                            assert distance > check_items["radius"]
                if check_items.get("range_filter", None):
                    for distance in res.distances():
                        if check_items["metric_type"] == "L2":
                            assert distance >= check_items["range_filter"]
                        else:
                            assert distance <= check_items["range_filter"]
                pk_list.extend(res.ids())
            except Exception as e:
                assert check_items["err_msg"] in str(e)
                return False
        if expected_iterate_times is not None:
            assert iterate_times <= expected_iterate_times
            if expected_iterate_times == 1:
                assert len(pk_list) == 0  # expected batch size =0 if external filter all
                assert iterate_times == 1
                return True
        log.debug(f"check: total {len(pk_list)} results, set len: {len(set(pk_list))}, iterate_times: {iterate_times}")
        assert len(pk_list) == len(set(pk_list)) != 0
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
        pk_name = check_items.get("pk_name", ct.default_primary_field_name)
        if exp_res is not None:
            if isinstance(query_res, list):
                assert pc.equal_entities_list(exp=exp_res, actual=query_res, primary_field=pk_name,
                                              with_vec=with_vec)
                return True
            else:
                log.error(f"Query result {query_res} is not list")
                return False
        log.warning(f'Expected query result is {exp_res}')

    @staticmethod
    def check_query_iterator(query_res, func_name, check_items):
        """
        target: check the query results
        method: 1. check the query number
                2. check the limit(topK) and ids
                3. check the distance
        expected: check the search is ok
        """
        log.info("query_iterator_results_check: checking the query results")
        if func_name != 'query_iterator':
            log.warning("The function name is {} rather than {}".format(func_name, "query_iterator"))
        query_iterator = query_res
        pk_list = []
        while True:
            res = query_iterator.next()
            if len(res) == 0:
                log.info("search iteration finished, close")
                query_iterator.close()
                break
            pk_name = check_items.get("pk_name", ct.default_primary_field_name)
            for i in range(len(res)):
                pk_list.append(res[i][pk_name])
            if check_items.get("limit", None):
                assert len(res) <= check_items["limit"]
        assert len(pk_list) == len(set(pk_list))
        if check_items.get("count", None):
            log.info(len(pk_list))
            assert len(pk_list) == check_items["count"]
        if check_items.get("exp_ids", None):
            assert pk_list == check_items["exp_ids"]
        log.info("check: total %d results" % len(pk_list))

        return True

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

    @staticmethod
    def check_auth_failure(res, actual=True):
        assert actual is False
        if isinstance(res, Error):
            assert "auth check failure" in res.message
        else:
            log.error("[CheckFunc] Response of API is not an error: %s" % str(res))
            assert False
        return True

    def check_insert_response(self, check_items):
        # check request successful
        self.assert_succ(self.succ, True)

        # get insert count
        real = check_items.get("insert_count", None) if isinstance(check_items, dict) else None
        if real is None:
            real = len(self.kwargs_dict.get("data", [[]])[0])

        # check insert count
        error_message = "[CheckFunc] Insert count does not meet expectations, response:{0} != expected:{1}"
        assert self.response.insert_count == real, error_message.format(self.response.insert_count, real)

        return True

    @staticmethod
    def check_describe_index_property(res, func_name, check_items):
        """
        According to the check_items to check collection properties of res, which return from func_name
        :param res: actual response of init collection
        :type res: Collection

        :param func_name: init collection API
        :type func_name: str

        :param check_items: which items expected to be checked, including name, schema, num_entities, primary
        :type check_items: dict, {check_key: expected_value}
        """
        exp_func_name = "describe_index"
        if func_name != exp_func_name:
            log.warning("The function name is {} rather than {}".format(func_name, exp_func_name))
        if len(check_items) == 0:
            raise Exception("No expect values found in the check task")
        if check_items.get("json_cast_type", None) is not None:
            assert res["json_cast_type"] == check_items.get("json_cast_type")
        if check_items.get("index_type", None) is not None:
            assert res["index_type"] == check_items.get("index_type")
        if check_items.get("json_path", None) is not None:
            assert res["json_path"] == check_items.get("json_path")
        if check_items.get("field_name", None) is not None:
            assert res["field_name"] == check_items.get("field_name")
        if check_items.get("index_name", None) is not None:
            assert res["index_name"] == check_items.get("index_name")

        return True