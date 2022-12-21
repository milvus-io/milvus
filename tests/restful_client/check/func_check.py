
class CheckTasks:
    """ The name of the method used to check the result """
    check_nothing = "check_nothing"
    err_res = "error_response"
    ccr = "check_connection_result"
    check_collection_property = "check_collection_property"
    check_partition_property = "check_partition_property"
    check_search_results = "check_search_results"
    check_query_results = "check_query_results"
    check_query_empty = "check_query_empty"  # verify that query result is empty
    check_query_not_empty = "check_query_not_empty"
    check_distance = "check_distance"
    check_delete_compact = "check_delete_compact"
    check_merge_compact = "check_merge_compact"
    check_role_property = "check_role_property"
    check_permission_deny = "check_permission_deny"
    check_value_equal = "check_value_equal"


class ResponseChecker:

    def __init__(self, check_task, check_items):
        self.check_task = check_task
        self.check_items = check_items


