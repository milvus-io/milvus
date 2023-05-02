import sys

from pymilvus import db

from utils.api_request import api_request
from check.func_check import ResponseChecker


class ApiDatabaseWrapper:
    """ wrapper of database """
    database = db

    def create_database(self, db_name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([self.database.create_database, db_name, using, timeout])
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ).run()
        return response, check_result

    def using_database(self, db_name, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([self.database.using_database, db_name, using])
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ).run()
        return response, check_result

    def drop_database(self, db_name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([self.database.drop_database, db_name, using, timeout])
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ).run()
        return response, check_result

    def list_database(self, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([self.database.list_database, using, timeout])
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ).run()
        return response, check_result
