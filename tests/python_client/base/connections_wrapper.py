from pymilvus import Connections
from pymilvus import DefaultConfig
from pymilvus import MilvusClient
import sys

sys.path.append("..")
from check.func_check import ResponseChecker
from utils.api_request import api_request


class ApiConnectionsWrapper:
    def __init__(self):
        self.connection = Connections()

    def add_connection(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([self.connection.add_connection], **kwargs)
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ, **kwargs).run()
        return response, check_result

    def disconnect(self, alias, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([self.connection.disconnect, alias])
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ, alias=alias).run()
        return response, check_result

    def remove_connection(self, alias, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([self.connection.remove_connection, alias])
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ, alias=alias).run()
        return response, check_result

    def connect(self, alias=DefaultConfig.DEFAULT_USING, user="", password="", db_name="", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        response, succ = api_request([self.connection.connect, alias, user, password, db_name], **kwargs)
        check_result = ResponseChecker(response, func_name, check_task, check_items, succ, alias=alias, **kwargs).run()
        return response, check_result

    def has_connection(self, alias=DefaultConfig.DEFAULT_USING, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        response, succ = api_request([self.connection.has_connection, alias])
        check_result = ResponseChecker(response, func_name, check_task, check_items, succ, alias=alias).run()
        return response, check_result

    #  def get_connection(self, alias=DefaultConfig.DEFAULT_USING, check_task=None, check_items=None):
    #      func_name = sys._getframe().f_code.co_name
    #      response, is_succ = api_request([self.connection.get_connection, alias])
    #      check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ, alias=alias).run()
    #      return response, check_result

    def list_connections(self, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([self.connection.list_connections])
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ).run()
        return response, check_result

    def get_connection_addr(self, alias, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        response, is_succ = api_request([self.connection.get_connection_addr, alias])
        check_result = ResponseChecker(response, func_name, check_task, check_items, is_succ, alias=alias).run()
        return response, check_result
    
    # high level api
    def MilvusClient(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        response, succ = api_request([MilvusClient], **kwargs)
        check_result = ResponseChecker(response, func_name, check_task, check_items, succ, **kwargs).run()
        return response, check_result
