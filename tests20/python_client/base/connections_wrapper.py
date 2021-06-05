from pymilvus_orm import Connections
from pymilvus_orm.types import DataType
from pymilvus_orm.default_config import DefaultConfig
import sys

sys.path.append("..")
from check.param_check import *
from check.func_check import *
from base.api_request import api_request


class ApiConnectionsWrapper:
    def __init__(self):
        self.connection = Connections()

    def add_connection(self, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.connection.add_connection], **kwargs)
        check_result = ResponseChecker(res, func_name, check_res, check_params, check, **kwargs).run()
        return res, check_result

    def disconnect(self, alias, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.connection.disconnect, alias])
        check_result = ResponseChecker(res, func_name, check_res, check_params, check, alias=alias).run()
        return res, check_result

    def remove_connection(self, alias, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.connection.remove_connection, alias])
        check_result = ResponseChecker(res, func_name, check_res, check_params, check, alias=alias).run()
        return res, check_result

    def connect(self, alias=DefaultConfig.DEFAULT_USING, check_res=None, check_params=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.connection.connect, alias], **kwargs)
        check_result = ResponseChecker(res, func_name, check_res, check_params, check, alias=alias, **kwargs).run()
        return res, check_result

    def get_connection(self, alias=DefaultConfig.DEFAULT_USING, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.connection.get_connection, alias])
        check_result = ResponseChecker(res, func_name, check_res, check_params, check, alias=alias).run()
        return res, check_result

    def list_connections(self, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.connection.list_connections])
        check_result = ResponseChecker(res, func_name, check_res, check_params, check).run()
        return res, check_result

    def get_connection_addr(self, alias, check_res=None, check_params=None):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.connection.get_connection_addr, alias])
        check_result = ResponseChecker(res, func_name, check_res, check_params, check, alias=alias).run()
        return res, check_result
