import pytest
import sys

sys.path.append("..")
from base.Connections import ApiConnections
from base.Collection import ApiCollection
from base.Partition import ApiPartition
from base.Index import ApiIndex
from base.Utility import ApiUtility

from config.my_info import my_info
from common.common_func import *
from check.func_check import *


def request_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs), True
            except Exception as e:
                log.error("[ClientRequest API Exception]%s: %s" % (str(func), str(e)))
                return e, False
        return inner_wrapper
    return wrapper


@request_catch()
def func_req(_list, **kwargs):
    if isinstance(_list, list):
        func = _list[0]
        if callable(func):
            arg = []
            if len(_list) > 1:
                for a in _list[1:]:
                    arg.append(a)
            return func(*arg, **kwargs)
    return False, False


class ParamInfo:
    def __init__(self):
        self.param_ip = ""
        self.param_port = ""
        self.param_handler = ""

    def prepare_param_info(self, ip, port, handler):
        self.param_ip = ip
        self.param_port = port
        self.param_handler = handler


param_info = ParamInfo()


class Base:

    """ Initialize class object """
    connection = None
    collection = None
    partition = None
    index = None
    utility = None

    def setup_class(self):
        log.info("[setup_class] Start setup class...")

    def teardown_class(self):
        pass

    def setup(self):
        log.error("*" * 80)
        self.connection = ApiConnections()
        self.collection = ApiCollection()
        self.partition = ApiPartition()
        self.index = ApiIndex()
        self.utility = ApiUtility()

    def teardown(self):
        pass

    @pytest.fixture(scope="module", autouse=True)
    def initialize_env(self, request):
        """ clean log before testing """
        modify_file([my_info.test_log, my_info.test_err])
        log.info("[initialize_milvus] Log cleaned up, start testing...")

        ip = request.config.getoption("--ip")
        port = request.config.getoption("--port")
        handler = request.config.getoption("--handler")
        param_info.prepare_param_info(ip, port, handler)


class ApiReq(Base):
    """
    Additional methods;
    Public methods that can be used to add cases.
    """

    def func(self):
        pass

    @staticmethod
    def func_2():
        pass
