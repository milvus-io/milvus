import pytest
import sys

sys.path.append("..")
from base.connections import ApiConnections
from base.collection import ApiCollection
from base.partition import ApiPartition
from base.index import ApiIndex
from base.utility import ApiUtility

from config.test_info import test_info
from utils.util_log import test_log as log
from common import common_func as cf


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
        self.param_host = ""
        self.param_port = ""
        self.param_handler = ""

    def prepare_param_info(self, host, port, handler):
        self.param_host = host
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
        cf.modify_file([test_info.log_info, test_info.log_err])
        log.info("[initialize_milvus] Log cleaned up, start testing...")

        host = request.config.getoption("--host")
        port = request.config.getoption("--port")
        handler = request.config.getoption("--handler")
        param_info.prepare_param_info(host, port, handler)


class ApiReq(Base):
    """
    Additional methods;
    Public methods that can be used to add cases.
    """

    def _connect(self):
        """ Testing func """
        self.connection.configure(check_res='', default={"host": "192.168.1.240", "port": 19530})
        res = self.connection.create_connection(alias='default')
        return res

    def _collection(self, name=None, data=None, schema=None, check_res=None, **kwargs):
        """ Testing func """
        self._connect()
        name = cf.gen_unique_str("ApiReq") if name is None else name
        schema = cf.gen_default_collection_schema() if schema is None else schema
        collection, _ = self.collection.collection_init(name=name, data=data, schema=schema, check_res=check_res, **kwargs)
        assert name == collection.name
        return collection
