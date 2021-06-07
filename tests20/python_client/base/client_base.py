import pytest
import sys

sys.path.append("..")
from base.connections_wrapper import ApiConnectionsWrapper
from base.collection_wrapper import ApiCollectionWrapper
from base.partition_wrapper import ApiPartitionWrapper
from base.index_wrapper import ApiIndexWrapper
from base.utility_wrapper import ApiUtilityWrapper

from config.test_info import test_info
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct


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
    connection_wrap = None
    collection_wrap = None
    partition_wrap = None
    index_wrap = None
    utility_wrap = None

    def setup_class(self):
        log.info("[setup_class] Start setup class...")

    def teardown_class(self):
        pass

    def setup(self):
        log.info(("*" * 35) + " setup " + ("*" * 35))
        self.connection_wrap = ApiConnectionsWrapper()
        self.collection_wrap = ApiCollectionWrapper()
        self.partition_wrap = ApiPartitionWrapper()
        self.index_wrap = ApiIndexWrapper()
        self.utility_wrap = ApiUtilityWrapper()

    def teardown(self):
        log.info(("*" * 35) + " teardown " + ("*" * 35))

        try:
            """ Drop collection before disconnect """
            if self.collection_wrap is not None and self.collection_wrap.collection is not None:
                self.collection_wrap.drop()
        except Exception as e:
            pass

        try:
            """ Delete connection and reset configuration"""
            res = self.connection_wrap.list_connections()
            for i in res[0]:
                self.connection_wrap.remove_connection(i[0])
        except Exception as e:
            pass

    @pytest.fixture(scope="module", autouse=True)
    def initialize_env(self, request):
        """ clean log before testing """
        cf.modify_file([test_info.log_debug, test_info.log_info, test_info.log_err])
        log.info("[initialize_milvus] Log cleaned up, start testing...")

        host = request.config.getoption("--host")
        port = request.config.getoption("--port")
        handler = request.config.getoption("--handler")
        param_info.prepare_param_info(host, port, handler)


class TestcaseBase(Base):
    """
    Additional methods;
    Public methods that can be used to add cases.
    """

    @pytest.fixture(scope="module", params=ct.get_invalid_strs)
    def get_invalid_string(self, request):
        yield request.param

    @pytest.fixture(scope="module", params=cf.gen_simple_index())
    def get_index_param(self, request):
        yield request.param

    def _connect(self):
        """ Add an connection and create the connect """
        self.connection_wrap.add_connection(default={"host": param_info.param_host, "port": param_info.param_port})
        res, _ = self.connection_wrap.connect(alias='default')
        return res
    '''
    def _collection(self, **kwargs):
        """ Init a collection and return the object of collection """
        name = cf.gen_unique_str()
        schema = cf.gen_default_collection_schema()
        if self.connection_wrap.get_connection(alias='default') is None:
            self._connect()
        res, cr = self.collection_wrap.init_collection(name=name, schema=schema, **kwargs)
        return res
    '''

    def init_collection_wrap(self, name=None, data=None, schema=None, check_task=None, **kwargs):
        name = cf.gen_unique_str('coll_') if name is None else name
        schema = cf.gen_default_collection_schema() if schema is None else schema
        if self.connection_wrap.get_connection(alias='default')[0] is None:
            self._connect()
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=name, data=data, schema=schema,
                                     check_task=check_task, **kwargs)
        return collection_w

    def init_partition_wrap(self, collection_wrap, name=None, description=None,
                            check_task=None, check_items=None, **kwargs):
        name = cf.gen_unique_str("partition_") if name is None else name
        description = cf.gen_unique_str("partition_des_") if description is None else description
        collection_wrap = self.init_collection_wrap() if collection_wrap is None else collection_wrap
        partition_wrap = ApiPartitionWrapper()
        partition_wrap.init_partition(collection_wrap.collection, name, description,
                                      check_task=check_task, check_items=check_items,
                                      **kwargs)
        return partition_wrap
