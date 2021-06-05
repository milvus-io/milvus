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
    partition_mul = None
    collection_mul = None

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
        self.partition_mul = ()
        self.collection_mul = ()

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

    def _collection(self, name=None, data=None, schema=None, check_res=None, c_object=None, **kwargs):
        """ Init a collection and return the object of collection """
        name = cf.gen_unique_str("ApiReq") if name is None else name
        schema = cf.gen_default_collection_schema() if schema is None else schema
        c_object = self.collection_wrap if c_object is None else c_object

        self._connect()

        res, cr = c_object.collection_init(name=name, data=data, schema=schema, check_res=check_res, **kwargs)
        assert name == c_object.name
        return res

    def _partition(self, c_object=None, p_object=None, name=None, descriptions=None, **kwargs):
        """ Init a partition in a collection and return the object of partition """
        c_object = self.collection_wrap.collection if c_object is None else c_object
        p_object = self.partition_wrap if p_object is None else p_object
        name = cf.gen_unique_str("partition_") if name is None else name
        descriptions = cf.gen_unique_str("partition_des_") if descriptions is None else descriptions

        res, cr = p_object.partition_init(c_object, name, description=descriptions, **kwargs)
        return res

    def _collection_object_multiple(self, mul_number=2):
        """ Initialize multiple objects of collection and return the list of objects """
        for i in range(int(mul_number)):
            par = ApiCollectionWrapper()
            self.collection_mul += (par, )
        log.debug("[_collection_object_multiple] All objects of collection are : %s" % str(self.collection_mul))
        return self.collection_mul

    def _partition_object_multiple(self, mul_number=2):
        """ Initialize multiple objects of partition in a collection and return the list of objects """
        for i in range(int(mul_number)):
            par = ApiPartitionWrapper()
            self.partition_mul += (par, )
        log.debug("[_partition_object_multiple] All objects of partition are : %s" % str(self.partition_mul))
        return self.partition_mul