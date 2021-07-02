import pytest
import sys
from pymilvus_orm.default_config import DefaultConfig

sys.path.append("..")
from base.connections_wrapper import ApiConnectionsWrapper
from base.collection_wrapper import ApiCollectionWrapper
from base.partition_wrapper import ApiPartitionWrapper
from base.index_wrapper import ApiIndexWrapper
from base.utility_wrapper import ApiUtilityWrapper
from base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper

from config.log_config import log_config
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from check.param_check import ip_check, number_check


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
    collection_schema_wrap = None
    field_schema_wrap = None
    collection_object_list = []

    def setup_class(self):
        log.info("[setup_class] Start setup class...")

    def teardown_class(self):
        log.info("[teardown_class] Start teardown class...")
        pass

    def setup(self):
        log.info(("*" * 35) + " setup " + ("*" * 35))
        self.connection_wrap = ApiConnectionsWrapper()
        self.utility_wrap = ApiUtilityWrapper()
        self.collection_wrap = ApiCollectionWrapper()
        self.partition_wrap = ApiPartitionWrapper()
        self.index_wrap = ApiIndexWrapper()
        self.collection_schema_wrap = ApiCollectionSchemaWrapper()
        self.field_schema_wrap = ApiFieldSchemaWrapper()

    def teardown(self):
        log.info(("*" * 35) + " teardown " + ("*" * 35))

        try:
            """ Drop collection before disconnect """
            if self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING)[0] is None:
                self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=param_info.param_host,
                                             port=param_info.param_port)

            for collection_object in self.collection_object_list:
                if collection_object is not None \
                        and collection_object.name in self.utility_wrap.list_collections()[0]:
                    collection_object.drop()

        except Exception as e:
            log.debug(str(e))

        try:
            """ Delete connection and reset configuration"""
            res = self.connection_wrap.list_connections()
            for i in res[0]:
                self.connection_wrap.remove_connection(i[0])

            # because the connection is in singleton mode, it needs to be restored to the original state after teardown
            self.connection_wrap.add_connection(default={"host": DefaultConfig.DEFAULT_HOST,
                                                         "port": DefaultConfig.DEFAULT_PORT})
        except Exception as e:
            log.debug(str(e))

    @pytest.fixture(scope="module", autouse=True)
    def initialize_env(self, request):
        """ clean log before testing """
        host = request.config.getoption("--host")
        port = request.config.getoption("--port")
        handler = request.config.getoption("--handler")
        clean_log = request.config.getoption("--clean_log")

        """ params check """
        assert ip_check(host) and number_check(port)

        """ modify log files """
        cf.modify_file(file_path_list=[log_config.log_debug, log_config.log_info, log_config.log_err], is_modify=clean_log)

        log.info("#" * 80)
        log.info("[initialize_milvus] Log cleaned up, start testing...")
        param_info.prepare_param_info(host, port, handler)


class TestcaseBase(Base):
    """
    Additional methods;
    Public methods that can be used to add cases.
    """

    # move to conftest.py
    # @pytest.fixture(scope="module", params=ct.get_invalid_strs)
    # def get_invalid_string(self, request):
    #     yield request.param
    #
    # @pytest.fixture(scope="module", params=cf.gen_simple_index())
    # def get_index_param(self, request):
    #     yield request.param

    def _connect(self):
        """ Add an connection and create the connect """
        res, is_succ = self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=param_info.param_host,
                                                    port=param_info.param_port)
        return res

    def init_collection_wrap(self, name=None, schema=None, check_task=None, check_items=None, **kwargs):
        name = cf.gen_unique_str('coll_') if name is None else name
        schema = cf.gen_default_collection_schema() if schema is None else schema
        if self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING)[0] is None:
            self._connect()
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=name, schema=schema, check_task=check_task, check_items=check_items, **kwargs)
        self.collection_object_list.append(collection_w)
        return collection_w

    def init_partition_wrap(self, collection_wrap=None, name=None, description=None,
                            check_task=None, check_items=None, **kwargs):
        name = cf.gen_unique_str("partition_") if name is None else name
        description = cf.gen_unique_str("partition_des_") if description is None else description
        collection_wrap = self.init_collection_wrap() if collection_wrap is None else collection_wrap
        partition_wrap = ApiPartitionWrapper()
        partition_wrap.init_partition(collection_wrap.collection, name, description,
                                      check_task=check_task, check_items=check_items,
                                      **kwargs)
        return partition_wrap

    def init_collection_general(self, prefix, insert_data=False, nb=ct.default_nb,
                                partition_num=0, is_binary=False, is_all_data_type=False):
        """
        target: create specified collections
        method: 1. create collections (binary/non-binary)
                2. create partitions if specified
                3. insert specified binary/non-binary data
                   into each partition if any
        expected: return collection and raw data
        """
        log.info("Test case of search interface: initialize before test case")
        self._connect()
        collection_name = cf.gen_unique_str(prefix)
        vectors = []
        binary_raw_vectors = []
        # 1 create collection
        default_schema = cf.gen_default_collection_schema()
        if is_binary:
            default_schema = cf.gen_default_binary_collection_schema()
        if is_all_data_type:
            default_schema = cf.gen_collection_schema_all_datatype()
        log.info("init_collection_general: collection creation")
        collection_w = self.init_collection_wrap(name=collection_name,
                                                 schema=default_schema)
        # 2 add extra partitions if specified (default is 1 partition named "_default")
        if partition_num > 0:
            cf.gen_partitions(collection_w, partition_num)
        # 3 insert data if specified
        if insert_data:
            collection_w, vectors, binary_raw_vectors = \
                cf.insert_data(collection_w, nb, is_binary, is_all_data_type)
            assert collection_w.is_empty is False
            assert collection_w.num_entities == nb
            collection_w.load()

        return collection_w, vectors, binary_raw_vectors
