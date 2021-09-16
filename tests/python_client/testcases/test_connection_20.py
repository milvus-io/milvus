import pytest
import concurrent.futures
from pymilvus import DefaultConfig

from base.client_base import TestcaseBase
from utils.utils import *
import common.common_type as ct
import common.common_func as cf
from common.code_mapping import ConnectionErrorMessage as cem

CONNECT_TIMEOUT = 12


class TestConnectionParams(TestcaseBase):
    """
    Test case of connections interface
    The author ： Ting.Wang
    """

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("data", ct.get_dict_without_host_port)
    def test_connection_add_connection_kwargs_param_check(self, data):
        """
        target: test **kwargs of add_connection
        method: passing wrong parameters of **kwargs
        expected: assert response is error
        """

        # check param of **kwargs
        self.connection_wrap.add_connection(_kwargs=data, check_task=ct.CheckTasks.err_res,
                                            check_items={ct.err_code: 0, ct.err_msg: cem.NoHostPort})

        # get addr of default alias
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': 'localhost', 'port': '19530'}})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_connect_kwargs_param_check(self):
        """
        target: test **kwargs of connect
        method: passing wrong parameters of **kwargs
        expected: assert response is error
        """

        # get addr of default alias
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING)

        # No check for **kwargs
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, _kwargs=[1, 2],
                                     check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 0, ct.err_msg: cem.NoHostPort})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("alias", ct.get_not_string)
    def test_connection_connect_alias_param_check(self, alias):
        """
        target: test connect passes wrong params of alias
        method: connect passes wrong params of alias
        expected: assert response is error
        """

        # check for alias
        self.connection_wrap.connect(alias=alias, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 0, ct.err_msg: cem.AliasType % type(alias)})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("alias", ct.get_not_string)
    def test_connection_get_alias_param_check(self, alias):
        """
        target: test get connection passes wrong params of alias
        method: get connection passes wrong params of alias
        expected: assert response is error
        """

        # check for alias
        self.connection_wrap.get_connection(alias=alias, check_task=ct.CheckTasks.err_res,
                                            check_items={ct.err_code: 0, ct.err_msg: cem.AliasType % type(alias)})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("alias", ct.get_not_string)
    def test_connection_get_addr_alias_param_check(self, alias):
        """
        target: test get connection addr passes wrong params of alias
        method: get connection addr passes wrong params of alias
        expected: assert response is error
        """

        # check for alias
        self.connection_wrap.get_connection_addr(alias=alias, check_task=ct.CheckTasks.err_res,
                                                 check_items={ct.err_code: 0, ct.err_msg: cem.AliasType % type(alias)})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("alias", ct.get_not_string)
    def test_connection_remove_alias_param_check(self, alias):
        """
        target: test remove connection passes wrong params of alias
        method: remove connection passes wrong params of alias
        expected: assert response is error
        """

        # check for alias
        self._connect()
        self.connection_wrap.remove_connection(alias=alias, check_task=ct.CheckTasks.err_res,
                                               check_items={ct.err_code: 0, ct.err_msg: cem.AliasType % type(alias)})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("alias", ct.get_not_string)
    def test_connection_disconnect_alias_param_check(self, alias):
        """
        target: test disconnect passes wrong params of alias
        method: disconnect passes wrong params of alias
        expected: assert response is error
        """

        # check for alias
        self._connect()
        self.connection_wrap.disconnect(alias=alias, check_task=ct.CheckTasks.err_res,
                                        check_items={ct.err_code: 0, ct.err_msg: cem.AliasType % type(alias)})


class TestConnectionOperation(TestcaseBase):
    """
    Test case of connections interface
    The author ： Ting.Wang
    """

    @pytest.mark.tags(ct.CaseLabel.L1)
    @pytest.mark.parametrize("data, err_msg", [(ct.get_wrong_format_dict[0], cem.PortType),
                                               (ct.get_wrong_format_dict[1], cem.HostType)])
    def test_connection_add_wrong_format(self, data, err_msg):
        """
        target: test add_connection, regardless of whether the connection exists
        method: add existing and non-existing configurations at the same time
        expected: list_connections include the configured connections
        """

        # add connections
        self.connection_wrap.add_connection(alias1={"host": "localhost", "port": "1"},
                                            alias2={"port": "-1", "host": "hostlocal"},
                                            testing=data,
                                            check_task=ct.CheckTasks.err_res,
                                            check_items={ct.err_code: 0, ct.err_msg: err_msg})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None),
                                                                             ('alias1', None), ('alias2', None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': 'localhost', 'port': '19530'}})
        self.connection_wrap.get_connection_addr(alias="alias1", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": "localhost", "port": "1"}})
        self.connection_wrap.get_connection_addr(alias="alias2", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": "hostlocal", "port": "-1"}})
        self.connection_wrap.get_connection_addr(alias="testing", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_add_more(self):
        """
        target: test add_connection passes in multiple parameters
        method: add two params of add_connection
        expected: added to the connection list successfully
        """

        # add connections
        self.connection_wrap.add_connection(alias1={"host": "localhost", "port": "1"},
                                            alias2={"host": "192.168.1.1", "port": "123"})

        # get the object of alias
        self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: None})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None),
                                                                             ('alias1', None), ('alias2', None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': 'localhost', 'port': '19530'}})
        self.connection_wrap.get_connection_addr(alias="alias1", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": "localhost", "port": "1"}})
        self.connection_wrap.get_connection_addr(alias="alias2", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": "192.168.1.1", "port": "123"}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_add_single_more(self):
        """
        target: test add connections separately
        method: add_connection twice
        expected: added to the connection list successfully
        """

        # add connections
        self.connection_wrap.add_connection(alias1={"host": "localhost", "port": "1"})
        self.connection_wrap.add_connection(alias2={"host": "192.168.1.1", "port": "123"})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None),
                                                                             ('alias1', None), ('alias2', None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': 'localhost', 'port': '19530'}})
        self.connection_wrap.get_connection_addr(alias="alias1", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": "localhost", "port": "1"}})
        self.connection_wrap.get_connection_addr(alias="alias2", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": "192.168.1.1", "port": "123"}})

    @pytest.mark.tags(ct.CaseLabel.L0)
    def test_connection_add_default(self):
        """
        target: add_connection passes default params successfully
        method: add_connection passes default params
        expected: response of add_connection is normal
        """

        # add connections
        self.connection_wrap.add_connection(default={'host': 'localhost', 'port': '19530'})
        self.connection_wrap.add_connection(default={'port': '19530', 'host': 'localhost'})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': 'localhost', 'port': '19530'}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_add_cover_default(self):
        """
        target: add a connection to override the default connection
        method: add_connection passes alias of default and different configure
        expected: the configuration was successfully overwritten
        """

        # get all addr of default alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': 'localhost', 'port': '19530'}})

        # add connections
        self.connection_wrap.add_connection(default={'host': '192.168.1.1', 'port': '12345'})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': '192.168.1.1',
                                                                                'port': '12345'}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_get_addr_not_exist(self):
        """
        target: get addr of alias that is not exist and return {}
        method: get_connection_addr passes alias that is not exist
        expected: response of get_connection_addr is None
        """

        # get an addr that not exist and return {}
        self.connection_wrap.get_connection_addr(alias=ct.Not_Exist, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {}})

    @pytest.mark.skip("The maximum number of add_connection is not set")
    @pytest.mark.tags(ct.CaseLabel.L2)
    def test_connection_add_max(self):
        """
        The maximum number of add_connection is not set
        """
        pass

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_add_after_connect(self, host, port):
        """
        target: add_connect passes different params after normal connect
        method: normal connection then add_connect passes different params
        expected: add_connect failed
        """

        # create connection that param of alias is not exist
        self.connection_wrap.connect(alias="test_alias_name", host=host, port=port, check_task=ct.CheckTasks.ccr)

        # add connection with diff params after that alias has been created
        err_msg = cem.ConnDiffConf % "test_alias_name"
        self.connection_wrap.add_connection(test_alias_name={"host": "localhost", "port": "1"},
                                            check_task=ct.CheckTasks.err_res,
                                            check_items={ct.err_code: 0, ct.err_msg: err_msg})

        # add connection with the same params
        self.connection_wrap.add_connection(test_alias_name={"host": host, "port": port})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_add_after_default_connect(self, host, port):
        """
        target: add_connect passes different params after normal connect passes default alias
        method: normal connection then add_connect passes different params
        expected: add_connect failed
        """
        # create connection that param of alias is default
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=host, port=port,
                                     check_task=ct.CheckTasks.ccr)

        # add connection after that alias has been created
        err_msg = cem.ConnDiffConf % DefaultConfig.DEFAULT_USING
        self.connection_wrap.add_connection(default={"host": "localhost", "port": "1"},
                                            check_task=ct.CheckTasks.err_res,
                                            check_items={ct.err_code: 0, ct.err_msg: err_msg})

        # add connection with the same params
        self.connection_wrap.add_connection(test_alias_name={"host": host, "port": port})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_add_after_disconnect(self, host, port):
        """
        target: add_connect after normal connect、disconnect
        method: normal connect, disconnect then add connect passes the same alias
        expected: add_connect successfully
        """

        # create connection that param of alias is not exist
        self.connection_wrap.connect(alias="test_alias_name", host=host, port=port, check_task=ct.CheckTasks.ccr)

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias="test_alias_name")

        # get an addr that is exist
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": host, "port": port}})

        # add connection after that alias has been disconnected
        self.connection_wrap.add_connection(test_alias_name={"host": "localhost", "port": "1"})

        # get an addr that is exist
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": "localhost", "port": "1"}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_add_after_remove(self, host, port):
        """
        target: add_connect after normal connect、remove_connection
        method: normal connect, remove_connection then add connect passes the same alias
        expected: add_connect successfully
        """

        # create connection that param of alias is not exist
        self.connection_wrap.connect(alias="test_alias_name", host=host, port=port, check_task=ct.CheckTasks.ccr)

        # disconnect alias is exist
        self.connection_wrap.remove_connection(alias="test_alias_name")

        # get an addr that is not exist
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {}})

        # add connection after that alias has been disconnected
        self.connection_wrap.add_connection(test_alias_name={"host": "localhost", "port": "1"})

        # get an addr that is exist
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": "localhost", "port": "1"}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_connect_alias_not_exist(self):
        """
        target: connect passes alias that is not exist and raise error
        method: connect passes alias that is not exist
        expected: response of connect is error
        """

        # create connection that param of alias is not exist
        err_msg = cem.ConnLackConf % ct.Not_Exist
        self.connection_wrap.connect(alias=ct.Not_Exist, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 0, ct.err_msg: err_msg})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': "localhost", 'port': "19530"}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_connect_default_alias_invalid(self, port):
        """
        target: connect passes configure is not exist and raise error
        method: connect passes configure is not exist
        expected: response of connect is error
        """

        # add invalid default connection
        self.connection_wrap.add_connection(default={'host': "host", 'port': port})

        # using default alias to create connection, the connection does not exist
        err_msg = cem.FailConnect % ("host", str(port))
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: -1, ct.err_msg: err_msg})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': "host", 'port': port}})

    @pytest.mark.tags(ct.CaseLabel.L0)
    def test_connection_connect_default_alias_effective(self, host, port):
        """
        target: connect passes useful configure that adds by add_connect
        method: connect passes configure that add by add_connect
        expected: connect successfully
        """

        # add a valid default connection
        self.connection_wrap.add_connection(default={'host': host, 'port': port})

        # successfully created default connection
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING,
                                                                              ct.Connect_Object_Name)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': host, 'port': port}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    def test_connection_connect_repeat(self, host, port, connect_name):
        """
        target: connect twice and return the same object
        method: connect twice
        expected: return the same object of connect
        """

        # add a valid default connection
        self.connection_wrap.add_connection(default={'host': host, 'port': port})

        # successfully created default connection
        self.connection_wrap.connect(alias=connect_name, check_task=ct.CheckTasks.ccr)

        # get the object of alias
        res_obj1 = self.connection_wrap.get_connection(alias=connect_name, check_task=ct.CheckTasks.ccr,
                                                       check_items={ct.value_content: ct.Connect_Object_Name})[0]

        # connect twice with the same params
        self.connection_wrap.connect(alias=connect_name, host=host, port=port, check_task=ct.CheckTasks.ccr)

        # get the object of alias
        res_obj2 = self.connection_wrap.get_connection(alias=connect_name, check_task=ct.CheckTasks.ccr,
                                                       check_items={ct.value_content: ct.Connect_Object_Name})[0]

        # check the response of the same alias is equal
        assert res_obj1 == res_obj2

        # connect twice with the different params
        err_msg = cem.ConnDiffConf % "default"
        self.connection_wrap.connect(alias=connect_name, host="host", port=port,
                                     check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 0, ct.err_msg: err_msg})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_nme"])
    def test_connection_connect_params(self, host, port, connect_name):
        """
        target: connect directly via parameters and return the object of connect successfully
        method: connect directly via parameters
        expected: response of connect is Milvus object
        """

        # successfully created default connection
        self.connection_wrap.connect(alias=connect_name, host=host, port=port, check_task=ct.CheckTasks.ccr)

        # get the object of alias
        self.connection_wrap.get_connection(alias=connect_name, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: ct.Connect_Object_Name})

        # list all connections and check the response
        list_content = [(connect_name, ct.Connect_Object_Name)] if connect_name is DefaultConfig.DEFAULT_USING else \
            [(DefaultConfig.DEFAULT_USING, None), (connect_name, ct.Connect_Object_Name)]
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: list_content})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=connect_name, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': host, 'port': port}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_nme"])
    def test_connection_connect_wrong_params(self, host, port, connect_name):
        """
        target: connect directly via wrong parameters and raise error
        method: connect directly via wrong parameters
        expected: response of connect is error
        """

        # created connection with wrong connect name
        self.connection_wrap.connect(alias=connect_name, ip=host, port=port, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 0,
                                                  ct.err_msg: cem.NoHostPort})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        dict_content = {'host': DefaultConfig.DEFAULT_HOST,
                        'port': DefaultConfig.DEFAULT_PORT} if connect_name == DefaultConfig.DEFAULT_USING else {}
        self.connection_wrap.get_connection_addr(alias=connect_name, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: dict_content})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, ct.Not_Exist])
    def test_connection_disconnect_not_exist(self, connect_name):
        """
        target: disconnect passes alias that is not exist
        method: disconnect passes alias that is not exist
        expected: check connection list is normal
        """

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})
        # disconnect alias is not exist
        self.connection_wrap.disconnect(alias=connect_name)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING,
                                                 check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"host": "localhost", "port": "19530"}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_disconnect_after_default_connect(self, host, port):
        """
        target: disconnect default connect and check result
        method: disconnect default connect
        expected: the connection was successfully terminated
        """

        # add a valid default connection
        self.connection_wrap.add_connection(default={'host': host, 'port': port})

        # successfully created default connection
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr)

        # get the object of alias
        self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: ct.Connect_Object_Name})

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)

        # get the object of alias
        self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: None})

        # disconnect twice
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': host, 'port': port}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_disconnect_after_connect(self, host, port):
        """
        target: disconnect test connect and check result
        method: disconnect test connect
        expected: the connection was successfully terminated
        """
        test_alias_name = "test_alias_name"

        # add a valid default connection
        self.connection_wrap.add_connection(test_alias_name={'host': host, 'port': port})

        # successfully created default connection
        self.connection_wrap.connect(alias=test_alias_name, host=host, port=port, check_task=ct.CheckTasks.ccr)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None),
                                                                             (
                                                                             test_alias_name, ct.Connect_Object_Name)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=test_alias_name, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': host, 'port': port}})

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias=test_alias_name)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None),
                                                                             (test_alias_name, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=test_alias_name, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'host': host, 'port': port}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_remove_connection_not_exist(self):
        """
        target: remove connection that is not exist and check result
        method: remove connection that is not exist
        expected: connection list is normal
        """

        # remove the connection that is not exist
        self.connection_wrap.remove_connection(alias=ct.Not_Exist)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_remove_default_alias(self):
        """
        target: remove default alias connect and check result
        method: remove default alias connect
        expected: list connection and return {}
        """

        # remove the connection that is not exist
        self.connection_wrap.remove_connection(alias=DefaultConfig.DEFAULT_USING)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr, check_items={ct.list_content: []})

    @pytest.mark.tags(ct.CaseLabel.L1)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_name"])
    def test_connection_remove_after_connect(self, host, port, connect_name):
        """
        target: remove connection after connect and check result
        method: remove connection after connect
        expected: addr is None, response of list_connection still included that configure
        """

        # successfully created default connection
        self.connection_wrap.connect(alias=connect_name, host=host, port=port, check_task=ct.CheckTasks.ccr)

        # remove the connection that is not exist
        self.connection_wrap.remove_connection(alias=connect_name)

        # get the object of alias
        self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: None})

        # list all connections and check the response
        list_content = [] if connect_name == DefaultConfig.DEFAULT_USING else [(DefaultConfig.DEFAULT_USING, None)]
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr, check_items={ct.list_content: list_content})

    @pytest.mark.tags(ct.CaseLabel.L1)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_name"])
    def test_connection_remove_after_disconnect(self, host, port, connect_name):
        """
        target: remove connection after disconnect and check result
        method: remove connection after disconnect
        expected: response of list_connection not included that configure
        """

        # successfully created default connection
        self.connection_wrap.connect(alias=connect_name, host=host, port=port, check_task=ct.CheckTasks.ccr)

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias=connect_name)

        # remove connection
        self.connection_wrap.remove_connection(alias=connect_name)

        # remove twice connection
        self.connection_wrap.remove_connection(alias=connect_name)

        # list all connections and check the response
        list_content = [] if connect_name == DefaultConfig.DEFAULT_USING else [(DefaultConfig.DEFAULT_USING, None)]
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr, check_items={ct.list_content: list_content})

    @pytest.mark.tags(ct.CaseLabel.L1)
    # @pytest.mark.parametrize("collection_name, schema", [(cf.gen_unique_str('connection_test_'),
    #                                                       cf.gen_default_collection_schema())])
    def test_connection_init_collection_invalid_connection(self):
        """
        target: create collection with invalid connection
        method: init collection with invalid connection
        expected: check result
        """

        # init collection failed
        collection_name = cf.gen_unique_str('connection_test_')
        schema = cf.gen_default_collection_schema()
        self.collection_wrap.init_collection(name=collection_name, schema=schema, check_task=ct.CheckTasks.err_res,
                                             check_items={ct.err_code: 0,
                                                          ct.err_msg: cem.ConnectFirst},
                                             _using=ct.Not_Exist)

    @pytest.mark.tags(ct.CaseLabel.L1)
    # @pytest.mark.parametrize("collection_name, schema", [(cf.gen_unique_str('connection_test_'),
    #                                                       cf.gen_default_collection_schema())])
    def test_connection_init_collection_connection(self, host, port):
        """
        target: create collection then disconnection
        method: connection, init collection, then disconnection
        expected: check result
        """

        # successfully created default connection
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=host, port=port,
                                     check_task=ct.CheckTasks.ccr)

        # init collection successfully
        collection_name = cf.gen_unique_str('connection_test_')
        schema = cf.gen_default_collection_schema()
        self.collection_wrap.init_collection(name=collection_name, schema=schema, _using=DefaultConfig.DEFAULT_USING)

        # remove connection
        self.connection_wrap.remove_connection(alias=DefaultConfig.DEFAULT_USING)

        # drop collection failed
        self.collection_wrap.drop(check_task=ct.CheckTasks.err_res,
                                  check_items={ct.err_code: 0, ct.err_msg: "should create connect first"})

        # successfully created default connection
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=host, port=port,
                                     check_task=ct.CheckTasks.ccr)

        # drop collection success
        self.collection_wrap.drop()


class TestConnect:

    def local_ip(self, args):
        '''
        check if ip is localhost or not
        '''
        if not args["ip"] or args["ip"] == 'localhost' or args["ip"] == "127.0.0.1":
            return True
        else:
            return False

    @pytest.mark.tags(ct.CaseLabel.L2)
    def test_close_repeatedly(self, dis_connect, args):
        '''
        target: test disconnect repeatedly
        method: disconnect a connected client, disconnect again
        expected: raise an error after disconnected
        '''
        with pytest.raises(Exception) as e:
            dis_connect.close()

    @pytest.mark.tags(ct.CaseLabel.L2)
    def test_connect_uri(self, args):
        '''
        target: test connect with correct uri
        method: uri format and value are both correct
        expected: connected is True
        '''
        uri_value = "tcp://%s:%s" % (args["ip"], args["port"])
        milvus = get_milvus(args["ip"], args["port"], uri=uri_value, handler=args["handler"])

    @pytest.mark.tags(ct.CaseLabel.L2)
    def test_connect_uri_null(self, args):
        '''
        target: test connect with null uri
        method: uri set null
        expected: connected is True
        '''
        uri_value = ""
        if self.local_ip(args):
            milvus = get_milvus(None, None, uri=uri_value, handler=args["handler"])
        else:
            with pytest.raises(Exception) as e:
                milvus = get_milvus(None, None, uri=uri_value, handler=args["handler"])

    @pytest.mark.tags(ct.CaseLabel.L2)
    def test_connect_with_multiprocess(self, args):
        '''
        target: test uri connect with multiprocess
        method: set correct uri, test with multiprocessing connecting
        expected: all connection is connected
        '''

        def connect():
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            assert milvus

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_results = {executor.submit(
                connect): i for i in range(100)}
            for future in concurrent.futures.as_completed(future_results):
                future.result()

    @pytest.mark.tags(ct.CaseLabel.L2)
    def test_connect_repeatedly(self, args):
        '''
        target: test connect repeatedly
        method: connect again
        expected: status.code is 0, and status.message shows have connected already
        '''
        uri_value = "tcp://%s:%s" % (args["ip"], args["port"])
        milvus = Milvus(uri=uri_value, handler=args["handler"])
        milvus = Milvus(uri=uri_value, handler=args["handler"])


class TestConnectIPInvalid(object):
    """
    Test connect server with invalid ip
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ips()
    )
    def get_invalid_ip(self, request):
        yield request.param

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.timeout(CONNECT_TIMEOUT)
    def test_connect_with_invalid_ip(self, args, get_invalid_ip):
        ip = get_invalid_ip
        with pytest.raises(Exception) as e:
            milvus = get_milvus(ip, args["port"], args["handler"])


class TestConnectPortInvalid(object):
    """
    Test connect server with invalid ip
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ints()
    )
    def get_invalid_port(self, request):
        yield request.param

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.timeout(CONNECT_TIMEOUT)
    def test_connect_with_invalid_port(self, args, get_invalid_port):
        '''
        target: test ip:port connect with invalid port value
        method: set port in gen_invalid_ports
        expected: connected is False
        '''
        port = get_invalid_port
        with pytest.raises(Exception) as e:
            milvus = get_milvus(args["ip"], port, args["handler"])


class TestConnectURIInvalid(object):
    """
    Test connect server with invalid uri
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_uris()
    )
    def get_invalid_uri(self, request):
        yield request.param

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.timeout(CONNECT_TIMEOUT)
    def test_connect_with_invalid_uri(self, get_invalid_uri, args):
        '''
        target: test uri connect with invalid uri value
        method: set port in gen_invalid_uris
        expected: connected is False
        '''
        uri_value = get_invalid_uri
        with pytest.raises(Exception) as e:
            milvus = get_milvus(uri=uri_value, handler=args["handler"])
