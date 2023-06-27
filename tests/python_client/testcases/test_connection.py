import pytest
from pymilvus import DefaultConfig

from base.client_base import TestcaseBase
import common.common_type as ct
import common.common_func as cf
from common.code_mapping import ConnectionErrorMessage as cem

# CONNECT_TIMEOUT = 12


class TestConnectionParams(TestcaseBase):
    """
    Test case of connections interface
    The author ： Ting.Wang
    """

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("data", ct.get_dict_without_host_port)
    def test_connection_add_connection_kwargs_without_host_port(self, data):
        """
        target: test **kwargs of add_connection
        method: passing no value for host and port
        expected: host port will be filled with default value:
                  "host": "localhost",
                  "port": "19530"
        """
        # check param of **kwargs
        self.connection_wrap.add_connection(_kwargs=data)

        # get addr of default alias
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'address': 'localhost:19530',
                                                                                "user": ""}})
        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None),
                                                                             ('_kwargs', None)]})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("data", ct.get_dict_invalid_host_port)
    def test_connection_add_connection_kwargs_invalid_host_port(self, data):
        """
        target: test **kwargs of add_connection
        method: passing invalid value for host and port
        expected: report error
        """

        # check param of **kwargs
        self.connection_wrap.add_connection(_kwargs=data, check_task=ct.CheckTasks.err_res,
                                            check_items={ct.err_code: 1, ct.err_msg: cem.NoHostPort})

        # get addr of default alias
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'address': 'localhost:19530',
                                                                                "user": ""}})

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
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=1,
                                     check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 1, ct.err_msg: cem.NoHostPort})

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

    @pytest.mark.skip("get_connection is replaced by has_connection")
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
                                            alias2={"port": "2", "host": "hostlocal"},
                                            testing=data,
                                            check_task=ct.CheckTasks.err_res,
                                            check_items={ct.err_code: 0, ct.err_msg: err_msg})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None),
                                                                             ('alias1', None), ('alias2', None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'address': 'localhost:19530',
                                                                                "user": ""}})
        self.connection_wrap.get_connection_addr(alias="alias1", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {'address': 'localhost:1',
                                                                                "user": ""}})
        self.connection_wrap.get_connection_addr(alias="alias2", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": "hostlocal:2",
                                                                                "user": ""}})
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
        self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: False})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None),
                                                                             ('alias1', None), ('alias2', None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": "localhost:19530",
                                                                                "user": ""}})
        self.connection_wrap.get_connection_addr(alias="alias1", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": "localhost:1",
                                                                                "user": ""}})
        self.connection_wrap.get_connection_addr(alias="alias2", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": "192.168.1.1:123",
                                                                                "user": ""}})

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
                                                 check_items={ct.dict_content: {"address": "localhost:19530",
                                                                                "user": ""}})
        self.connection_wrap.get_connection_addr(alias="alias1", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": "localhost:1",
                                                                                "user": ""}})
        self.connection_wrap.get_connection_addr(alias="alias2", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": "192.168.1.1:123",
                                                                                "user": ""}})

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
                                                 check_items={ct.dict_content: {"address": "localhost:19530",
                                                                                "user": ""}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_add_cover_default(self):
        """
        target: add a connection to override the default connection
        method: add_connection passes alias of default and different configure
        expected: the configuration was successfully overwritten
        """

        # get all addr of default alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": "localhost:19530",
                                                                                "user": ""}})

        # add connections
        self.connection_wrap.add_connection(default={'host': '192.168.1.1', 'port': '12345'})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": "192.168.1.1:12345",
                                                                                "user": ""}})

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
        target: add_connection with different params after normal connect
        method: then add_connection with different params
        expected: add_connection failed
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
        target: add_connection with different params after default alias connected
        method: 1. connect with default alias
                2. add_connection with the same alias but different params
        expected: add_connection failed
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
        target: update connection params after connection disconnected
        method: 1. connect and disconnect a connection
                2. re-add connection by the same alias with different connection params
        expected: re-add_connection successfully with new params
        """

        # add a new connection and connect
        self.connection_wrap.connect(alias="test_alias_name", host=host, port=port, check_task=ct.CheckTasks.ccr)

        # disconnect the connection
        self.connection_wrap.disconnect(alias="test_alias_name")

        # get the connection address after it disconnected
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": f"{host}:{port}",
                                                                                "user": ""}})

        # re-add connection by the same alias with different connection params
        self.connection_wrap.add_connection(test_alias_name={"host": "localhost", "port": "1"})

        # re-get the connection address
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": "localhost:1",
                                                                                "user": ""}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_add_after_remove(self, host, port):
        """
        target: add_connection after normal connect、remove_connection
        method: 1. connect and remove_connection
                2. add connection by the same alias with different params
        expected: add_connection by the same alias with different params successfully
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
                                                 check_items={ct.dict_content: {"address": "localhost:1",
                                                                                "user": ""}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_connect_alias_not_exist(self):
        """
        target: connect with a non existing alias and raise error
        method: connect with a non existing alias
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
                                                 check_items={ct.dict_content: {"address": "localhost:19530",
                                                                                "user": ""}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_connect_default_alias_invalid(self, port):
        """
        target: connect with non existing params
        method: 1. add connection with non existing params
                2. try to connect
        expected: raise an exception
        """

        # add invalid default connection
        self.connection_wrap.add_connection(default={'host': "host", 'port': port})

        # using default alias to create connection, the connection does not exist
        err_msg = cem.FailConnect % ("host", str(port))
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2, ct.err_msg: err_msg})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": f"host:{port}",
                                                                                "user": ""}})

    @pytest.mark.tags(ct.CaseLabel.L0)
    def test_connection_connect_default_alias_effective(self, host, port):
        """
        target: verify connections by default alias
        method: 1. add connection with default alias
                2. connect with default alias
                3. list connections and get connection address
        expected: 1. add connection, connect, list and get connection address successfully
        """

        # add a valid default connection
        self.connection_wrap.add_connection(default={'host': host, 'port': port})

        # successfully created default connection
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING,
                                                                              "GrpcHandler")]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": f"{host}:{port}",
                                                                                "user": ""}})

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
        res_obj1 = self.connection_wrap.has_connection(alias=connect_name, check_task=ct.CheckTasks.ccr,
                                                       check_items={ct.value_content: ct.Connect_Object_Name})[0]

        # connect twice with the same params
        self.connection_wrap.connect(alias=connect_name, host=host, port=port, check_task=ct.CheckTasks.ccr)

        # get the object of alias
        res_obj2 = self.connection_wrap.has_connection(alias=connect_name, check_task=ct.CheckTasks.ccr,
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
        self.connection_wrap.has_connection(alias=connect_name, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: ct.Connect_Object_Name})

        # list all connections and check the response
        list_content = [(connect_name, "GrpcHandler")] if connect_name is DefaultConfig.DEFAULT_USING else \
            [(DefaultConfig.DEFAULT_USING, None), (connect_name, "GrpcHandler")]
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: list_content})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=connect_name, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": f"{host}:{port}",
                                                                                "user": ""}})
     
    @pytest.mark.skip("not support now")
    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_nme"])
    def test_connection_connect_wrong_params(self, host, port, connect_name):
        """
        target: connect directly via invalid parameters and raise error
        method: connect directly via invalid parameters
        expected: raise exception with error msg
        """

        # created connection with wrong connect name
        self.connection_wrap.connect(alias=connect_name, ip=host, port=port, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2,
                                                  ct.err_msg: cem.NoHostPort})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        dict_content = {'address': f"{DefaultConfig.DEFAULT_HOST}:{DefaultConfig.DEFAULT_PORT}",
                        'user': ''} if connect_name == DefaultConfig.DEFAULT_USING else {}
        self.connection_wrap.get_connection_addr(alias=connect_name, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: dict_content})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, ct.Not_Exist])
    def test_connection_disconnect_not_exist(self, connect_name):
        """
        target: disconnect with non existing alias
        method: disconnect with non existing alias
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
                                                 check_items={ct.dict_content: {"address": "localhost:19530",
                                                                                "user": ""}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_disconnect_after_default_connect(self, host, port):
        """
        target: check results after disconnect with default alias
        method: 1. connect with default alias
                2. get connection
                3. disconnect with default alias
                4. has connection
                5. disconnect again
                6. list connections and get connection address
        expected: the connection was successfully terminated
        """

        # add a valid default connection
        self.connection_wrap.add_connection(default={'host': host, 'port': port})

        # successfully created default connection
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr)

        # get the object of alias
        self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: ct.Connect_Object_Name})

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)

        # get the object of alias
        self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: False})

        # disconnect twice
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": f"{host}:{port}",
                                                                                "user": ""}})

    @pytest.mark.tags(ct.CaseLabel.L1)
    def test_connection_disconnect_after_connect(self, host, port):
        """
        target: disconnect with customized alias and check results
        method: 1. connect with customized alias
                2. disconnect with the alias
                3. list connections and get connection address
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
                                                                             test_alias_name, "GrpcHandler")]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=test_alias_name, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": f"{host}:{port}",
                                                                                "user": ""}})

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias=test_alias_name)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr,
                                              check_items={ct.list_content: [(DefaultConfig.DEFAULT_USING, None),
                                                                             (test_alias_name, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=test_alias_name, check_task=ct.CheckTasks.ccr,
                                                 check_items={ct.dict_content: {"address": f"{host}:{port}",
                                                                                "user": ""}})

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
        self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING, check_task=ct.CheckTasks.ccr,
                                            check_items={ct.value_content: False})

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


class TestConnect(TestcaseBase):
    """
    Test disconnect server
    """

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_name"])
    def test_close_repeatedly(self, host, port, connect_name):
        """
        target: test disconnect repeatedly
        method: disconnect a connected client, disconnect again
        expected: status ok after disconnected
        """
         # successfully created default connection
        self.connection_wrap.connect(alias=connect_name, host=host, port=port, check_task=ct.CheckTasks.ccr)
         
         # disconnect alias is exist 
        self.connection_wrap.disconnect(alias=connect_name)
        
         # disconnect alias is not exist
        self.connection_wrap.disconnect(alias=connect_name)

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("protocol", ["http", "ftp", "tcp"])
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    def test_parameters_with_uri_connection(self, host, port, connect_name, protocol):
        """
        target: test the uri parameter to get a normal connection
        method: get a connection with the uri parameter
        expected: connected is True
        """

        uri = "{}://{}:{}".format(protocol, host, port)
        self.connection_wrap.connect(alias=connect_name, uri=uri, check_task=ct.CheckTasks.ccr)

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    def test_parameters_with_address_connection(self, host, port, connect_name):
        """
        target: test the uri parameter to get a normal connection
        method: get a connection with the address parameter
        expected: connected is True
        """
        address = "{}:{}".format(host, port)
        self.connection_wrap.connect(alias=connect_name, address=address, check_task=ct.CheckTasks.ccr)

    @pytest.mark.tags(ct.CaseLabel.L3)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    def test_connect_with_default_user_password(self, host, port, connect_name):
        """
        target: test the user and password parameter to get a normal connection
        method: get a connection with the user and password parameter
        expected: connected is True
        """
        self.connection_wrap.connect(alias=connect_name, host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        res = self.utility_wrap.list_collections()[0]
        assert len(res) == 0


class TestConnectIPInvalid(TestcaseBase):
    """
    Test connect server with invalid ip
    """
    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("host", ct.get_not_string)
    def test_connect_with_invalid_ip(self, host, port):
        """
        target: test ip:port connect with invalid host value
        method: set host in get_not_string
        expected: connected is False
        """
        err_msg = "Type of 'host' must be str."
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=host, port=port,
                                     check_task=ct.CheckTasks.check_value_equal,
                                     check_items={ct.err_code: 1, ct.err_msg: err_msg})


class TestConnectPortInvalid(TestcaseBase):
    """
    Test connect server with invalid ip
    """

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("port", ct.get_not_string)
    def test_connect_with_invalid_port(self, host, port):
        """
        target: test ip:port connect with invalid port value
        method: set port in get_not_string
        expected: connected is False
        """
        err_msg = "Type of 'host' must be str."
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=host, port=port,
                                     check_task=ct.CheckTasks.check_value_equal,
                                     check_items={ct.err_code: 1, ct.err_msg: err_msg})


class TestConnectUriInvalid(TestcaseBase):
    """
    Test connect server with invalid uri , the result should be failed
    """
    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("protocol", ["quic,xxx"])
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    def test_parameters_with_invalid_protocol(self, host, port, connect_name, protocol):
        """
        target: test the protocol part of the uri parameter
        method: with non-existent protocols and unsupported protocols
        expected: the connection is false
        """

        uri = "{}://{}:{}".format(protocol, host, port)
        self.connection_wrap.connect(alias=connect_name, uri=uri, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 1})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("host", ["256.256.256.256", "10.1.0"])
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    @pytest.mark.parametrize("protocol", ["http", "https"])
    def test_parameters_with_invalid_host(self, host, port, connect_name, protocol):
        """
        target: test the host part of the uri parameter
        method: use a non-existent or wrong host
        expected: connection is False
        """

        uri = "{}://{}:{}".format(protocol, host, port)
        self.connection_wrap.connect(alias=connect_name, uri=uri, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("port", ["8080", "443", "0", "65534"])
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    @pytest.mark.parametrize("protocol", ["http", "https"])
    def test_parameters_with_invalid_port(self, host, port, connect_name, protocol):
        """
        target: test the port part of the uri parameter
        method: use a non-existent or wrong port
        expected: connection is False
        """

        uri = "{}://{}:{}".format(protocol, host, port)
        self.connection_wrap.connect(alias=connect_name, uri=uri, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("host", ["www.google.com"])
    @pytest.mark.parametrize("port", ["65534", "19530"])
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    @pytest.mark.parametrize("protocol", ["http", "https"])
    def test_parameters_with_invalid_url(self, host, port, connect_name, protocol):
        """
        target: test the host part of the uri parameter
        method: use a domain name that does not exist in error
        expected: connection is False
        """

        uri = "{}://{}:{}".format(protocol, host, port)
        self.connection_wrap.connect(alias=connect_name, uri=uri, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2})


class TestConnectAddressInvalid(TestcaseBase):
    """
    Test connect server with invalid address
    """
    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("host", ["192.168.1.256", "10.10.10.10"])
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    def test_parameters_with_invalid_address(self, host, port, connect_name):
        """
        target: create collection with invalid connection
        method: get a connection with the address parameter
        expected: connected is False
        """
        address = "{}:{}".format(host, port)
        self.connection_wrap.connect(alias=connect_name, address=address, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2})

    @pytest.mark.tags(ct.CaseLabel.L2)
    @pytest.mark.parametrize("port", ["100", "65536"])
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    def test_parameters_with_invalid_address_port(self, host, port, connect_name):
        """
        target: create collection with invalid connection
        method: get a connection with the address parameter
        expected: connected is False
        """
        address = "{}:{}".format(host, port)
        self.connection_wrap.connect(alias=connect_name, address=address, check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2})


class TestConnectUserPasswordInvalid(TestcaseBase):
    """
    Test connect server with user and password , the result should be failed
    """

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("port", ["19530"])
    def test_connect_without_user_password_after_authorization_enabled(self, host, port):
        """
        target: test connect without user password after authorization enabled
        method: connect without parameters of user and password
        excepted: connected is false
        """
        self.connection_wrap.connect(host=host, port=port,
                                     check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2,
                                                  ct.err_msg: "Fail connecting to server"})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["alice3333"])
    def test_connect_with_invalid_user_connection(self, host, port, user):
        """
        target: test the nonexistent to connect
        method: connect with the nonexistent user
        excepted: connected is false
        """
        self.connection_wrap.connect(host=host, port=port, user=user, password="abc123",
                                     check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2,
                                                  ct.err_msg: "Fail connecting to server"})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["anny015"])
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    def test_connect_with_password_invalid(self, host, port, user, connect_name):
        """
        target: test the wrong password when connecting
        method: connect with the wrong password
        excepted: connected is false
        """
        # 1.default user login
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        # 2.create a credential
        self.utility_wrap.create_user(user=user, password="qwaszx0")

        # 3.connect with the created user and wrong password
        self.connection_wrap.disconnect(alias=connect_name)
        self.connection_wrap.connect(host=host, port=port, user=user, password=ct.default_password,
                                     check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2,
                                                  ct.err_msg: "Fail connecting to server"})
