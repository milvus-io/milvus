import pytest
import os

from pymilvus_orm.default_config import DefaultConfig
from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common.common_type import *
from check.param_check import *


class TestConnectionParams(TestcaseBase):
    """
    Test case of connections interface
    The author ： Ting.Wang
    """

    @pytest.mark.xfail(reason="Issue #5684")
    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_add_connection_kwargs_param_check(self):
        """
        target: test **kwargs of add_connection
        method: passing wrong parameters of **kwargs
        expected: assert response is error
        """

        # No check for **kwargs
        self.connection_wrap.add_connection(_kwargs=[1, 2])

        # get addr of default alias
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})

    @pytest.mark.xfail(reason="Issue #5723")
    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_connect_kwargs_param_check(self):
        """
        target: test **kwargs of connect
        method: passing wrong parameters of **kwargs
        expected: assert response is error
        """

        # get addr of default alias
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING)

        # No check for **kwargs
        res = self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, _kwargs=[1, 2])
        log.info(res[0].args[0])
        assert "Fail connecting to server" in res[0].args[0]

    @pytest.mark.xfail(reason="Feature #5725")
    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("alias", get_invalid_strs)
    def test_connection_connect_alias_param_check(self, alias):
        """
        target: test connect passes wrong params of alias
        method: connect passes wrong params of alias
        expected: assert response is error
        """

        # No check for alias
        res = self.connection_wrap.connect(alias=alias)
        log.info(res[0])

    @pytest.mark.xfail(reason="Feature #5725")
    @pytest.mark.parametrize("alias", get_invalid_strs)
    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_get_alias_param_check(self, alias):
        """
        target: test get connection passes wrong params of alias
        method: get connection passes wrong params of alias
        expected: assert response is error
        """

        # not check for alias
        res = self.connection_wrap.get_connection(alias=alias)
        log.info(res[0])

    @pytest.mark.xfail(reason="Feature #5725")
    @pytest.mark.parametrize("alias", get_invalid_strs)
    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_get_addr_alias_param_check(self, alias):
        """
        target: test get connection addr passes wrong params of alias
        method: get connection addr passes wrong params of alias
        expected: assert response is error
        """

        # not check for alias
        res = self.connection_wrap.get_connection_addr(alias=alias)
        log.info(res[0])

    @pytest.mark.xfail(reason="Feature #5725")
    @pytest.mark.parametrize("alias", get_invalid_strs)
    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_remove_alias_param_check(self, alias):
        """
        target: test remove connection passes wrong params of alias
        method: remove connection passes wrong params of alias
        expected: assert response is error
        """

        # not check for alias
        self._connect()
        res = self.connection_wrap.remove_connection(alias=alias)
        log.info(res[0])

    @pytest.mark.xfail(reason="Feature #5725")
    @pytest.mark.parametrize("alias", get_invalid_strs)
    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_disconnect_alias_param_check(self, alias):
        """
        target: test disconnect passes wrong params of alias
        method: disconnect passes wrong params of alias
        expected: assert response is error
        """

        # not check for alias
        self._connect()
        res = self.connection_wrap.disconnect(alias=alias)
        log.info(res[0])


class TestConnectionOperation(TestcaseBase):
    """
    Test case of connections interface
    The author ： Ting.Wang
    """

    @pytest.mark.xfail(reason="#5684")
    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_add_wrong_format(self):
        """
        target: test add_connection, regardless of whether the connection exists
        method: add existing and non-existing configurations at the same time
        expected: list_connections include the configured connections
        """

        # add connections
        self.connection_wrap.add_connection(alias1={"host": "localhost", "port": "1"},
                                            alias2={"port": "-1", "host": "hostlocal"},
                                            testing={"": ""})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None), 
                                                                            ('alias1', None), ('alias2', None), 
                                                                            ('testing', None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': 'localhost', 'port': '19530'}})
        self.connection_wrap.get_connection_addr(alias="alias1", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"host": "localhost", "port": "1"}})
        self.connection_wrap.get_connection_addr(alias="alias2", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"host": "hostlocal", "port": "-1"}})
        self.connection_wrap.get_connection_addr(alias="testing", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"": ""}})

    @pytest.mark.tags(CaseLabel.L3)
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
        self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                            check_items={"value_content": None})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None), 
                                                                            ('alias1', None), ('alias2', None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': 'localhost', 'port': '19530'}})
        self.connection_wrap.get_connection_addr(alias="alias1", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"host": "localhost", "port": "1"}})
        self.connection_wrap.get_connection_addr(alias="alias2", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"host": "192.168.1.1", "port": "123"}})

    @pytest.mark.tags(CaseLabel.L3)
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
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None),
                                                                            ('alias1', None), ('alias2', None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': 'localhost', 'port': '19530'}})
        self.connection_wrap.get_connection_addr(alias="alias1", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"host": "localhost", "port": "1"}})
        self.connection_wrap.get_connection_addr(alias="alias2", check_task=CheckTasks.check_connection_result,
                                                 check_items={
                                                     "dict_content": {"host": "192.168.1.1", "port": "123"}})

    @pytest.mark.tags(CaseLabel.L3)
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
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': 'localhost', 'port': '19530'}})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_add_cover_default(self):
        """
        target: add a connection to override the default connection
        method: add_connection passes alias of default and different configure
        expected: the configuration was successfully overwritten
        """

        # get all addr of default alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': 'localhost', 'port': '19530'}})

        # add connections
        self.connection_wrap.add_connection(default={'host': '192.168.1.1', 'port': '12345'})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': '192.168.1.1', 'port': '12345'}})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_get_addr_not_exist(self):
        """
        target: get addr of alias that is not exist and return {}
        method: get_connection_addr passes alias that is not exist
        expected: response of get_connection_addr is None
        """

        # get an addr that not exist and return {}
        self.connection_wrap.get_connection_addr(alias=Not_Exist, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {}})

    @pytest.mark.skip("The maximum number of add_connection is not set")
    def test_connection_add_max(self):
        """
        The maximum number of add_connection is not set
        """
        pass

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_add_after_connect(self, host, port):
        """
        target: add_connect passes different params after normal connect
        method: normal connection then add_connect passes different params
        expected: add_connect failed
        """

        # create connection that param of alias is not exist
        self.connection_wrap.connect(alias="test_alias_name", host=host, port=port, check_task=CheckTasks.check_connection_result)

        # add connection with diff params after that alias has been created
        err_msg = "alias of 'test_alias_name' already creating connections, "\
                  + "but the configure is not the same as passed in."
        self.connection_wrap.add_connection(test_alias_name={"host": "localhost", "port": "1"},
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": -1, "err_msg": err_msg})

        # add connection with the same params
        self.connection_wrap.add_connection(test_alias_name={"host": host, "port": port})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_add_after_default_connect(self, host, port):
        """
        target: add_connect passes different params after normal connect passes default alias
        method: normal connection then add_connect passes different params
        expected: add_connect failed
        """
        # create connection that param of alias is default
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=host, port=port,
                                     check_task=CheckTasks.check_connection_result)

        # add connection after that alias has been created
        err_msg = "alias of 'test_alias_name' already creating connections, " \
                  + "but the configure is not the same as passed in."
        self.connection_wrap.add_connection(default={"host": "localhost", "port": "1"},
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": -1, "err_msg": err_msg})

        # add connection with the same params
        self.connection_wrap.add_connection(test_alias_name={"host": host, "port": port})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_add_after_disconnect(self, host, port):
        """
        target: add_connect after normal connect、disconnect
        method: normal connect, disconnect then add connect passes the same alias
        expected: add_connect successfully
        """

        # create connection that param of alias is not exist
        self.connection_wrap.connect(alias="test_alias_name", host=host, port=port, check_task=CheckTasks.check_connection_result)

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias="test_alias_name")

        # get an addr that is exist
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"host": host, "port": port}})

        # add connection after that alias has been disconnected
        self.connection_wrap.add_connection(test_alias_name={"host": "localhost", "port": "1"})

        # get an addr that is exist
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"host": "localhost", "port": "1"}})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_add_after_remove(self, host, port):
        """
        target: add_connect after normal connect、remove_connection
        method: normal connect, remove_connection then add connect passes the same alias
        expected: add_connect successfully
        """

        # create connection that param of alias is not exist
        self.connection_wrap.connect(alias="test_alias_name", host=host, port=port, check_task=CheckTasks.check_connection_result)

        # disconnect alias is exist
        self.connection_wrap.remove_connection(alias="test_alias_name")

        # get an addr that is not exist
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {}})

        # add connection after that alias has been disconnected
        self.connection_wrap.add_connection(test_alias_name={"host": "localhost", "port": "1"})

        # get an addr that is exist
        self.connection_wrap.get_connection_addr(alias="test_alias_name", check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"host": "localhost", "port": "1"}})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_connect_alias_not_exist(self):
        """
        target: connect passes alias that is not exist and raise error
        method: connect passes alias that is not exist
        expected: response of connect is error
        """

        # create connection that param of alias is not exist
        err_msg = "You need to pass in the configuration of the connection named '%s'" % Not_Exist
        self.connection_wrap.connect(alias=Not_Exist, check_task=CheckTasks.err_res, check_items={"err_code": -1,
                                                                                                  "err_msg": err_msg})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': "localhost", 'port': "19530"}})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_connect_default_alias_invalid(self, port):
        """
        target: connect passes configure is not exist and raise error
        method: connect passes configure is not exist
        expected: response of connect is error
        """

        # add invalid default connection
        self.connection_wrap.add_connection(default={'host': "host", 'port': port})

        # using default alias to create connection, the connection does not exist
        err_msg = "Fail connecting to server on localhost:19530. Timeout"
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.err_res,
                                     check_items={"err_code": -1, "err_msg": err_msg})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': "host", 'port': port}})

    @ pytest.mark.tags(CaseLabel.L3)
    def test_connection_connect_default_alias_effective(self, host, port):
        """
        target: connect passes useful configure that adds by add_connect
        method: connect passes configure that add by add_connect
        expected: connect successfully
        """

        # add a valid default connection
        self.connection_wrap.add_connection(default={'host': host, 'port': port})

        # successfully created default connection
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING,
                                                                             Connect_Object_Name)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': host, 'port': port}})

    @pytest.mark.tags(CaseLabel.L3)
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
        self.connection_wrap.connect(alias=connect_name, check_task=CheckTasks.check_connection_result)

        # get the object of alias
        res_obj1 = self.connection_wrap.get_connection(alias=connect_name, check_task=CheckTasks.check_connection_result,
                                                       check_items={"value_content": Connect_Object_Name})[0]

        # connect twice with the same params
        self.connection_wrap.connect(alias=connect_name, host=host, port=port,
                                     check_task=CheckTasks.check_connection_result)

        # get the object of alias
        res_obj2 = self.connection_wrap.get_connection(alias=connect_name, check_task=CheckTasks.check_connection_result,
                                                       check_items={"value_content": Connect_Object_Name})[0]

        # check the response of the same alias is equal
        assert res_obj1 == res_obj2

        # connect twice with the different params
        err_msg = "The connection named default already creating," \
                  + " but passed parameters don't match the configured parameters,"
        self.connection_wrap.connect(alias=connect_name, host="host", port=port,
                                     check_task=CheckTasks.err_res, check_items={"err_code": -1, "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_nme"])
    def test_connection_connect_params(self, host, port, connect_name):
        """
        target: connect directly via parameters and return the object of connect successfully
        method: connect directly via parameters
        expected: response of connect is Milvus object
        """

        # successfully created default connection
        self.connection_wrap.connect(alias=connect_name, host=host, port=port, check_task=CheckTasks.check_connection_result)

        # get the object of alias
        self.connection_wrap.get_connection(alias=connect_name, check_task=CheckTasks.check_connection_result,
                                            check_items={"value_content": Connect_Object_Name})

        # list all connections and check the response
        list_content = [(connect_name, Connect_Object_Name)] if connect_name is DefaultConfig.DEFAULT_USING else\
            [(DefaultConfig.DEFAULT_USING, None), (connect_name, Connect_Object_Name)]
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": list_content})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=connect_name, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': host, 'port': port}})

    @pytest.mark.xfail(reason="5697")
    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_nme"])
    def test_connection_connect_wrong_params(self, host, port, connect_name):
        """
        target: connect directly via wrong parameters and raise error
        method: connect directly via wrong parameters
        expected: response of connect is error
        """

        # created connection with wrong connect name
        self.connection_wrap.connect(alias=connect_name, ip=host, port=port, check_task=CheckTasks.err_res,
                                     check_items={"err_code": -1,
                                                  "err_msg": "Param is not complete. Please invoke as follow:"})

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        dict_content = {'host': host, 'port': port} if connect_name == DefaultConfig.DEFAULT_USING else {}
        self.connection_wrap.get_connection_addr(alias=connect_name, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": dict_content})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, Not_Exist])
    def test_connection_disconnect_not_exist(self, connect_name):
        """
        target: disconnect passes alias that is not exist
        method: disconnect passes alias that is not exist
        expected: check connection list is normal
        """

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})
        # disconnect alias is not exist
        self.connection_wrap.disconnect(alias=connect_name)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {"host": "localhost", "port": "19530"}})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_disconnect_after_default_connect(self, host, port):
        """
        target: disconnect default connect and check result
        method: disconnect default connect
        expected: the connection was successfully terminated
        """

        # add a valid default connection
        self.connection_wrap.add_connection(default={'host': host, 'port': port})

        # successfully created default connection
        self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result)

        # get the object of alias
        self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                            check_items={"value_content": Connect_Object_Name})

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)

        # get the object of alias
        self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                            check_items={"value_content": None})

        # disconnect twice
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=DefaultConfig.DEFAULT_USING, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': host, 'port': port}})

    @pytest.mark.tags(CaseLabel.L3)
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
        self.connection_wrap.connect(alias=test_alias_name, host=host, port=port,
                                     check_task=CheckTasks.check_connection_result)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None),
                                                                            (test_alias_name, Connect_Object_Name)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=test_alias_name, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': host, 'port': port}})

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias=test_alias_name)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None),
                                                                            (test_alias_name, None)]})

        # get all addr of alias and check the response
        self.connection_wrap.get_connection_addr(alias=test_alias_name, check_task=CheckTasks.check_connection_result,
                                                 check_items={"dict_content": {'host': host, 'port': port}})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_remove_connection_not_exist(self):
        """
        target: remove connection that is not exist and check result
        method: remove connection that is not exist
        expected: connection list is normal
        """

        # remove the connection that is not exist
        self.connection_wrap.remove_connection(alias=Not_Exist)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": [(DefaultConfig.DEFAULT_USING, None)]})

    @pytest.mark.tags(CaseLabel.L3)
    def test_connection_remove_default_alias(self):
        """
        target: remove default alias connect and check result
        method: remove default alias connect
        expected: list connection and return {}
        """

        # remove the connection that is not exist
        self.connection_wrap.remove_connection(alias=DefaultConfig.DEFAULT_USING)

        # list all connections and check the response
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": []})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_name"])
    def test_connection_remove_after_connect(self, host, port, connect_name):
        """
        target: remove connection after connect and check result
        method: remove connection after connect
        expected: addr is None, response of list_connection still included that configure
        """

        # successfully created default connection
        self.connection_wrap.connect(alias=connect_name, host=host, port=port,
                                     check_task=CheckTasks.check_connection_result)

        # remove the connection that is not exist
        self.connection_wrap.remove_connection(alias=connect_name)

        # get the object of alias
        self.connection_wrap.get_connection(alias=DefaultConfig.DEFAULT_USING,
                                            check_task=CheckTasks.check_connection_result,
                                            check_items={"value_content": None})

        # list all connections and check the response
        list_content = [] if connect_name == DefaultConfig.DEFAULT_USING else [(DefaultConfig.DEFAULT_USING, None)]
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": list_content})

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING, "test_alias_name"])
    def test_connection_remove_after_disconnect(self, host, port, connect_name):
        """
        target: remove connection after disconnect and check result
        method: remove connection after disconnect
        expected: response of list_connection not included that configure
        """

        # successfully created default connection
        self.connection_wrap.connect(alias=connect_name, host=host, port=port,
                                     check_task=CheckTasks.check_connection_result)

        # disconnect alias is exist
        self.connection_wrap.disconnect(alias=connect_name)

        # remove connection
        self.connection_wrap.remove_connection(alias=connect_name)

        # remove twice connection
        self.connection_wrap.remove_connection(alias=connect_name)

        # list all connections and check the response
        list_content = [] if connect_name == DefaultConfig.DEFAULT_USING else [(DefaultConfig.DEFAULT_USING, None)]
        self.connection_wrap.list_connections(check_task=CheckTasks.check_connection_result,
                                              check_items={"list_content": list_content})
