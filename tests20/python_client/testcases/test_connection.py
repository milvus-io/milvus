import pytest

from pymilvus_orm.default_config import DefaultConfig
from base.client_request import ApiReq
from utils.util_log import test_log as log
from common.common_type import *


class TestConnectionParams(ApiReq):
    """
    Test case of connections interface
    The author ： Ting.Wang
    """

    @pytest.mark.skip("No check for **kwargs")
    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_kwargs_param_check(self):
        """
        target: test **kwargs of connection
        method: passing wrong parameters of **kwargs
        expected: assert response is error
        """

        # No check for **kwargs
        res, check_result = self.connection.configure(_kwargs=[1, 2])
        log.info(res)

        res, cr = self.connection.get_connection_addr(alias='default')
        assert res == {}

        # No check for **kwargs
        res, check_result = self.connection.create_connection(alias=DefaultConfig.DEFAULT_USING, _kwargs=[1, 2])
        log.info(res.args[0])
        assert res.args[0] == "Fail connecting to server on localhost:19530. Timeout"

    @pytest.mark.skip("No check for alias")
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("alias", get_invalid_strs)
    def test_connection_create_alias_param_check(self, alias):
        """
        target: test create connection with wrong params of alias
        method: create connection with wrong params of alias
        expected: assert response is error
        """
        # No check for alias
        res, cr = self.connection.create_connection(alias=alias)
        log.info(res)

    @pytest.mark.skip("No check for alias")
    @pytest.mark.parametrize("alias", get_invalid_strs)
    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_get_alias_param_check(self, alias):
        """
        target: test get connection with wrong params of alias
        method: get connection with wrong params of alias
        expected: assert response is error
        """
        # not check for alias
        res, cr = self.connection.get_connection(alias=alias)
        log.info(res)

    @pytest.mark.skip("No check for alias")
    @pytest.mark.parametrize("alias", get_invalid_strs)
    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_get_addr_alias_param_check(self, alias):
        """
        target: test get connection addr with wrong params of alias
        method: get connection addr with wrong params of alias
        expected: assert response is error
        """
        # not check for alias
        res, cr = self.connection.get_connection_addr(alias=alias)
        log.info(res)

    @pytest.mark.skip("No check for alias")
    @pytest.mark.parametrize("alias", get_invalid_strs)
    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_remove_alias_param_check(self, alias):
        """
        target: test remove connection with wrong params of alias
        method: remove connection with wrong params of alias
        expected: assert response is error
        """
        # not check for alias
        self._connect()
        res, cr = self.connection.remove_connection(alias=alias)
        log.info(res)


class TestConnectionOperation(ApiReq):
    """
    Test case of connections interface
    The author ： Ting.Wang
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_configure_repeat(self, host, port):
        """
        target: test connection configure four times
        method: connection configure twice with the same params
        expected: assert the configuration is successful
        """
        self.connection.configure(default={"host": host, "port": port}, dev={"host": host, "port": port})
        assert self.connection.list_connections()[0] == ['default', 'dev']
        assert self.connection.get_connection_addr(alias='default')[0] == {}

        self.connection.configure(default={"host": host, "port": port}, dev={"host": host, "port": port})
        assert self.connection.list_connections()[0] == ['default', 'dev']

        self.connection.configure(default1={"host": host, "port": port})
        assert self.connection.list_connections()[0] == ['default1']

        self.connection.configure()
        assert self.connection.list_connections()[0] == []

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_remove_connection_not_exist(self):
        """
        target: test remove connection that is not exist
        method: 1、remove connection that is not exist
                2、create connection with default alias
                3、remove connection that is not exist
        expected: assert alias of Not_exist is not exist
        """
        res, cr = self.connection.remove_connection(alias=Not_Exist, check_res="")
        assert res.args[0] == "There is no connection with alias '%s'." % Not_Exist

        self._connect()

        res, cr = self.connection.remove_connection(alias=Not_Exist, check_res="")
        assert res.args[0] == "There is no connection with alias '%s'." % Not_Exist

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_remove_connection_repeat(self):
        """
        target: test remove connection twice
        method: remove connection twice
        expected: assert the second response is an error
        """
        self._connect()

        self.connection.remove_connection(alias='default')

        res, cr = self.connection.remove_connection(alias='default', check_res='')
        assert res.args[0] == "There is no connection with alias 'default'."

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_normal_remove_connection_repeat(self, host, port):
        """
        target: test remove connection twice
        method: remove connection twice
        expected: assert the responses are True
        """
        self.connection.configure(default={"host": host, "port": port}, dev={"host": host, "port": port})

        self.connection.create_connection(alias='default')
        res, cr = self.connection.get_connection_addr(alias='default')
        assert res["host"] == host
        assert res["port"] == port
        self.connection.create_connection(alias='dev')

        self.connection.remove_connection(alias='default')
        self.connection.remove_connection(alias='dev')

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_remove_connection_100_repeat(self):
        """
        target: test delete the same connection 100 times
        method: delete the same connection 100 times
        expected: assert the remaining 99 delete errors
        """
        self._connect()
        self.connection.remove_connection(alias='default')

        for i in range(100):
            res, cr = self.connection.remove_connection(alias='default', check_res='')
            assert res.args[0] == "There is no connection with alias 'default'."

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_configure_remove_connection(self, host, port):
        """
        target: test remove configure alias
        method: remove configure alias
        expected: assert res is err
        """
        self.connection.configure(default={"host": host, "port": port})
        alias_name = 'default'

        res, cr = self.connection.remove_connection(alias=alias_name, check_res='')
        assert res.args[0] == "There is no connection with alias '%s'." % alias_name

    @pytest.mark.skip("error res")
    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_create_connection_remove_configure(self, host, port):
        """
        target: test create connection before remove configure
        method: create connection before reset configure
        expected: assert res
        """
        alias_name = "default"
        self.connection.create_connection(alias=alias_name, host=host, port=port)
        self.connection.configure()
        self.connection.get_connection(alias=alias_name)

    @pytest.mark.skip("error res")
    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_create_connection_reset_configure(self, host, port):
        """
        target: test params of create connection are different with configure
        method: params of create connection are different with configure
        expected: assert res
        """
        alias_name = "default"
        self.connection.create_connection(alias=alias_name, host=host, port=port)
        self.connection.configure(default={'host': host, 'port': port})
        self.connection.get_connection(alias=alias_name)

    @pytest.mark.skip("res needs to be confirmed")
    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_create_connection_diff_configure(self, host, port):
        """
        target: test params of create connection are different with configure
        method: params of create connection are different with configure
        expected: assert res
        """
        # error
        self.connection.configure(default={"host": 'host', "port": port})
        res, cr = self.connection.create_connection(alias="default", host=host, port=port, check_res='')
        log.info(res)
        res, cr = self.connection.create_connection(alias="default", host=host, port=port, check_res='')
        log.info(res)

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_create_connection_repeat(self, host, port):
        """
        target: test create connection twice
        method: create connection twice
        expected: res is True
        """
        self._connect()
        self.connection.get_connection(alias='default')

        self.connection.create_connection(alias='default', host=host, port=port)
        self.connection.get_connection(alias='default')

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_create_connection_not_exist(self, port):
        """
        target: test create connection is not exist
        method: create connection with not exist link
        expected: assert res is wrong
        """
        self.connection.get_connection(alias='default', check_res=CheckParams.false)
        res, cr = self.connection.create_connection(alias="default", host='host', port=port, check_res='')
        assert res.args[0] == "Fail connecting to server on host:19530. Timeout"

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_create_remove(self, host, port):
        """
        target: test create and remove connection twice
        method: create and remove connection twice
        expected: assert res is correct
        """
        alias_name = "default"
        self.connection.create_connection(alias=alias_name, host=host, port=port)
        self.connection.get_connection(alias=alias_name)
        self.connection.remove_connection(alias=alias_name)
        self.connection.get_connection(alias=alias_name, check_res=CheckParams.false)
        self.connection.create_connection(alias=alias_name, host=host, port=port)
        self.connection.get_connection(alias=alias_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_list_configure(self, host, port):
        """
        target: test list connection of configure
        method:  list connection of configure
        expected: assert res is correct
        """
        self.connection.configure(default={"host": host, "port": port}, dev={"host": host, "port": port})
        self.connection.create_connection(alias="default")
        assert self.connection.list_connections()[0] == ['default', 'dev']

    @pytest.mark.skip("Behavior to be determined")
    @pytest.mark.tags(CaseLabel.L1)
    def test_connection_list_create_configure(self, host, port):
        """
        target: test list connection of configure
        method:  list connection of configure
        expected: assert res is correct
        """
        self.connection.create_connection(alias="default1", host=host, port=port)
        self.connection.configure(default={"host": host, "port": port}, dev={"host": host, "port": port})
        log.info(self.connection.list_connections()[0])
        assert self.connection.list_connections()[0] == ['default', 'dev']




