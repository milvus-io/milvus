import logging
import pytest

__version__ = '0.10.0'


class TestPing:
    def test_server_version(self, connect):
        '''
        target: test get the server version
        method: call the server_version method after connected
        expected: version should be the pymilvus version
        '''
        res = connect.server_version()
        assert res == __version__

    def test_server_status(self, connect):
        '''
        target: test get the server status
        method: call the server_status method after connected
        expected: status returned should be ok
        '''
        msg = connect.server_status()
        assert msg == "OK"

    def test_server_cmd_with_params_version(self, connect):
        '''
        target: test cmd: version
        method: cmd = "version" ...
        expected: when cmd = 'version', return version of server;
        '''
        cmd = "version"
        msg = connect._cmd(cmd)
        logging.getLogger().info(msg)
        assert msg == __version__

    def test_server_cmd_with_params_others(self, connect):
        '''
        target: test cmd: lalala
        method: cmd = "lalala" ...
        expected: when cmd = 'version', return version of server;
        '''
        cmd = "rm -rf test"
        msg = connect._cmd(cmd)

    def test_connected(self, connect):
        # assert connect.connected()
        assert connect


class TestPingDisconnect:
    def test_server_version(self, dis_connect):
        '''
        target: test get the server version, after disconnect
        method: call the server_version method after connected
        expected: version should not be the pymilvus version
        '''
        with pytest.raises(Exception) as e:
            res = dis_connect.server_version()

    def test_server_status(self, dis_connect):
        '''
        target: test get the server status, after disconnect
        method: call the server_status method after connected
        expected: status returned should be not ok
        '''
        with pytest.raises(Exception) as e:
            res = dis_connect.server_status()
