import logging
import pytest
import pdb
from utils import *

__version__ = '0.11.0'


class TestPing:
    def test_server_version(self, client):
        '''
        target: test get the server version
        method: call the server_version method after connected
        expected: version should be the milvus version
        '''
        res = client.system_cmd("version")
        assert res == __version__

    def test_server_status(self, client):
        '''
        target: test get the server status
        method: call the server_status method after connected
        expected: status returned should be ok
        '''
        msg = client.system_cmd("status")
        assert msg 

    def test_server_cmd_with_params_version(self, client):
        '''
        target: test cmd: version
        method: cmd = "version" ...
        expected: when cmd = 'version', return version of server;
        '''
        cmd = "version"
        msg = client.system_cmd(cmd)
        logging.getLogger().info(msg)
        assert msg == __version__

    def test_server_cmd_with_params_others(self, client):
        '''
        target: test cmd: lalala
        method: cmd = "lalala" ...
        expected: when cmd = 'version', return version of server;
        '''
        cmd = "rm -rf test"
        msg = client.system_cmd(cmd)
        assert msg == default_unknown_cmd