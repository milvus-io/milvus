# import logging
# import pytest
#
# __version__ = '0.11.1'


# class TestPing:
#     def test_server_version(self, connect):
#         '''
#         target: test get the server version
#         method: call the server_version method after connected
#         expected: version should be the milvus version
#         '''
#         res = connect.server_version()
#         assert res == __version__
#
#     def test_server_status(self, connect):
#         '''
#         target: test get the server status
#         method: call the server_status method after connected
#         expected: status returned should be ok
#         '''
#         msg = connect.server_status()
#         assert msg
#
#     def test_server_cmd_with_params_version(self, connect):
#         '''
#         target: test cmd: version
#         method: cmd = "version" ...
#         expected: when cmd = 'version', return version of server;
#         '''
#         cmd = "version"
#         msg = connect._cmd(cmd)
#         logging.getLogger().info(msg)
#         assert msg == __version__
#
#     def test_server_cmd_with_params_others(self, connect):
#         '''
#         target: test cmd: lalala
#         method: cmd = "lalala" ...
#         expected: when cmd = 'version', return version of server;
#         '''
#         cmd = "rm -rf test"
#         msg = connect._cmd(cmd)
#
#     def test_connected(self, connect):
#         # assert connect.connected()
#         assert connect
#
#
# class TestPingWithTimeout:
#     def test_server_version_legal_timeout(self, connect):
#         '''
#         target: test get the server version with legal timeout
#         method: call the server_version method after connected with altering timeout
#         expected: version should be the milvus version
#         '''
#         res = connect.server_version(20)
#         assert res == __version__
#
#     def test_server_version_negative_timeout(self, connect):
#         '''
#         target: test get the server version with negative timeout
#         method: call the server_version method after connected with altering timeout
#         expected: when timeout is illegal raises an error;
#         '''
#         with pytest.raises(Exception) as e:
#             res = connect.server_version(-1)
#
#     def test_server_cmd_with_params_version_with_legal_timeout(self, connect):
#         '''
#         target: test cmd: version and timeout
#         method: cmd = "version" , timeout=10
#         expected: when cmd = 'version', return version of server;
#         '''
#         cmd = "version"
#         msg = connect._cmd(cmd, 10)
#         logging.getLogger().info(msg)
#         assert msg == __version__
#
#     def test_server_cmd_with_params_version_with_illegal_timeout(self, connect):
#         '''
#         target: test cmd: version and timeout
#         method: cmd = "version" , timeout=-1
#         expected: when timeout is illegal raises an error;
#         '''
#         with pytest.raises(Exception) as e:
#             res = connect.server_version(-1)
#
#     def test_server_cmd_with_params_others_with_illegal_timeout(self, connect):
#         '''
#         target: test cmd: lalala, timeout = -1
#         method: cmd = "lalala", timeout = -1
#         expected: when timeout is illegal raises an error;
#         '''
#         cmd = "rm -rf test"
#         with pytest.raises(Exception) as e:
#             res = connect.server_version(-1)
#
#
# class TestPingDisconnect:
#     def test_server_version(self, dis_connect):
#         '''
#         target: test get the server version, after disconnect
#         method: call the server_version method after connected
#         expected: version should not be the pymilvus version
#         '''
#         with pytest.raises(Exception) as e:
#             res = dis_connect.server_version()
#
#     def test_server_status(self, dis_connect):
#         '''
#         target: test get the server status, after disconnect
#         method: call the server_status method after connected
#         expected: status returned should be not ok
#         '''
#         with pytest.raises(Exception) as e:
#             res = dis_connect.server_status()
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_server_version_with_timeout(self, dis_connect):
#         '''
#         target: test get the server status with timeout settings after disconnect
#         method: call the server_status method after connected
#         expected: status returned should be not ok
#         '''
#         status = None
#         with pytest.raises(Exception) as e:
#             res = connect.server_status(100)
