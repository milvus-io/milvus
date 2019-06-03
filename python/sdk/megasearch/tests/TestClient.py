import logging
import pytest
import mock

from megasearch.client.Client import Connection
from megasearch.client.Abstract import ConnectParam
from megasearch.client.Status import Status
from ..client.Exceptions import (ConnectParamMissingError,
                                 RepeatingConnectError)

from ..thrift.ttypes import Exception as TException

from thrift.transport.TSocket import TSocket

LOGGER = logging.getLogger(__name__)


class TestConnection:
    param = ConnectParam(host='0.0.0.0', port='5000')

    @mock.patch.object(TSocket, 'open')
    def test_true_connect(self, open):
        open.return_value = None
        cnn = Connection()

        with pytest.raises(ConnectParamMissingError):
            cnn.connect()

        cnn.connect(self.param)
        assert cnn.connect_status == Status.OK
        assert cnn.connected

        with pytest.raises(RepeatingConnectError):
            cnn.connect(*self.param)
            cnn.connect()

    def test_false_connect(self):
        cnn = Connection()

        cnn.connect(self.param)
        assert cnn.connect_status != Status.OK
        assert cnn.connect_status == Status.CONNECT_FAILED

    def test_disconnected(self):
        pass


