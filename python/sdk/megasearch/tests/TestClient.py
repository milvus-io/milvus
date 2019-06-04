import logging
import pytest
import mock
import faker
import random
from faker.providers import BaseProvider

from megasearch.client.Client import Connection, Prepare
from megasearch.client.Abstract import (
    ConnectIntf, TableSchema,
    IndexType, ColumnType,
    ConnectParam, Column,
    VectorColumn, Range,
    CreateTablePartitionParam,
    DeleteTablePartitionParam,
    RowRecord, QueryRecord,
    QueryResult, TopKQueryResult,
)
from megasearch.client.Status import Status
from ..client.Exceptions import (
    ConnectParamMissingError,
    RepeatingConnectError,
    DisconnectNotConnectedClientError
)

from thrift.transport.TSocket import TSocket
from thrift.transport import TTransport
from ..thrift import ttypes

LOGGER = logging.getLogger(__name__)


class FakerProvider(BaseProvider):

    def table_name(self):
        return 'table_name' + str(random.randint(100, 999))

    def name(self):
        return 'name' + str(random.randint(100, 999))

    def dim(self):
        return random.randint(0, 999)


fake = faker.Faker()
fake.add_provider(FakerProvider)


def vector_column_factory():
    return {
        'name': fake.name(),
        'dimension': fake.dim(),
        'index_type': IndexType.IVFFLAT,
        'store_raw_vector': True
    }


def column_factory():
    return {
        'name': fake.table_name(),
        'type': IndexType.RAW
    }


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


class TestDisconnection:
    # def setUp(self):
    def test_disconnected_error(self):
        cnn = Connection()
        cnn.connect_status = Status(Status.INVALID)
        with pytest.raises(DisconnectNotConnectedClientError):
            cnn.disconnect()

    # @mock.patch.object(TTransport.TBufferedTransport, 'close')
    # def test_disconnected(self):


class TestPrepare:

    def test_column(self):
        param = {
            'name': 'test01',
            'type': ColumnType.DATE
        }
        res = Prepare.column(**param)
        LOGGER.error('{}'.format(res))
        assert res.name == 'test01'
        assert res.type == ColumnType.DATE
        assert isinstance(res, ttypes.Column)

    def test_vector_column(self):
        param = vector_column_factory()

        res = Prepare.vector_column(**param)
        LOGGER.error('{}'.format(res))
        assert isinstance(res, ttypes.VectorColumn)

    def test_table_schema(self):

        vec_params = [vector_column_factory() for i in range(10)]
        column_params = [column_factory() for i in range(5)]

        param = {
            'table_name': 'test03',
            'vector_columns': [Prepare.vector_column(**pa) for pa in vec_params],
            'attribute_columns': [Prepare.column(**pa) for pa in column_params],
            'partition_column_names': [str(x) for x in range(2)]
        }
        res = Prepare.table_schema(**param)
        assert isinstance(res, ttypes.TableSchema)

    def test_range(self):
        param = {
            'start': '200',
            'end': '1000'
        }

        res = Prepare.range(**param)
        LOGGER.error('{}'.format(res))
        assert isinstance(res, ttypes.Range)
        assert res.start_value == '200'
        assert res.end_value == '1000'

    def test_create_table_partition_param(self):
        # TODO Range factory
        param = {
            'table_name': fake.table_name(),
            'partition_name': fake.table_name(),
            'column_name_to_range':{}
        }



