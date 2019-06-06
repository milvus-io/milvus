import logging
import pytest
import mock
import faker
import random
from faker.providers import BaseProvider

from client.Client import MegaSearch, Prepare, IndexType, ColumnType
from client.Status import Status
from client.Exceptions import (
    RepeatingConnectError,
    DisconnectNotConnectedClientError
)

from thrift.transport.TSocket import TSocket
from megasearch.thrift import ttypes, MegasearchService

LOGGER = logging.getLogger(__name__)


class FakerProvider(BaseProvider):

    def table_name(self):
        return 'table_name' + str(random.randint(1000, 9999))

    def name(self):
        return 'name' + str(random.randint(1000, 9999))

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


def range_factory():
    return {
        'start': str(random.randint(1, 10)),
        'end': str(random.randint(11, 20)),
    }


def table_schema_factory():
    vec_params = [vector_column_factory() for i in range(10)]
    column_params = [column_factory() for i in range(5)]
    param = {
            'table_name': fake.table_name(),
            'vector_columns': [Prepare.vector_column(**pa) for pa in vec_params],
            'attribute_columns': [Prepare.column(**pa) for pa in column_params],
            'partition_column_names': [str(x) for x in range(2)]
        }
    return Prepare.table_schema(**param)


def create_table_partition_param_factory():
    param = {
            'table_name': fake.table_name(),
            'partition_name': fake.table_name(),
            'column_name_to_range': {fake.name(): range_factory() for _ in range(3)}
        }
    return Prepare.create_table_partition_param(**param)


def delete_table_partition_param_factory():
    param = {
        'table_name': fake.table_name(),
        'partition_names': [fake.name() for i in range(5)]
    }
    return Prepare.delete_table_partition_param(**param)


def row_record_factory():
    param = {
        'column_name_to_vector': {fake.name(): [random.random() for i in range(256)]},
        'column_name_to_attribute': {fake.name(): fake.name()}
    }
    return Prepare.row_record(**param)


class TestConnection:
    param = {'host':'localhost', 'port': '5000'}

    @mock.patch.object(TSocket, 'open')
    def test_true_connect(self, open):
        open.return_value = None
        cnn = MegaSearch()

        cnn.connect(**self.param)
        assert cnn.status == Status.OK
        assert cnn.connected
        assert isinstance(cnn.client, MegasearchService.Client)

        with pytest.raises(RepeatingConnectError):
            cnn.connect(**self.param)
            cnn.connect()

    def test_false_connect(self):
        cnn = MegaSearch()

        cnn.connect(self.param)
        assert cnn.status != Status.OK

    def test_disconnected_error(self):
        cnn = MegaSearch()
        cnn.connect_status = Status(Status.INVALID)
        with pytest.raises(DisconnectNotConnectedClientError):
            cnn.disconnect()


class TestTable:

    @pytest.fixture
    @mock.patch.object(TSocket, 'open')
    def client(self, open):
        param = {'host': 'localhost', 'port': '5000'}
        open.return_value = None

        cnn = MegaSearch()
        cnn.connect(**param)
        return cnn

    @mock.patch.object(MegasearchService.Client, 'CreateTable')
    def test_create_table(self, CreateTable, client):
        CreateTable.return_value = None

        param = table_schema_factory()
        res = client.create_table(param)
        assert res == Status.OK

    def test_false_create_table(self, client):
        param = table_schema_factory()
        res = client.create_table(param)
        LOGGER.error('{}'.format(res))
        assert res != Status.OK

    @mock.patch.object(MegasearchService.Client, 'DeleteTable')
    def test_delete_table(self, DeleteTable, client):
        DeleteTable.return_value = None
        table_name = 'fake_table_name'
        res = client.delete_table(table_name)
        assert res == Status.OK

    def test_false_delete_table(self, client):
        table_name = 'fake_table_name'
        res = client.delete_table(table_name)
        assert res != Status.OK


class TestVector:

    @pytest.fixture
    @mock.patch.object(TSocket, 'open')
    def client(self, open):
        param = {'host': 'localhost', 'port': '5000'}
        open.return_value = None

        cnn = MegaSearch()
        cnn.connect(**param)
        return cnn

    @mock.patch.object(MegasearchService.Client, 'CreateTablePartition')
    def test_create_table_partition(self, CreateTablePartition, client):
        CreateTablePartition.return_value = None

        param = create_table_partition_param_factory()
        res = client.create_table_partition(param)
        assert res == Status.OK

    def test_false_table_partition(self, client):
        param = create_table_partition_param_factory()
        res = client.create_table_partition(param)
        assert res != Status.OK

    @mock.patch.object(MegasearchService.Client, 'DeleteTablePartition')
    def test_delete_table_partition(self, DeleteTablePartition, client):
        DeleteTablePartition.return_value = None

        param = delete_table_partition_param_factory()
        res = client.delete_table_partition(param)
        assert res == Status.OK

    def test_false_delete_table_partition(self, client):
        param = delete_table_partition_param_factory()
        res = client.delete_table_partition(param)
        assert res != Status.OK

    @mock.patch.object(MegasearchService.Client, 'AddVector')
    def test_add_vector(self, AddVector, client):
        AddVector.return_value = None

        param ={
            'table_name': fake.table_name(),
            'records': [row_record_factory() for _ in range(1000)]
        }
        res, ids = client.add_vector(**param)
        assert res == Status.OK

    def test_false_add_vector(self, client):
        param ={
            'table_name': fake.table_name(),
            'records': [row_record_factory() for _ in range(1000)]
        }
        res, ids = client.add_vector(**param)
        assert res != Status.OK

    @mock.patch.object(MegasearchService.Client, 'SearchVector')
    def test_search_vector(self, SearchVector, client):
        SearchVector.return_value = None
        param = {
            'table_name': fake.table_name(),
            'query_records': [row_record_factory() for _ in range(1000)],
            'top_k':  random.randint(0,10)
        }
        res, results = client.search_vector(**param)
        assert res == Status.OK

    def test_false_vector(self, client):
        param = {
            'table_name': fake.table_name(),
            'query_records': [row_record_factory() for _ in range(1000)],
            'top_k':  random.randint(0,10)
        }
        res, results = client.search_vector(**param)
        assert res != Status.OK

    @mock.patch.object(MegasearchService.Client, 'DescribeTable')
    def test_describe_table(self, DescribeTable, client):
        DescribeTable.return_value = table_schema_factory()

        table_name = fake.table_name()
        res, table_schema = client.describe_table(table_name)
        assert res == Status.OK
        assert isinstance(table_schema, ttypes.TableSchema)

    def test_false_decribe_table(self, client):
        table_name = fake.table_name()
        res, table_schema = client.describe_table(table_name)
        assert res != Status.OK
        assert not table_schema

    @mock.patch.object(MegasearchService.Client, 'ShowTables')
    def test_show_tables(self, ShowTables, client):
        ShowTables.return_value = [fake.table_name() for _ in range(10)]
        res, tables = client.show_tables()
        assert res == Status.OK
        assert isinstance(tables, list)

    def test_false_show_tables(self, client):
        res, tables = client.show_tables()
        assert res != Status.OK
        assert not tables

    def test_client_version(self, client):
        res = client.client_version()
        assert res == '0.0.1'


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
        param = {
            'table_name': fake.table_name(),
            'partition_name': fake.table_name(),
            'column_name_to_range': {fake.name(): range_factory() for _ in range(3)}
        }
        res = Prepare.create_table_partition_param(**param)
        LOGGER.error('{}'.format(res))
        assert isinstance(res, ttypes.CreateTablePartitionParam)

    def test_delete_table_partition_param(self):
        param = {
            'table_name': fake.table_name(),
            'partition_names': [fake.name() for i in range(5)]
        }
        res = Prepare.delete_table_partition_param(**param)
        assert isinstance(res, ttypes.DeleteTablePartitionParam)

    def test_row_record(self):
        param={
            'column_name_to_vector': {fake.name(): [random.random() for i in range(256)]},
            'column_name_to_attribute': {fake.name(): fake.name()}
        }
        res = Prepare.row_record(**param)
        assert isinstance(res, ttypes.RowRecord)

    def test_query_record(self):
        param = {
            'column_name_to_vector': {fake.name(): [random.random() for i in range(256)]},
            'selected_columns': [fake.name() for _ in range(10)],
            'name_to_partition_ranges': {fake.name(): [range_factory() for _ in range(5)]}
        }
        res = Prepare.query_record(**param)
        assert isinstance(res, ttypes.QueryRecord)


