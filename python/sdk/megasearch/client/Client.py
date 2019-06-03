import logging
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol, TJSONProtocol

from thrift.Thrift import TException
from ..thrift import MegasearchService
from .Abstract import ConnectIntf
from .Status import Status

# from .Abstract import *
from .Exceptions import (RepeatingConnectError, ConnectParamMissingError,
                         DisconnectNotConnectedClientError)

LOGGER = logging.getLogger(__name__)

__VERSION__ = '0.0.1'
__NAME__ = 'Client'


class Connection(ConnectIntf):

    def __init__(self):
        self.transport = None
        self.client = None
        self.connect_status = None

    def __repr__(self):
        return '{}'.format(self.connect_status)

    @staticmethod
    def create():
        # TODO in python, maybe this method is useless
        return Connection()

    @staticmethod
    def destroy(connection):
        """Destroy the connection instance"""
        # TODO in python, maybe this method is useless

        pass

    def connect(self, param=None, uri=None):

        if self.connect_status and self.connect_status == Status(message='Connected'):
            raise RepeatingConnectError("You have already connected!")

        if not param and not uri:
            raise ConnectParamMissingError('Need host/port or uri param to connect with!')

        try:
            transport = TSocket.TSocket(*param)
            LOGGER.info('transport: {}, {}'.format(transport.host, transport.port))
            self.transport = TTransport.TBufferedTransport(transport)
            protocol = TJSONProtocol.TJSONProtocol(transport)
            self.client = MegasearchService.Client(protocol)
            transport.open()
            self.connect_status = Status(Status.OK, 'Connected')
            LOGGER.info('Connected!')

        except TException as e:
            self.connect_status = Status(Status.CONNECT_FAILED, message=str(e))
            LOGGER.error(self.connect_status)
        finally:
            return self.connect_status

    @property
    def connected(self):
        return self.connect_status == Status()

    def disconnect(self):

        if self.connect_status != Status.OK:
            raise DisconnectNotConnectedClientError("Client is not connected")

        try:

            self.transport.close()
            LOGGER.info('Client Disconnected!')
            self.connect_status = None

        except TException as e:
            return Status(Status.INVALID, str(e))
        return Status(Status.OK, 'Disconnected')

    def create_table(self, param):
        """
        Create table
        should be implemented

        :type  param: TableSchema
        :param param: provide table information to be created

        :return: Status, indicate if connect is successful
        """
        pass

    def delete_table(self, table_name):
        """
        Delete table
        should be implemented

        :type  table_name: str
        :param table_name: table_name of the deleting table

        :return: Status, indicate if connect is successful
        """
        pass

    def create_table_partition(self, param):
        """
        Create table partition
        should be implemented

        :type  param: CreateTablePartitionParam
        :param param: provide partition information

        :return: Status, indicate if table partition is created successfully
        """
        pass

    def delete_table_partition(self, param):
        """
        Delete table partition
        should be implemented

        :type  param: DeleteTablePartitionParam
        :param param: provide partition information to be deleted
        :return: Status, indicate if partition is deleted successfully
        """
        pass

    def add_vector(self, table_name, records, ids):
        """
        Add vectors to table
        should be implemented

        :type  table_name: str
        :param table_name: table name been inserted

        :type  records: list[RowRecord]
        :param records: list of vectors been inserted

        :type  ids: list[int]
        :param ids: list of ids

        :return: Status, indicate if vectors inserted successfully
        """
        pass

    def search_vector(self, table_name, query_records, query_results, top_k):
        """
        Query vectors in a table
        should be implemented

        :type  table_name: str
        :param table_name: table name been queried

        :type  query_records: list[QueryRecord]
        :param query_records: all vectors going to be queried

        :type  query_results: list[TopKQueryResult]
        :param query_results: list of results

        :type  top_k: int
        :param top_k: how many similar vectors will be searched

        :return: Status, indicate if query is successful
        """
        pass

    def describe_table(self, table_name, table_schema):
        """
        Show table information
        should be implemented

        :type  table_name: str
        :param table_name: which table to be shown

        :type  table_schema: TableSchema
        :param table_schema: table schema is given when operation is successful

        :return: Status, indicate if query is successful
        """
        pass

    def show_tables(self, tables):
        """
        Show all tables in database
        should be implemented

        :type  tables: list[str]
        :param tables: list of tables

        :return: Status, indicate if this operation is successful
        """
        pass

    def client_version(self):
        """
        Provide server version
        should be implemented

        :return: Server version
        """
        pass

    def server_status(self):
        """
        Provide server status
        should be implemented

        :return: Server status
        """
        pass
