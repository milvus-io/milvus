import logging, logging.config

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TException, TApplicationException

from milvus.thrift import MilvusService
from milvus.thrift import ttypes
from client.Abstract import (
    ConnectIntf,
    TableSchema,
    Range,
    RowRecord,
    QueryResult,
    TopKQueryResult,
    IndexType
)

from client.Status import Status
from client.Exceptions import (
    RepeatingConnectError,
    DisconnectNotConnectedClientError,
    NotConnectError
)

LOGGER = logging.getLogger(__name__)

__VERSION__ = '0.1.0'
__NAME__ = 'Milvus Python SDK'


class Prepare(object):

    @classmethod
    def table_schema(cls,
                     table_name,
                     dimension,
                     index_type=IndexType.INVALIDE,
                     store_raw_vector = False):
        """
        :type table_name: str
        :type dimension: int
        :type index_type: IndexType
        :type store_raw_vector: bool
        :param table_name: (Required) name of table
        :param dimension: (Required) dimension of the table
        :param index_type: (Optional) index type, default = IndexType.INVALID
        :param store_raw_vector: (Optional) default = False

        :return: TableSchema object
        """
        temp = TableSchema(table_name,dimension, index_type, store_raw_vector)

        return ttypes.TableSchema(table_name=temp.table_name,
                                  dimension=dimension,
                                  index_type=index_type,
                                  store_raw_vector=store_raw_vector)

    @classmethod
    def range(cls, start, end):
        """
        :type start: str
        :type end: str
        :param start: (Required) range start
        :param end: (Required) range end

        :return: Range object
        """
        temp = Range(start=start, end=end)
        return ttypes.Range(start_value=temp.start, end_value=temp.end)

    @classmethod
    def row_record(cls, vector_data):
        """
        Transfer a float binary str to RowRecord and return

        :type vector_data: bytearray or bytes
        :param vector_data: (Required) binary vector to store

        :return: RowRecord object

        """
        temp = RowRecord(vector_data)
        return ttypes.RowRecord(vector_data=temp.vector_data)


class Milvus(ConnectIntf):
    """
    The Milvus object is used to connect and communicate with the server
    """

    def __init__(self):
        self.status = None
        self._transport = None
        self._client = None

    def __repr__(self):
        return '{}'.format(self.status)

    def connect(self, host='localhost', port='9090', uri=None):
        """
        Connect method should be called before any operations.
        Server will be connected after connect return OK

        :type  host: str
        :type  port: str
        :type  uri: str
        :param host: (Required) host of the server
        :param port: (Required) port of the server
        :param uri: (Optional)

        :return: Status, indicate if connect is successful
        :rtype: Status
        """
        # TODO URI
        if self.status and self.status == Status.SUCCESS:
            raise RepeatingConnectError("You have already connected!")

        transport = TSocket.TSocket(host=host, port=port)
        self._transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self._client = MilvusService.Client(protocol)

        try:
            transport.open()
            self.status = Status(Status.SUCCESS, 'Connected')
            LOGGER.info('Connected!')

        except (TTransport.TTransportException, TException) as e:
            self.status = Status(Status.CONNECT_FAILED, message=str(e))
            LOGGER.error('logger.error: {}'.format(self.status))
        finally:
            return self.status

    @property
    def connected(self):
        """
        Check if client is connected to the server

        :return: if client is connected
        :rtype bool
        """
        return self.status == Status.SUCCESS

    def disconnect(self):
        """
        Disconnect the client

        :return: Status, indicate if disconnect is successful
        :rtype: Status
        """

        if not self._transport:
            raise DisconnectNotConnectedClientError('Error')

        try:

            self._transport.close()
            LOGGER.info('Client Disconnected!')
            self.status = None

        except TException as e:
            return Status(Status.PERMISSION_DENIED, str(e))
        return Status(Status.SUCCESS, 'Disconnected')

    def create_table(self, param):
        """Create table

        :type  param: TableSchema
        :param param: Provide table information to be created

                `Please use Prepare.table_schema generate param`

        :return: Status, indicate if operation is successful
        :rtype: Status
        """
        if not self._client:
            raise NotConnectError('Please Connect to the server first!')

        try:
            self._client.CreateTable(param)
        except (TApplicationException, ) as e:
            LOGGER.error('Unable to create table')
            return Status(Status.PERMISSION_DENIED, str(e))
        return Status(message='Table {} created!'.format(param.table_name))

    def delete_table(self, table_name):
        """
        Delete table with table_name

        :type  table_name: str
        :param table_name: Name of the table being deleted

        :return: Status, indicate if operation is successful
        :rtype: Status
        """
        try:
            self._client.DeleteTable(table_name)
        except (TApplicationException, TException) as e:
            LOGGER.error('Unable to delete table {}'.format(table_name))
            return Status(Status.PERMISSION_DENIED, str(e))
        return Status(message='Table {} deleted!'.format(table_name))

    def add_vectors(self, table_name, records):
        """
        Add vectors to table

        :type  table_name: str
        :type  records: list[RowRecord]

        :param table_name: table name been inserted
        :param records: list of vectors been inserted

                `Please use Prepare.row_record generate records`

        :returns:
            Status: indicate if vectors inserted successfully

            ids: list of id, after inserted every vector is given a id
        :rtype: (Status, list(str))
        """
        try:
            ids = self._client.AddVector(table_name=table_name, record_array=records)
        except (TApplicationException, TException) as e:
            LOGGER.error('{}'.format(e))
            return Status(Status.PERMISSION_DENIED, str(e)), None
        return Status(message='Vectors added successfully!'), ids

    def search_vectors(self, table_name, top_k, query_records, query_ranges=None):
        """
        Query vectors in a table



        :param query_ranges: Optional ranges for conditional search.
            If not specified, search whole table
        :type  query_ranges: list[Range]
        :param table_name: table name been queried
        :type  table_name: str
        :param query_records: all vectors going to be queried

                `Please use Prepare.query_record generate QueryRecord`
        :type  query_records: list[RowRecord]
        :param top_k: int, how many similar vectors will be searched
        :type  top_k: int

        :returns: (Status, res)

            Status:  indicate if query is successful

            res: return when operation is successful
        :rtype: (Status, list[TopKQueryResult])
        """
        res = []
        try:
            top_k_query_results = self._client.SearchVector(
                table_name=table_name,
                query_record_array=query_records,
                query_range_array=query_ranges,
                topk=top_k)

            if top_k_query_results:
                for top_k in top_k_query_results:
                    if top_k:
                        res.append(TopKQueryResult([QueryResult(qr.id, qr.score)
                                                for qr in top_k.query_result_arrays]))

        except (TApplicationException, TException) as e:
            LOGGER.error('{}'.format(e))
            return Status(Status.PERMISSION_DENIED, str(e)), None
        return Status(message='Success!'), res

    def describe_table(self, table_name):
        """
        Show table information

        :type  table_name: str
        :param table_name: which table to be shown

        :returns: (Status, table_schema)
            Status: indicate if query is successful
            table_schema: return when operation is successful
        :rtype: (Status, TableSchema)
        """
        try:
            temp = self._client.DescribeTable(table_name)

        except (TApplicationException, TException) as e:
            LOGGER.error('{}'.format(e))
            return Status(Status.PERMISSION_DENIED, str(e)), None
        return Status(message='Success!'), temp

    def show_tables(self):
        """
        Show all tables in database

        :return:
            Status: indicate if this operation is successful

            tables: list of table names, return when operation
                    is successful
        :rtype:
            (Status, list[str])
        """
        try:
            res = self._client.ShowTables()
            tables = []
            if res:
                tables = res

        except (TApplicationException, TException) as e:
            LOGGER.error('{}'.format(e))
            return Status(Status.PERMISSION_DENIED, str(e)), None
        return Status(message='Success!'), tables

    def get_table_row_count(self, table_name):
        """
        Get table row count

        :type  table_name: str
        :param table_name: target table name.

        :returns:
            Status: indicate if operation is successful

            res: int, table row count

        """
        try:
            count = self._client.GetTableRowCount(table_name)

        except (TApplicationException, TException) as e:
            LOGGER.error('{}'.format(e))
            return Status(Status.PERMISSION_DENIED, str(e)), None
        return Status(message='Success'), count

    def client_version(self):
        """
        Provide client version

        :return: Client version
        :rtype: str
        """
        return __VERSION__

    def server_version(self):
        """
        Provide server version

        :return: Server version
        """
        if not self.connected:
            raise NotConnectError('You have to connect first')

        return self._client.Ping('version')

    def server_status(self, cmd=None):
        """
        Provide server status

        :return: Server status
        :rtype : str
        """
        if not self.connected:
            raise NotConnectError('You have to connect first')

        return self._client.Ping(cmd)
