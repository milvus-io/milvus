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

__VERSION__ = '0.0.1'
__NAME__ = 'Thrift_Client'


class Prepare(object):

    @classmethod
    def table_schema(cls,
                     table_name,
                     dimension,
                     index_type=IndexType.INVALIDE,
                     store_raw_vector = False):
        """

        :param table_name: str, (Required) name of table
        :param index_type: IndexType, (Required) index type, default = IndexType.INVALID
        :param dimension: int64, (Optional) dimension of the table
        :param store_raw_vector: bool, (Optional) default = False

        :return: TableSchema
        """
        temp = TableSchema(table_name,dimension, index_type, store_raw_vector)

        return ttypes.TableSchema(table_name=temp.table_name,
                                  dimension=dimension,
                                  index_type=index_type,
                                  store_raw_vector=store_raw_vector)

    @classmethod
    def range(cls, start, end):
        """
        :param start: str, (Required) range start
        :param end: str (Required) range end

        :return Range
        """
        temp = Range(start=start, end=end)
        return ttypes.Range(start_value=temp.start, end_value=temp.end)

    @classmethod
    def row_record(cls, vector_data):
        """
        Record inserted

        :param vector_data: float binary str, (Required) a binary str

        """
        temp = RowRecord(vector_data)
        return ttypes.RowRecord(vector_data=temp.vector_data)


class Milvus(ConnectIntf):

    def __init__(self):
        self.status = None
        self._transport = None
        self._client = None

    def __repr__(self):
        return '{}'.format(self.status)

    def connect(self, host='localhost', port='9090', uri=None):
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
        return self.status == Status.SUCCESS

    def disconnect(self):

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

        :param param: Provide table information to be created,

                `Please use Prepare.table_schema generate param`

        :return: Status, indicate if operation is successful
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
        """Delete table

        :param table_name: Name of the table being deleted

        :return: Status, indicate if operation is successful
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

        :param table_name: table name been inserted
        :param records: List[RowRecord], list of vectors been inserted

                `Please use Prepare.row_record generate records`

        :returns:
            Status : indicate if vectors inserted successfully
            ids :list of id, after inserted every vector is given a id
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

        :param table_name: str, table name been queried
        :param query_records: list[QueryRecord], all vectors going to be queried

                `Please use Prepare.query_record generate QueryRecord`

        :param top_k: int, how many similar vectors will be searched
        :param query_ranges, (Optional) list[Range], search range

        :returns:
            Status:  indicate if query is successful
            res: list[TopKQueryResult], return when operation is successful
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

        :param table_name: str, which table to be shown

        :returns:
            Status: indicate if query is successful
            table_schema: TableSchema, return when operation is successful
        """
        try:
            temp = self._client.DescribeTable(table_name)

            # res = TableSchema(table_name=temp.table_name, dimension=temp.dimension,
            #                   index_type=temp.index_type, store_raw_vector=temp.store_raw_vector)
        except (TApplicationException, TException) as e:
            LOGGER.error('{}'.format(e))
            return Status(Status.PERMISSION_DENIED, str(e)), None
        return Status(message='Success!'), temp

    def show_tables(self):
        """
        Show all tables in database

        :return:
            Status: indicate if this operation is successful
            tables: list[str], list of table names, return when operation
                    is successful
        """
        try:
            res = self._client.ShowTables()
            tables = []
            if res:
                tables, _ = res

        except (TApplicationException, TException) as e:
            LOGGER.error('{}'.format(e))
            return Status(Status.PERMISSION_DENIED, str(e)), None
        return Status(message='Success!'), tables

    def get_table_row_count(self, table_name):
        """
        Get table row count

        :type  table_name, str
        :param table_name, target table name.

        :returns:
            Status: indicate if operation is successful
            res: int, table row count

        """
        try:
            count, _ = self._client.GetTableRowCount(table_name)

        except (TApplicationException, TException) as e:
            LOGGER.error('{}'.format(e))
            return Status(Status.PERMISSION_DENIED, str(e)), None
        return Status(message='Success'), count

    def client_version(self):
        """
        Provide client version

        :return: Client version
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
        """
        if not self.connected:
            raise NotConnectError('You have to connect first')

        return self._client.Ping(cmd)
