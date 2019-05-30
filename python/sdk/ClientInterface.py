from enum import IntEnum
from sdk.exceptions import ConnectParamMissingError
from sdk.Status import Status


class IndexType(IntEnum):
    RAW = 1
    IVFFLAT = 2


class ColumnType(IntEnum):
    INVALID = 1
    INT8 = 2
    INT16 = 3
    INT32 = 4
    INT64 = 5
    FLOAT32 = 6
    FLOAT64 = 7
    DATE = 8
    VECTOR = 9


class ConnectParam(object):
    """
    Connect API parameter

    :type ip_address: str
    :param ip_address: Server IP address

    :type port: str,
    :param port: Sever PORT

    """
    def __init__(self, ip_address, port):

        self.ip_address = ip_address
        self.port = port


class Column(object):
    """
    Table column description

    :type  type: ColumnType
    :param type: type of the column

    :type  name: str
    :param name: name of the column

    """
    def __init__(self, name=None, type=ColumnType.INVALID):
        self.type = type
        self.name = name


class VectorColumn(Column):
    """
    Table vector column description

    :type  dimension: int, int64
    :param dimension: vector dimension

    :type  index_type: IndexType
    :param index_type: IndexType

    :type  store_raw_vector: bool
    :param store_raw_vector: Is vector self stored in the table

    """
    def __init__(self, dimension=0,
                    index_type=IndexType.RAW,
                    store_raw_vector=False):
        self.dimension = dimension
        self.index_type = index_type
        self.store_raw_vector = store_raw_vector
        super(VectorColumn, self).__init__(type=ColumnType.VECTOR)


class TableSchema(object):
    """
    Table Schema

    :type  table_name: str
    :param table_name: Table name

    :type  vector_columns: list[VectorColumn]
    :param vector_columns: vector column description

    :type  attribute_columns: list[Column]
    :param attribute_columns: Columns description

    :type  partition_column_names: list[str]
    :param partition_column_names: Partition column name

    """
    def __init__(self, table_name, vector_columns,
                 attribute_columns, partition_column_names):
        self.table_name = table_name
        self.vector_columns = vector_columns
        self.attribute_columns = attribute_columns
        self.partition_column_names = partition_column_names


class Range(object):
    """
    Range information

    :type  start: str
    :param start: Range start value

    :type  end: str
    :param end: Range end value

    """
    def __init__(self, start, end):
        self.start = start
        self.end = end


class CreateTablePartitionParam(object):
    """
    Create table partition parameters

    :type  table_name: str
    :param table_name: Table name,
                    VECTOR/FLOAT32/FLOAT64 ColumnType is not allowed for partition

    :type  partition_name: str
    :param partition_name: partition name, created partition name

    :type  column_name_to_range: dict{str : Range}
    :param column_name_to_range: Column name to PartitionRange dictionary
    """
    def __init__(self, table_name, partition_name, **column_name_to_range):
        self.table_name = table_name
        self.partition_name = partition_name
        self.column_name_to_range = column_name_to_range


class DeleteTablePartitionParam(object):
    """
    Delete table partition parameters

    :type  table_name: str
    :param table_name: Table name

    :type  partition_names: iterable, str
    :param partition_names: Partition name array

    """
    def __init__(self, table_name, *partition_names):
        self.table_name = table_name
        self.partition_names = partition_names


class RowRecord(object):
    """
    Record inserted

    :type  column_name_to_vector: dict{str : list[float]}
    :param column_name_to_vector: Column name to vector map

    :type  column_name_to_value: dict{str: str}
    :param column_name_to_value: Other attribute columns
    """
    def __init__(self, column_name_to_vector, column_name_to_value):
        self.column_name_to_vector = column_name_to_vector
        self.column_name_to_value = column_name_to_value


class QueryRecord(object):
    """
    Query record

    :type  column_name_to_vector: dict{str : list[float]}
    :param column_name_to_vector: Query vectors, column name to vector map

    :type  selected_columns: list[str]
    :param selected_columns: Output column array

    :type  name_to_partition_ranges: dict{str : list[Range]}
    :param name_to_partition_ranges: Range used to select partitions

    """
    def __init__(self, column_name_to_vector, selected_columns, **name_to_partition_ranges):
        self.column_name_to_vector = column_name_to_vector
        self.selected_columns = selected_columns
        self.name_to_partition_ranges = name_to_partition_ranges


class QueryResult(object):
    """
    Query result

    :type  id: int
    :param id: Output result

    :type  score: float
    :param score: Vector similarity 0 <= score <= 100

    :type  column_name_to_value: dict{str : str}
    :param column_name_to_value: Other columns

    """
    def __init__(self, id, score, **column_name_to_value):
        self.id = id
        self.score = score
        self.column_name_to_value = column_name_to_value


class TopKQueryResult(object):
    """
    TopK query results

    :type  query_results: list[QueryResult]
    :param query_results: TopK query results

    """
    def __init__(self, query_results):
        self.query_results = query_results


def _abstract():
    raise NotImplementedError('You need to override this function')


class Connection(object):
    """SDK client class"""

    @staticmethod
    def create():
        """Create a connection instance and return it
        should be implemented
        
        :return connection: Connection
        """
        _abstract()

    @staticmethod
    def destroy(connection):
        """Destroy the connection instance
        should be implemented

        :type  connection: Connection
        :param connection: The connection instance to be destroyed

        :return bool, return True if destroy is successful
        """
        _abstract()

    def connect(self, param=None, uri=None):
        """
        Connect method should be called before any operations
        Server will be connected after connect return OK
        should be implemented

        :type  param: ConnectParam
        :param param: ConnectParam

        :type  uri: str
        :param uri: uri param

        :return: Status,  indicate if connect is successful
        """
        if (not param and not uri) or (param and uri):
            raise ConnectParamMissingError('You need to parse exact one param')
        _abstract()

    def connected(self):
        """
        connected, connection status
        should be implemented

        :return: Status,  indicate if connect is successful
        """
        _abstract()

    def disconnect(self):
        """
        Disconnect, server will be disconnected after disconnect return OK
        should be implemented

        :return: Status,  indicate if connect is successful
        """
        _abstract()

    def create_table(self, param):
        """
        Create table
        should be implemented

        :type  param: TableSchema
        :param param: provide table information to be created

        :return: Status, indicate if connect is successful
        """
        _abstract()

    def delete_table(self, table_name):
        """
        Delete table
        should be implemented

        :type  table_name: str
        :param table_name: table_name of the deleting table

        :return: Status, indicate if connect is successful
        """
        _abstract()

    def create_table_partition(self, param):
        """
        Create table partition
        should be implemented

        :type  param: CreateTablePartitionParam
        :param param: provide partition information

        :return: Status, indicate if table partition is created successfully
        """
        _abstract()

    def delete_table_partition(self, param):
        """
        Delete table partition
        should be implemented

        :type  param: DeleteTablePartitionParam
        :param param: provide partition information to be deleted
        :return: Status, indicate if partition is deleted successfully
        """
        _abstract()

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
        _abstract()

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
        _abstract()

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
        _abstract()

    def show_tables(self, tables):
        """
        Show all tables in database
        should be implemented

        :type  tables: list[str]
        :param tables: list of tables

        :return: Status, indicate if this operation is successful
        """
        _abstract()

    def client_version(self):
        """
        Provide server version
        should be implemented

        :return: Server version
        """
        _abstract()
        pass

    def server_status(self):
        """
        Provide server status
        should be implemented

        :return: Server status
        """
        _abstract()
        pass














       
