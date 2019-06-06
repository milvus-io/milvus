from enum import IntEnum
from .Exceptions import ConnectParamMissingError


class AbstactIndexType(object):
    RAW = 1
    IVFFLAT = 2


class AbstractColumnType(object):
    INVALID = 1
    INT8 = 2
    INT16 = 3
    INT32 = 4
    INT64 = 5
    FLOAT32 = 6
    FLOAT64 = 7
    DATE = 8
    VECTOR = 9


class Column(object):
    """
    Table column description

    :type  type: ColumnType
    :param type: type of the column

    :type  name: str
    :param name: name of the column

    """
    def __init__(self, name=None, type=AbstractColumnType.INVALID):
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

    `Column`:
        :type  name: str
        :param name: Name of the column

        :type  type: ColumnType
        :param type: Default type is ColumnType.VECTOR, can't change

    """
    def __init__(self, name,
                 dimension=0,
                 index_type=None,
                 store_raw_vector=False,
                 type=None):
        self.dimension = dimension
        self.index_type = index_type
        self.store_raw_vector = store_raw_vector
        super(VectorColumn, self).__init__(name, type=type)


class TableSchema(object):
    """
    Table Schema

    :type  table_name: str
    :param table_name: name of table

    :type  vector_columns: list[VectorColumn]
    :param vector_columns: a list of VectorColumns,

            Stores different types of vectors

    :type  attribute_columns: list[Column]
    :param attribute_columns: Columns description

            List of `Columns` whose type isn't VECTOR

    :type  partition_column_names: list[str]
    :param partition_column_names: Partition column name

            `Partition columns` are `attribute columns`, the number of
        partition columns may be less than or equal to attribute columns,
        this param only stores `column name`

    """
    def __init__(self, table_name, vector_columns,
                 attribute_columns, partition_column_names, **kwargs):
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
    # TODO Iterable
    def __init__(self, table_name, partition_name, column_name_to_range):
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
    # TODO Iterable
    def __init__(self, table_name, partition_names):
        self.table_name = table_name
        self.partition_names = partition_names


class RowRecord(object):
    """
    Record inserted

    :type  column_name_to_vector: dict{str : list[float]}
    :param column_name_to_vector: Column name to vector map

    :type  column_name_to_attribute: dict{str: str}
    :param column_name_to_attribute: Other attribute columns
    """
    def __init__(self, column_name_to_vector, column_name_to_attribute):
        self.column_name_to_vector = column_name_to_vector
        self.column_name_to_attribute = column_name_to_attribute


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
    def __init__(self, column_name_to_vector, selected_columns, name_to_partition_ranges):
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

    :type  column_name_to_attribute: dict{str : str}
    :param column_name_to_attribute: Other columns

    """
    def __init__(self, id, score, column_name_to_attribute):
        self.id = id
        self.score = score
        self.column_name_to_value = column_name_to_attribute


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


class ConnectIntf(object):
    """SDK client abstract class

    Connection is a abstract class

    """

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

    def add_vector(self, table_name, records):
        """
        Add vectors to table
        should be implemented

        :type  table_name: str
        :param table_name: table name been inserted

        :type  records: list[RowRecord]
        :param records: list of vectors been inserted

        :returns:
            Status : indicate if vectors inserted successfully
            ids :list of id, after inserted every vector is given a id
        """
        _abstract()

    def search_vector(self, table_name, query_records, top_k):
        """
        Query vectors in a table
        should be implemented

        :type  table_name: str
        :param table_name: table name been queried

        :type  query_records: list[QueryRecord]
        :param query_records: all vectors going to be queried

        :type  top_k: int
        :param top_k: how many similar vectors will be searched

        :returns:
            Status:  indicate if query is successful
            query_results: list[TopKQueryResult]
        """
        _abstract()

    def describe_table(self, table_name):
        """
        Show table information
        should be implemented

        :type  table_name: str
        :param table_name: which table to be shown

        :returns:
            Status: indicate if query is successful
            table_schema: TableSchema, given when operation is successful
        """
        _abstract()

    def show_tables(self):
        """
        Show all tables in database
        should be implemented

        :return:
            Status: indicate if this operation is successful
            tables: list[str], list of table names
        """
        _abstract()

    def client_version(self):
        """
        Provide client version
        should be implemented

        :return: Client version
        """
        _abstract()
        pass

    def server_version(self):
        """
        Provide server version
        should be implemented

        :return: Server version
        """

    def server_status(self, cmd):
        """
        Provide server status
        should be implemented
        # TODO What is cmd
        :type cmd
        :param cmd

        :return: Server status
        """
        _abstract()
        pass














       
