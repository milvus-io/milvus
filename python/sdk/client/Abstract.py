from enum import IntEnum


class IndexType(IntEnum):
    INVALIDE = 0
    IDMAP = 1
    IVFLAT = 2


class TableSchema(object):
    """
    Table Schema

    :type  table_name: str
    :param table_name: (Required) name of table

    :type  index_type: IndexType
    :param index_type: (Optional) index type, default = 0

        `IndexType`: 0-invalid, 1-idmap, 2-ivflat

    :type  dimension: int64
    :param dimension: (Required) dimension of vector

    :type  store_raw_vector: bool
    :param store_raw_vector: (Optional) default = False

    """
    def __init__(self, table_name,
                 dimension=0,
                 index_type=IndexType.INVALIDE,
                 store_raw_vector=False):
        self.table_name = table_name
        self.index_type = index_type
        self.dimension = dimension
        self.store_raw_vector = store_raw_vector


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


class RowRecord(object):
    """
    Record inserted

    :type  vector_data: binary str
    :param vector_data: (Required) a vector

    """
    def __init__(self, vector_data):
        self.vector_data = vector_data


class QueryResult(object):
    """
    Query result

    :type  id: int64
    :param id: id of the vector

    :type  score: float
    :param score: Vector similarity 0 <= score <= 100

    """
    def __init__(self, id, score):
        self.id = id
        self.score = score

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))


class TopKQueryResult(object):
    """
    TopK query results

    :type  query_results: list[QueryResult]
    :param query_results: TopK query results

    """
    def __init__(self, query_results):
        self.query_results = query_results

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))


def _abstract():
    raise NotImplementedError('You need to override this function')


class ConnectIntf(object):
    """SDK client abstract class

    Connection is a abstract class

    """

    def connect(self, host=None, port=None, uri=None):
        """
        Connect method should be called before any operations
        Server will be connected after connect return OK
        Should be implemented

        :type  host: str
        :param host: host

        :type  port: str
        :param port: port

        :type  uri: str
        :param uri: (Optional) uri

        :return: Status,  indicate if connect is successful
        """
        _abstract()

    def connected(self):
        """
        connected, connection status
        Should be implemented

        :return: Status,  indicate if connect is successful
        """
        _abstract()

    def disconnect(self):
        """
        Disconnect, server will be disconnected after disconnect return SUCCESS
        Should be implemented

        :return: Status,  indicate if connect is successful
        """
        _abstract()

    def create_table(self, param):
        """
        Create table
        Should be implemented

        :type  param: TableSchema
        :param param: provide table information to be created

        :return: Status, indicate if connect is successful
        """
        _abstract()

    def delete_table(self, table_name):
        """
        Delete table
        Should be implemented

        :type  table_name: str
        :param table_name: table_name of the deleting table

        :return: Status, indicate if connect is successful
        """
        _abstract()

    def add_vectors(self, table_name, records):
        """
        Add vectors to table
        Should be implemented

        :type  table_name: str
        :param table_name: table name been inserted

        :type  records: list[RowRecord]
        :param records: list of vectors been inserted

        :returns:
            Status : indicate if vectors inserted successfully
            ids :list of id, after inserted every vector is given a id
        """
        _abstract()

    def search_vectors(self, table_name, query_records, query_ranges, top_k):
        """
        Query vectors in a table
        Should be implemented

        :type  table_name: str
        :param table_name: table name been queried

        :type  query_records: list[RowRecord]
        :param query_records: all vectors going to be queried

        :type  query_ranges: list[Range]
        :param query_ranges: Optional ranges for conditional search.
            If not specified, search whole table

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
        Should be implemented

        :type  table_name: str
        :param table_name: which table to be shown

        :returns:
            Status: indicate if query is successful
            table_schema: TableSchema, given when operation is successful
        """
        _abstract()

    def get_table_row_count(self, table_name):
        """
        Get table row count
        Should be implemented

        :type  table_name, str
        :param table_name, target table name.

        :returns:
            Status: indicate if operation is successful
            count: int, table row count
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

        :return: str, client version
        """
        _abstract()

    def server_version(self):
        """
        Provide server version
        should be implemented

        :return: str, server version
        """
        _abstract()

    def server_status(self, cmd):
        """
        Provide server status
        should be implemented
        :type cmd, str

        :return: str, server status
        """
        _abstract()














       
