import logging
from contextlib import contextmanager
from milvus.grpc_gen import milvus_pb2, milvus_pb2_grpc, status_pb2

from grpc_utils.grpc_args_parser import GrpcArgsParser as Parser

logger = logging.getLogger(__name__)


class ServiceHandler(milvus_pb2_grpc.MilvusServiceServicer):
    def __init__(self, conn_mgr, *args, **kwargs):
        self.conn_mgr = conn_mgr
        self.table_meta = {}

    @property
    def connection(self):
        conn = self.conn_mgr.conn('WOSERVER')
        if conn:
            conn.on_connect()
        return conn.conn

    def CreateTable(self, request, context):
        _status, _table_schema = Parser.parse_proto_TableSchema(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code, reason=_status.message)

        logger.info('CreateTable {}'.format(_table_schema['table_name']))

        _status = self.connection.create_table(_table_schema)

        return status_pb2.Status(error_code=_status.code, reason=_status.message)

    def HasTable(self, request, context):
        _status, _table_name = Parser.parse_proto_TableName(request)

        if not _status.OK():
            return milvus_pb2.BoolReply(
                status=status_pb2.Status(error_code=_status.code, reason=_status.message),
                bool_reply=False
            )

        logger.info('HasTable {}'.format(_table_name))

        _bool = self.connection.has_table(_table_name)

        return milvus_pb2.BoolReply(
            status=status_pb2.Status(error_code=status_pb2.SUCCESS, reason="OK"),
            bool_reply=_bool
        )

    def DropTable(self, request, context):
        _status, _table_name = Parser.parse_proto_TableName(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code, reason=_status.message)

        logger.info('DropTable {}'.format(_table_name))

        _status = self.connection.delete_table(_table_name)

        return status_pb2.Status(error_code=_status.code, reason=_status.message)

    def CreateIndex(self, request, context):
        _status, unpacks = Parser.parse_proto_IndexParam(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code, reason=_status.message)

        _table_name, _index = unpacks

        logger.info('CreateIndex {}'.format(_table_name))

        # TODO: interface create_table incompleted
        _status = self.connection.create_index(_table_name, _index)

        return status_pb2.Status(error_code=_status.code, reason=_status.message)

    def Insert(self, request, context):
        logger.info('Insert')
        # TODO: Ths SDK interface add_vectors() could update, add a key 'row_id_array'
        _status, _ids = self.connection.add_vectors(None, None, insert_param=request)
        return milvus_pb2.VectorIds(
            status=status_pb2.Status(error_code=_status.code, reason=_status.message),
            vector_id_array=_ids
        )

    def Search(self, request, context):

        try:
            table_name = request.table_name

            topk = request.topk
            nprobe = request.nprobe

            logger.info('Search {}: topk={} nprobe={}'.format(table_name, topk, nprobe))

            if nprobe > 2048 or nprobe <= 0:
                raise exceptions.GRPCInvlidArgument('Invalid nprobe: {}'.format(nprobe))

            table_meta = self.table_meta.get(table_name, None)
            if not table_meta:
                status, info = self.connection.describe_table(table_name)
                if not status.OK():
                    raise TableNotFoundException(table_name)

                self.table_meta[table_name] = info
                table_meta = info

            start = time.time()

            query_record_array = []

            for query_record in request.query_record_array:
                query_record_array.append(list(query_record.vector_data))

            query_range_array = []
            for query_range in request.query_range_array:
                query_range_array.append(
                    Range(query_range.start_value, query_range.end_value))
        except (TableNotFoundException, exceptions.GRPCInvlidArgument) as exc:
            return milvus_pb2.TopKQueryResultList(
                status=status_pb2.Status(error_code=exc.code, reason=exc.message)
            )
        except Exception as e:
            return milvus_pb2.TopKQueryResultList(
                status=status_pb2.Status(error_code=status_pb2.UNEXPECTED_ERROR, reason=str(e))
            )

        try:
            results = workflow.query_vectors(table_name, table_meta, query_record_array, topk,
                                             nprobe, query_range_array)
        except (exceptions.GRPCQueryInvalidRangeException, TableNotFoundException) as exc:
            return milvus_pb2.TopKQueryResultList(
                status=status_pb2.Status(error_code=exc.code, reason=exc.message)
            )
        except exceptions.ServiceNotFoundException as exc:
            return milvus_pb2.TopKQueryResultList(
                status=status_pb2.Status(error_code=status_pb2.UNEXPECTED_ERROR, reason=exc.message)
            )
        except Exception as e:
            logger.error(e)
            results = workflow.query_vectors(table_name, table_meta, query_record_array,
                                             topk, nprobe, query_range_array)

        now = time.time()
        logger.info('SearchVector Ends @{}'.format(now))
        logger.info('SearchVector takes: {}'.format(now - start))

        topk_result_list = milvus_pb2.TopKQueryResultList(
            status=status_pb2.Status(error_code=status_pb2.SUCCESS, reason="Success"),
            topk_query_result=results
        )
        return topk_result_list

    def SearchInFiles(self, request, context):
        try:
            file_id_array = list(request.file_id_array)
            search_param = request.search_param
            table_name = search_param.table_name
            topk = search_param.topk
            nprobe = search_param.nprobe

            query_record_array = []

            for query_record in search_param.query_record_array:
                query_record_array.append(list(query_record))

            query_range_array = []
            for query_range in search_param.query_range_array:
                query_range_array.append("")
        except Exception as e:
            milvus_pb2.TopKQueryResultList(
                status=status_pb2.Status(error_code=status_pb2.UNEXPECTED_ERROR, reason=str(e)),
            )

        res = search_vector_in_files.delay(table_name=table_name,
                                           file_id_array=file_id_array,
                                           query_record_array=query_record_array,
                                           query_range_array=query_range_array,
                                           topk=topk,
                                           nprobe=nprobe)
        status, result = res.get(timeout=1)

        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        res = TopKQueryResult()
        for top_k_query_results in result:
            res.query_result_arrays.append([QueryResult(id=qr.id, distance=qr.distance)
                                            for qr in top_k_query_results])
        return res

    def DescribeTable(self, request, context):
        _status, _table_name = Parser.parse_proto_TableName(request)

        if not _status.OK():
            table_name = milvus_pb2.TableName(
                status=status_pb2.Status(error_code=_status.code, reason=_status.message)
            )
            return milvus_pb2.TableSchema(
                table_name=table_name
            )

        logger.info('DescribeTable {}'.format(_table_name))
        _status, _table = self.connection.describe_table(_table_name)

        if _status.OK():
            _grpc_table_name = milvus_pb2.TableName(
                status=status_pb2.Status(error_code=_status.code, reason=_status.message),
                table_name=_table.table_name
            )

            return milvus_pb2.TableSchema(
                table_name=_grpc_table_name,
                index_file_size=_table.index_file_size,
                dimension=_table.dimension,
                metric_type=_table.metric_type
            )

        return milvus_pb2.TableSchema(
            table_name=milvus_pb2.TableName(
                status=status_pb2.Status(error_code=_status.code, reason=_status.message)
            )
        )

    def CountTable(self, request, context):
        _status, _table_name = Parser.parse_proto_TableName(request)

        if not _status.OK():
            status = status_pb2.Status(error_code=_status.code, reason=_status.message)

            return milvus_pb2.TableRowCount(
                status=status
            )

        logger.info('CountTable {}'.format(_table_name))

        _status, _count = self.connection.get_table_row_count(_table_name)

        return milvus_pb2.TableRowCount(
            status=status_pb2.Status(error_code=_status.code, reason=_status.message),
            table_row_count=_count if isinstance(_count, int) else -1)

    def Cmd(self, request, context):
        _status, _cmd = Parser.parse_proto_Command(request)
        logger.info('Cmd: {}'.format(_cmd))

        if not _status.OK():
            return milvus_pb2.StringReply(
                status_pb2.Status(error_code=_status.code, reason=_status.message)
            )

        if _cmd == 'version':
            _status, _reply = self.connection.server_version()
        else:
            _status, _reply = self.connection.server_status()

        return milvus_pb2.StringReply(
            status=status_pb2.Status(error_code=_status.code, reason=_status.message),
            string_reply=_reply
        )

    def ShowTables(self, request, context):
        logger.info('ShowTables')
        _status, _results = self.connection.show_tables()

        if not _status.OK():
            _results = []

        for _result in _results:
            yield milvus_pb2.TableName(
                status=status_pb2.Status(error_code=_status.code, reason=_status.message),
                table_name=_result
            )

    def DeleteByRange(self, request, context):
        _status, unpacks = \
            Parser.parse_proto_DeleteByRangeParam(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code, reason=_status.message)

        _table_name, _start_date, _end_date = unpacks

        logger.info('DeleteByRange {}: {} {}'.format(_table_name, _start_date, _end_date))
        _status = self.connection.delete_vectors_by_range(_table_name, _start_date, _end_date)
        return status_pb2.Status(error_code=_status.code, reason=_status.message)

    def PreloadTable(self, request, context):
        _status, _table_name = Parser.parse_proto_TableName(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code, reason=_status.message)

        logger.info('PreloadTable {}'.format(_table_name))
        _status = self.connection.preload_table(_table_name)
        return status_pb2.Status(error_code=_status.code, reason=_status.message)

    def DescribeIndex(self, request, context):
        _status, _table_name = Parser.parse_proto_TableName(request)

        if not _status.OK():
            return milvus_pb2.IndexParam(
                table_name=milvus_pb2.TableName(
                    status=status_pb2.Status(error_code=_status.code, reason=_status.message)
                )
            )

        logger.info('DescribeIndex {}'.format(_table_name))
        _status, _index_param = self.connection.describe_index(_table_name)

        _index = milvus_pb2.Index(index_type=_index_param._index_type, nlist=_index_param._nlist)
        _tablename = milvus_pb2.TableName(
            status=status_pb2.Status(error_code=_status.code, reason=_status.message),
            table_name=_table_name)

        return milvus_pb2.IndexParam(table_name=_tablename, index=_index)

    def DropIndex(self, request, context):
        _status, _table_name = Parser.parse_proto_TableName(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code, reason=_status.message)

        logger.info('DropIndex {}'.format(_table_name))
        _status = self.connection.drop_index(_table_name)
        return status_pb2.Status(error_code=_status.code, reason=_status.message)
