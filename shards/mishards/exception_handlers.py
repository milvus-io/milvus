import logging
from milvus.grpc_gen import milvus_pb2, milvus_pb2_grpc, status_pb2
from mishards import grpc_server as server, exceptions

logger = logging.getLogger(__name__)


def resp_handler(err, error_code):
    if not isinstance(err, exceptions.BaseException):
        return status_pb2.Status(error_code=error_code, reason=str(err))

    status = status_pb2.Status(error_code=error_code, reason=err.message)

    if err.metadata is None:
        return status

    resp_class = err.metadata.get('resp_class', None)
    if not resp_class:
        return status

    if resp_class == milvus_pb2.BoolReply:
        return resp_class(status=status, bool_reply=False)

    if resp_class == milvus_pb2.VectorIds:
        return resp_class(status=status, vector_id_array=[])

    if resp_class == milvus_pb2.TopKQueryResult:
        return resp_class(status=status,
                          row_num=0,
                          ids=[],
                          distances=[])

    if resp_class == milvus_pb2.CollectionRowCount:
        return resp_class(status=status, collection_row_count=-1)

    if resp_class == milvus_pb2.CollectionName:
        return resp_class(status=status, collection_name=[])

    if resp_class == milvus_pb2.StringReply:
        return resp_class(status=status, string_reply='')

    if resp_class == milvus_pb2.CollectionSchema:
        return milvus_pb2.CollectionSchema(
            status=status
        )

    if resp_class == milvus_pb2.IndexParam:
        return milvus_pb2.IndexParam(
            collection_name=milvus_pb2.CollectionName(
                status=status
            )
        )

    status.error_code = status_pb2.UNEXPECTED_ERROR
    return status


@server.errorhandler(exceptions.CollectionNotFoundError)
def CollectionNotFoundErrorHandler(err):
    logger.error(err)
    return resp_handler(err, status_pb2.COLLECTION_NOT_EXISTS)


@server.errorhandler(exceptions.InvalidTopKError)
def InvalidTopKErrorHandler(err):
    logger.error(err)
    return resp_handler(err, status_pb2.ILLEGAL_TOPK)


@server.errorhandler(exceptions.InvalidArgumentError)
def InvalidArgumentErrorHandler(err):
    logger.error(err)
    return resp_handler(err, status_pb2.ILLEGAL_ARGUMENT)


@server.errorhandler(exceptions.DBError)
def DBErrorHandler(err):
    logger.error(err)
    return resp_handler(err, status_pb2.UNEXPECTED_ERROR)


@server.errorhandler(exceptions.InvalidRangeError)
def InvalidArgumentErrorHandler(err):
    logger.error(err)
    return resp_handler(err, status_pb2.ILLEGAL_RANGE)
