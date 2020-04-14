import ujson
from milvus import Status
from functools import wraps


def error_status(func):
    @wraps(func)
    def inner(*args, **kwargs):
        try:
            results = func(*args, **kwargs)
        except Exception as e:
            return Status(code=Status.UNEXPECTED_ERROR, message=str(e)), None

        return Status(code=0, message="Success"), results

    return inner


class GrpcArgsParser(object):

    @classmethod
    @error_status
    def parse_proto_CollectionSchema(cls, param):
        _collection_schema = {
            'collection_name': param.collection_name,
            'dimension': param.dimension,
            'index_file_size': param.index_file_size,
            'metric_type': param.metric_type
        }

        return param.status, _collection_schema

    @classmethod
    @error_status
    def parse_proto_CollectionName(cls, param):
        return param.collection_name

    @classmethod
    @error_status
    def parse_proto_FlushParam(cls, param):
        return list(param.collection_name_array)

    @classmethod
    @error_status
    def parse_proto_Index(cls, param):
        _index = {
            'index_type': param.index_type,
            'params': param.extra_params[0].value
        }

        return _index

    @classmethod
    @error_status
    def parse_proto_IndexParam(cls, param):
        _collection_name = param.collection_name
        _index_type = param.index_type
        _index_param = {}

        for params in param.extra_params:
            if params.key == 'params':
                _index_param = ujson.loads(str(params.value))

        return _collection_name, _index_type, _index_param

    @classmethod
    @error_status
    def parse_proto_Command(cls, param):
        _cmd = param.cmd

        return _cmd

    @classmethod
    @error_status
    def parse_proto_RowRecord(cls, param):
        return list(param.vector_data)

    @classmethod
    def parse_proto_PartitionParam(cls, param):
        _collection_name = param.collection_name
        _tag = param.tag

        return _collection_name, _tag

    @classmethod
    @error_status
    def parse_proto_SearchParam(cls, param):
        _collection_name = param.collection_name
        _topk = param.topk

        if len(param.extra_params) == 0:
            raise Exception("Search param loss")
        _params = ujson.loads(str(param.extra_params[0].value))

        _query_record_array = []
        if param.query_record_array:
            for record in param.query_record_array:
                if record.float_data:
                    _query_record_array.append(list(record.float_data))
                else:
                    _query_record_array.append(bytes(record.binary_data))
        else:
            raise Exception("Search argument parse error: record array is empty")

        return _collection_name, _query_record_array, _topk, _params

    @classmethod
    @error_status
    def parse_proto_DeleteByIDParam(cls, param):
        _collection_name = param.collection_name
        _id_array = list(param.id_array)

        return _collection_name, _id_array

    @classmethod
    @error_status
    def parse_proto_VectorIdentity(cls, param):
        _collection_name = param.collection_name
        _id = param.id

        return _collection_name, _id

    @classmethod
    @error_status
    def parse_proto_GetVectorIDsParam(cls, param):
        _collection__name = param.collection_name
        _segment_name = param.segment_name

        return _collection__name, _segment_name
