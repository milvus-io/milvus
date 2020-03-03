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
    def parse_proto_TableSchema(cls, param):
        _table_schema = {
            'status': param.status,
            'table_name': param.table_name,
            'dimension': param.dimension,
            'index_file_size': param.index_file_size,
            'metric_type': param.metric_type
        }

        return _table_schema

    @classmethod
    @error_status
    def parse_proto_TableName(cls, param):
        return param.table_name

    @classmethod
    @error_status
    def parse_proto_Index(cls, param):
        _index = {
            'index_type': param.index_type,
            'nlist': param.nlist
        }

        return _index

    @classmethod
    @error_status
    def parse_proto_IndexParam(cls, param):
        _table_name = param.table_name
        _status, _index = cls.parse_proto_Index(param.index)

        if not _status.OK():
            raise Exception("Argument parse error")

        return _table_name, _index

    @classmethod
    @error_status
    def parse_proto_Command(cls, param):
        _cmd = param.cmd

        return _cmd

    @classmethod
    @error_status
    def parse_proto_Range(cls, param):
        _start_value = param.start_value
        _end_value = param.end_value

        return _start_value, _end_value

    @classmethod
    @error_status
    def parse_proto_RowRecord(cls, param):
        return list(param.vector_data)

    @classmethod
    def parse_proto_PartitionParam(cls, param):
        _table_name = param.table_name
        _partition_name = param.partition_name
        _tag = param.tag

        return _table_name, _partition_name, _tag

    @classmethod
    @error_status
    def parse_proto_SearchParam(cls, param):
        _table_name = param.table_name
        _topk = param.topk
        _nprobe = param.nprobe
        _status, _range = cls.parse_proto_Range(param.query_range_array)

        if not _status.OK():
            raise Exception("Argument parse error")

        _row_record = param.query_record_array

        return _table_name, _row_record, _range, _topk

    @classmethod
    @error_status
    def parse_proto_DeleteByRangeParam(cls, param):
        _table_name = param.table_name
        _range = param.range
        _start_value = _range.start_value
        _end_value = _range.end_value

        return _table_name, _start_value, _end_value
