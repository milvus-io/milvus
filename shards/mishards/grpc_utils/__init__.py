from grpc_opentracing import SpanDecorator
from milvus.grpc_gen import status_pb2


class GrpcSpanDecorator(SpanDecorator):
    def __call__(self, span, rpc_info):
        status = None
        if not rpc_info.response:
            return
        if isinstance(rpc_info.response, status_pb2.Status):
            status = rpc_info.response
        else:
            try:
                status = rpc_info.response.status
            except Exception as e:
                status = status_pb2.Status(error_code=status_pb2.UNEXPECTED_ERROR,
                                           reason='Should not happen')

        if status.error_code == 0:
            return
        error_log = {'event': 'error',
                     'request': rpc_info.request,
                     'response': rpc_info.response
                     }
        span.set_tag('error', True)
        span.log_kv(error_log)


def mark_grpc_method(func):
    setattr(func, 'grpc_method', True)
    return func


def is_grpc_method(func):
    if not func:
        return False
    return getattr(func, 'grpc_method', False)
