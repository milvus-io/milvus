from grpc_opentracing import SpanDecorator
from milvus.grpc_gen import status_pb2


class GrpcSpanDecorator(SpanDecorator):
    def __call__(self, span, rpc_info):
        status = None
        if isinstance(rpc_info.response, status_pb2.Status):
            status = rpc_info.response
        else:
            status = rpc_info.response.status
        if status.error_code == 0:
            return
        span.set_tag('error', True)
        span.set_tag('error_code', status.error_code)
        error_log = {'event': 'error',
                'request': rpc_info.request,
                'response': rpc_info.response
        }
        span.log_kv(error_log)

def mark_grpc_method(func):
    setattr(func, 'grpc_method', True)
    return func
