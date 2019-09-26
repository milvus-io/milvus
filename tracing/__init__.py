from grpc_opentracing import SpanDecorator

class GrpcSpanDecorator(SpanDecorator):
    def __call__(self, span, rpc_info):
        if rpc_info.response.status.error_code == 0:
            return
        span.set_tag('error', True)
        error_log = {'event': 'error',
                'error.kind': str(rpc_info.response.status.error_code),
                'message': rpc_info.response.status.reason
        }
        span.log_kv(error_log)

def empty_server_interceptor_decorator(target_server, interceptor):
    return target_server

class Tracer:
    def __init__(self, tracer=None,
            interceptor=None,
            server_decorator=empty_server_interceptor_decorator):
        self.tracer = tracer
        self.interceptor = interceptor
        self.server_decorator=server_decorator

    def decorate(self, server):
        return self.server_decorator(server, self.interceptor)

    def close(self):
        self.tracer and self.tracer.close()
