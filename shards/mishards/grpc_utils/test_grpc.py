import logging
import opentracing
from mishards.grpc_utils import GrpcSpanDecorator, is_grpc_method
from milvus.grpc_gen import status_pb2, milvus_pb2

logger = logging.getLogger(__name__)


class FakeTracer(opentracing.Tracer):
    pass


class FakeSpan(opentracing.Span):
    def __init__(self, context, tracer, **kwargs):
        super(FakeSpan, self).__init__(tracer, context)
        self.reset()

    def set_tag(self, key, value):
        self.tags.append({key: value})

    def log_kv(self, key_values, timestamp=None):
        self.logs.append(key_values)

    def reset(self):
        self.tags = []
        self.logs = []


class FakeRpcInfo:
    def __init__(self, request, response):
        self.request = request
        self.response = response


class TestGrpcUtils:
    def test_span_deco(self):
        request = 'request'
        OK = status_pb2.Status(error_code=status_pb2.SUCCESS, reason='Success')
        response = OK
        rpc_info = FakeRpcInfo(request=request, response=response)
        span = FakeSpan(context=None, tracer=FakeTracer())
        span_deco = GrpcSpanDecorator()
        span_deco(span, rpc_info)
        assert len(span.logs) == 0
        assert len(span.tags) == 0

        response = milvus_pb2.BoolReply(status=OK, bool_reply=False)
        rpc_info = FakeRpcInfo(request=request, response=response)
        span = FakeSpan(context=None, tracer=FakeTracer())
        span_deco = GrpcSpanDecorator()
        span_deco(span, rpc_info)
        assert len(span.logs) == 0
        assert len(span.tags) == 0

        response = 1
        rpc_info = FakeRpcInfo(request=request, response=response)
        span = FakeSpan(context=None, tracer=FakeTracer())
        span_deco = GrpcSpanDecorator()
        span_deco(span, rpc_info)
        assert len(span.logs) == 1
        assert len(span.tags) == 1

        response = 0
        rpc_info = FakeRpcInfo(request=request, response=response)
        span = FakeSpan(context=None, tracer=FakeTracer())
        span_deco = GrpcSpanDecorator()
        span_deco(span, rpc_info)
        assert len(span.logs) == 0
        assert len(span.tags) == 0

    def test_is_grpc_method(self):
        target = 1
        assert not is_grpc_method(target)
        target = None
        assert not is_grpc_method(target)
