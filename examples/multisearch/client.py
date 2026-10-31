"""Prototype MultiSearch client: one gRPC invocation, no client-side fan-out.

The message imports reuse PyMilvus's ordinary SearchRequest/SearchResults.
This adapter is intentionally separate from the released PyMilvus package.
"""
import base64
import sys
from collections import Counter

import grpc
from pymilvus.grpc_gen import common_pb2, milvus_pb2, milvus_pb2_grpc

# protoc's upstream imports are unqualified; bind them to PyMilvus's descriptors
# instead of generating a second, conflicting copy of the public Milvus API.
sys.modules.setdefault("common_pb2", common_pb2)
sys.modules.setdefault("milvus_pb2", milvus_pb2)
import multisearch_pb2  # noqa: E402


class _Calls(grpc.UnaryUnaryClientInterceptor):
    def __init__(self):
        self.counts = Counter()

    def intercept_unary_unary(self, continuation, details, request):
        self.counts[details.method] += 1
        return continuation(details, request)


class MultiSearchClient:
    def __init__(self, target="localhost:29550", token="root:Milvus", db_name="default"):
        self.calls = _Calls()
        self.channel = grpc.intercept_channel(
            grpc.insecure_channel(target, options=[("grpc.max_receive_message_length", 64 << 20)]),
            self.calls,
        )
        self.metadata = (("authorization", base64.b64encode(token.encode()).decode()), ("dbname", db_name))
        self.db_name = db_name
        self.stub = milvus_pb2_grpc.MilvusServiceStub(self.channel)
        self._multi = self.channel.unary_unary(
            "/milvus.proto.multisearch.MultiSearchService/MultiSearch",
            request_serializer=multisearch_pb2.MultiSearchRequest.SerializeToString,
            response_deserializer=multisearch_pb2.MultiSearchResponse.FromString,
        )

    def multi_search(self, items, timeout=10):
        """Return all outcomes, including individual failures, without retrying."""
        request = multisearch_pb2.MultiSearchRequest(db_name=self.db_name)
        for item_id, search_request in items:
            request.items.add(id=item_id, search=search_request)
        return self._multi(request, timeout=timeout, metadata=self.metadata)

    def search(self, request, timeout=10):
        return self.stub.Search(request, timeout=timeout, metadata=self.metadata)

    def close(self):
        self.channel.close()
