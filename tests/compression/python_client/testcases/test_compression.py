import grpc
from pymilvus.grpc_gen import milvus_pb2_grpc, milvus_pb2
import pytest

from pymilvus import connections, Collection, DataType, FieldSchema, CollectionSchema
collection_name = "book"
dim = 128
book_id = FieldSchema(
    name="book_id",
    dtype=DataType.INT64,
    auto_id=True,
    is_primary=True,
)
word = FieldSchema(
    name="word",
    dtype=DataType.VARCHAR,
    max_length=65535,
)
word_count = FieldSchema(
    name="word_count",
    dtype=DataType.INT64,
)
book_intro = FieldSchema(
    name="book_intro",
    dtype=DataType.FLOAT_VECTOR,
    dim=dim
)
schema = CollectionSchema(
    fields=[book_id, word, word_count, book_intro],
)

@pytest.fixture
def create_collection(endpoint):
    connections.connect("default", host=endpoint.split(":")[0], port=endpoint.split(":")[1])
    if connections.has_connection("default") is False:
        raise Exception("no connections")
    collection = Collection(
        name=collection_name,
        schema=schema,
        using='default',
    )

def insert(endpoint, compression_name, request):
    channel = grpc.insecure_channel(endpoint, compression=compression_name)
    stub = milvus_pb2_grpc.MilvusServiceStub(channel)
    response = stub.Insert.future(request).result()

@pytest.mark.normal
class TestNormal():

    @pytest.mark.parametrize("compression_name", [grpc.Compression.NoCompression, grpc.Compression.Deflate, grpc.Compression.Gzip])
    def test_show_collections(self, compression_name, create_collection, endpoint):
        channel = grpc.insecure_channel(endpoint, compression=compression_name)
        stub = milvus_pb2_grpc.MilvusServiceStub(channel)
        request = milvus_pb2.ShowCollectionsRequest()
        response = stub.ShowCollections.future(request).result()

@pytest.mark.bench
class TestBench():

    @pytest.mark.parametrize("compression_name", [grpc.Compression.NoCompression, grpc.Compression.Deflate, grpc.Compression.Gzip])
    @pytest.mark.parametrize("rows_num", [128, 256, 512, 1024])
    @pytest.mark.parametrize("content_length", [1024, 4096, 16384, 32768, 63488])
    def test_insert(self, compression_name, rows_num, content_length, benchmark, create_collection, endpoint):
        from pymilvus.client.prepare import Prepare
        import random, string
        rows = [{"word": ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(content_length)]),
                 "book_intro": [random.random() for _ in range(dim)], "word_count": 1} for _ in range(rows_num)]
        request = Prepare.row_insert_param("", rows, "", fields_info=schema.to_dict()["fields"], enable_dynamic=True)
        benchmark(insert, endpoint, compression_name, request)