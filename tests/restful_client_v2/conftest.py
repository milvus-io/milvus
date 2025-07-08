import pytest
import yaml


def pytest_addoption(parser):
    parser.addoption("--endpoint", action="store", default="http://127.0.0.1:19530", help="endpoint")
    parser.addoption("--token", action="store", default="root:Milvus", help="token")
    parser.addoption("--minio_host", action="store", default="127.0.0.1", help="minio host")
    parser.addoption("--bucket_name", action="store", default="milvus-bucket", help="minio bucket name")
    parser.addoption("--root_path", action="store", default="file", help="minio bucket root path")
    parser.addoption("--release_name", action="store", default="my-release", help="release name")
    # a tei endpoint for text embedding, default is http://text-embeddings-service.milvus-ci.svc.cluster.local:80 which is deployed in house
    parser.addoption("--tei_endpoint", action="store", default="http://text-embeddings-service.milvus-ci.svc.cluster.local:80", help="tei endpoint")
    parser.addoption("--tei_reranker_endpoint", action="store", default="http://text-rerank-service.milvus-ci.svc.cluster.local:80", help="tei reranker endpoint")

@pytest.fixture
def endpoint(request):
    return request.config.getoption("--endpoint")


@pytest.fixture
def token(request):
    return request.config.getoption("--token")


@pytest.fixture
def minio_host(request):
    return request.config.getoption("--minio_host")


@pytest.fixture
def bucket_name(request):
    return request.config.getoption("--bucket_name")


@pytest.fixture
def root_path(request):
    return request.config.getoption("--root_path")


@pytest.fixture
def release_name(request):
    return request.config.getoption("--release_name")

@pytest.fixture
def tei_endpoint(request):
    return request.config.getoption("--tei_endpoint")

@pytest.fixture
def tei_reranker_endpoint(request):
    return request.config.getoption("--tei_reranker_endpoint")