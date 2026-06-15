import pytest
import yaml


def pytest_addoption(parser):
    parser.addoption("--endpoint", action="store", default="http://127.0.0.1:19530", help="endpoint")
    parser.addoption("--token", action="store", default="root:Milvus", help="token")
    parser.addoption("--minio_host", action="store", default="127.0.0.1", help="minio host")
    parser.addoption("--bucket_name", action="store", default="milvus-bucket", help="minio bucket name")
    parser.addoption("--root_path", action="store", default="file", help="minio bucket root path")
    parser.addoption("--release_name", action="store", default="my-release", help="release name")
    parser.addoption("--secondary_endpoint", action="store", default=None, help="secondary Milvus endpoint")
    parser.addoption("--secondary_token", action="store", default="root:Milvus", help="secondary Milvus token")
    parser.addoption("--secondary_release_name", action="store", default=None, help="secondary Milvus release name")
    parser.addoption("--secondary_minio_host", action="store", default=None, help="secondary MinIO host")
    parser.addoption("--secondary_bucket_name", action="store", default=None, help="secondary MinIO bucket name")
    parser.addoption("--secondary_root_path", action="store", default="file", help="secondary MinIO root path")
    parser.addoption(
        "--source-cluster-id", "--source_cluster_id", action="store", default=None, help="CDC source cluster ID"
    )
    parser.addoption(
        "--target-cluster-id", "--target_cluster_id", action="store", default=None, help="CDC target cluster ID"
    )
    parser.addoption(
        "--pchannel-num", "--pchannel_num", action="store", default="16", help="CDC physical channel count"
    )
    # a tei endpoint for text embedding, default is http://text-embeddings-service.milvus-ci.svc.cluster.local:80 which is deployed in house
    parser.addoption(
        "--tei_endpoint",
        action="store",
        default="http://text-embeddings-service.milvus-ci.svc.cluster.local:80",
        help="tei endpoint",
    )
    parser.addoption(
        "--tei_reranker_endpoint",
        action="store",
        default="http://text-rerank-service.milvus-ci.svc.cluster.local:80",
        help="tei reranker endpoint",
    )


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
def secondary_endpoint(request):
    return request.config.getoption("--secondary_endpoint")


@pytest.fixture
def secondary_token(request):
    return request.config.getoption("--secondary_token")


@pytest.fixture
def secondary_release_name(request):
    return request.config.getoption("--secondary_release_name")


@pytest.fixture
def secondary_minio_host(request):
    return request.config.getoption("--secondary_minio_host")


@pytest.fixture
def secondary_bucket_name(request):
    return request.config.getoption("--secondary_bucket_name")


@pytest.fixture
def secondary_root_path(request):
    return request.config.getoption("--secondary_root_path")


@pytest.fixture
def source_cluster_id(request):
    return request.config.getoption("--source_cluster_id")


@pytest.fixture
def target_cluster_id(request):
    return request.config.getoption("--target_cluster_id")


@pytest.fixture
def pchannel_num(request):
    return int(request.config.getoption("--pchannel_num"))


@pytest.fixture
def tei_endpoint(request):
    return request.config.getoption("--tei_endpoint")


@pytest.fixture
def tei_reranker_endpoint(request):
    return request.config.getoption("--tei_reranker_endpoint")
