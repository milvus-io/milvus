import pytest
import yaml


def pytest_addoption(parser):
    parser.addoption("--endpoint", action="store", default="http://127.0.0.1:19530", help="endpoint")
    parser.addoption("--token", action="store", default="root:Milvus", help="token")
    parser.addoption("--minio_host", action="store", default="127.0.0.1", help="minio host")


@pytest.fixture
def endpoint(request):
    return request.config.getoption("--endpoint")


@pytest.fixture
def token(request):
    return request.config.getoption("--token")


@pytest.fixture
def minio_host(request):
    return request.config.getoption("--minio_host")
