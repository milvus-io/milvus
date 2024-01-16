import pytest
import yaml


def pytest_addoption(parser):
    parser.addoption("--endpoint", action="store", default="http://127.0.0.1:19530", help="endpoint")
    parser.addoption("--token", action="store", default="root:Milvus", help="token")


@pytest.fixture
def endpoint(request):
    return request.config.getoption("--endpoint")


@pytest.fixture
def token(request):
    return request.config.getoption("--token")


