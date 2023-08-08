import pytest
import yaml


def pytest_addoption(parser):
    parser.addoption("--protocol", action="store", default="http", help="host")
    parser.addoption("--host", action="store", default="127.0.0.1", help="host")
    parser.addoption("--port", action="store", default="19530", help="port")
    parser.addoption("--username", action="store", default="root", help="email")
    parser.addoption("--password", action="store", default="Milvus", help="password")


@pytest.fixture
def protocol(request):
    return request.config.getoption("--protocol")


@pytest.fixture
def host(request):
    return request.config.getoption("--host")


@pytest.fixture
def port(request):
    return request.config.getoption("--port")


@pytest.fixture
def username(request):
    return request.config.getoption("--username")


@pytest.fixture
def password(request):
    return request.config.getoption("--password")

