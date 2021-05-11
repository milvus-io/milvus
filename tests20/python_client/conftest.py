import pytest


def pytest_addoption(parser):
    parser.addoption("--ip", action="store", default="localhost", help="service's ip")
    parser.addoption("--service", action="store", default="", help="service address")
    parser.addoption("--port", action="store", default=19530, help="service's port")
    parser.addoption("--http_port", action="store", default=19121, help="http's port")
    parser.addoption("--handler", action="store", default="GRPC", help="handler of request")
    parser.addoption("--tag", action="store", default="all", help="only run tests matching the tag.")
    parser.addoption('--dry_run', action='store_true', default=False, help="")


@pytest.fixture
def ip(request):
    return request.config.getoption("--ip")


@pytest.fixture
def service(request):
    return request.config.getoption("--service")


@pytest.fixture
def port(request):
    return request.config.getoption("--port")


@pytest.fixture
def http_port(request):
    return request.config.getoption("--http_port")


@pytest.fixture
def handler(request):
    return request.config.getoption("--handler")


@pytest.fixture
def tag(request):
    return request.config.getoption("--tag")


@pytest.fixture
def dry_run(request):
    return request.config.getoption("--dry_run")



