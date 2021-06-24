import pytest


def pytest_addoption(parser):
    parser.addoption("--ip", action="store", default="localhost", help="service's ip")
    parser.addoption("--host", action="store", default="localhost", help="service's ip")
    parser.addoption("--service", action="store", default="", help="service address")
    parser.addoption("--port", action="store", default=19530, help="service's port")
    parser.addoption("--http_port", action="store", default=19121, help="http's port")
    parser.addoption("--handler", action="store", default="GRPC", help="handler of request")
    parser.addoption("--tag", action="store", default="all", help="only run tests matching the tag.")
    parser.addoption('--dry_run', action='store_true', default=False, help="")
    parser.addoption('--partition_name', action='store', default="partition_name", help="name of partition")
    parser.addoption('--connect_name', action='store', default="connect_name", help="name of connect")
    parser.addoption('--descriptions', action='store', default="partition_des", help="descriptions of partition")
    parser.addoption('--collection_name', action='store', default="collection_name", help="name of collection")
    parser.addoption('--search_vectors', action='store', default="search_vectors", help="vectors of search")
    parser.addoption('--index_param', action='store', default="index_param", help="index_param of index")
    parser.addoption('--data', action='store', default="data", help="data of request")
    parser.addoption('--clean_log', action='store_true', default=False, help="clean log before testing")
    parser.addoption('--schema', action='store', default="schema", help="schema of test interface")
    parser.addoption('--err_msg', action='store', default="err_msg", help="error message of test")


@pytest.fixture
def ip(request):
    return request.config.getoption("--ip")


@pytest.fixture
def host(request):
    return request.config.getoption("--host")


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


@pytest.fixture
def connect_name(request):
    return request.config.getoption("--connect_name")


@pytest.fixture
def partition_name(request):
    return request.config.getoption("--partition_name")


@pytest.fixture
def descriptions(request):
    return request.config.getoption("--descriptions")


@pytest.fixture
def collection_name(request):
    return request.config.getoption("--collection_name")


@pytest.fixture
def search_vectors(request):
    return request.config.getoption("--search_vectors")


@pytest.fixture
def index_param(request):
    return request.config.getoption("--index_param")


@pytest.fixture
def data(request):
    return request.config.getoption("--data")


@pytest.fixture
def clean_log(request):
    return request.config.getoption("--clean_log")


@pytest.fixture
def schema(request):
    return request.config.getoption("--schema")


@pytest.fixture
def err_msg(request):
    return request.config.getoption("--err_msg")
