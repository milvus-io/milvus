import pytest

import common.common_type as ct
import common.common_func as cf
from utils.util_log import test_log as log
from base.client_base import param_info
from check.param_check import ip_check, number_check
from config.log_config import log_config


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
    parser.addoption('--term_expr', action='store', default="term_expr", help="expr of query quest")
    parser.addoption('--check_content', action='store', default="check_content", help="content of check")
    parser.addoption('--field_name', action='store', default="field_name", help="field_name of index")


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


@pytest.fixture
def term_expr(request):
    return request.config.getoption("--term_expr")


@pytest.fixture
def check_content(request):
    log.error("^" * 50)
    log.error("check_content")
    return request.config.getoption("--check_content")


@pytest.fixture
def field_name(request):
    return request.config.getoption("--field_name")


""" fixture func """


@pytest.fixture(scope="session", autouse=True)
def initialize_env(request):
    """ clean log before testing """
    host = request.config.getoption("--host")
    port = request.config.getoption("--port")
    handler = request.config.getoption("--handler")
    clean_log = request.config.getoption("--clean_log")

    """ params check """
    assert ip_check(host) and number_check(port)

    """ modify log files """
    cf.modify_file(file_path_list=[log_config.log_debug, log_config.log_info, log_config.log_err], is_modify=clean_log)

    log.info("#" * 80)
    log.info("[initialize_milvus] Log cleaned up, start testing...")
    param_info.prepare_param_info(host, port, handler)


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_string(request):
    yield request.param


@pytest.fixture(params=cf.gen_simple_index())
def get_index_param(request):
    yield request.param


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_collection_name(request):
    yield request.param


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_field_name(request):
    yield request.param


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_index_type(request):
    yield request.param


# TODO: construct invalid index params for all index types
@pytest.fixture(params=[{"metric_type": "L3", "index_type": "IVF_FLAT"},
                        {"metric_type": "L2", "index_type": "IVF_FLAT", "err_params": {"nlist": 10}},
                        {"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": -1}}])
def get_invalid_index_params(request):
    yield request.param


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_partition_name(request):
    yield request.param
