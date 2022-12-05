import pytest
import common.common_func as cf
from check.param_check import ip_check, number_check
from config.log_config import log_config
from utils.util_log import test_log as log
from common.common_func import param_info


def pytest_addoption(parser):
    parser.addoption("--host", action="store", default="127.0.0.1", help="Milvus host")
    parser.addoption("--port", action="store", default="9091", help="Milvus http port")
    parser.addoption('--clean_log', action='store_true', default=False, help="clean log before testing")


@pytest.fixture
def host(request):
    return request.config.getoption("--host")


@pytest.fixture
def port(request):
    return request.config.getoption("--port")


@pytest.fixture
def clean_log(request):
    return request.config.getoption("--clean_log")


@pytest.fixture(scope="session", autouse=True)
def initialize_env(request):
    """ clean log before testing """
    host = request.config.getoption("--host")
    port = request.config.getoption("--port")
    clean_log = request.config.getoption("--clean_log")


    """ params check """
    assert ip_check(host) and number_check(port)

    """ modify log files """
    file_path_list = [log_config.log_debug, log_config.log_info, log_config.log_err]
    if log_config.log_worker != "":
        file_path_list.append(log_config.log_worker)
    cf.modify_file(file_path_list=file_path_list, is_modify=clean_log)

    log.info("#" * 80)
    log.info("[initialize_milvus] Log cleaned up, start testing...")
    param_info.prepare_param_info(host, port)
