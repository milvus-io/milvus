import pytest


def pytest_addoption(parser):
    parser.addoption("--chaos_type", action="store", default="pod_kill", help="chaos_type")
    parser.addoption("--role_type", action="store", default="activated", help="role_type")
    parser.addoption("--target_component", action="store", default="querynode", help="target_component")
    parser.addoption("--target_pod", action="store", default="etcd_leader", help="target_pod")
    parser.addoption("--target_scope", action="store", default="all", help="target_scope")
    parser.addoption("--target_number", action="store", default="1", help="target_number")
    parser.addoption("--chaos_duration", action="store", default="7m", help="chaos_duration")
    parser.addoption("--chaos_interval", action="store", default="2m", help="chaos_interval")
    parser.addoption("--wait_signal", action="store", type=bool, default=True, help="wait_signal")
    parser.addoption("--enable_import", action="store", type=bool, default=False, help="enable_import")
    parser.addoption("--collection_num", action="store", default="1", help="collection_num")


@pytest.fixture
def chaos_type(request):
    return request.config.getoption("--chaos_type")


@pytest.fixture
def role_type(request):
    return request.config.getoption("--role_type")


@pytest.fixture
def target_component(request):
    return request.config.getoption("--target_component")


@pytest.fixture
def target_pod(request):
    return request.config.getoption("--target_pod")


@pytest.fixture
def target_scope(request):
    return request.config.getoption("--target_scope")


@pytest.fixture
def target_number(request):
    return request.config.getoption("--target_number")


@pytest.fixture
def collection_num(request):
    return request.config.getoption("--collection_num")


@pytest.fixture
def chaos_duration(request):
    return request.config.getoption("--chaos_duration")


@pytest.fixture
def chaos_interval(request):
    return request.config.getoption("--chaos_interval")


@pytest.fixture
def wait_signal(request):
    return request.config.getoption("--wait_signal")


@pytest.fixture
def enable_import(request):
    return request.config.getoption("--enable_import")
