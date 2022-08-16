import pytest


def pytest_addoption(parser):
    parser.addoption("--milvus_ns", action="store", default="chaos-testing", help="milvus_ns")
    parser.addoption("--chaos_type", action="store", default="pod_kill", help="chaos_type")
    parser.addoption("--target_component", action="store", default="querynode", help="target_component")
    parser.addoption("--chaos_duration", action="store", default="1m", help="chaos_duration")
    parser.addoption("--chaos_interval", action="store", default="10s", help="chaos_interval")


@pytest.fixture
def milvus_ns(request):
    return request.config.getoption("--milvus_ns")

@pytest.fixture
def chaos_type(request):
    return request.config.getoption("--chaos_type")


@pytest.fixture
def target_component(request):
    return request.config.getoption("--target_component")


@pytest.fixture
def chaos_duration(request):
    return request.config.getoption("--chaos_duration")


@pytest.fixture
def chaos_interval(request):
    return request.config.getoption("--chaos_interval")
