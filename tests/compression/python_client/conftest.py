import pytest
import yaml


def pytest_addoption(parser):
    parser.addoption("--endpoint", action="store", default="127.0.0.1:19530", help="endpoint")


@pytest.fixture
def endpoint(request):
    return request.config.getoption("--endpoint")

