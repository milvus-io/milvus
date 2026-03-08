import pytest


def pytest_addoption(parser):
    parser.addoption("--file_type", action="store", default="json", help="filetype")
    parser.addoption("--create_index", action="store",  default="create_index", help="whether creating index")
    parser.addoption("--nb", action="store", default=50000, help="nb")
    parser.addoption("--dim", action="store",  default=768, help="dim")
    parser.addoption("--varchar_len", action="store", default=2000, help="varchar_len")
    parser.addoption("--with_varchar_field", action="store", default="true", help="with varchar field or not")

@pytest.fixture
def file_type(request):
    return request.config.getoption("--file_type")


@pytest.fixture
def create_index(request):
    return request.config.getoption("--create_index")

@pytest.fixture
def nb(request):
    return request.config.getoption("--nb")

@pytest.fixture
def dim(request):
    return request.config.getoption("--dim")

@pytest.fixture
def varchar_len(request):
    return request.config.getoption("--varchar_len")

@pytest.fixture
def with_varchar_field(request):
    return request.config.getoption("--with_varchar_field")
