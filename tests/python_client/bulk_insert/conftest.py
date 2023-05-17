import pytest


def pytest_addoption(parser):
    parser.addoption("--file_type", action="store", default="json", help="filetype")
    parser.addoption("--create_index", action="store",  default="create_index", help="whether creating index")
    parser.addoption("--is_loaded", action="store", default="loaded", help="is loaded")
    parser.addoption("--use_same_collection", action="store", default="true", help="use same collection")
    parser.addoption("--nb", action="store", default="", help="nb")
    parser.addoption("--dim", action="store",  default="2048", help="dim")


@pytest.fixture
def file_type(request):
    return request.config.getoption("--file_type")


@pytest.fixture
def create_index(request):
    return request.config.getoption("--create_index")


@pytest.fixture
def is_loaded(request):
    return request.config.getoption("--is_loaded")


@pytest.fixture
def use_same_collection(request):
    return request.config.getoption("--use_same_collection")


@pytest.fixture
def nb(request):
    return request.config.getoption("--nb")


@pytest.fixture
def dim(request):
    return request.config.getoption("--dim")