import pytest


def pytest_addoption(parser):
    parser.addoption("--es_host", action="store", default="locust", help="es_host")
    parser.addoption("--dataset_dir", action="store", default="~/beir-dataset", help="beir-dataset directory")

@pytest.fixture
def es_host(request):
    return request.config.getoption("--es_host")

@pytest.fixture
def dataset_dir(request):
    return request.config.getoption("--dataset_dir")
