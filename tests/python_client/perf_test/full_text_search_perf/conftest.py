import pytest


def pytest_addoption(parser):
    parser.addoption("--es_host", action="store", default="locust", help="es_host")
    parser.addoption("--dataset_dir", action="store", default="~/beir-dataset", help="beir-dataset directory")
    parser.addoption("--dataset_name", action="store", default="msmarco", help="beir-dataset name")


@pytest.fixture
def es_host(request):
    return request.config.getoption("--es_host")


@pytest.fixture
def dataset_dir(request):
    return request.config.getoption("--dataset_dir")


@pytest.fixture
def dataset_name(request):
    return request.config.getoption("--dataset_name")
