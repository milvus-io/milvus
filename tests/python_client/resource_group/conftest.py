import pytest


def pytest_addoption(parser):
    parser.addoption("--image_tag", action="store", default="master-20240510-f7d29118-amd64", help="image_tag")


@pytest.fixture
def image_tag(request):
    return request.config.getoption("--image_tag")

