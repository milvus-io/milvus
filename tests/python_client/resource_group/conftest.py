import pytest


def pytest_addoption(parser):
    parser.addoption("--image_tag", action="store", default="master-20240514-89a7c34c", help="image_tag")


@pytest.fixture
def image_tag(request):
    return request.config.getoption("--image_tag")

