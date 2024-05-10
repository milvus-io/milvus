import pytest


def pytest_addoption(parser):
    parser.addoption("--image_tag", action="store", default="master-latest", help="image_tag")


@pytest.fixture
def image_tag(request):
    return request.config.getoption("--image_tag")

