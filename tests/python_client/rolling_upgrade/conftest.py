import pytest


timeout = 60
dimension = 128
delete_timeout = 60


def pytest_addoption(parser):
    parser.addoption(
        "--release_name",
        type=str,
        action="store",
        default="deploy-test",
        help="release name for deploy test",
    )
    parser.addoption(
        "--new_image_repo",
        type=str,
        action="store",
        default="harbor.milvus.io/dockerhub/milvusdb/milvus",
        help="image repo",
    )
    parser.addoption(
        "--new_image_tag",
        type=str,
        action="store",
        default="master-20231031-ab6dbf76",
        help="image tag",
    )
    parser.addoption(
        "--components_order",
        type=str,
        action="store",
        default="['indexNode', 'rootCoord', ['dataCoord', 'indexCoord'], 'queryCoord', 'queryNode', 'dataNode', 'proxy']",
        help="components update order",
    )
    parser.addoption(
        "--paused_components",
        type=str,
        action="store",
        default="['queryNode']",
        help="components will be paused during rolling update",
    )
    parser.addoption(
        "--paused_duration",
        type=int,
        action="store",
        default=300,
        help="paused duration for rolling update in some components",
    )
    parser.addoption(
        "--prepare_data", action="store", type=bool, default=False, help="prepare_data"
    )


@pytest.fixture
def release_name(request):
    return request.config.getoption("--release_name")


@pytest.fixture
def new_image_repo(request):
    return request.config.getoption("--new_image_repo")


@pytest.fixture
def new_image_tag(request):
    return request.config.getoption("--new_image_tag")


@pytest.fixture
def components_order(request):
    return request.config.getoption("--components_order")


@pytest.fixture
def paused_components(request):
    return request.config.getoption("--paused_components")


@pytest.fixture
def paused_duration(request):
    return request.config.getoption("--paused_duration")


@pytest.fixture
def prepare_data(request):
    return request.config.getoption("--prepare_data")
