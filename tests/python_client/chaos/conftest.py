import pytest


def pytest_addoption(parser):
    parser.addoption("--chaos_type", action="store", default="pod_kill", help="chaos_type")
    parser.addoption("--role_type", action="store", default="activated", help="role_type")
    parser.addoption("--target_component", action="store", default="querynode", help="target_component")
    parser.addoption("--target_pod", action="store", default="etcd_leader", help="target_pod")
    parser.addoption("--target_scope", action="store", default="all", help="target_scope")
    parser.addoption("--target_number", action="store", default="1", help="target_number")
    parser.addoption("--chaos_duration", action="store", default="7m", help="chaos_duration")
    parser.addoption("--chaos_interval", action="store", default="2m", help="chaos_interval")
    parser.addoption("--wait_signal", action="store", type=bool, default=True, help="wait_signal")
    parser.addoption("--enable_import", action="store", type=bool, default=False, help="enable_import")
    parser.addoption(
        "--search_consistency_level",
        action="store",
        default="",
        help="consistency level for chaos search requests",
    )
    parser.addoption(
        "--query_consistency_level",
        action="store",
        default="",
        help="consistency level for chaos query requests",
    )
    parser.addoption(
        "--target_rgs",
        action="store",
        default="",
        help="comma-separated resource group names to target for chaos (e.g. rg1,rg2)",
    )
    parser.addoption(
        "--chaos_mode",
        action="store",
        default="one",
        help="chaos mode: 'one' (random single pod) or 'all' (all matching pods)",
    )
    parser.addoption(
        "--target_components",
        action="store",
        default="querynode,streamingnode",
        help="comma-separated components to inject chaos (e.g. querynode,streamingnode)",
    )
    parser.addoption(
        "--chaos_template",
        action="store",
        default="",
        help="path to external ChaosMesh YAML template (overrides built-in config)",
    )
    parser.addoption("--collection_num", action="store", default="1", help="collection_num")
    parser.addoption(
        "--search_timeout",
        action="store",
        type=float,
        default=None,
        help="search API timeout in seconds for chaos client requests",
    )
    parser.addoption(
        "--query_timeout",
        action="store",
        type=float,
        default=None,
        help="query API timeout in seconds for chaos client requests",
    )


@pytest.fixture(scope="session", autouse=True)
def configure_client_timeouts(request):
    import chaos.checker as checker

    search_timeout = request.config.getoption("--search_timeout")
    query_timeout = request.config.getoption("--query_timeout")
    search_consistency_level = request.config.getoption("--search_consistency_level")
    query_consistency_level = request.config.getoption("--query_consistency_level")
    checker.configure_request_options(
        search_timeout_value=search_timeout,
        query_timeout_value=query_timeout,
        search_consistency_level_value=search_consistency_level,
        query_consistency_level_value=query_consistency_level,
    )


@pytest.fixture
def chaos_type(request):
    return request.config.getoption("--chaos_type")


@pytest.fixture
def role_type(request):
    return request.config.getoption("--role_type")


@pytest.fixture
def target_component(request):
    return request.config.getoption("--target_component")


@pytest.fixture
def target_pod(request):
    return request.config.getoption("--target_pod")


@pytest.fixture
def target_scope(request):
    return request.config.getoption("--target_scope")


@pytest.fixture
def target_number(request):
    return request.config.getoption("--target_number")


@pytest.fixture
def collection_num(request):
    return request.config.getoption("--collection_num")


@pytest.fixture
def chaos_duration(request):
    return request.config.getoption("--chaos_duration")


@pytest.fixture
def chaos_interval(request):
    return request.config.getoption("--chaos_interval")


@pytest.fixture
def wait_signal(request):
    return request.config.getoption("--wait_signal")


@pytest.fixture
def enable_import(request):
    return request.config.getoption("--enable_import")


@pytest.fixture
def search_timeout(request):
    return request.config.getoption("--search_timeout")


@pytest.fixture
def query_timeout(request):
    return request.config.getoption("--query_timeout")


@pytest.fixture
def search_consistency_level(request):
    return request.config.getoption("--search_consistency_level")


@pytest.fixture
def query_consistency_level(request):
    return request.config.getoption("--query_consistency_level")


@pytest.fixture
def target_rgs(request):
    return request.config.getoption("--target_rgs")


@pytest.fixture
def chaos_mode(request):
    return request.config.getoption("--chaos_mode")


@pytest.fixture
def target_components(request):
    return request.config.getoption("--target_components")


@pytest.fixture
def chaos_template(request):
    return request.config.getoption("--chaos_template")
