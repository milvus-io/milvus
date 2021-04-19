import socket
import pytest

from .utils import *

timeout = 60
dimension = 128
delete_timeout = 60


def pytest_addoption(parser):
    parser.addoption("--ip", action="store", default="localhost")
    parser.addoption("--service", action="store", default="")
    parser.addoption("--port", action="store", default=19530)
    parser.addoption("--http-port", action="store", default=19121)
    parser.addoption("--handler", action="store", default="GRPC")
    parser.addoption("--tag", action="store", default="all", help="only run tests matching the tag.")
    parser.addoption('--dry-run', action='store_true', default=False)


def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line(
        "markers", "tag(name): mark test to run only matching the tag"
    )


def pytest_runtest_setup(item):
    tags = list()
    for marker in item.iter_markers(name="tag"):
        for tag in marker.args:
            tags.append(tag)
    if tags:
        cmd_tag = item.config.getoption("--tag")
        if cmd_tag != "all" and cmd_tag not in tags:
            pytest.skip("test requires tag in {!r}".format(tags))


def pytest_runtestloop(session):
    if session.config.getoption('--dry-run'):
        total_passed = 0
        total_skipped = 0
        test_file_to_items = {}
        for item in session.items:
            file_name, test_class, test_func = item.nodeid.split("::")
            if test_file_to_items.get(file_name) is not None:
                test_file_to_items[file_name].append(item)
            else:
                test_file_to_items[file_name] = [item]
        for k, items in test_file_to_items.items():
            skip_case = []
            should_pass_but_skipped = []
            skipped_other_reason = []

            level2_case = []
            for item in items:
                if "pytestmark" in item.keywords.keys():
                    markers = item.keywords["pytestmark"]
                    skip_case.extend([item.nodeid for marker in markers if marker.name == 'skip'])
                    should_pass_but_skipped.extend([item.nodeid for marker in markers if marker.name == 'skip' and len(marker.args) > 0 and marker.args[0] == "should pass"])
                    skipped_other_reason.extend([item.nodeid for marker in markers if marker.name == 'skip' and (len(marker.args) < 1 or marker.args[0] != "should pass")])
                    level2_case.extend([item.nodeid for marker in markers if marker.name == 'level' and marker.args[0] == 2])

            print("")
            print(f"[{k}]:")
            print(f"    Total   : {len(items):13}")
            print(f"    Passed  : {len(items) - len(skip_case):13}")
            print(f"    Skipped : {len(skip_case):13}")
            print(f"      - should pass:   {len(should_pass_but_skipped):4}")
            print(f"      - not supported: {len(skipped_other_reason):4}")
            print(f"    Level2  : {len(level2_case):13}")

            print(f"    ---------------------------------------")
            print(f"    should pass but skipped: ")
            print("")
            for nodeid in should_pass_but_skipped:
                name, test_class, test_func = nodeid.split("::")
                print(f"         {name:8}: {test_class}.{test_func}")
            print("")
            print(f"===============================================")
            total_passed += len(items) - len(skip_case)
            total_skipped += len(skip_case)

        print("Total tests : ", len(session.items))
        print("Total passed: ", total_passed)
        print("Total skiped: ", total_skipped)
        return True


def check_server_connection(request):
    ip = request.config.getoption("--ip")
    port = request.config.getoption("--port")

    connected = True
    if ip and (ip not in ['localhost', '127.0.0.1']):
        try:
            socket.getaddrinfo(ip, port, 0, 0, socket.IPPROTO_TCP) 
        except Exception as e:
            print("Socket connnet failed: %s" % str(e))
            connected = False
    return connected


@pytest.fixture(scope="module")
def connect(request):
    ip = request.config.getoption("--ip")
    service_name = request.config.getoption("--service")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http-port")
    handler = request.config.getoption("--handler")
    if handler == "HTTP":
        port = http_port
    try:
        milvus = get_milvus(host=ip, port=port, handler=handler)
        # reset_build_index_threshold(milvus)
    except Exception as e:
        logging.getLogger().error(str(e))
        pytest.exit("Milvus server can not connected, exit pytest ...")
    def fin():
        try:
            milvus.close()
            pass
        except Exception as e:
            logging.getLogger().info(str(e))
    request.addfinalizer(fin)
    return milvus


@pytest.fixture(scope="module")
def dis_connect(request):
    ip = request.config.getoption("--ip")
    service_name = request.config.getoption("--service")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http-port")
    handler = request.config.getoption("--handler")
    if handler == "HTTP":
        port = http_port
    milvus = get_milvus(host=ip, port=port, handler=handler)
    milvus.close()
    return milvus


@pytest.fixture(scope="module")
def args(request):
    ip = request.config.getoption("--ip")
    service_name = request.config.getoption("--service")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http-port")
    handler = request.config.getoption("--handler")
    if handler == "HTTP":
        port = http_port
    args = {"ip": ip, "port": port, "handler": handler, "service_name": service_name}
    return args


@pytest.fixture(scope="module")
def milvus(request):
    ip = request.config.getoption("--ip")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http-port")
    handler = request.config.getoption("--handler")
    if handler == "HTTP":
        port = http_port
    return get_milvus(host=ip, port=port, handler=handler)


@pytest.fixture(scope="function")
def collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    try:
        default_fields = gen_default_fields()
        connect.create_collection(collection_name, default_fields)
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        if connect.has_collection(collection_name):
            connect.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert connect.has_collection(collection_name)
    return collection_name


# customised id
@pytest.fixture(scope="function")
def id_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    try:
        fields = gen_default_fields(auto_id=False)
        connect.create_collection(collection_name, fields)
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        if connect.has_collection(collection_name):
            connect.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert connect.has_collection(collection_name)
    return collection_name


@pytest.fixture(scope="function")
def binary_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    try:
        fields = gen_binary_default_fields()
        connect.create_collection(collection_name, fields)
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        collection_names = connect.list_collections()
        if connect.has_collection(collection_name):
            connect.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert connect.has_collection(collection_name)
    return collection_name


# customised id
@pytest.fixture(scope="function")
def binary_id_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    try:
        fields = gen_binary_default_fields(auto_id=False)
        connect.create_collection(collection_name, fields)
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        if connect.has_collection(collection_name):
            connect.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert connect.has_collection(collection_name)
    return collection_name
