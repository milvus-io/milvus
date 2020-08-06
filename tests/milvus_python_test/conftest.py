import pdb
import logging
import socket
import pytest
from utils import gen_unique_str
from milvus import Milvus, DataType
from utils import *

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
        collection_names = connect.list_collections()
        for collection_name in collection_names:
            connect.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert connect.has_collection(collection_name)
    return collection_name


@pytest.fixture(scope="function")
def id_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    try:
        fields = gen_default_fields(auto_id=True)
        connect.create_collection(collection_name, fields)
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        collection_names = connect.list_collections()
        for collection_name in collection_names:
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
        for collection_name in collection_names:
            connect.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert connect.has_collection(collection_name)
    return collection_name


@pytest.fixture(scope="function")
def binary_id_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    try:
        fields = gen_binary_default_fields(auto_id=True)
        connect.create_collection(collection_name, fields)
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        collection_names = connect.list_collections()
        for collection_name in collection_names:
            connect.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert connect.has_collection(collection_name)
    return collection_name
