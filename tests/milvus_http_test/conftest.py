import pdb
import logging
import socket
import pytest
import requests
from utils import gen_unique_str
from client import MilvusClient
from utils import *

timeout = 60
dimension = 128
delete_timeout = 60


def pytest_addoption(parser):
    parser.addoption("--ip", action="store", default="localhost")
    parser.addoption("--service", action="store", default="")
    parser.addoption("--port", action="store", default=19530)
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
        for item in session.items:
            print(item.nodeid)
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
def args(request):
    ip = request.config.getoption("--ip")
    service_name = request.config.getoption("--service")
    port = request.config.getoption("--port")
    args = {"ip": ip, "port": port, "service_name": service_name, "url": "http://%s:%s/" % (ip, port)}
    return args


@pytest.fixture(scope="function")
def collection(request, args):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    client = MilvusClient(args["url"])
    try:
        client.create_collection(collection_name, gen_default_fields())
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        collection_names = client.list_collections()
        for collection_name in collection_names:
            client.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert client.has_collection(collection_name)
    return collection_name


# customised id
@pytest.fixture(scope="function")
def id_collection(request, args):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    client = MilvusClient(args["url"])
    try:
        client.create_collection(collection_name, gen_default_fields(auto_id=False))
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        collection_names = client.list_collections()
        for collection_name in collection_names:
            client.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert client.has_collection(collection_name)
    return collection_name

@pytest.fixture(scope="function")
def binary_collection(request, args):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    client = MilvusClient(args["url"])
    try:
        client.create_collection(collection_name, gen_default_fields(binary=True))
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        collection_names = client.list_collections()
        for collection_name in collection_names:
            client.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert client.has_collection(collection_name)
    return collection_name


# customised id
@pytest.fixture(scope="function")
def binary_id_collection(request, args):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    try:
        client.create_collection(collection_name, gen_default_fields(auto_id=False, binary=True))
    except Exception as e:
        pytest.exit(str(e))
    def teardown():
        collection_names = client.list_collections()
        for collection_name in collection_names:
            client.drop_collection(collection_name, timeout=delete_timeout)
    request.addfinalizer(teardown)
    assert client.has_collection(collection_name)
    return collection_name