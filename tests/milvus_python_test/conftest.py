import socket 
import pdb
import logging

import pytest
from utils import gen_unique_str
from milvus import Milvus, IndexType, MetricType
from utils import *

index_file_size = 10
timeout = 1 


def pytest_addoption(parser):
    parser.addoption("--ip", action="store", default="localhost")
    parser.addoption("--port", action="store", default=19530)
    parser.addoption("--http-port", action="store", default=19121)
    parser.addoption("--handler", action="store", default="GRPC")


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
            # milvus.disconnect()
            pass
        except Exception as e:
            logging.getLogger().info(str(e))
    request.addfinalizer(fin)
    return milvus


# @pytest.fixture(scope="module")
# def dis_connect(request):
#     ip = request.config.getoption("--ip")
#     port = request.config.getoption("--port")
#     http_port = request.config.getoption("--http-port")
#     handler = request.config.getoption("--handler")
#     if handler == "HTTP":
#         port = http_port
#     milvus = get_milvus(host=ip, port=port, handler=handler)
#     milvus.disconnect()
#     return milvus


@pytest.fixture(scope="module")
def args(request):
    ip = request.config.getoption("--ip")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http-port")
    handler = request.config.getoption("--handler")
    if handler == "HTTP":
        port = http_port
    args = {"ip": ip, "port": port, "handler": handler}
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
    dim = getattr(request.module, "dim", "128")
    param = {'collection_name': collection_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.L2}
    result = connect.create_collection(param, timeout=timeout)
    status = result
    if isinstance(result, tuple):
        status = result[0]
    if not status.OK():
        pytest.exit("collection can not be created, exit pytest ...")

    def teardown():
        status, collection_names = connect.list_collections()
        for collection_name in collection_names:
            connect.drop_collection(collection_name)

    request.addfinalizer(teardown)

    return collection_name


@pytest.fixture(scope="function")
def ip_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    dim = getattr(request.module, "dim", "128")
    param = {'collection_name': collection_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.IP}
    result = connect.create_collection(param, timeout=timeout)
    status = result
    if isinstance(result, tuple):
        status = result[0]
    if not status.OK():
        pytest.exit("collection can not be created, exit pytest ...")

    def teardown():
        status, collection_names = connect.list_collections()
        for collection_name in collection_names:
            connect.drop_collection(collection_name)

    request.addfinalizer(teardown)

    return collection_name


@pytest.fixture(scope="function")
def jac_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    dim = getattr(request.module, "dim", "128")
    param = {'collection_name': collection_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.JACCARD}
    result = connect.create_collection(param, timeout=timeout)
    status = result
    if isinstance(result, tuple):
        status = result[0]
    if not status.OK():
        pytest.exit("collection can not be created, exit pytest ...")

    def teardown():
        status, collection_names = connect.list_collections()
        for collection_name in collection_names:
            connect.drop_collection(collection_name)

    request.addfinalizer(teardown)

    return collection_name

@pytest.fixture(scope="function")
def ham_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    dim = getattr(request.module, "dim", "128")
    param = {'collection_name': collection_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.HAMMING}
    result = connect.create_collection(param, timeout=timeout)
    status = result
    if isinstance(result, tuple):
        status = result[0]
    if not status.OK():
        pytest.exit("collection can not be created, exit pytest ...")

    def teardown():
        status, collection_names = connect.list_collections()
        for collection_name in collection_names:
            connect.drop_collection(collection_name)

    request.addfinalizer(teardown)

    return collection_name

@pytest.fixture(scope="function")
def tanimoto_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    dim = getattr(request.module, "dim", "128")
    param = {'collection_name': collection_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.TANIMOTO}
    result = connect.create_collection(param, timeout=timeout)
    status = result
    if isinstance(result, tuple):
        status = result[0]
    if not status.OK():
        pytest.exit("collection can not be created, exit pytest ...")

    def teardown():
        status, collection_names = connect.list_collections()
        for collection_name in collection_names:
            connect.drop_collection(collection_name)

    request.addfinalizer(teardown)
    return collection_name

@pytest.fixture(scope="function")
def substructure_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    dim = getattr(request.module, "dim", "128")
    param = {'collection_name': collection_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.SUBSTRUCTURE}
    result = connect.create_collection(param, timeout=timeout)
    status = result
    if isinstance(result, tuple):
        status = result[0]
    if not status.OK():
        pytest.exit("collection can not be created, exit pytest ...")

    def teardown():
        status, collection_names = connect.list_collections()
        for collection_name in collection_names:
            connect.drop_collection(collection_name)

    request.addfinalizer(teardown)
    return collection_name

@pytest.fixture(scope="function")
def superstructure_collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    dim = getattr(request.module, "dim", "128")
    param = {'collection_name': collection_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.SUPERSTRUCTURE}
    result = connect.create_collection(param, timeout=timeout)
    status = result
    if isinstance(result, tuple):
        status = result[0]
    if not status.OK():
        pytest.exit("collection can not be created, exit pytest ...")

    def teardown():
        status, collection_names = connect.list_collections()
        for collection_name in collection_names:
            connect.drop_collection(collection_name)

    request.addfinalizer(teardown)
    return collection_name
