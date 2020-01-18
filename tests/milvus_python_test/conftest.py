import socket 
import pdb
import logging

import pytest
from utils import gen_unique_str
from milvus import Milvus, IndexType, MetricType
from utils import *

index_file_size = 10


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
    milvus = get_milvus(handler=handler)
    try:
        if handler == "HTTP":
            port = http_port
        status = milvus.connect(host=ip, port=port)
        logging.getLogger().info(status)
        if not status.OK():
            # try again
            logging.getLogger().info("------------------------------------")
            logging.getLogger().info("Try to connect again")
            logging.getLogger().info("------------------------------------")
            res = milvus.connect(host=ip, port=port)
    except Exception as e:
        logging.getLogger().error(str(e))
        pytest.exit("Milvus server can not connected, exit pytest ...")
    def fin():
        try:
            milvus.disconnect()
        except:
            pass
    request.addfinalizer(fin)
    return milvus


@pytest.fixture(scope="module")
def dis_connect(request):
    ip = request.config.getoption("--ip")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http-port")
    handler = request.config.getoption("--handler")
    milvus = get_milvus(handler=handler)
    return milvus


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
    handler = request.config.getoption("--handler")
    return get_milvus(handler=handler)


@pytest.fixture(scope="function")
def table(request, connect):
    ori_table_name = getattr(request.module, "table_id", "test")
    table_name = gen_unique_str(ori_table_name)
    dim = getattr(request.module, "dim", "128")
    param = {'table_name': table_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.L2}
    status = connect.create_table(param)
    # logging.getLogger().info(status)
    if not status.OK():
        pytest.exit("Table can not be created, exit pytest ...")

    def teardown():
        status, table_names = connect.show_tables()
        for table_name in table_names:
            connect.delete_table(table_name)

    request.addfinalizer(teardown)

    return table_name


@pytest.fixture(scope="function")
def ip_table(request, connect):
    ori_table_name = getattr(request.module, "table_id", "test")
    table_name = gen_unique_str(ori_table_name)
    dim = getattr(request.module, "dim", "128")
    param = {'table_name': table_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.IP}
    status = connect.create_table(param)
    # logging.getLogger().info(status)
    if not status.OK():
        pytest.exit("Table can not be created, exit pytest ...")

    def teardown():
        status, table_names = connect.show_tables()
        for table_name in table_names:
            connect.delete_table(table_name)

    request.addfinalizer(teardown)

    return table_name


@pytest.fixture(scope="function")
def jac_table(request, connect):
    ori_table_name = getattr(request.module, "table_id", "test")
    table_name = gen_unique_str(ori_table_name)
    dim = getattr(request.module, "dim", "128")
    param = {'table_name': table_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.JACCARD}
    status = connect.create_table(param)
    # logging.getLogger().info(status)
    if not status.OK():
        pytest.exit("Table can not be created, exit pytest ...")

    def teardown():
        status, table_names = connect.show_tables()
        for table_name in table_names:
            connect.delete_table(table_name)

    request.addfinalizer(teardown)

    return table_name

@pytest.fixture(scope="function")
def ham_table(request, connect):
    ori_table_name = getattr(request.module, "table_id", "test")
    table_name = gen_unique_str(ori_table_name)
    dim = getattr(request.module, "dim", "128")
    param = {'table_name': table_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.HAMMING}
    status = connect.create_table(param)
    # logging.getLogger().info(status)
    if not status.OK():
        pytest.exit("Table can not be created, exit pytest ...")

    def teardown():
        status, table_names = connect.show_tables()
        for table_name in table_names:
            connect.delete_table(table_name)

    request.addfinalizer(teardown)

    return table_name

@pytest.fixture(scope="function")
def tanimoto_table(request, connect):
    ori_table_name = getattr(request.module, "table_id", "test")
    table_name = gen_unique_str(ori_table_name)
    dim = getattr(request.module, "dim", "128")
    param = {'table_name': table_name,
             'dimension': dim,
             'index_file_size': index_file_size,
             'metric_type': MetricType.TANIMOTO}
    status = connect.create_table(param)
    # logging.getLogger().info(status)
    if not status.OK():
        pytest.exit("Table can not be created, exit pytest ...")

    def teardown():
        status, table_names = connect.show_tables()
        for table_name in table_names:
            connect.delete_table(table_name)

    request.addfinalizer(teardown)

    return table_name