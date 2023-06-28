import logging

import pytest
import functools
import socket

import common.common_type as ct
import common.common_func as cf
from utils.util_log import test_log as log
from common.common_func import param_info
from check.param_check import ip_check, number_check
from config.log_config import log_config
from utils.util_pymilvus import get_milvus, gen_unique_str, gen_default_fields, gen_binary_default_fields
from pymilvus.orm.types import CONSISTENCY_STRONG

timeout = 60
dimension = 128
delete_timeout = 60


def pytest_addoption(parser):
    parser.addoption("--host", action="store", default="localhost", help="service's ip")
    parser.addoption("--service", action="store", default="", help="service address")
    parser.addoption("--port", action="store", default=19530, help="service's port")
    parser.addoption("--user", action="store", default="", help="user name for connection")
    parser.addoption("--password", action="store", default="", help="password for connection")
    parser.addoption("--secure", type=bool, action="store", default=True, help="secure for connection")
    parser.addoption("--milvus_ns", action="store", default="chaos-testing", help="milvus_ns")
    parser.addoption("--http_port", action="store", default=19121, help="http's port")
    parser.addoption("--handler", action="store", default="GRPC", help="handler of request")
    parser.addoption("--tag", action="store", default="all", help="only run tests matching the tag.")
    parser.addoption('--dry_run', action='store_true', default=False, help="")
    parser.addoption('--partition_name', action='store', default="partition_name", help="name of partition")
    parser.addoption('--connect_name', action='store', default="connect_name", help="name of connect")
    parser.addoption('--descriptions', action='store', default="partition_des", help="descriptions of partition")
    parser.addoption('--collection_name', action='store', default="collection_name", help="name of collection")
    parser.addoption('--search_vectors', action='store', default="search_vectors", help="vectors of search")
    parser.addoption('--index_param', action='store', default="index_param", help="index_param of index")
    parser.addoption('--data', action='store', default="data", help="data of request")
    parser.addoption('--clean_log', action='store_true', default=False, help="clean log before testing")
    parser.addoption('--schema', action='store', default="schema", help="schema of test interface")
    parser.addoption('--err_msg', action='store', default="err_msg", help="error message of test")
    parser.addoption('--term_expr', action='store', default="term_expr", help="expr of query quest")
    parser.addoption('--check_content', action='store', default="check_content", help="content of check")
    parser.addoption('--field_name', action='store', default="field_name", help="field_name of index")
    parser.addoption('--replica_num', type='int', action='store', default=ct.default_replica_num, help="memory replica number")
    parser.addoption('--minio_host', action='store', default="localhost", help="minio service's ip")
    parser.addoption('--uri', action='store', default="", help="uri for high level api")
    parser.addoption('--token', action='store', default="", help="token for high level api")


@pytest.fixture
def host(request):
    return request.config.getoption("--host")


@pytest.fixture
def service(request):
    return request.config.getoption("--service")


@pytest.fixture
def port(request):
    return request.config.getoption("--port")


@pytest.fixture
def user(request):
    return request.config.getoption("--user")


@pytest.fixture
def password(request):
    return request.config.getoption("--password")


@pytest.fixture
def secure(request):
    return request.config.getoption("--secure")


@pytest.fixture
def milvus_ns(request):
    return request.config.getoption("--milvus_ns") 


@pytest.fixture
def http_port(request):
    return request.config.getoption("--http_port")


@pytest.fixture
def handler(request):
    return request.config.getoption("--handler")


@pytest.fixture
def tag(request):
    return request.config.getoption("--tag")


@pytest.fixture
def dry_run(request):
    return request.config.getoption("--dry_run")


@pytest.fixture
def connect_name(request):
    return request.config.getoption("--connect_name")


@pytest.fixture
def partition_name(request):
    return request.config.getoption("--partition_name")


@pytest.fixture
def descriptions(request):
    return request.config.getoption("--descriptions")


@pytest.fixture
def collection_name(request):
    return request.config.getoption("--collection_name")


@pytest.fixture
def search_vectors(request):
    return request.config.getoption("--search_vectors")


@pytest.fixture
def index_param(request):
    return request.config.getoption("--index_param")


@pytest.fixture
def data(request):
    return request.config.getoption("--data")


@pytest.fixture
def clean_log(request):
    return request.config.getoption("--clean_log")


@pytest.fixture
def schema(request):
    return request.config.getoption("--schema")


@pytest.fixture
def err_msg(request):
    return request.config.getoption("--err_msg")


@pytest.fixture
def term_expr(request):
    return request.config.getoption("--term_expr")


@pytest.fixture
def check_content(request):
    log.error("^" * 50)
    log.error("check_content")
    return request.config.getoption("--check_content")


@pytest.fixture
def field_name(request):
    return request.config.getoption("--field_name")


@pytest.fixture
def minio_host(request):
    return request.config.getoption("--minio_host")


@pytest.fixture
def uri(request):
    return request.config.getoption("--uri")


@pytest.fixture
def token(request):
    return request.config.getoption("--token")


""" fixture func """


@pytest.fixture(scope="session", autouse=True)
def initialize_env(request):
    """ clean log before testing """
    host = request.config.getoption("--host")
    port = request.config.getoption("--port")
    handler = request.config.getoption("--handler")
    user = request.config.getoption("--user")
    password = request.config.getoption("--password")
    secure = request.config.getoption("--secure")
    clean_log = request.config.getoption("--clean_log")
    replica_num = request.config.getoption("--replica_num")
    uri = request.config.getoption("--uri")
    token = request.config.getoption("--token")

    """ params check """
    assert ip_check(host) and number_check(port)

    """ modify log files """
    file_path_list = [log_config.log_debug, log_config.log_info, log_config.log_err]
    if log_config.log_worker != "":
        file_path_list.append(log_config.log_worker)
    cf.modify_file(file_path_list=file_path_list, is_modify=clean_log)

    log.info("#" * 80)
    log.info("[initialize_milvus] Log cleaned up, start testing...")
    param_info.prepare_param_info(host, port, handler, replica_num, user, password, secure, uri, token)


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_string(request):
    yield request.param


@pytest.fixture(params=cf.gen_simple_index())
def get_index_param(request):
    yield request.param


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_collection_name(request):
    yield request.param


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_field_name(request):
    yield request.param


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_index_type(request):
    yield request.param


# TODO: construct invalid index params for all index types
@pytest.fixture(params=[{"metric_type": "L3", "index_type": "IVF_FLAT"},
                        {"metric_type": "L2", "index_type": "IVF_FLAT", "err_params": {"nlist": 10}},
                        {"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": -1}}])
def get_invalid_index_params(request):
    yield request.param


@pytest.fixture(params=ct.get_invalid_strs)
def get_invalid_partition_name(request):
    yield request.param


@pytest.fixture(params=ct.get_invalid_dict)
def get_invalid_vector_dict(request):
    yield request.param


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
    if session.config.getoption('--dry_run'):
        total_num = 0
        file_num = 0
        tags_num = 0
        res = {"total_num": total_num, "tags_num": tags_num}
        for item in session.items:
            print(item.nodeid)
            if item.fspath.basename not in res:
                res.update({item.fspath.basename: {"total": 1, "tags": 0}})
            else:
                res[item.fspath.basename]["total"] += 1
            res["total_num"] += 1
            for marker in item.own_markers:
                if marker.name == "tags" and "0331" in marker.args:
                    res["tags_num"] += 1
                    res[item.fspath.basename]["tags"] += 1
        print(res)
        return True


def check_server_connection(request):
    host = request.config.getoption("--host")
    port = request.config.getoption("--port")

    connected = True
    if host and (host not in ['localhost', '127.0.0.1']):
        try:
            socket.getaddrinfo(host, port, 0, 0, socket.IPPROTO_TCP)
        except Exception as e:
            print("Socket connnet failed: %s" % str(e))
            connected = False
    return connected


# @pytest.fixture(scope="session", autouse=True)
# def change_mutation_result_to_primary_keys():
#     def insert_future_decorator(func):
#         @functools.wraps(func)
#         def change(*args, **kwargs):
#             try:
#                 return func(*args, **kwargs).primary_keys
#             except Exception as e:
#                 raise e
#         return change
#
#     from pymilvus import MutationFuture
#     MutationFuture.result = insert_future_decorator(MutationFuture.result)
#
#     def insert_decorator(func):
#         @functools.wraps(func)
#         def change(*args, **kwargs):
#             if kwargs.get("_async", False):
#                 return func(*args, **kwargs)
#             try:
#                 return func(*args, **kwargs).primary_keys
#             except Exception as e:
#                 raise e
#         return change
#     Milvus.insert = insert_decorator(Milvus.insert)
#     yield


@pytest.fixture(scope="module")
def connect(request):
    host = request.config.getoption("--host")
    service_name = request.config.getoption("--service")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http_port")
    handler = request.config.getoption("--handler")
    if handler == "HTTP":
        port = http_port
    try:
        milvus = get_milvus(host=host, port=port, handler=handler)
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
    host = request.config.getoption("--host")
    service_name = request.config.getoption("--service")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http_port")
    handler = request.config.getoption("--handler")
    if handler == "HTTP":
        port = http_port
    milvus = get_milvus(host=host, port=port, handler=handler)
    milvus.close()
    return milvus


@pytest.fixture(scope="module")
def args(request):
    host = request.config.getoption("--host")
    service_name = request.config.getoption("--service")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http_port")
    handler = request.config.getoption("--handler")
    if handler == "HTTP":
        port = http_port
    args = {"ip": host, "port": port, "handler": handler, "service_name": service_name}
    return args


@pytest.fixture(scope="module")
def milvus(request):
    host = request.config.getoption("--host")
    port = request.config.getoption("--port")
    http_port = request.config.getoption("--http_port")
    handler = request.config.getoption("--handler")
    if handler == "HTTP":
        port = http_port
    return get_milvus(host=host, port=port, handler=handler)


@pytest.fixture(scope="function")
def collection(request, connect):
    ori_collection_name = getattr(request.module, "collection_id", "test")
    collection_name = gen_unique_str(ori_collection_name)
    log.debug(f'collection_name: {collection_name}')
    try:
        default_fields = gen_default_fields()
        connect.create_collection(collection_name, default_fields, consistency_level=CONSISTENCY_STRONG)
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
    log.debug(f'id_collection_name: {collection_name}')
    try:
        fields = gen_default_fields(auto_id=False)
        connect.create_collection(collection_name, fields, consistency_level=CONSISTENCY_STRONG)
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
        connect.create_collection(collection_name, fields, consistency_level=CONSISTENCY_STRONG)
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
        connect.create_collection(collection_name, fields, consistency_level=CONSISTENCY_STRONG)
    except Exception as e:
        pytest.exit(str(e))

    def teardown():
        if connect.has_collection(collection_name):
            connect.drop_collection(collection_name, timeout=delete_timeout)

    request.addfinalizer(teardown)
    assert connect.has_collection(collection_name)
    return collection_name

# for test exit in the future
# @pytest.hookimpl(hookwrapper=True, tryfirst=True)
# def pytest_runtest_makereport():
#     result = yield
#     report = result.get_result()
#     if report.outcome == "failed":
#         msg = "The execution of the test case fails and the test exits..."
#         log.error(msg)
#         pytest.exit(msg)
