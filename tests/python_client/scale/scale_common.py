import os

from pymilvus import connections, Index

from scale import constants
from utils.util_log import test_log as log
from base.collection_wrapper import ApiCollectionWrapper
from common import common_func as cf
from common import common_type as ct


def get_milvus_chart_env_var(var=constants.MILVUS_CHART_ENV):
    """ get log path for testing """
    try:
        milvus_helm_chart = os.environ[var]
        return str(milvus_helm_chart)
    except Exception as e:
        log.error(f'Failed to get environment variables {var}, with exception {str(e)}')
        # milvus_helm_chart = constants.MILVUS_CHART_PATH
        # log.error(f'Failed to get environment variables {var}, please set.')
        # log.warning(
        # f"Failed to get environment variables: {var}, use default: {constants.MILVUS_CHART_PATH}, {str(e)}")
    # if not os.path.exists(milvus_helm_chart):
    #     raise Exception(f'milvus_helm_chart: {milvus_helm_chart} not exist')
    # return milvus_helm_chart


def e2e_milvus(host, c_name, collection_exist=False):
    # connect
    connections.add_connection(default={"host": host, "port": 19530})
    connections.connect(alias='default')

    # create
    collection_w = ApiCollectionWrapper()
    if collection_exist:
        collection_w.init_collection(name=c_name)
    else:
        collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())

    # insert
    data = cf.gen_default_list_data(ct.default_nb)
    mutation_res, _ = collection_w.insert(data)
    assert mutation_res.insert_count == ct.default_nb
    log.debug(collection_w.num_entities)

    # create index
    collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
    assert collection_w.has_index()[0]
    assert collection_w.index()[0] == Index(collection_w.collection, ct.default_float_vec_field_name,
                                            ct.default_index)

    # search
    collection_w.load()
    search_res, _ = collection_w.search(data[-1][:ct.default_nq], ct.default_float_vec_field_name,
                                        ct.default_search_params, ct.default_limit)
    assert len(search_res[0]) == ct.default_limit
    log.debug(search_res[0][0].id)

    # query
    ids = search_res[0].ids[0]
    term_expr = f'{ct.default_int64_field_name} in [{ids}]'
    query_res, _ = collection_w.query(term_expr, output_fields=["*", "%"])
    assert query_res[0][ct.default_int64_field_name] == ids