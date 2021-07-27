import os

from pymilvus_orm import connections, Index

from scale import constants
from utils.util_log import test_log as log
from base.collection_wrapper import ApiCollectionWrapper
from common import common_func as cf
from common import common_type as ct


def get_milvus_chart_env_var(var=constants.MILVUS_CHART_ENV):
    """ get log path for testing """
    try:
        milvus_helm_chart = os.environ[var]
        log.debug(milvus_helm_chart)
        return str(milvus_helm_chart)
    except Exception as e:
        # milvus_helm_chart = constants.MILVUS_CHART
        # log.warning(f"Failed to get environment variables: {var}, use default: {milvus_helm_chart}, error: {str(e)}")
        # return milvus_helm_chart
        raise


def e2e_milvus(host, c_name):
    # connect
    connections.add_connection(default={"host": host, "port": 19530})
    connections.connect(alias='default')

    # create
    # c_name = cf.gen_unique_str(prefix)
    collection_w = ApiCollectionWrapper()
    collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())

    # insert
    data = cf.gen_default_list_data(ct.default_nb)
    collection_w.insert(data)
    assert collection_w.num_entities == ct.default_nb

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

    # query
    ids = search_res[0].ids[0]
    term_expr = f'{ct.default_int64_field_name} in [{ids}]'
    query_res, _ = collection_w.query(term_expr, output_fields=["*", "%"])
    assert query_res[0][ct.default_int64_field_name] == ids