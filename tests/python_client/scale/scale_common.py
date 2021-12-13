from pymilvus import connections, Index

from utils.util_log import test_log as log
from base.collection_wrapper import ApiCollectionWrapper
from common import common_func as cf
from common import common_type as ct


def e2e_milvus(host, c_name):
    # connect
    connections.add_connection(default={"host": host, "port": 19530})
    connections.connect(alias='default')

    # create
    collection_w = ApiCollectionWrapper()
    collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())

    # insert
    data = cf.gen_default_list_data()
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
