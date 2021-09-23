import pdb
import copy
import pytest
import threading
import datetime
import logging
from time import sleep
from multiprocessing import Process
import sklearn.preprocessing
from utils.utils import *
from common.common_type import CaseLabel

index_file_size = 10
vectors = gen_vectors(10000, default_dim)
vectors = sklearn.preprocessing.normalize(vectors, axis=1, norm='l2')
vectors = vectors.tolist()
top_k = 1
nprobe = 1
epsilon = 0.001
nlist = 128
# index_params = {'index_type': IndexType.IVFLAT, 'nlist': 16384}
default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 16384}, "metric_type": "L2"}


class TestMixBase:
    # TODO
    @pytest.mark.tags(CaseLabel.L2)
    def _test_mix_base(self, connect, collection):
        nb = 200000
        nq = 5
        entities = gen_entities(nb=nb)
        ids = connect.insert(collection, entities)
        assert len(ids) == nb
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, default_index)
        index = connect.describe_index(collection, "")
        create_target_index(default_index, default_float_vec_field_name)
        assert index == default_index
        query, vecs = gen_query_vectors(default_float_vec_field_name, entities, default_top_k, nq)
        connect.load_collection(collection)
        res = connect.search(collection, query)
        assert len(res) == nq
        assert len(res[0]) == default_top_k
        assert res[0]._distances[0] <= epsilon
        assert check_id_result(res[0], ids[0])

    # disable
    @pytest.mark.tags(CaseLabel.L2)
    def _test_search_during_createIndex(self, args):
        loops = 10000
        collection = gen_unique_str()
        query_vecs = [vectors[0], vectors[1]]
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        id_0 = 0
        id_1 = 0
        milvus_instance = get_milvus(args["handler"])
        # milvus_instance.connect(uri=uri)
        milvus_instance.create_collection({'collection_name': collection,
                                           'dimension': default_dim,
                                           'index_file_size': index_file_size,
                                           'metric_type': "L2"})
        for i in range(10):
            status, ids = milvus_instance.bulk_insert(collection, vectors)
            # logging.getLogger().info(ids)
            if i == 0:
                id_0 = ids[0]
                id_1 = ids[1]

        # def create_index(milvus_instance):
        #     logging.getLogger().info("In create index")
        #     status = milvus_instance.create_index(collection, index_params)
        #     logging.getLogger().info(status)
        #     status, result = milvus_instance.get_index_info(collection)
        #     logging.getLogger().info(result)
        def insert(milvus_instance):
            logging.getLogger().info("In add vectors")
            status, ids = milvus_instance.bulk_insert(collection, vectors)
            logging.getLogger().info(status)

        def search(milvus_instance):
            logging.getLogger().info("In search vectors")
            for i in range(loops):
                status, result = milvus_instance.search(collection, top_k, nprobe, query_vecs)
                logging.getLogger().info(status)
                assert result[0][0].id == id_0
                assert result[1][0].id == id_1

        milvus_instance = get_milvus(args["handler"])
        # milvus_instance.connect(uri=uri)
        p_search = Process(target=search, args=(milvus_instance,))
        p_search.start()
        milvus_instance = get_milvus(args["handler"])
        # milvus_instance.connect(uri=uri)
        p_create = Process(target=insert, args=(milvus_instance,))
        p_create.start()
        p_create.join()

    @pytest.mark.tags(CaseLabel.L2)
    def _test_mix_multi_collections(self, connect):
        '''
        target: test functions with multiple collections of different metric_types and index_types
        method: create 60 collections which 30 are L2 and the other are IP, add vectors into them
                and test describe index and search
        expected: status ok
        '''
        nq = 10000
        collection_list = []
        idx = []
        index_param = {'nlist': nlist}

        # create collection and add vectors
        for i in range(30):
            collection_name = gen_unique_str('test_mix_multi_collections')
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': default_dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_collection(param)
            status, ids = connect.bulk_insert(collection_name=collection_name, records=vectors)
            idx.append(ids[0])
            idx.append(ids[10])
            idx.append(ids[20])
            assert status.OK()
        for i in range(30):
            collection_name = gen_unique_str('test_mix_multi_collections')
            collection_list.append(collection_name)
            param = {'collection_name': collection_name,
                     'dimension': default_dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_collection(param)
            status, ids = connect.bulk_insert(collection_name=collection_name, records=vectors)
            assert status.OK()
            status = connect.flush([collection_name])
            assert status.OK()
            idx.append(ids[0])
            idx.append(ids[10])
            idx.append(ids[20])
            assert status.OK()
        for i in range(10):
            status = connect.create_index(collection_list[i], IndexType.FLAT, index_param)
            assert status.OK()
            status = connect.create_index(collection_list[30 + i], IndexType.FLAT, index_param)
            assert status.OK()
            status = connect.create_index(collection_list[10 + i], IndexType.IVFLAT, index_param)
            assert status.OK()
            status = connect.create_index(collection_list[40 + i], IndexType.IVFLAT, index_param)
            assert status.OK()
            status = connect.create_index(collection_list[20 + i], IndexType.IVF_SQ8, index_param)
            assert status.OK()
            status = connect.create_index(collection_list[50 + i], IndexType.IVF_SQ8, index_param)
            assert status.OK()

        # describe index
        for i in range(10):
            status, result = connect.get_index_info(collection_list[i])
            assert result._index_type == IndexType.FLAT
            status, result = connect.get_index_info(collection_list[10 + i])
            assert result._index_type == IndexType.IVFLAT
            status, result = connect.get_index_info(collection_list[20 + i])
            assert result._index_type == IndexType.IVF_SQ8
            status, result = connect.get_index_info(collection_list[30 + i])
            assert result._index_type == IndexType.FLAT
            status, result = connect.get_index_info(collection_list[40 + i])
            assert result._index_type == IndexType.IVFLAT
            status, result = connect.get_index_info(collection_list[50 + i])
            assert result._index_type == IndexType.IVF_SQ8

        # search
        query_vecs = [vectors[0], vectors[10], vectors[20]]
        for i in range(60):
            collection = collection_list[i]
            status, result = connect.search(collection, top_k, query_records=query_vecs, params={"nprobe": 1})
            assert status.OK()
            assert len(result) == len(query_vecs)
            logging.getLogger().info(i)
            for j in range(len(query_vecs)):
                assert len(result[j]) == top_k
            for j in range(len(query_vecs)):
                if not check_result(result[j], idx[3 * i + j]):
                    logging.getLogger().info(result[j]._id_list)
                    logging.getLogger().info(idx[3 * i + j])
                assert check_result(result[j], idx[3 * i + j])


def check_result(result, id):
    if len(result) >= 5:
        return id in [result[0].id, result[1].id, result[2].id, result[3].id, result[4].id]
    else:
        return id in (i.id for i in result)


def check_id_result(result, id):
    limit_in = 5
    ids = [entity.id for entity in result]
    if len(result) >= limit_in:
        return id in ids[:limit_in]
    else:
        return id in ids
