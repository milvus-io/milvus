import pdb
import pytest
import logging
import itertools
from time import sleep
from multiprocessing import Process
from utils import *

uid = "collection_logic"

def create_collection(connect, **params):
    connect.create_collection(params["collection_name"], const.default_fields)

def search_collection(connect, **params):
    status, result = connect.search(
        params["collection_name"], 
        params["top_k"], 
        params["query_vectors"],
        params={"nprobe": params["nprobe"]})
    return status

def load_collection(connect, **params):
    connect.load_collection(params["collection_name"])

def has(connect, **params):
    status, result = connect.has_collection(params["collection_name"])
    return status

def show(connect, **params):
    status, result = connect.list_collections()
    return status

def delete(connect, **params):
    status = connect.drop_collection(params["collection_name"])
    return status

def describe(connect, **params):
    status, result = connect.get_collection_info(params["collection_name"])
    return status

def rowcount(connect, **params):
    status, result = connect.count_entities(params["collection_name"])
    return status

def create_index(connect, **params):
    status = connect.create_index(params["collection_name"], params["index_type"], params["index_param"])
    return status

func_map = { 
    # 0:has, 
    1:show,
    10:create_collection, 
    11:describe,
    12:rowcount,
    13:search_collection,
    14:load_collection,
    15:create_index,
    30:delete
}

def gen_sequence():
    raw_seq = func_map.keys()
    result = itertools.permutations(raw_seq)
    for x in result:
        yield x


class TestCollectionLogic(object):
    @pytest.mark.parametrize("logic_seq", gen_sequence())
    @pytest.mark.level(2)
    def _test_logic(self, connect, logic_seq, args):
        if args["handler"] == "HTTP":
            pytest.skip("Skip in http mode")
        if self.is_right(logic_seq):
            self.execute(logic_seq, connect)
        else:
            self.execute_with_error(logic_seq, connect)
        self.tear_down(connect)

    def is_right(self, seq):
        if sorted(seq) == seq:
            return True

        not_created = True
        has_deleted = False
        for i in range(len(seq)):
            if seq[i] > 10 and not_created:
                return False
            elif seq [i] > 10 and has_deleted:
                return False
            elif seq[i] == 10:
                not_created = False
            elif seq[i] == 30:
                has_deleted = True

        return True

    def execute(self, logic_seq, connect):
        basic_params = self.gen_params()
        for i in range(len(logic_seq)):
            # logging.getLogger().info(logic_seq[i])
            f = func_map[logic_seq[i]]
            status = f(connect, **basic_params)
            assert status.OK()

    def execute_with_error(self, logic_seq, connect):
        basic_params = self.gen_params()

        error_flag = False
        for i in range(len(logic_seq)):
            f = func_map[logic_seq[i]]
            status = f(connect, **basic_params)
            if not status.OK():
                # logging.getLogger().info(logic_seq[i])
                error_flag = True
                break
        assert error_flag == True

    def tear_down(self, connect):
        names = connect.list_collections()[1]
        for name in names:
            connect.drop_collection(name)

    def gen_params(self):
        collection_name = gen_unique_str(uid)
        top_k = 1
        vectors = gen_vectors(2, dim)
        param = {'collection_name': collection_name,
                 'dimension': dim,
                 'metric_type': "L2",
                 'nprobe': 1,
                 'top_k': top_k,
                 'index_type': "IVF_SQ8",
                 'index_param': {
                        'nlist': 16384
                 },
                 'query_vectors': vectors}
        return param
