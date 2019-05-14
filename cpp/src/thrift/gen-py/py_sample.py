import time
import struct

from zilliz import VecService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol, TJSONProtocol

def print_time_cost(desc, t):
    time_now = time.time()
    print(desc + ' cost ', time_now - t, ' sec')
    return time_now

def test_vecwise():
    try:
        time_start = time.time()

        #connect
        transport = TSocket.TSocket('localhost', 33001)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TJSONProtocol.TJSONProtocol(transport)
        client = VecService.Client(protocol)
        transport.open()

        time_start = print_time_cost('connection', time_start)

        #add group
        group = VecService.VecGroup("py_group_" + time.strftime('%H%M%S'), 256)
        client.add_group(group)

        time_start = print_time_cost('add group', time_start)

        #build vectors
        # vec_list = VecService.VecTensorList([])
        # for i in range(10000):
        #     vec = VecService.VecTensor("normal_"+str(i), [])
        #     for k in range(group.dimension):
        #         vec.tensor.append(k)
        #     vec_list.tensor_list.append(vec)
        #time_start = print_time_cost('build normal vectors', time_start)

        #add vectors
        #client.add_vector_batch(group.id, vec_list)
        #time_start = print_time_cost('add normal vectors', time_start))

        # build binary vectors
        bin_vec_list = VecService.VecBinaryTensorList([])
        for i in range(10000):
            a=[]
            for k in range(group.dimension):
                a.append(i + k)
            bin_vec = VecService.VecBinaryTensor("binary_" + str(i), bytes())
            bin_vec.tensor = struct.pack(str(group.dimension)+"d", *a)
            bin_vec_list.tensor_list.append(bin_vec)

        time_start = print_time_cost('build binary vectors', time_start)

        # add vectors
        client.add_binary_vector_batch(group.id, bin_vec_list)
        time_start = print_time_cost('add binary vectors', time_start)

        time.sleep(5)
        time_start = print_time_cost('sleep 5 seconds', time_start)

        # search vector
        a = []
        for k in range(group.dimension):
            a.append(300 + k)
        bin_vec = VecService.VecBinaryTensor("binary_search", bytes())
        bin_vec.tensor = struct.pack(str(group.dimension) + "d", *a)
        filter = VecService.VecSearchFilter()
        res = VecService.VecSearchResult()
        res = client.search_binary_vector(group.id, 5, bin_vec, filter)
        time_start = print_time_cost('search binary vectors', time_start)

        print('result count: ' + str(len(res.result_list)))
        for item in res.result_list:
            print(item.uid)

        transport.close()
        time_start = print_time_cost('close connection', time_start)

    except VecService.VecException as ex:
        print(ex.reason)


test_vecwise()