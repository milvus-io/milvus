import time
import struct

from megasearch import VecService

#Note: pip install thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol, TJSONProtocol

def test_megasearch():
    try:
        #connect
        transport = TSocket.TSocket('localhost', 33001)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TJSONProtocol.TJSONProtocol(transport)
        client = VecService.Client(protocol)
        transport.open()

        print("connected");

        #add group
        group = VecService.VecGroup("test_" + time.strftime('%H%M%S'), 256)
        client.add_group(group)

        print("group added");

        # build binary vectors
        bin_vec_list = VecService.VecBinaryTensorList([])
        for i in range(10000):
            a=[]
            for k in range(group.dimension):
                a.append(i + k)
            bin_vec = VecService.VecBinaryTensor("binary_" + str(i), bytes())
            bin_vec.tensor = struct.pack(str(group.dimension)+"d", *a)
            bin_vec_list.tensor_list.append(bin_vec)

        # add vectors
        client.add_binary_vector_batch(group.id, bin_vec_list)

        wait_storage = 5
        print("wait {} seconds for persisting data".format(wait_storage))
        time.sleep(wait_storage)

        # search vector
        a = []
        for k in range(group.dimension):
            a.append(300 + k)
        bin_vec = VecService.VecBinaryTensor("binary_search", bytes())
        bin_vec.tensor = struct.pack(str(group.dimension) + "d", *a)
        filter = VecService.VecSearchFilter()
        res = VecService.VecSearchResult()

        print("begin search ...");
        res = client.search_binary_vector(group.id, 5, bin_vec, filter)

        print('result count: ' + str(len(res.result_list)))
        for item in res.result_list:
            print(item.uid)

        transport.close()
        print("disconnected");

    except VecService.VecException as ex:
        print(ex.reason)


test_megasearch()