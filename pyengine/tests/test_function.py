import unittest
import numpy as np
import requests
import logging
import json

url = "http://127.0.0.1:5000"


# TODO: LOG and Assert
class TestEngineFunction(unittest.TestCase):
    def test_1m_add(self):
        d = 4
        nb = 120
        nq = 1
        k = 10
        _, xb, xq = get_dataset(d, nb, 1, nq)

        groupid = "5m"

        # route_group = url + "/vector/group/" + groupid
        # r = requests.post(route_group, json={"dimension": d})
        #
        # # import dataset
        # vector_add_route = url + "/vector/add/" + groupid
        # for i in xb:
        #     data = dict()
        #     data['vector'] = i.tolist()
        #     r = requests.post(vector_add_route, json=data)

        # search dataset
        vector_search_route = url + "/vector/search/" + groupid
        data = dict()
        data['vector'] = xq.tolist()
        data['limit'] = k
        r = requests.get(vector_search_route, json=data)

        print("finish")

def get_dataset(d, nb, nt, nq):
    d1 = 10  # intrinsic dimension (more or less)
    n = nb + nt + nq
    rs = np.random.RandomState(1338)
    x = rs.normal(size=(n, d1))
    x = np.dot(x, rs.rand(d1, d))
    x = x * (rs.rand(d) * 4 + 0.1)
    x = np.sin(x)
    x = x.astype('float32')
    return x[:nt], x[nt:-nq], x[-nq:]


if __name__ == "__main__":
    unittest.main()
