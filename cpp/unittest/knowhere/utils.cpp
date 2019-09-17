// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include <faiss/IndexFlat.h>

#include "utils.h"


void DataGenBase::GenData(const int &dim, const int &nb, const int &nq,
                          float *xb, float *xq, long *ids,
                          const int &k, long *gt_ids, float *gt_dis) {
    for (auto i = 0; i < nb; ++i) {
        for (auto j = 0; j < dim; ++j) {
            //p_data[i * d + j] = float(base + i);
            xb[i * dim + j] = drand48();
        }
        xb[dim * i] += i / 1000.;
        ids[i] = i;
    }
    for (size_t i = 0; i < nq * dim; ++i) {
        xq[i] = xb[i];
    }

    faiss::IndexFlatL2 index(dim);
    //index.add_with_ids(nb, xb, ids);
    index.add(nb, xb);
    index.search(nq, xq, k, gt_dis, gt_ids);
}

void DataGenBase::GenData(const int &dim,
                          const int &nb,
                          const int &nq,
                          std::vector<float> &xb,
                          std::vector<float> &xq,
                          std::vector<long> &ids,
                          const int &k,
                          std::vector<long> &gt_ids,
                          std::vector<float> &gt_dis) {
    xb.resize(nb * dim);
    xq.resize(nq * dim);
    ids.resize(nb);
    gt_ids.resize(nq * k);
    gt_dis.resize(nq * k);
    GenData(dim, nb, nq, xb.data(), xq.data(), ids.data(), k, gt_ids.data(), gt_dis.data());
}
