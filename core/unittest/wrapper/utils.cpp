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


#include <gtest/gtest.h>
#include <faiss/IndexFlat.h>

#include "wrapper/utils.h"

void
DataGenBase::GenData(const int &dim, const int &nb, const int &nq,
                     float *xb, float *xq, int64_t *ids,
                     const int &k, int64_t *gt_ids, float *gt_dis) {
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

void
DataGenBase::GenData(const int &dim,
                     const int &nb,
                     const int &nq,
                     std::vector<float> &xb,
                     std::vector<float> &xq,
                     std::vector<int64_t> &ids,
                     const int &k,
                     std::vector<int64_t> &gt_ids,
                     std::vector<float> &gt_dis) {
    xb.resize(nb * dim);
    xq.resize(nq * dim);
    ids.resize(nb);
    gt_ids.resize(nq * k);
    gt_dis.resize(nq * k);
    GenData(dim, nb, nq, xb.data(), xq.data(), ids.data(), k, gt_ids.data(), gt_dis.data());
}

void
DataGenBase::AssertResult(const std::vector<int64_t>& ids, const std::vector<float>& dis) {
        EXPECT_EQ(ids.size(), nq * k);
        EXPECT_EQ(dis.size(), nq * k);

        for (auto i = 0; i < nq; i++) {
            EXPECT_EQ(ids[i * k], gt_ids[i * k]);
            //EXPECT_EQ(dis[i * k], gt_dis[i * k]);
        }

        int match = 0;
        for (int i = 0; i < nq; ++i) {
            for (int j = 0; j < k; ++j) {
                for (int l = 0; l < k; ++l) {
                    if (ids[i * nq + j] == gt_ids[i * nq + l]) match++;
                }
            }
        }

        auto precision = float(match) / (nq * k);
        EXPECT_GT(precision, 0.5);
        std::cout << std::endl << "Precision: " << precision
                  << ", match: " << match
                  << ", total: " << nq * k
                  << std::endl;

}
