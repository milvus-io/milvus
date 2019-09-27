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


#pragma once

#include <memory>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <fstream>

class DataGenBase;

using DataGenPtr = std::shared_ptr<DataGenBase>;

class DataGenBase {
 public:
    virtual void GenData(const int &dim, const int &nb, const int &nq, float *xb, float *xq, long *ids,
                         const int &k, long *gt_ids, float *gt_dis);

    virtual void GenData(const int &dim,
                         const int &nb,
                         const int &nq,
                         std::vector<float> &xb,
                         std::vector<float> &xq,
                         std::vector<long> &ids,
                         const int &k,
                         std::vector<long> &gt_ids,
                         std::vector<float> &gt_dis);
};


//class SanityCheck : public DataGenBase {
// public:
//    void GenData(const int &dim, const int &nb, const int &nq, float *xb, float *xq, long *ids,
//                 const int &k, long *gt_ids, float *gt_dis) override;
//};

