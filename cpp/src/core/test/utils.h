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

#include <vector>
#include <cstdlib>
#include <cstdio>
#include <fstream>

#include "knowhere/adapter/Structure.h"
#include "knowhere/common/Log.h"

class DataGen  {
 protected:
    void Init_with_default();

    void Generate(const int &dim, const int &nb, const int &nq);

    zilliz::knowhere::DatasetPtr GenQuery(const int&nq);

 protected:
    int nb = 10000;
    int nq = 10;
    int dim = 64;
    int k = 10;
    std::vector<float> xb;
    std::vector<float> xq;
    std::vector<int64_t> ids;
    zilliz::knowhere::DatasetPtr base_dataset = nullptr;
    zilliz::knowhere::DatasetPtr query_dataset = nullptr;
};


extern void GenAll(const int64_t dim,
                   const int64_t &nb,
                   std::vector<float> &xb,
                   std::vector<int64_t> &ids,
                   const int64_t &nq,
                   std::vector<float> &xq);

extern void GenAll(const int64_t &dim,
                   const int64_t &nb,
                   float *xb,
                   int64_t *ids,
                   const int64_t &nq,
                   float *xq);

extern void GenBase(const int64_t &dim,
                    const int64_t &nb,
                    float *xb,
                    int64_t *ids);

extern void InitLog();

zilliz::knowhere::DatasetPtr
generate_dataset(int64_t nb, int64_t dim, float *xb, long *ids);

zilliz::knowhere::DatasetPtr
generate_query_dataset(int64_t nb, int64_t dim, float *xb);

struct FileIOWriter {
    std::fstream fs;
    std::string name;

    FileIOWriter(const std::string &fname);
    ~FileIOWriter();
    size_t operator()(void *ptr, size_t size);
};

struct FileIOReader {
    std::fstream fs;
    std::string name;

    FileIOReader(const std::string &fname);
    ~FileIOReader();
    size_t operator()(void *ptr, size_t size);
};

