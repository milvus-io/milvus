// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>

#include "knowhere/common/Dataset.h"
#include "knowhere/common/Log.h"
#include "faiss/FaissHook.h"

class DataGen {
 public:
    DataGen() {
        std::string cpu_flag;
        faiss::hook_init(cpu_flag);
        std::cout << cpu_flag << std::endl;
    }

 protected:
    void
    Init_with_default(const bool is_binary = false);

    void
    Generate(const int dim, const int nb, const int nq, const bool is_binary = false);

 protected:
    int nb = 10000;
    int nq = 10;
    int dim = 64;
    int k = 10;
    int buffer_size = 16384;
    float radius = 2.8;
    std::vector<float> xb;
    std::vector<float> xq;
    std::vector<uint8_t> xb_bin;
    std::vector<uint8_t> xq_bin;
    std::vector<int64_t> ids;
    std::vector<int64_t> xids;
    milvus::knowhere::DatasetPtr base_dataset = nullptr;
    milvus::knowhere::DatasetPtr query_dataset = nullptr;
    milvus::knowhere::DatasetPtr id_dataset = nullptr;
    milvus::knowhere::DatasetPtr xid_dataset = nullptr;
};

extern void
GenAll(const int64_t dim,
       const int64_t nb,
       std::vector<float>& xb,
       std::vector<int64_t>& ids,
       std::vector<int64_t>& xids,
       const int64_t nq,
       std::vector<float>& xq);

extern void
GenAll(const int64_t dim,
       const int64_t nb,
       std::vector<uint8_t>& xb,
       std::vector<int64_t>& ids,
       std::vector<int64_t>& xids,
       const int64_t nq,
       std::vector<uint8_t>& xq);

extern void
GenBase(const int64_t dim,
        const int64_t nb,
        const void* xb,
        int64_t* ids,
        const int64_t nq,
        const void* xq,
        int64_t* xids,
        const bool is_binary);

extern void
InitLog();

enum class CheckMode {
    CHECK_EQUAL = 0,
    CHECK_NOT_EQUAL = 1,
    CHECK_APPROXIMATE_EQUAL = 2,
};

void
AssertAnns(const milvus::knowhere::DatasetPtr& result,
           const int nq,
           const int k,
           const CheckMode check_mode = CheckMode::CHECK_EQUAL);

void
AssertVec(const milvus::knowhere::DatasetPtr& result,
          const milvus::knowhere::DatasetPtr& base_dataset,
          const milvus::knowhere::DatasetPtr& id_dataset,
          const int n,
          const int dim,
          const CheckMode check_mode = CheckMode::CHECK_EQUAL);

void
AssertBinVec(const milvus::knowhere::DatasetPtr& result,
             const milvus::knowhere::DatasetPtr& base_dataset,
             const milvus::knowhere::DatasetPtr& id_dataset,
             const int n,
             const int dim,
             const CheckMode check_mode = CheckMode::CHECK_EQUAL);

void
PrintResult(const milvus::knowhere::DatasetPtr& result, const int& nq, const int& k);

void
ReleaseQueryResult(const milvus::knowhere::DatasetPtr& result);

struct FileIOWriter {
    std::fstream fs;
    std::string name;

    explicit FileIOWriter(const std::string& fname);
    ~FileIOWriter();
    size_t
    operator()(void* ptr, size_t size);
};

struct FileIOReader {
    std::fstream fs;
    std::string name;

    explicit FileIOReader(const std::string& fname);
    ~FileIOReader();
    size_t
    operator()(void* ptr, size_t size);
};

void
Load_nns_graph(std::vector<std::vector<int64_t>>& final_graph_, const char* filename);

float*
fvecs_read(const char* fname, size_t* d_out, size_t* n_out);

int*
ivecs_read(const char* fname, size_t* d_out, size_t* n_out);
