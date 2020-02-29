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

class DataGen {
 protected:
    void
    Init_with_default();

    void
    Generate(const int& dim, const int& nb, const int& nq);

    knowhere::DatasetPtr
    GenQuery(const int& nq);

 protected:
    int nb = 10000;
    int nq = 10;
    int dim = 64;
    int k = 10;
    std::vector<float> xb;
    std::vector<float> xq;
    std::vector<int64_t> ids;
    std::vector<int64_t> xids;
    knowhere::DatasetPtr base_dataset = nullptr;
    knowhere::DatasetPtr query_dataset = nullptr;
    knowhere::DatasetPtr id_dataset = nullptr;
    knowhere::DatasetPtr xid_dataset = nullptr;
};

class BinaryDataGen {
 protected:
    void
    Init_with_binary_default();

    void
    Generate(const int& dim, const int& nb, const int& nq);

    knowhere::DatasetPtr
    GenQuery(const int& nq);

 protected:
    int nb = 10000;
    int nq = 10;
    int dim = 512;
    int k = 10;
    std::vector<uint8_t> xb;
    std::vector<uint8_t> xq;
    std::vector<int64_t> ids;
    std::vector<int64_t> xids;
    knowhere::DatasetPtr base_dataset = nullptr;
    knowhere::DatasetPtr query_dataset = nullptr;
    knowhere::DatasetPtr id_dataset = nullptr;
    knowhere::DatasetPtr xid_dataset = nullptr;
};

extern void
GenAll(const int64_t dim, const int64_t& nb, std::vector<float>& xb, std::vector<int64_t>& ids,
       std::vector<int64_t>& xids, const int64_t& nq, std::vector<float>& xq);

extern void
GenAll(const int64_t& dim, const int64_t& nb, float* xb, int64_t* ids, int64_t* xids, const int64_t& nq, float* xq);

extern void
GenBinaryAll(const int64_t dim, const int64_t& nb, std::vector<uint8_t>& xb, std::vector<int64_t>& ids,
             std::vector<int64_t>& xids, const int64_t& nq, std::vector<uint8_t>& xq);

extern void
GenBinaryAll(const int64_t& dim, const int64_t& nb, uint8_t* xb, int64_t* ids, int64_t* xids, const int64_t& nq,
             uint8_t* xq);

extern void
GenBase(const int64_t& dim, const int64_t& nb, float* xb, int64_t* ids);

extern void
GenBinaryBase(const int64_t& dim, const int64_t& nb, uint8_t* xb, int64_t* ids);

extern void
InitLog();

knowhere::DatasetPtr
generate_dataset(int64_t nb, int64_t dim, const float* xb, const int64_t* ids);

knowhere::DatasetPtr
generate_binary_dataset(int64_t nb, int64_t dim, const uint8_t* xb, const int64_t* ids);

knowhere::DatasetPtr
generate_query_dataset(int64_t nb, int64_t dim, const float* xb);

knowhere::DatasetPtr
generate_id_dataset(int64_t nb, const int64_t* ids);

knowhere::DatasetPtr
generate_binary_query_dataset(int64_t nb, int64_t dim, const uint8_t* xb);

enum class CheckMode {
    CHECK_EQUAL = 0,
    CHECK_NOT_EQUAL = 1,
    CHECK_APPROXIMATE_EQUAL = 2,
};

void
AssertAnns(const knowhere::DatasetPtr& result, const int nq, const int k,
           const CheckMode check_mode = CheckMode::CHECK_EQUAL);

void
AssertVec(const knowhere::DatasetPtr& result, const knowhere::DatasetPtr& base_dataset,
          const knowhere::DatasetPtr& id_dataset, const int n, const int dim,
          const CheckMode check_mode = CheckMode::CHECK_EQUAL);

void
AssertBinVeceq(const knowhere::DatasetPtr& result, const knowhere::DatasetPtr& base_dataset,
               const knowhere::DatasetPtr& id_dataset, const int n, const int dim,
               const CheckMode check_mode = CheckMode::CHECK_EQUAL);

void
PrintResult(const knowhere::DatasetPtr& result, const int& nq, const int& k);

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
