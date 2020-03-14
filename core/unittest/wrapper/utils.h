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

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <vector>

#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "wrapper/ConfAdapter.h"
#include "wrapper/ConfAdapterMgr.h"
#include "wrapper/VecIndex.h"
#include "wrapper/utils.h"

class DataGenBase;

using DataGenPtr = std::shared_ptr<DataGenBase>;

constexpr int64_t DIM = 128;
constexpr int64_t NB = 100000;
constexpr int64_t NQ = 10;
constexpr int64_t DEVICEID = 0;
constexpr int64_t PINMEM = 1024 * 1024 * 200;
constexpr int64_t TEMPMEM = 1024 * 1024 * 300;
constexpr int64_t RESNUM = 2;

static const char* CONFIG_PATH = "/tmp/milvus_test";
static const char* CONFIG_FILE = "/server_config.yaml";

class KnowhereTest : public ::testing::Test {
 protected:
    void
    SetUp() override;
    void
    TearDown() override;
};

class DataGenBase {
 public:
    virtual void
    GenData(const int& dim, const int& nb, const int& nq, float* xb, float* xq, int64_t* ids, const int& k,
            int64_t* gt_ids, float* gt_dis);

    virtual void
    GenData(const int& dim, const int& nb, const int& nq, std::vector<float>& xb, std::vector<float>& xq,
            std::vector<int64_t>& ids, const int& k, std::vector<int64_t>& gt_ids, std::vector<float>& gt_dis);

    void
    AssertResult(const std::vector<int64_t>& ids, const std::vector<float>& dis);

    int dim = DIM;
    int nb = NB;
    int nq = NQ;
    int k = 10;
    std::vector<float> xb;
    std::vector<float> xq;
    std::vector<int64_t> ids;

    // Ground Truth
    std::vector<int64_t> gt_ids;
    std::vector<float> gt_dis;
};

class BinDataGen {
 public:
    virtual void
    GenData(const int& dim, const int& nb, const int& nq, uint8_t* xb, uint8_t* xq, int64_t* ids, const int& k,
            int64_t* gt_ids, float* gt_dis);

    virtual void
    GenData(const int& dim, const int& nb, const int& nq, std::vector<uint8_t>& xb, std::vector<uint8_t>& xq,
            std::vector<int64_t>& ids, const int& k, std::vector<int64_t>& gt_ids, std::vector<float>& gt_dis);

    void
    AssertResult(const std::vector<int64_t>& ids, const std::vector<float>& dis);

    void
    Generate(const int& dim, const int& nb, const int& nq, const int& k);

    int dim = DIM;
    int nb = NB;
    int nq = NQ;
    int k = 10;
    std::vector<uint8_t> xb;
    std::vector<uint8_t> xq;
    std::vector<int64_t> ids;

    // Ground Truth
    std::vector<int64_t> gt_ids;
    std::vector<float> gt_dis;
};

class ParamGenerator {
 public:
    static ParamGenerator&
    GetInstance() {
        static ParamGenerator instance;
        return instance;
    }

    knowhere::Config
    GenSearchConf(const milvus::engine::IndexType& type, const milvus::json& conf) {
        auto search_cfg = conf;
        switch (type) {
            case milvus::engine::IndexType::FAISS_IDMAP: {
                break;
            }
			case milvus::engine::IndexType::FAISS_BIN_IVFLAT_CPU:
            case milvus::engine::IndexType::FAISS_IVFFLAT_CPU:
            case milvus::engine::IndexType::FAISS_IVFFLAT_GPU:
            case milvus::engine::IndexType::FAISS_IVFFLAT_MIX:
            case milvus::engine::IndexType::FAISS_IVFPQ_CPU:
            case milvus::engine::IndexType::FAISS_IVFPQ_GPU:
            case milvus::engine::IndexType::FAISS_IVFPQ_MIX:
            case milvus::engine::IndexType::FAISS_IVFSQ8_HYBRID:
            case milvus::engine::IndexType::FAISS_IVFSQ8_CPU:
            case milvus::engine::IndexType::FAISS_IVFSQ8_GPU:
            case milvus::engine::IndexType::FAISS_IVFSQ8_MIX: {
                search_cfg[knowhere::IndexParams::nprobe] = 16;
                break;
            }
            case milvus::engine::IndexType::NSG_MIX: {
                search_cfg[knowhere::IndexParams::search_length] = 20;
                break;
            }
            case milvus::engine::IndexType::HNSW: {
                search_cfg[knowhere::IndexParams::ef] = conf[knowhere::meta::TOPK].get<int64_t>() + 10;
                break;
            }
        }
        auto adapter = milvus::engine::AdapterMgr::GetInstance().GetAdapter(type);
        adapter->CheckSearch(search_cfg, type);
        return search_cfg;
    }

    knowhere::Config
    GenBuild(const milvus::engine::IndexType& type, const milvus::json& conf) {
        auto build_cfg = conf;
        switch (type) {
            case milvus::engine::IndexType::FAISS_IDMAP: {
                case milvus::engine::IndexType::FAISS_BIN_IDMAP: {
                    break;
                }
                case milvus::engine::IndexType::FAISS_BIN_IVFLAT_CPU:
                case milvus::engine::IndexType::FAISS_IVFFLAT_CPU:
                case milvus::engine::IndexType::FAISS_IVFFLAT_GPU:
                case milvus::engine::IndexType::FAISS_IVFFLAT_MIX:
                case milvus::engine::IndexType::FAISS_IVFSQ8_HYBRID:
                case milvus::engine::IndexType::FAISS_IVFSQ8_CPU:
                case milvus::engine::IndexType::FAISS_IVFSQ8_GPU:
                case milvus::engine::IndexType::FAISS_IVFSQ8_MIX: {
                    build_cfg[knowhere::IndexParams::nlist] = 16;
                    break;
                }
                case milvus::engine::IndexType::FAISS_IVFPQ_CPU:
                case milvus::engine::IndexType::FAISS_IVFPQ_GPU:
                case milvus::engine::IndexType::FAISS_IVFPQ_MIX: {
                    build_cfg[knowhere::IndexParams::nlist] = 16;
                    build_cfg[knowhere::IndexParams::m] = 8;
                    break;
                }
                case milvus::engine::IndexType::NSG_MIX: {
                    build_cfg[knowhere::IndexParams::knng] = 10;
                    build_cfg[knowhere::IndexParams::search_length] = 20;
                    build_cfg[knowhere::IndexParams::out_degree] = 10;
                    build_cfg[knowhere::IndexParams::candidate] = 50;
                    break;
                }
                case milvus::engine::IndexType::HNSW: {
                    build_cfg[knowhere::IndexParams::efConstruction] = 100;
                    build_cfg[knowhere::IndexParams::M] = 12;
                    break;
                }
            }
        }
        auto adapter = milvus::engine::AdapterMgr::GetInstance().GetAdapter(type);
        adapter->CheckTrain(build_cfg);
        return build_cfg;
    }
};

// class SanityCheck : public DataGenBase {
// public:
//    void GenData(const int &dim, const int &nb, const int &nq, float *xb, float *xq, long *ids,
//                 const int &k, long *gt_ids, float *gt_dis) override;
//};
