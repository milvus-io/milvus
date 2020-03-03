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

#include <memory>
#include <string>

#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"
#endif

int DEVICEID = 0;
constexpr int64_t DIM = 128;
constexpr int64_t NB = 10000;
constexpr int64_t NQ = 10;
constexpr int64_t K = 10;
constexpr int64_t PINMEM = 1024 * 1024 * 200;
constexpr int64_t TEMPMEM = 1024 * 1024 * 300;
constexpr int64_t RESNUM = 2;

knowhere::IVFIndexPtr
IndexFactory(const std::string& type) {
    if (type == "IVF") {
        return std::make_shared<knowhere::IVF>();
    } else if (type == "IVFPQ") {
        return std::make_shared<knowhere::IVFPQ>();
    } else if (type == "IVFSQ") {
        return std::make_shared<knowhere::IVFSQ>();
#ifdef MILVUS_GPU_VERSION
    } else if (type == "GPUIVF") {
        return std::make_shared<knowhere::GPUIVF>(DEVICEID);
    } else if (type == "GPUIVFPQ") {
        return std::make_shared<knowhere::GPUIVFPQ>(DEVICEID);
    } else if (type == "GPUIVFSQ") {
        return std::make_shared<knowhere::GPUIVFSQ>(DEVICEID);
#ifdef CUSTOMIZATION
    } else if (type == "IVFSQHybrid") {
        return std::make_shared<knowhere::IVFSQHybrid>(DEVICEID);
#endif
#endif
    }
}

enum class ParameterType {
    ivf,
    ivfpq,
    ivfsq,
};

class ParamGenerator {
 public:
    static ParamGenerator&
    GetInstance() {
        static ParamGenerator instance;
        return instance;
    }

    knowhere::Config
    Gen(const ParameterType& type) {
        if (type == ParameterType::ivf) {
            auto tempconf = std::make_shared<knowhere::IVFCfg>();
            tempconf->d = DIM;
            tempconf->gpu_id = DEVICEID;
            tempconf->nlist = 100;
            tempconf->nprobe = 4;
            tempconf->k = K;
            tempconf->metric_type = knowhere::METRICTYPE::L2;
            return tempconf;
        } else if (type == ParameterType::ivfpq) {
            auto tempconf = std::make_shared<knowhere::IVFPQCfg>();
            tempconf->d = DIM;
            tempconf->gpu_id = DEVICEID;
            tempconf->nlist = 100;
            tempconf->nprobe = 4;
            tempconf->k = K;
            tempconf->m = 4;
            tempconf->nbits = 8;
            tempconf->metric_type = knowhere::METRICTYPE::L2;
            return tempconf;
        } else if (type == ParameterType::ivfsq) {
            auto tempconf = std::make_shared<knowhere::IVFSQCfg>();
            tempconf->d = DIM;
            tempconf->gpu_id = DEVICEID;
            tempconf->nlist = 100;
            tempconf->nprobe = 4;
            tempconf->k = K;
            tempconf->nbits = 8;
            tempconf->metric_type = knowhere::METRICTYPE::L2;
            return tempconf;
        }
    }
};

#include <gtest/gtest.h>

class TestGpuIndexBase : public ::testing::Test {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);
#endif
    }

    void
    TearDown() override {
#ifdef MILVUS_GPU_VERSION
        knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
    }
};
