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

#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/IndexType.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
#endif

int DEVICEID = 0;
constexpr int64_t DIM = 128;
constexpr int64_t NB = 10000;
constexpr int64_t NQ = 10;
constexpr int64_t K = 10;
constexpr int64_t PINMEM = 1024 * 1024 * 200;
constexpr int64_t TEMPMEM = 1024 * 1024 * 300;
constexpr int64_t RESNUM = 2;

knowhere::IVFPtr
IndexFactory(const knowhere::IndexType type, const knowhere::IndexMode mode) {
    if (mode == knowhere::IndexMode::MODE_CPU) {
        switch (type) {
            case knowhere::IndexType::INDEX_FAISS_IVFFLAT: {
                return std::make_shared<knowhere::IVF>();
            }
            case knowhere::IndexType::INDEX_FAISS_IVFPQ: {
                return std::make_shared<knowhere::IVFPQ>();
            }
            case knowhere::IndexType::INDEX_FAISS_IVFSQ8: {
                return std::make_shared<knowhere::IVFSQ>();
            }
            case knowhere::IndexType::INDEX_FAISS_IVFSQ8H: {
                std::cout << "IVFSQ8H does not support MODE_CPU" << std::endl;
            }
            default:
                std::cout << "Invalid IndexType " << (int) type << std::endl;
        }
#ifdef MILVUS_GPU_VERSION
    } else {
        switch (type) {
            case knowhere::IndexType::INDEX_FAISS_IVFFLAT: {
                return std::make_shared<knowhere::GPUIVF>(DEVICEID);
            }
            case knowhere::IndexType::INDEX_FAISS_IVFPQ: {
                return std::make_shared<knowhere::GPUIVFPQ>(DEVICEID);
            }
            case knowhere::IndexType::INDEX_FAISS_IVFSQ8: {
                return std::make_shared<knowhere::GPUIVFSQ>(DEVICEID);
            }
#ifdef CUSTOMIZATION
            case knowhere::IndexType::INDEX_FAISS_IVFSQ8H: {
                return std::make_shared<knowhere::IVFSQHybrid>(DEVICEID);
            }
#endif
            default:
                std::cout << "Invalid IndexType " << (int) type << std::endl;
        }
#endif
    }
}

class ParamGenerator {
 public:
    static ParamGenerator&
    GetInstance() {
        static ParamGenerator instance;
        return instance;
    }

    knowhere::Config
    Gen(const knowhere::IndexType type) {
        switch (type) {
            case knowhere::IndexType::INDEX_FAISS_IVFFLAT:
                return knowhere::Config{
                            {knowhere::meta::DIM, DIM},
                            {knowhere::meta::TOPK, K},
                            {knowhere::IndexParams::nlist, 100},
                            {knowhere::IndexParams::nprobe, 4},
                            {knowhere::Metric::TYPE, knowhere::Metric::L2},
                            {knowhere::meta::DEVICEID, DEVICEID},
                            };
            case knowhere::IndexType::INDEX_FAISS_IVFPQ:
                return knowhere::Config{
                            {knowhere::meta::DIM, DIM},
                            {knowhere::meta::TOPK, K},
                            {knowhere::IndexParams::nlist, 100},
                            {knowhere::IndexParams::nprobe, 4},
                            {knowhere::IndexParams::m, 4},
                            {knowhere::IndexParams::nbits, 8},
                            {knowhere::Metric::TYPE, knowhere::Metric::L2},
                            {knowhere::meta::DEVICEID, DEVICEID},
                            };
            case knowhere::IndexType::INDEX_FAISS_IVFSQ8:
            case knowhere::IndexType::INDEX_FAISS_IVFSQ8H:
                return knowhere::Config{
                            {knowhere::meta::DIM, DIM},
                            {knowhere::meta::TOPK, K},
                            {knowhere::IndexParams::nlist, 100},
                            {knowhere::IndexParams::nprobe, 4},
                            {knowhere::IndexParams::nbits, 8},
                            {knowhere::Metric::TYPE, knowhere::Metric::L2},
                            {knowhere::meta::DEVICEID, DEVICEID},
                            };
            default:
                std::cout << "Invalid index type " << (int)type << std::endl;
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
