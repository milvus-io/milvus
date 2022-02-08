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

#include <memory>
#include <string>

#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFHNSW.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "knowhere/index/vector_offset_index/IndexIVF_NM.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_offset_index/gpu/IndexGPUIVF_NM.h"
#endif

constexpr int DEVICEID = 0;
constexpr int64_t DIM = 128;
constexpr int64_t NB = 10000;
constexpr int64_t NQ = 10;
constexpr int64_t K = 10;
constexpr int64_t PINMEM = 1024 * 1024 * 200;
constexpr int64_t TEMPMEM = 1024 * 1024 * 300;
constexpr int64_t RESNUM = 2;

inline milvus::knowhere::IVFPtr
IndexFactory(const milvus::knowhere::IndexType& type, const milvus::knowhere::IndexMode mode) {
    if (mode == milvus::knowhere::IndexMode::MODE_CPU) {
        if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT) {
            return std::make_shared<milvus::knowhere::IVF>();
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
            return std::make_shared<milvus::knowhere::IVFPQ>();
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8) {
            return std::make_shared<milvus::knowhere::IVFSQ>();
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFHNSW) {
            return std::make_shared<milvus::knowhere::IVFHNSW>();
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H) {
            std::cout << "IVFSQ8H does not support MODE_CPU" << std::endl;
        } else {
            std::cout << "Invalid IndexType " << type << std::endl;
        }
#ifdef MILVUS_GPU_VERSION
    } else {
        if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT) {
            return std::make_shared<milvus::knowhere::GPUIVF>(DEVICEID);
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
            return std::make_shared<milvus::knowhere::GPUIVFPQ>(DEVICEID);
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8) {
            return std::make_shared<milvus::knowhere::GPUIVFSQ>(DEVICEID);
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H) {
            return std::make_shared<milvus::knowhere::IVFSQHybrid>(DEVICEID);
        } else {
            std::cout << "Invalid IndexType " << type << std::endl;
        }
#endif
    }
    return nullptr;
}

inline milvus::knowhere::IVFNMPtr
IndexFactoryNM(const milvus::knowhere::IndexType& type, const milvus::knowhere::IndexMode mode) {
    if (mode == milvus::knowhere::IndexMode::MODE_CPU) {
        if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT) {
            return std::make_shared<milvus::knowhere::IVF_NM>();
        } else {
            std::cout << "Invalid IndexType " << type << std::endl;
        }
    }
    return nullptr;
}

class ParamGenerator {
 public:
    static ParamGenerator&
    GetInstance() {
        static ParamGenerator instance;
        return instance;
    }

    milvus::knowhere::Config
    Gen(const milvus::knowhere::IndexType& type) {
        if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT) {
            return milvus::knowhere::Config{
                {milvus::knowhere::meta::DIM, DIM},
                {milvus::knowhere::meta::TOPK, K},
                {milvus::knowhere::IndexParams::nlist, 100},
                {milvus::knowhere::IndexParams::nprobe, 4},
                {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
                {milvus::knowhere::meta::DEVICEID, DEVICEID},
            };
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
            return milvus::knowhere::Config{
                {milvus::knowhere::meta::DIM, DIM},
                {milvus::knowhere::meta::TOPK, K},
                {milvus::knowhere::IndexParams::nlist, 100},
                {milvus::knowhere::IndexParams::nprobe, 4},
                {milvus::knowhere::IndexParams::m, 4},
                {milvus::knowhere::IndexParams::nbits, 8},
                {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
                {milvus::knowhere::meta::DEVICEID, DEVICEID},
            };
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8 ||
                   type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H) {
            return milvus::knowhere::Config{
                {milvus::knowhere::meta::DIM, DIM},
                {milvus::knowhere::meta::TOPK, K},
                {milvus::knowhere::IndexParams::nlist, 100},
                {milvus::knowhere::IndexParams::nprobe, 4},
                {milvus::knowhere::IndexParams::nbits, 8},
                {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
                {milvus::knowhere::meta::DEVICEID, DEVICEID},
            };
        } else if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFHNSW) {
            return milvus::knowhere::Config{
                {milvus::knowhere::meta::DIM, DIM},
                {milvus::knowhere::meta::TOPK, K},
                {milvus::knowhere::IndexParams::nlist, 100},
                {milvus::knowhere::IndexParams::nprobe, 4},
                {milvus::knowhere::IndexParams::M, 16},
                {milvus::knowhere::IndexParams::efConstruction, 200},
                {milvus::knowhere::IndexParams::ef, 200},
                {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
                {milvus::knowhere::meta::DEVICEID, DEVICEID},
            };
        } else {
            std::cout << "Invalid index type " << type << std::endl;
        }
        return milvus::knowhere::Config();
    }
};

#include <gtest/gtest.h>

class TestGpuIndexBase : public ::testing::Test {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);
#endif
    }

    void
    TearDown() override {
#ifdef MILVUS_GPU_VERSION
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
    }
};
