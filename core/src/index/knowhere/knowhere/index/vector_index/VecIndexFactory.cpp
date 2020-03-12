// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "knowhere/index/vector_index/VecIndexFactory.h"

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/IndexHNSW.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/IndexNSG.h"
#include "knowhere/index/vector_index/IndexSPTAG.h"

#ifdef MILVUS_GPU_VERSION
#include <cuda.h>
#include "knowhere/index/vector_index/gpu/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#endif

namespace knowhere {

VecIndexPtr
VecIndexFactory::CreateVecIndex(const IndexType type, const IndexMode mode) {
    std::shared_ptr<knowhere::VecIndex> index;

#ifdef MILVUS_GPU_VERSION
    auto gpu_device = -1;  // TODO: remove hardcode here, get from invoker
    if (mode == IndexMode::MODE_GPU) {
        switch (type) {
            // case IndexType::INDEX_FAISS_IDMAP {
            //     return std::make_shared<knowhere::GPUIDMAP>(gpu_device);
            // }
            case IndexType::INDEX_FAISS_IVFFLAT: {
                return std::make_shared<knowhere::GPUIVF>(gpu_device);
            }
            case IndexType::INDEX_FAISS_IVFSQ8: {
                return std::make_shared<knowhere::GPUIVFSQ>(gpu_device);
            }
            case IndexType::INDEX_FAISS_IVFPQ: {
                return std::make_shared<knowhere::GPUIVFPQ>(gpu_device);
            }
            case IndexType::INDEX_FAISS_IVFSQ8H: {
                return std::make_shared<knowhere::IVFSQHybrid>(gpu_device);
            }
        }
    }
#endif

    switch (type) {
        case IndexType::INDEX_FAISS_IDMAP: {
            return std::make_shared<knowhere::IDMAP>();
        }
        case IndexType::INDEX_FAISS_IVFFLAT: {
            return std::make_shared<knowhere::IVF>();
        }
        case IndexType::INDEX_FAISS_IVFSQ8: {
            return std::make_shared<knowhere::IVFSQ>();
        }
        case IndexType::INDEX_FAISS_IVFPQ: {
            return std::make_shared<knowhere::IVFPQ>();
        }
        case IndexType::INDEX_SPTAG_KDT_RNT: {
            return std::make_shared<knowhere::CPUSPTAGRNG>("KDT");
        }
        case IndexType::INDEX_SPTAG_BKT_RNT: {
            return std::make_shared<knowhere::CPUSPTAGRNG>("BKT");
        }
        case IndexType::INDEX_NSG: {
            return std::make_shared<knowhere::NSG>(-1);
        }
        case IndexType::INDEX_HNSW: {
            return std::make_shared<knowhere::IndexHNSW>();
        }
    }
    return nullptr;
}

}  // namespace knowhere
