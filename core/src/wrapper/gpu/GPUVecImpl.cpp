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

#include "wrapper/gpu/GPUVecImpl.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "src/wrapper/DataTransfer.h"
#include "utils/Log.h"
#include "wrapper/VecImpl.h"

#include <fiu-local.h>
/*
 * no parameter check in this layer.
 * only responible for index combination
 */

namespace milvus {
namespace engine {

// TODO(linxj): add lock here.
Status
IVFMixIndex::BuildAll(const int64_t& nb, const float* xb, const int64_t* ids, const Config& cfg, const int64_t& nt,
                      const float* xt) {
    try {
        fiu_do_on("IVFMixIndex.BuildAll.throw_knowhere_exception", throw knowhere::KnowhereException(""));
        fiu_do_on("IVFMixIndex.BuildAll.throw_std_exception", throw std::exception());

        dim = cfg->d;
        auto dataset = GenDatasetWithIds(nb, dim, xb, ids);
        auto preprocessor = index_->BuildPreprocessor(dataset, cfg);
        index_->set_preprocessor(preprocessor);
        auto model = index_->Train(dataset, cfg);
        index_->set_index_model(model);
        index_->Add(dataset, cfg);

        if (auto device_index = std::dynamic_pointer_cast<knowhere::GPUIndex>(index_)) {
            auto host_index = device_index->CopyGpuToCpu(Config());
            index_ = host_index;
            type = ConvertToCpuIndexType(type);
        } else {
            WRAPPER_LOG_ERROR << "Build IVFMIXIndex Failed";
            return Status(KNOWHERE_ERROR, "Build IVFMIXIndex Failed");
        }
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

Status
IVFMixIndex::Load(const knowhere::BinarySet& index_binary) {
    index_->Load(index_binary);
    dim = Dimension();
    return Status::OK();
}

#ifdef CUSTOMIZATION
knowhere::QuantizerPtr
IVFHybridIndex::LoadQuantizer(const Config& conf) {
    // TODO(linxj): Hardcode here
    if (auto new_idx = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_)) {
        return new_idx->LoadQuantizer(conf);
    } else {
        WRAPPER_LOG_ERROR << "Hybrid mode not support for index type: " << int(type);
        return nullptr;
    }
}

Status
IVFHybridIndex::SetQuantizer(const knowhere::QuantizerPtr& q) {
    try {
        fiu_do_on("IVFHybridIndex.SetQuantizer.throw_knowhere_exception", throw knowhere::KnowhereException(""));
        fiu_do_on("IVFHybridIndex.SetQuantizer.throw_std_exception", throw std::exception());

        // TODO(linxj): Hardcode here
        if (auto new_idx = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_)) {
            new_idx->SetQuantizer(q);
        } else {
            WRAPPER_LOG_ERROR << "Hybrid mode not support for index type: " << int(type);
            return Status(KNOWHERE_ERROR, "not support");
        }
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

Status
IVFHybridIndex::UnsetQuantizer() {
    try {
        fiu_do_on("IVFHybridIndex.UnsetQuantizer.throw_knowhere_exception", throw knowhere::KnowhereException(""));
        fiu_do_on("IVFHybridIndex.UnsetQuantizer.throw_std_exception", throw std::exception());

        // TODO(linxj): Hardcode here
        if (auto new_idx = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_)) {
            new_idx->UnsetQuantizer();
        } else {
            WRAPPER_LOG_ERROR << "Hybrid mode not support for index type: " << int(type);
            return Status(KNOWHERE_ERROR, "not support");
        }
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

VecIndexPtr
IVFHybridIndex::LoadData(const knowhere::QuantizerPtr& q, const Config& conf) {
    try {
        fiu_do_on("IVFHybridIndex.LoadData.throw_knowhere_exception", throw knowhere::KnowhereException(""));
        fiu_do_on("IVFHybridIndex.LoadData.throw_std_exception", throw std::exception());

        // TODO(linxj): Hardcode here
        if (auto new_idx = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_)) {
            return std::make_shared<IVFHybridIndex>(new_idx->LoadData(q, conf), type);
        } else {
            WRAPPER_LOG_ERROR << "Hybrid mode not support for index type: " << int(type);
        }
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
    }
    return nullptr;
}

std::pair<VecIndexPtr, knowhere::QuantizerPtr>
IVFHybridIndex::CopyToGpuWithQuantizer(const int64_t& device_id, const Config& cfg) {
    try {
        fiu_do_on("IVFHybridIndex.CopyToGpuWithQuantizer.throw_knowhere_exception",
                  throw knowhere::KnowhereException(""));
        fiu_do_on("IVFHybridIndex.CopyToGpuWithQuantizer.throw_std_exception", throw std::exception());

        // TODO(linxj): Hardcode here
        if (auto hybrid_idx = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_)) {
            auto pair = hybrid_idx->CopyCpuToGpuWithQuantizer(device_id, cfg);
            auto new_idx = std::make_shared<IVFHybridIndex>(pair.first, type);
            return std::make_pair(new_idx, pair.second);
        } else {
            WRAPPER_LOG_ERROR << "Hybrid mode not support for index type: " << int(type);
        }
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
    }
    return std::make_pair(nullptr, nullptr);
}
#endif

}  // namespace engine
}  // namespace milvus
