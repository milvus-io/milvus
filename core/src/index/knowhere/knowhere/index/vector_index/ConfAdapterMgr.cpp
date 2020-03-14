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

#include "knowhere/index/vector_index/ConfAdapterMgr.h"

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"

namespace milvus {
namespace knowhere {

ConfAdapterPtr
AdapterMgr::GetAdapter(const IndexType type, const IndexMode mode) {
    if (!init_)
        RegisterAdapter();

    try {
        return table_.at(type).at(mode)();
    } catch (...) {
        KNOWHERE_THROW_MSG("Can not find this type of confadapter");
    }
}

#define REGISTER_CONF_ADAPTER(T, TYPE, MODE, NAME) static AdapterMgr::register_t<T> reg_##NAME##_(TYPE, MODE)

void
AdapterMgr::RegisterAdapter() {
    init_ = true;

    REGISTER_CONF_ADAPTER(ConfAdapter, IndexType::INDEX_FAISS_IDMAP, IndexMode::MODE_CPU, idmap_cpu);

    REGISTER_CONF_ADAPTER(IVFConfAdapter, IndexType::INDEX_FAISS_IVFFLAT, IndexMode::MODE_CPU, ivf_cpu);
    REGISTER_CONF_ADAPTER(IVFConfAdapter, IndexType::INDEX_FAISS_IVFFLAT, IndexMode::MODE_GPU, ivf_gpu);

    REGISTER_CONF_ADAPTER(IVFPQConfAdapter, IndexType::INDEX_FAISS_IVFPQ, IndexMode::MODE_CPU, ivfpq_cpu);
    REGISTER_CONF_ADAPTER(IVFPQConfAdapter, IndexType::INDEX_FAISS_IVFPQ, IndexMode::MODE_GPU, ivfpq_gpu);

    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexType::INDEX_FAISS_IVFSQ8, IndexMode::MODE_CPU, ivfsq8_cpu);
    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexType::INDEX_FAISS_IVFSQ8, IndexMode::MODE_GPU, ivfsq8_gpu);

    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexType::INDEX_FAISS_IVFSQ8H, IndexMode::MODE_GPU, ivfsq8h_gpu);

    REGISTER_CONF_ADAPTER(BinIDMAPConfAdapter, IndexType::INDEX_FAISS_BIN_IDMAP, IndexMode::MODE_CPU, idmap_bin_cpu);

    REGISTER_CONF_ADAPTER(BinIDMAPConfAdapter, IndexType::INDEX_FAISS_BIN_IVFFLAT, IndexMode::MODE_CPU, ivf_bin_cpu);

    REGISTER_CONF_ADAPTER(NSGConfAdapter, IndexType::INDEX_NSG, IndexMode::MODE_CPU, nsg_cpu);

    REGISTER_CONF_ADAPTER(ConfAdapter, IndexType::INDEX_SPTAG_KDT_RNT, IndexMode::MODE_CPU, sptag_kdt_cpu);
    REGISTER_CONF_ADAPTER(ConfAdapter, IndexType::INDEX_SPTAG_BKT_RNT, IndexMode::MODE_CPU, sptag_bkt_cpu);

    REGISTER_CONF_ADAPTER(HNSWConfAdapter, IndexType::INDEX_HNSW, IndexMode::MODE_CPU, hnsw_cpu);
}

}  // namespace knowhere
}  // namespace milvus
