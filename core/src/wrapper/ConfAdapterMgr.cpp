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

#include "wrapper/ConfAdapterMgr.h"
#include "utils/Exception.h"

namespace milvus {
namespace engine {

ConfAdapterPtr
AdapterMgr::GetAdapter(const IndexType& indexType) {
    if (!init_)
        RegisterAdapter();

    auto it = table_.find(indexType);
    if (it != table_.end()) {
        return it->second();
    } else {
        throw Exception(KNOWHERE_INVALID_ARGUMENT, "Can not find this type of confadapter");
    }
}

#define REGISTER_CONF_ADAPTER(T, KEY, NAME) static AdapterMgr::register_t<T> reg_##NAME##_(KEY)

void
AdapterMgr::RegisterAdapter() {
    init_ = true;

    REGISTER_CONF_ADAPTER(ConfAdapter, IndexType::FAISS_IDMAP, idmap);
    REGISTER_CONF_ADAPTER(BinIDMAPConfAdapter, IndexType::FAISS_BIN_IDMAP, idmap_bin);

    REGISTER_CONF_ADAPTER(IVFConfAdapter, IndexType::FAISS_IVFFLAT_CPU, ivf_cpu);
    REGISTER_CONF_ADAPTER(IVFConfAdapter, IndexType::FAISS_IVFFLAT_GPU, ivf_gpu);
    REGISTER_CONF_ADAPTER(IVFConfAdapter, IndexType::FAISS_IVFFLAT_MIX, ivf_mix);
    REGISTER_CONF_ADAPTER(BinIVFConfAdapter, IndexType::FAISS_BIN_IVFLAT_CPU, ivf_bin_cpu);

    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexType::FAISS_IVFSQ8_CPU, ivfsq8_cpu);
    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexType::FAISS_IVFSQ8_GPU, ivfsq8_gpu);
    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexType::FAISS_IVFSQ8_MIX, ivfsq8_mix);
    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexType::FAISS_IVFSQ8_HYBRID, ivfsq8_h);

    REGISTER_CONF_ADAPTER(IVFPQConfAdapter, IndexType::FAISS_IVFPQ_CPU, ivfpq_cpu);
    REGISTER_CONF_ADAPTER(IVFPQConfAdapter, IndexType::FAISS_IVFPQ_GPU, ivfpq_gpu);
    REGISTER_CONF_ADAPTER(IVFPQConfAdapter, IndexType::FAISS_IVFPQ_MIX, ivfpq_mix);

    REGISTER_CONF_ADAPTER(NSGConfAdapter, IndexType::NSG_MIX, nsg_mix);

    REGISTER_CONF_ADAPTER(SPTAGKDTConfAdapter, IndexType::SPTAG_KDT_RNT_CPU, sptag_kdt);
    REGISTER_CONF_ADAPTER(SPTAGBKTConfAdapter, IndexType::SPTAG_BKT_RNT_CPU, sptag_bkt);
}

}  // namespace engine
}  // namespace milvus
