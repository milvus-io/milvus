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

#include <mutex>
#include "exceptions/EasyAssert.h"
#include "knowhere/archive/KnowhereConfig.h"
#include "easyloggingpp/easylogging++.h"
#include "ConfigKnowhere.h"
#include "faiss/FaissHook.h"

namespace milvus {
namespace config {

std::once_flag init_knowhere_once_;

void
KnowhereInitImpl() {
    auto init = []() {
        namespace eg = milvus::engine;
        eg::KnowhereConfig::SetSimdType(eg::KnowhereConfig::SimdType::AUTO);
        eg::KnowhereConfig::SetBlasThreshold(16384);
        eg::KnowhereConfig::SetEarlyStopThreshold(0);
        eg::KnowhereConfig::SetLogHandler();
        eg::KnowhereConfig::SetStatisticsLevel(0);
        el::Configurations el_conf;
        el_conf.setGlobally(el::ConfigurationType::Enabled, std::to_string(false));
    };

    std::call_once(init_knowhere_once_, init);
}

std::string
KnowhereSetSimdType(const char* value) {
    milvus::engine::KnowhereConfig::SimdType simd_type;
    if (strcmp(value, "auto") == 0) {
        simd_type = milvus::engine::KnowhereConfig::SimdType::AUTO;
    } else if (strcmp(value, "avx512") == 0) {
        simd_type = milvus::engine::KnowhereConfig::SimdType::AVX512;
    } else if (strcmp(value, "avx2") == 0) {
        simd_type = milvus::engine::KnowhereConfig::SimdType::AVX2;
    } else if (strcmp(value, "sse") == 0) {
        simd_type = milvus::engine::KnowhereConfig::SimdType::SSE;
    } else {
        PanicInfo("invalid SIMD type: " + std::string(value));
    }
    return milvus::engine::KnowhereConfig::SetSimdType(simd_type);
}

}  // namespace config
}  // namespace milvus
