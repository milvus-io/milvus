// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <mutex>

#include "ConfigKnowhere.h"
#include "common/Log.h"
#include "exceptions/EasyAssert.h"
#include "knowhere/archive/KnowhereConfig.h"

namespace milvus::config {

std::once_flag init_knowhere_once_;

void
KnowhereInitImpl(const char* conf_file) {
    auto init = [&]() {
        knowhere::KnowhereConfig::SetBlasThreshold(16384);
        knowhere::KnowhereConfig::SetEarlyStopThreshold(0);
        knowhere::KnowhereConfig::SetLogHandler();
        knowhere::KnowhereConfig::SetStatisticsLevel(0);

#ifdef EMBEDDED_MILVUS
        // always disable all logs for embedded milvus.
        el::Configurations el_conf;
        el_conf.setGlobally(el::ConfigurationType::Enabled, "false");
        el::Loggers::reconfigureAllLoggers(el_conf);
#else
        if (conf_file != nullptr) {
            el::Configurations el_conf(conf_file);
            el::Loggers::reconfigureAllLoggers(el_conf);
            LOG_SERVER_DEBUG_ << "Config easylogging with yaml file: " << conf_file;
        }
#endif
    };

    std::call_once(init_knowhere_once_, init);
}

std::string
KnowhereSetSimdType(const char* value) {
    knowhere::KnowhereConfig::SimdType simd_type;
    if (strcmp(value, "auto") == 0) {
        simd_type = knowhere::KnowhereConfig::SimdType::AUTO;
    } else if (strcmp(value, "avx512") == 0) {
        simd_type = knowhere::KnowhereConfig::SimdType::AVX512;
    } else if (strcmp(value, "avx2") == 0) {
        simd_type = knowhere::KnowhereConfig::SimdType::AVX2;
    } else if (strcmp(value, "avx") == 0 || strcmp(value, "sse4_2") == 0) {
        simd_type = knowhere::KnowhereConfig::SimdType::SSE4_2;
    } else {
        PanicInfo("invalid SIMD type: " + std::string(value));
    }
    try {
        return knowhere::KnowhereConfig::SetSimdType(simd_type);
    } catch (std::exception& e) {
        LOG_SERVER_ERROR_ << e.what();
        PanicInfo(e.what());
    }
}

void
KnowhereSetIndexSliceSize(const int64_t size) {
    knowhere::KnowhereConfig::SetIndexFileSliceSize(size);
}

}  // namespace milvus::config
