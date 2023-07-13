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
#include "exceptions/EasyAssert.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "knowhere/comp/thread_pool.h"
#include "knowhere/comp/knowhere_config.h"

namespace milvus::config {

std::once_flag init_knowhere_once_;

void
KnowhereInitImpl(const char* conf_file) {
    auto init = [&]() {
        knowhere::KnowhereConfig::SetBlasThreshold(16384);
        knowhere::KnowhereConfig::SetEarlyStopThreshold(0);
        knowhere::KnowhereConfig::ShowVersion();
        google::InitGoogleLogging("milvus");
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
KnowhereInitThreadPool(const uint32_t num_threads) {
    knowhere::ThreadPool::InitGlobalThreadPool(num_threads);
    knowhere::KnowhereConfig::SetAioContextPool(num_threads, 32);
}

}  // namespace milvus::config
