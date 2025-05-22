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
#include "common/EasyAssert.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "knowhere/comp/knowhere_config.h"
#include "knowhere/version.h"

namespace milvus::config {

std::once_flag init_knowhere_once_;

void
KnowhereInitImpl(const char* conf_file) {
    auto init = [&]() {
        knowhere::KnowhereConfig::SetBlasThreshold(16384);
        knowhere::KnowhereConfig::SetEarlyStopThreshold(0);
        knowhere::KnowhereConfig::ShowVersion();
        if (!google::IsGoogleLoggingInitialized()) {
            google::InitGoogleLogging("milvus");
        }

#ifdef EMBEDDED_MILVUS
        // always disable all logs for embedded milvus
        google::SetCommandLineOption("minloglevel", "4");
#else
        if (conf_file != nullptr) {
            gflags::SetCommandLineOption("flagfile", conf_file);
        }
#endif
        setbuf(stdout, NULL);
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
        PanicInfo(ConfigInvalid, "invalid SIMD type: " + std::string(value));
    }
    try {
        return knowhere::KnowhereConfig::SetSimdType(simd_type);
    } catch (std::exception& e) {
        LOG_ERROR(e.what());
        PanicInfo(ConfigInvalid, e.what());
    }
}

void
EnableKnowhereScoreConsistency() {
    knowhere::KnowhereConfig::EnablePatchForComputeFP32AsBF16();
}

void
KnowhereInitBuildThreadPool(const uint32_t num_threads) {
    knowhere::KnowhereConfig::SetBuildThreadPoolSize(num_threads);
}

void
KnowhereInitSearchThreadPool(const uint32_t num_threads) {
    knowhere::KnowhereConfig::SetSearchThreadPoolSize(num_threads);
    if (!knowhere::KnowhereConfig::SetAioContextPool(num_threads)) {
        PanicInfo(ConfigInvalid,
                  "Failed to set aio context pool with num_threads " +
                      std::to_string(num_threads));
    }
}

void
KnowhereInitGPUMemoryPool(const uint32_t init_size, const uint32_t max_size) {
    if (init_size == 0 && max_size == 0) {
        knowhere::KnowhereConfig::SetRaftMemPool();
        return;
    } else if (init_size > max_size) {
        PanicInfo(ConfigInvalid,
                  "Error Gpu memory pool params: init_size {} can't not large "
                  "than max_size {}.",
                  init_size,
                  max_size);
    } else {
        knowhere::KnowhereConfig::SetRaftMemPool(size_t{init_size},
                                                 size_t{max_size});
    }
}

int32_t
GetMinimalIndexVersion() {
    return knowhere::Version::GetMinimalVersion().VersionNumber();
}

int32_t
GetCurrentIndexVersion() {
    return knowhere::Version::GetCurrentVersion().VersionNumber();
}

int32_t
GetMaximumIndexVersion() {
    return knowhere::Version::GetMaximumVersion().VersionNumber();
}

}  // namespace milvus::config
