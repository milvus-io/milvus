// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/EasyAssert.h"
#include "storage/loon_ffi/ffi_writer_c.h"
#include "common/common_type_c.h"
#include "storage/KeyRetriever.h"
#include "monitor/scope_metric.h"

/**
 * @brief Retrieves encryption parameters for CMEK.
 *
 * @details Calls milvus::storage::GetWriterEncryptionParams(c_plugin_context)
 * to derive and validate the encryption key and encoded metadata from the
 * supplied writer keyset. Keyset-aware plugins do this without updating cached
 * ordinary keys. Copies the parameters into *out_key and *out_meta with strdup
 * for the caller to free. Converts C++ exceptions to CStatus using
 * milvus::FailureCStatus(&e).
 *
 * @see GetEncParams declaration in ffi_writer_c.h for parameter documentation
 */
CStatus
GetEncParams(CPluginContext* c_plugin_context,
             char** out_key,
             char** out_meta) {
    try {
        auto [key, metadata] =
            milvus::storage::GetWriterEncryptionParams(c_plugin_context);
        *out_key = strdup(key.c_str());
        *out_meta = strdup(metadata.c_str());
        return milvus::SuccessCStatus();

    } catch (const std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
