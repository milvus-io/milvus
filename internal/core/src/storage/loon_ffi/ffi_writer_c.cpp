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

#include <string.h>
#include <exception>
#include <memory>
#include <string>
#include <utility>

#include "PluginInterface.h"
#include "common/EasyAssert.h"
#include "common/common_type_c.h"
#include "storage/KeyRetriever.h"
#include "storage/PluginLoader.h"
#include "storage/loon_ffi/ffi_writer_c.h"

/**
 * @brief Implementation of GetEncParams - retrieves encryption parameters for CMEK.
 *
 * @details This function performs the following steps:
 *   1. Loads the cipher plugin from PluginLoader singleton
 *   2. Updates the plugin with encryption zone ID, collection ID, and key
 *   3. Retrieves the encryptor for the given zone and collection
 *   4. Encodes key metadata containing zone ID, collection ID, and key version
 *   5. Returns the encryption key and metadata as newly allocated strings
 *
 * @see GetEncParams declaration in ffi_writer_c.h for parameter documentation
 */
CStatus
GetEncParams(CPluginContext* c_plugin_context,
             char** out_key,
             char** out_meta) {
    try {
        AssertInfo(c_plugin_context != nullptr, "c_plugin_context is nullptr");
        auto plugin_ptr =
            milvus::storage::PluginLoader::GetInstance().getCipherPlugin();
        AssertInfo(plugin_ptr != nullptr, "plugin_ptr is nullptr");

        plugin_ptr->Update(c_plugin_context->ez_id,
                           c_plugin_context->collection_id,
                           std::string(c_plugin_context->key));
        auto got = plugin_ptr->GetEncryptor(c_plugin_context->ez_id,
                                            c_plugin_context->collection_id);
        auto metadata =
            milvus::storage::EncodeKeyMetadata(c_plugin_context->ez_id,
                                               c_plugin_context->collection_id,
                                               got.second);
        *out_key = strdup(got.first->GetKey().c_str());
        *out_meta = strdup(metadata.c_str());
        return milvus::SuccessCStatus();

    } catch (std::exception& e) {
        return milvus::FailureCStatus(milvus::ErrorCode::UnexpectedError,
                                      e.what());
    }
}