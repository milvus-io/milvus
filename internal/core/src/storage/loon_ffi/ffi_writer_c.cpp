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

#include <arrow/c/bridge.h>
#include <exception>
#include <memory>
#include <string>
#include <utility>

#include "PluginInterface.h"
#include "common/EasyAssert.h"
#include "common/common_type_c.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/writer.h"
#include "storage/KeyRetriever.h"
#include "storage/PluginLoader.h"
#include "storage/loon_ffi/ffi_writer_c.h"

CStatus
NewPackedFFIWriterWithCMEK(const char* base_path,
                           struct ArrowSchema* schema,
                           const LoonProperties* properties,
                           CPluginContext* plugin_context,
                           LoonWriterHandle* out_handle) {
    try {
        AssertInfo(out_handle != nullptr, "writer output handle is nullptr");
        *out_handle = 0;
        AssertInfo(base_path != nullptr && schema != nullptr &&
                       properties != nullptr && plugin_context != nullptr &&
                       plugin_context->key != nullptr,
                   "invalid encrypted writer arguments");

        milvus_storage::api::Properties properties_map;
        auto error = milvus_storage::api::ConvertFFIProperties(properties_map,
                                                               properties);
        AssertInfo(!error.has_value(),
                   "invalid packed writer properties: {}",
                   error.value_or(""));

        auto plugin =
            milvus::storage::PluginLoader::GetInstance().getCipherPlugin();
        AssertInfo(plugin != nullptr, "cipher plugin is nullptr");
        plugin->Update(plugin_context->ez_id,
                       plugin_context->collection_id,
                       std::string(plugin_context->key));
        auto [encryptor, edek] = plugin->GetEncryptor(
            plugin_context->ez_id, plugin_context->collection_id);

        // DEKs are binary. Keep the key in a C++ string: GetEncParams followed
        // by Go/C string properties truncated it at the first NUL byte.
        properties_map[PROPERTY_WRITER_ENC_ENABLE] = true;
        properties_map[PROPERTY_WRITER_ENC_KEY] = encryptor->GetKey();
        properties_map[PROPERTY_WRITER_ENC_META] =
            milvus::storage::EncodeKeyMetadata(
                plugin_context->ez_id, plugin_context->collection_id, edek);
        properties_map[PROPERTY_WRITER_ENC_ALGORITHM] =
            std::string("AES_GCM_V1");

        auto schema_result = arrow::ImportSchema(schema);
        if (!schema_result.ok()) {
            throw milvus_storage::ToSegcoreError(schema_result.status());
        }
        auto arrow_schema = schema_result.ValueOrDie();
        auto policy_result =
            milvus_storage::api::ColumnGroupPolicy::create_column_group_policy(
                properties_map, arrow_schema);
        if (!policy_result.ok()) {
            throw milvus_storage::ToSegcoreError(policy_result.status());
        }
        auto writer = milvus_storage::api::Writer::create(
            std::string(base_path),
            arrow_schema,
            std::move(policy_result).ValueOrDie(),
            properties_map);
        *out_handle = reinterpret_cast<LoonWriterHandle>(writer.release());
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    } catch (...) {
        return milvus::FailureCStatus(milvus::ErrorCode::UnexpectedError,
                                      "unknown exception");
    }
}
