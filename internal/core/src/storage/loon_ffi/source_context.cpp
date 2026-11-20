// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/loon_ffi/source_context.h"
#include "storage/loon_ffi/source_context_c.h"
#include "storage/StatusToErrorCode.h"
#include "milvus-storage/ffi_source_read_context_c.h"
#include "common/EasyAssert.h"
#include <atomic>
#include <algorithm>
#include <cstring>
#include "milvus-storage/common/layout.h"
#include "milvus-storage/filesystem/ffi/filesystem_internal.h"
#include <nlohmann/json.hpp>

namespace {
std::atomic<MilvusSourceContextFactory> source_factory{nullptr};
}

extern "C" void
MilvusSetSourceContextFactory(MilvusSourceContextFactory factory) {
    MilvusSourceContextFactory empty = nullptr;
    source_factory.compare_exchange_strong(empty, factory);
}

namespace milvus::storage {
arrow::Result<std::optional<milvus_storage::AzureSourceResource>>
SourceContextResource(const std::string& source, const std::string& spec) {
    using milvus_storage::AzureSourceResource;
    using milvus_storage::AzureSourceStorageBinding;
    if (spec.empty())
        return std::optional<AzureSourceResource>{};
    const auto parsed = nlohmann::json::parse(spec, nullptr, false);
    if (!parsed.is_object())
        return arrow::Status::Invalid("invalid external source specification");
    if (!parsed.contains("credential_binding"))
        return std::optional<AzureSourceResource>{};
    const auto invalid = [] {
        return arrow::Status::Invalid(
            "invalid source context routing or credential configuration");
    };
    const auto extfs = parsed.find("extfs");
    if (!parsed["credential_binding"].is_object() || extfs == parsed.end() ||
        !extfs->is_object())
        return invalid();
    std::string account, cloud;
    for (const auto& [key, value] : extfs->items()) {
        if (!value.is_string())
            return invalid();
        const auto& text = value.get_ref<const std::string&>();
        if (key == "cloud_provider")
            cloud = text;
        else if (key == "access_key_id")
            account = text;
        else if (key == "use_ssl") {
            if (text != "true")
                return invalid();
        } else if (key != "region") {
            // Reject even empty credential/endpoint fields: never silently
            // convert a mixed credential configuration into context mode.
            return invalid();
        }
    }
    if (cloud != "azure" || account.empty())
        return invalid();

    auto resource = AzureSourceResource::Parse(source, account);
    if (!resource &&
        (source.rfind("azure://", 0) == 0 || source.rfind("az://", 0) == 0)) {
        // Milvus shorthand has an explicit suffix/container but omits account.
        // Extract routing fields only; native binding validates the entire URI,
        // including encodings. A container-only Lance alias is not sufficient
        // as a top-level source because it omits the endpoint suffix as well.
        const auto start = source.find("://") + 3;
        const auto slash = source.find('/', start);
        if (slash == std::string::npos)
            return invalid();
        const auto suffix = source.substr(start, slash - start);
        // Azure container names cannot contain dots. Require a dotted endpoint
        // suffix here so a container-only alias cannot invent a new endpoint.
        if (suffix.find('.') == std::string::npos)
            return invalid();
        const auto end = source.find('/', slash + 1);
        const auto binding = AzureSourceStorageBinding::Create(
            account, suffix, source.substr(slash + 1, end - slash - 1));
        if (binding)
            resource = binding->ResolveUri(source);
    }
    if (!resource)
        return invalid();
    return resource;
}

milvus_storage::SourceReadContextPtr
OpenSourceReadContext(const std::string& source, const std::string& spec) {
    if (spec.empty())
        return nullptr;
    auto parsed = nlohmann::json::parse(spec, nullptr, false);
    if (parsed.is_discarded() || !parsed.is_object()) {
        ThrowInfo(static_cast<ErrorCode>(121),
                  "invalid source authorization specification");
    }
    if (!parsed.contains("credential_binding"))
        return nullptr;
    auto factory = source_factory.load();
    if (!factory) {
        ThrowInfo(static_cast<ErrorCode>(117),
                  "source authorization provider is not installed");
    }
    uintptr_t handle = 0;
    auto status = factory(source.c_str(), spec.c_str(), 0, &handle);
    if (status != LOON_SOURCE_CREDENTIAL_OK || handle == 0) {
        if (handle)
            loon_source_read_context_release(handle);
        auto code = (status >= LOON_SOURCE_CREDENTIAL_DENIED &&
                     status <= LOON_SOURCE_CREDENTIAL_INVALID_RESPONSE)
                        ? 116 + status
                        : 121;
        ThrowInfo(static_cast<ErrorCode>(code),
                  "source authorization context creation failed");
    }
    auto context = milvus_storage::GetSourceReadContext(handle);
    loon_source_read_context_release(handle);
    if (!context) {
        ThrowInfo(static_cast<ErrorCode>(121),
                  "source authorization context is invalid");
    }
    return context;
}

milvus_storage::SourceReadContextPtr
RetainOrOpenSourceReadContext(uintptr_t handle,
                              const std::string& source,
                              const std::string& spec) {
    if (!handle)
        return OpenSourceReadContext(source, spec);
    auto context = milvus_storage::GetSourceReadContext(handle);
    if (!context) {
        ThrowInfo(static_cast<ErrorCode>(117), "invalid source context handle");
    }
    return context;
}

std::unique_ptr<milvus_storage::api::Reader>
CreateSourceReader(
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& groups,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<std::vector<std::string>>& columns,
    const std::shared_ptr<const milvus_storage::api::Properties>& properties,
    const milvus_storage::SourceReadContextPtr& context,
    const std::vector<size_t>& internal_column_group_indices) {
    if (!context) {
        return milvus_storage::api::Reader::create(
            groups, schema, columns, properties);
    }
    auto reader = internal_column_group_indices.empty()
                      ? milvus_storage::api::Reader::create_with_context(
                            groups, schema, columns, properties, context)
                      : milvus_storage::api::Reader::create_with_context(
                            groups,
                            schema,
                            columns,
                            properties,
                            context,
                            internal_column_group_indices);
    if (!reader.ok()) {
        auto error = SourceAwareStorageError(reader.status());
        ThrowInfo(error.get_error_code(),
                  "source reader creation failed: {}",
                  error.what());
    }
    return std::move(reader).ValueOrDie();
}

std::vector<size_t>
SelectInternalColumnGroups(
    const milvus_storage::api::ColumnGroups& groups,
    const std::string& manifest_path,
    const std::unordered_set<std::string>& generated_columns) {
    if (generated_columns.empty())
        return {};
    const auto manifest = nlohmann::json::parse(manifest_path, nullptr, false);
    if (!manifest.is_object() || !manifest.contains("base_path") ||
        !manifest["base_path"].is_string()) {
        ThrowInfo(static_cast<ErrorCode>(117),
                  "generated data has no trusted segment manifest");
    }
    const auto base = manifest["base_path"].get<std::string>();
    // Internal keys are complete bucket-relative keys or absolute local paths.
    // Do not normalize them: a changed spelling must not acquire authority.
    const auto clean_key = [](const std::string& key) {
        if (key.empty() || key.back() == '/' ||
            key.find_first_of(":\\?#%") != std::string::npos ||
            key.find("//") != std::string::npos)
            return false;
        size_t start = key.front() == '/' ? 1 : 0;
        while (start < key.size()) {
            const auto end = key.find('/', start);
            const auto part = key.substr(start, end - start);
            if (part == "." || part == ".." ||
                std::any_of(part.begin(), part.end(), [](unsigned char ch) {
                    return ch < 32 || ch == 127;
                }))
                return false;
            if (end == std::string::npos)
                break;
            start = end + 1;
        }
        return true;
    };
    if (!clean_key(base)) {
        ThrowInfo(static_cast<ErrorCode>(117),
                  "generated data has an invalid segment base");
    }
    const auto prefix = base + "/" + milvus_storage::kDataPath;
    std::vector<size_t> selected;
    std::unordered_set<std::string> internal_files, source_files;
    for (size_t i = 0; i < groups.size(); ++i) {
        const auto& group = groups[i];
        if (!group)
            continue;
        const bool generated =
            !group->columns.empty() &&
            std::all_of(group->columns.begin(),
                        group->columns.end(),
                        [&](const auto& column) {
                            return generated_columns.count(column) != 0;
                        });
        for (const auto& file : group->files) {
            if (!generated) {
                source_files.insert(file.path);
                continue;
            }
            // Packed writers put generated files directly in this segment's
            // data directory. Siblings, source URIs and unresolved paths fail
            // closed before either credential provider is consulted.
            if (file.path.compare(0, prefix.size(), prefix) != 0 ||
                file.path.size() == prefix.size() ||
                file.path.find('/', prefix.size()) != std::string::npos ||
                !clean_key(file.path)) {
                ThrowInfo(static_cast<ErrorCode>(117),
                          "generated data is outside its owning segment");
            }
            internal_files.insert(file.path);
        }
        if (generated)
            selected.push_back(i);
    }
    for (const auto& path : internal_files) {
        if (source_files.count(path)) {
            ThrowInfo(static_cast<ErrorCode>(117),
                      "source and generated data share an ambiguous file");
        }
    }
    return selected;
}
}  // namespace milvus::storage

extern "C" int
MilvusSourceFilesystemPath(uintptr_t filesystem, const char* uri, char** out) {
    if (out)
        *out = nullptr;
    if (!filesystem || !uri || !out)
        return 121;
    try {
        auto fs = reinterpret_cast<FileSystemWrapper*>(filesystem)->get();
        auto path = fs->PathFromUri(uri);
        if (!path.ok()) {
            auto code = static_cast<int>(
                milvus::storage::ArrowStatusToErrorCode(path.status()));
            return code >= 117 && code <= 121 ? code : 121;
        }
        *out = strdup(path->c_str());
        return *out ? 0 : 121;
    } catch (...) {
        return 121;
    }
}
