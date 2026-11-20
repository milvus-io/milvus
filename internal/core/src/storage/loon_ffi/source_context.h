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

#pragma once
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include "milvus-storage/reader.h"
#include "milvus-storage/source_read_context.h"

namespace milvus::storage {
// Derive routing only, without authorizing I/O. The reader's SourceReadContext
// must subsequently check this exact account/endpoint/container and full URI.
// A present binding never falls back to legacy credential configuration.
arrow::Result<std::optional<milvus_storage::AzureSourceResource>>
SourceContextResource(const std::string& source, const std::string& spec);

// Source/spec MUST come from trusted coordinator schema/task metadata.
milvus_storage::SourceReadContextPtr
OpenSourceReadContext(const std::string& source, const std::string& spec);

// Resolve an explicit handle exactly once, retaining the validated context.
// A released explicit handle must never select the legacy reader path.
milvus_storage::SourceReadContextPtr
RetainOrOpenSourceReadContext(uintptr_t handle,
                              const std::string& source,
                              const std::string& spec);

// Select generated column groups only from trusted coordinator schema and the
// owning segment manifest. Manifest properties and path spelling alone never
// grant internal storage authority. Files must already be resolved by Loon.
std::vector<size_t>
SelectInternalColumnGroups(
    const milvus_storage::api::ColumnGroups& groups,
    const std::string& manifest_path,
    const std::unordered_set<std::string>& generated_columns);

std::unique_ptr<milvus_storage::api::Reader>
CreateSourceReader(
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& groups,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<std::vector<std::string>>& columns,
    const std::shared_ptr<const milvus_storage::api::Properties>& properties,
    const milvus_storage::SourceReadContextPtr& context,
    const std::vector<size_t>& internal_column_group_indices = {});
}  // namespace milvus::storage
