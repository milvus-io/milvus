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

#pragma once

#include <filesystem>
#include <string_view>

#include "common/EasyAssert.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/FileSource.h"

namespace milvus::storage {

template <typename T>
T
ReadRequiredMeta(FileSource& source,
                 std::string_view key,
                 std::string_view context) {
    const auto value = source.GetMeta(key);
    if (!value.has_value()) {
        ThrowInfo(DataFormatBroken, "{} metadata {} is missing", context, key);
    }
    try {
        return value->get<T>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid {} metadata {}: {}",
                  context,
                  key,
                  error.what());
    }
}

// Only a single filename is allowed. Reserved sidecars and duplicate names
// are format-specific and are checked separately by each consumer.
inline void
ValidateArtifactEntryName(std::string_view name, std::string_view context) {
    const std::filesystem::path path(name);
    if (name.empty() || name.find('\0') != std::string_view::npos ||
        path.filename().string() != name || name == "." || name == "..") {
        ThrowInfo(DataFormatBroken, "invalid {} entry name {}", context, name);
    }
}

}  // namespace milvus::storage
