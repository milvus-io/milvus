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

#include <memory>
#include "storage/IndexEntryFormat.h"
#include "storage/plugin/PluginInterface.h"
#include "nlohmann/json.hpp"

namespace milvus::storage {

/**
 * @brief Validated, immutable packed-file state shared by storage readers.
 * Published after magic, directory, metadata CRC and JSON validation. Retains
 * the exact opened artifact, never request context or decoded-entry caches.
 * Artifact files must not be overwritten while an index version uses them.
 */
class IndexEntryFile {
 private:
    friend class IndexEntryReader;
    friend class AsyncIndexEntryReader;

    IndexEntryFile() = default;

    std::shared_ptr<milvus::InputStream> input;
    int64_t file_size{0};
    int64_t collection_id{0};
    std::optional<IndexFileEncryption> encryption;
    std::shared_ptr<plugin::ICipherPlugin> cipher_plugin;
    IndexEntryDirectory directory;
    nlohmann::json metadata;
};

}  // namespace milvus::storage
