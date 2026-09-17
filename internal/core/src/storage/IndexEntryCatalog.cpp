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

#include "storage/IndexEntryCatalog.h"
#include "storage/IndexEntryFormat.h"

namespace milvus::storage {

const IndexEntryCatalogEntry&
IndexEntryCatalog::At(std::string_view name) const {
    const auto it =
        std::lower_bound(entries_.begin(),
                         entries_.end(),
                         name,
                         [](const auto& entry, std::string_view key) {
                             return entry.name < key;
                         });
    if (it == entries_.end() || it->name != name) {
        ThrowInfo(ErrorCode::DataFormatBroken, "Entry not found: {}", name);
    }
    return *it;
}

IndexEntryCatalog::IndexEntryCatalog(const IndexEntryDirectory& directory,
                                     int64_t file_size) {
    AssertInfo(directory.entry_names_.size() == directory.entry_index_.size(),
               "Duplicate entries in V3 directory");
    entry_names_ = directory.entry_names_;
    auto& entries = entries_;
    entries.reserve(directory.entry_index_.size());
    for (const auto& [name, meta] : directory.entry_index_) {
        if (!meta.encrypted) {
            AssertInfo(
                meta.plain.offset <= static_cast<uint64_t>(
                                         file_size - MILVUS_V3_MAGIC_SIZE) &&
                    meta.plain.size <=
                        file_size - MILVUS_V3_MAGIC_SIZE - meta.plain.offset,
                "Entry '{}' range exceeds packed file",
                name);
            entries.push_back(
                {name,
                 meta.plain.size,
                 meta.plain.crc32,
                 PlainEntrySource{MILVUS_V3_MAGIC_SIZE + meta.plain.offset}});
            continue;
        }
        EncryptedEntrySource source{meta.enc.original_size, {}};
        source.slices.reserve(meta.enc.slices.size());
        size_t offset = 0;
        for (const auto& slice : meta.enc.slices) {
            const auto bytes =
                std::min(directory.slice_size_,
                         static_cast<size_t>(meta.enc.original_size - offset));
            AssertInfo(
                slice.size > 0 &&
                    slice.offset <= static_cast<uint64_t>(
                                        file_size - MILVUS_V3_MAGIC_SIZE) &&
                    slice.size <=
                        file_size - MILVUS_V3_MAGIC_SIZE - slice.offset,
                "Encrypted entry '{}' has an empty or out-of-bounds range",
                name);
            source.slices.push_back({MILVUS_V3_MAGIC_SIZE + slice.offset,
                                     slice.size,
                                     offset,
                                     bytes});
            offset += bytes;
        }
        entries.push_back(
            {name, meta.enc.original_size, meta.enc.crc32, std::move(source)});
    }
    std::sort(
        entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.name < rhs.name;
        });
}

}  // namespace milvus::storage
