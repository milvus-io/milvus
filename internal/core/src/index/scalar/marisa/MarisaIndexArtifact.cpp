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

#include "index/scalar/marisa/MarisaIndexArtifact.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "index/scalar/marisa/MarisaIndexReader.h"
#include "storage/artifact/LocalFileUtils.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr uint32_t kCsrFormatVersion = 1;
constexpr std::string_view kCsrFormatVersionMeta = "marisa_csr_format_version";
constexpr std::string_view kCsrNumKeysMeta = "csr_num_keys";

size_t
SerializedTrieSize(int fd, const std::string& path) {
    struct stat status {};
    if (fstat(fd, &status) != 0) {
        const auto error = errno;
        ThrowInfo(FileReadFailed,
                  "failed to stat serialized marisa trie {}: {}",
                  path,
                  std::strerror(error));
    }
    if (!S_ISREG(status.st_mode) || status.st_size < 0 ||
        static_cast<uintmax_t>(status.st_size) >
            static_cast<uintmax_t>(std::numeric_limits<size_t>::max())) {
        ThrowInfo(FileReadFailed,
                  "serialized marisa trie has an invalid size: {}",
                  path);
    }
    return static_cast<size_t>(status.st_size);
}

std::pair<int, storage::LocalEntryGuard>
CreateTrieFile() {
    auto pattern =
        (std::filesystem::temp_directory_path() / "marisa_trie_XXXXXX")
            .string();
    storage::LocalEntryGuard file(std::move(pattern));
    const auto fd = mkstemp(file.MutablePath());
    if (fd == -1) {
        const auto error = errno;
        static_cast<void>(file.Release());
        ThrowInfo(FileCreateFailed,
                  "failed to create marisa trie file: {}",
                  std::strerror(error));
    }
    return {fd, std::move(file)};
}

size_t
CheckedBytes(size_t count, size_t width, std::string_view entry) {
    AssertInfo(
        width == 0 || count <= std::numeric_limits<size_t>::max() / width,
        "marisa entry {} byte size overflows size_t",
        entry);
    return count * width;
}

void
ValidateStorage(const MarisaIndexStorage& storage) {
    AssertInfo(storage.trie != nullptr, "marisa artifact requires a trie");
    AssertInfo(storage.value_type == DataType::STRING ||
                   storage.value_type == DataType::VARCHAR ||
                   storage.value_type == DataType::TEXT,
               "marisa artifact requires STRING, VARCHAR, or TEXT");
    AssertInfo(storage.csr_num_keys == storage.trie->num_keys(),
               "marisa artifact CSR key count mismatch");
    AssertInfo(storage.str_ids_size == 0 || storage.str_ids != nullptr,
               "marisa artifact is missing row ids");
    AssertInfo(storage.csr_index != nullptr,
               "marisa artifact is missing CSR index");
    AssertInfo(storage.csr_index[storage.csr_num_keys] == 0 ||
                   storage.csr_offsets != nullptr,
               "marisa artifact is missing CSR offsets");
}

std::shared_ptr<const MarisaIndexStorage>
MakeBuilderStorage(std::shared_ptr<marisa::Trie> trie,
                   std::vector<int64_t> str_ids,
                   std::vector<uint32_t> csr_index,
                   std::vector<uint32_t> csr_offsets,
                   DataType value_type) {
    auto storage = std::make_shared<MarisaIndexStorage>();
    storage->str_ids_owner =
        std::make_shared<const std::vector<int64_t>>(std::move(str_ids));
    storage->csr_index_owner =
        std::make_shared<const std::vector<uint32_t>>(std::move(csr_index));
    storage->csr_offsets_owner =
        std::make_shared<const std::vector<uint32_t>>(std::move(csr_offsets));
    storage->trie = std::move(trie);
    storage->str_ids = storage->str_ids_owner->data();
    storage->str_ids_size = storage->str_ids_owner->size();
    storage->csr_index = storage->csr_index_owner->data();
    storage->csr_offsets = storage->csr_offsets_owner->data();
    storage->csr_num_keys =
        storage->trie == nullptr ? 0 : storage->trie->num_keys();
    storage->value_type = value_type;
    AssertInfo(storage->csr_num_keys < std::numeric_limits<size_t>::max(),
               "marisa artifact CSR index count overflows size_t");
    AssertInfo(storage->csr_index_owner->size() == storage->csr_num_keys + 1,
               "invalid marisa artifact CSR index size");
    ValidateStorage(*storage);
    AssertInfo(storage->csr_offsets_owner->size() ==
                   storage->csr_index[storage->csr_num_keys],
               "invalid marisa artifact CSR offsets size");
    return storage;
}

}  // namespace

MarisaIndexArtifact::MarisaIndexArtifact(std::shared_ptr<marisa::Trie> trie,
                                         std::vector<int64_t> str_ids,
                                         std::vector<uint32_t> csr_index,
                                         std::vector<uint32_t> csr_offsets,
                                         DataType value_type)
    : storage_(MakeBuilderStorage(std::move(trie),
                                  std::move(str_ids),
                                  std::move(csr_index),
                                  std::move(csr_offsets),
                                  value_type)) {
    AssertInfo(storage_ != nullptr, "marisa artifact requires shared storage");
    ValidateStorage(*storage_);
}

MarisaIndexArtifact::~MarisaIndexArtifact() = default;

void
MarisaIndexArtifact::Serialize(storage::FileSink& sink) const {
    auto [fd, file] = CreateTrieFile();
    storage::MappedRegionGuard legacy_mapping;
    try {
        storage_->trie->write(fd);
        if (sink.Gen() == storage::Generation::V1V2) {
            const auto size = SerializedTrieSize(fd, file.Path());
            if (size != 0) {
                auto* mapped = static_cast<char*>(
                    mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
                if (mapped == MAP_FAILED) {
                    const auto error = errno;
                    ThrowInfo(MmapError,
                              "failed to mmap serialized marisa trie {}: {}",
                              file.Path(),
                              std::strerror(error));
                }
                legacy_mapping = storage::MappedRegionGuard(mapped, size);
            }
        }
    } catch (...) {
        close(fd);
        throw;
    }
    if (close(fd) != 0) {
        const auto error = errno;
        ThrowInfo(FileWriteFailed,
                  "failed to close marisa trie file {}: {}",
                  file.Path(),
                  std::strerror(error));
    }
    if (sink.Gen() == storage::Generation::V1V2) {
        sink.WriteEntry(MARISA_TRIE_INDEX,
                        legacy_mapping.Data(),
                        legacy_mapping.Size());
        legacy_mapping = {};
    } else {
        sink.WriteEntryFromLocalFile(MARISA_TRIE_INDEX, file.Path());
    }

    sink.WriteEntry(
        MARISA_STR_IDS,
        storage_->str_ids,
        CheckedBytes(storage_->str_ids_size, sizeof(int64_t), MARISA_STR_IDS));

    // V1/V2 is exactly the historical two-entry BinarySet. CSR was introduced
    // only in the existing V3 entry format and must be rebuilt by legacy loads.
    if (sink.Gen() == storage::Generation::V1V2) {
        return;
    }

    AssertInfo(storage_->csr_num_keys < std::numeric_limits<size_t>::max(),
               "marisa CSR index count overflows size_t");
    const auto csr_index_count = storage_->csr_num_keys + 1;
    const auto csr_offsets_count =
        static_cast<size_t>(storage_->csr_index[storage_->csr_num_keys]);
    sink.WriteEntry(
        MARISA_CSR_INDEX,
        storage_->csr_index,
        CheckedBytes(csr_index_count, sizeof(uint32_t), MARISA_CSR_INDEX));
    sink.WriteEntry(
        MARISA_CSR_OFFSETS,
        storage_->csr_offsets,
        CheckedBytes(csr_offsets_count, sizeof(uint32_t), MARISA_CSR_OFFSETS));
    sink.PutMeta(kCsrFormatVersionMeta, nlohmann::json(kCsrFormatVersion));
    sink.PutMeta(kCsrNumKeysMeta, nlohmann::json(storage_->csr_num_keys));
}

}  // namespace milvus::index
