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

#include "index/scalar/marisa/MarisaIndexLoader.h"
#include "index/scalar/marisa/MarisaIndexParams.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include <marisa.h>

#include "index/ParamUtils.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "index/scalar/marisa/MarisaIndexArtifact.h"
#include "index/scalar/marisa/MarisaIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

using marisa_params::ParseNested;
using marisa_params::ParseValueType;

constexpr uint32_t kCsrFormatVersion = 1;
constexpr std::string_view kCsrFormatVersionMeta = "marisa_csr_format_version";
constexpr std::string_view kCsrNumKeysMeta = "csr_num_keys";

struct EffectiveLoadOptions {
    bool enable_mmap{false};
    std::string mmap_dir;
};

EffectiveLoadOptions
ResolveLoadOptions(const storage::LoadOptions& opts) {
    EffectiveLoadOptions result;
    result.enable_mmap =
        opts.enable_mmap || GetValueFromConfigOrFallback<bool>(
                                opts.params, ENABLE_MMAP, false) ||
        (opts.params.is_object() && opts.params.contains(MMAP_FILE_PATH));
    result.mmap_dir = opts.mmap_dir_path;
    if (result.mmap_dir.empty() && opts.params.is_object() &&
        opts.params.contains(MMAP_FILE_PATH)) {
        try {
            result.mmap_dir = opts.params.at(MMAP_FILE_PATH).get<std::string>();
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataFormatBroken,
                      "marisa mmap path must be a string: {}",
                      error.what());
        }
    }
    if (result.enable_mmap && result.mmap_dir.empty()) {
        ThrowInfo(FileCreateFailed, "marisa mmap directory must not be empty");
    }
    return result;
}

using storage::LocalEntryGuard;

using storage::FileDescriptorGuard;

template <typename T>
std::shared_ptr<std::vector<T>>
ReadEntryVector(storage::FileSource& source,
                std::string_view entry_name,
                const std::string& staging_dir,
                std::optional<size_t> expected_bytes = std::nullopt) {
    if (!source.HasEntry(entry_name)) {
        ThrowInfo(DataFormatBroken,
                  "marisa artifact entry {} is missing",
                  entry_name);
    }

    auto path =
        (std::filesystem::path(staging_dir) / std::string(entry_name)).string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(entry_name, local.Path());

    const auto bytes = storage::LocalFileSize(
        local.Path(), "failed to determine marisa entry size for");
    if (expected_bytes.has_value() && bytes != *expected_bytes) {
        ThrowInfo(DataFormatBroken,
                  "invalid {} size: expected {}, got {}",
                  entry_name,
                  *expected_bytes,
                  bytes);
    }
    if (bytes % sizeof(T) != 0) {
        ThrowInfo(DataFormatBroken,
                  "invalid {} size: expected a multiple of {}, got {}",
                  entry_name,
                  sizeof(T),
                  bytes);
    }

    auto result = std::make_shared<std::vector<T>>(bytes / sizeof(T));
    if (bytes != 0) {
        const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
        if (fd == -1) {
            ThrowInfo(FileOpenFailed,
                      "failed to open marisa staging file {}: {}",
                      local.Path(),
                      std::strerror(errno));
        }
        FileDescriptorGuard descriptor(fd);
        storage::ReadAll(descriptor.Get(),
                         result->data(),
                         bytes,
                         local.Path(),
                         "marisa staging file");
        descriptor.CloseChecked(local.Path(), "marisa staging file");
    }
    local.RemoveChecked("marisa staging file");
    return result;
}

storage::MappedRegionGuard
MapReadOnly(const std::string& path, size_t size) {
    if (size == 0) {
        return {};
    }
    const auto fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open marisa mmap file {}: {}",
                  path,
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    auto* mapped =
        static_cast<char*>(mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
    const auto saved_errno = errno;
    if (mapped == MAP_FAILED) {
        ThrowInfo(MmapError,
                  "failed to mmap marisa file {}: {}",
                  path,
                  std::strerror(saved_errno));
    }
    return {mapped, size};
}

std::shared_ptr<marisa::Trie>
OpenTrie(const std::string& path, bool mmap_enabled) {
    auto trie = std::make_shared<marisa::Trie>();
    try {
        if (mmap_enabled) {
            trie->mmap(path.c_str());
            return trie;
        }
        const auto fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd == -1) {
            ThrowInfo(FileOpenFailed,
                      "failed to open marisa trie {}: {}",
                      path,
                      std::strerror(errno));
        }
        try {
            trie->read(fd);
        } catch (...) {
            close(fd);
            throw;
        }
        if (close(fd) != 0) {
            ThrowInfo(FileOpenFailed,
                      "failed to close marisa trie {}: {}",
                      path,
                      std::strerror(errno));
        }
        return trie;
    } catch (const marisa::Exception& error) {
        // A blanket DataFormatBroken here would report an allocation failure
        // as permanently corrupt data, so the index scheduler gives up on an
        // artifact that a retry would have loaded: MARISA_MEMORY_ERROR must
        // stay MemAllocateFailed (retriable).
        //
        // MARISA_IO_ERROR, however, is NOT transient at this site. `path` is
        // always a file this loader already materialised into its own staging
        // directory (ReadEntryToLocalFile above); any object-storage failure
        // was raised there. What reaches marisa is the artifact's own bytes,
        // so "size_read <= 0" means a truncated/corrupt payload -- permanent.
        // Hence DataFormatBroken for both the IO and the format/size arm.
        // (Master passes FileReadFailed here because its call sites read the
        // index file directly; the refactor's staging step removes that case.)
        ThrowInfo(
            ClassifyMarisaError(error, DataFormatBroken, DataFormatBroken),
            "invalid marisa trie entry: {}",
            error.what());
    }
}

template <typename T>
T
GetMeta(storage::FileSource& source, std::string_view key) {
    auto value = source.GetMeta(key);
    if (!value.has_value()) {
        ThrowInfo(DataFormatBroken, "marisa V3 metadata {} is missing", key);
    }
    try {
        return value->get<T>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid marisa V3 metadata {}: {}",
                  key,
                  error.what());
    }
}

void
ValidateStrIds(const int64_t* str_ids, size_t count, size_t num_keys) {
    AssertInfo(count <= std::numeric_limits<uint32_t>::max(),
               "marisa row count {} exceeds uint32_t CSR capacity",
               count);
    for (size_t row = 0; row < count; ++row) {
        const auto key_id = str_ids[row];
        if (key_id == static_cast<int64_t>(MARISA_NULL_KEY_ID)) {
            continue;
        }
        if (key_id < 0 || static_cast<size_t>(key_id) >= num_keys) {
            ThrowInfo(DataFormatBroken,
                      "invalid marisa key id {} at row {} for {} keys",
                      key_id,
                      row,
                      num_keys);
        }
    }
}

void
BuildCsr(const int64_t* str_ids,
         size_t count,
         size_t num_keys,
         std::shared_ptr<std::vector<uint32_t>>& csr_index,
         std::shared_ptr<std::vector<uint32_t>>& csr_offsets) {
    AssertInfo(num_keys < std::numeric_limits<uint32_t>::max(),
               "marisa key count {} exceeds uint32_t CSR capacity",
               num_keys);
    ValidateStrIds(str_ids, count, num_keys);

    csr_index = std::make_shared<std::vector<uint32_t>>(num_keys + 1, 0);
    for (size_t row = 0; row < count; ++row) {
        const auto key_id = str_ids[row];
        if (key_id != static_cast<int64_t>(MARISA_NULL_KEY_ID)) {
            ++(*csr_index)[static_cast<size_t>(key_id) + 1];
        }
    }
    for (size_t i = 1; i < csr_index->size(); ++i) {
        (*csr_index)[i] += (*csr_index)[i - 1];
    }

    csr_offsets = std::make_shared<std::vector<uint32_t>>(csr_index->back());
    std::vector<uint32_t> write_pos(csr_index->begin(), csr_index->end() - 1);
    for (size_t row = 0; row < count; ++row) {
        const auto key_id = str_ids[row];
        if (key_id != static_cast<int64_t>(MARISA_NULL_KEY_ID)) {
            (*csr_offsets)[write_pos[static_cast<size_t>(key_id)]++] =
                static_cast<uint32_t>(row);
        }
    }
}

void
ValidateCsr(const int64_t* str_ids,
            size_t count,
            size_t num_keys,
            const uint32_t* csr_index,
            size_t csr_index_count,
            const uint32_t* csr_offsets,
            size_t csr_offsets_count) {
    if (num_keys > std::numeric_limits<size_t>::max() / sizeof(uint32_t) - 1 ||
        csr_index_count != num_keys + 1) {
        ThrowInfo(DataFormatBroken,
                  "invalid marisa CSR index length: expected {}, got {}",
                  num_keys + 1,
                  csr_index_count);
    }
    if (csr_index[0] != 0 || csr_index[num_keys] != csr_offsets_count) {
        ThrowInfo(DataFormatBroken,
                  "invalid marisa CSR bounds: first {}, last {}, offsets {}",
                  csr_index[0],
                  csr_index[num_keys],
                  csr_offsets_count);
    }

    size_t valid_rows = 0;
    for (size_t row = 0; row < count; ++row) {
        if (str_ids[row] != static_cast<int64_t>(MARISA_NULL_KEY_ID)) {
            ++valid_rows;
        }
    }
    if (valid_rows != csr_offsets_count) {
        ThrowInfo(DataFormatBroken,
                  "marisa CSR offset count {} does not match {} valid rows",
                  csr_offsets_count,
                  valid_rows);
    }

    for (size_t key_id = 0; key_id < num_keys; ++key_id) {
        const auto begin = csr_index[key_id];
        const auto end = csr_index[key_id + 1];
        if (begin > end || end > csr_offsets_count) {
            ThrowInfo(DataFormatBroken,
                      "invalid marisa CSR range [{}, {}) for key {}",
                      begin,
                      end,
                      key_id);
        }
        uint32_t previous = 0;
        bool first = true;
        for (size_t i = begin; i < end; ++i) {
            const auto row = csr_offsets[i];
            if (row >= count || str_ids[row] != static_cast<int64_t>(key_id)) {
                ThrowInfo(DataFormatBroken,
                          "invalid marisa CSR row {} for key {}",
                          row,
                          key_id);
            }
            if (!first && row <= previous) {
                ThrowInfo(DataFormatBroken,
                          "marisa CSR rows for key {} are not increasing",
                          key_id);
            }
            previous = row;
            first = false;
        }
    }
}

struct CsrPresence {
    bool complete{false};
    size_t index_bytes{0};
    size_t offsets_bytes{0};
};

CsrPresence
ReadCsrPresence(storage::FileSource& source, size_t trie_num_keys) {
    if (source.Gen() == storage::Generation::V1V2) {
        return {};
    }

    const bool has_index = source.HasEntry(MARISA_CSR_INDEX);
    const bool has_offsets = source.HasEntry(MARISA_CSR_OFFSETS);
    const bool has_version = source.GetMeta(kCsrFormatVersionMeta).has_value();
    const bool has_num_keys = source.GetMeta(kCsrNumKeysMeta).has_value();
    if (!(has_index || has_offsets || has_version || has_num_keys)) {
        return {};
    }
    if (!(has_index && has_offsets && has_version && has_num_keys)) {
        ThrowInfo(DataFormatBroken,
                  "incomplete marisa V3 CSR side data: index {}, offsets {}, "
                  "version {}, num_keys {}",
                  has_index,
                  has_offsets,
                  has_version,
                  has_num_keys);
    }

    const auto version = GetMeta<uint32_t>(source, kCsrFormatVersionMeta);
    if (version != kCsrFormatVersion) {
        ThrowInfo(DataFormatBroken,
                  "unsupported marisa CSR format version {}, expected {}",
                  version,
                  kCsrFormatVersion);
    }
    const auto num_keys = GetMeta<uint64_t>(source, kCsrNumKeysMeta);
    if (num_keys != trie_num_keys ||
        num_keys >= std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "invalid marisa CSR key count {}, trie has {}",
                  num_keys,
                  trie_num_keys);
    }
    const auto index_size = source.EntrySize(MARISA_CSR_INDEX);
    const auto offsets_size = source.EntrySize(MARISA_CSR_OFFSETS);
    if (index_size < 0 || offsets_size < 0) {
        ThrowInfo(DataFormatBroken, "marisa CSR entry has a negative size");
    }
    const auto expected_index_bytes =
        (static_cast<size_t>(num_keys) + 1) * sizeof(uint32_t);
    if (static_cast<size_t>(index_size) != expected_index_bytes) {
        ThrowInfo(DataFormatBroken,
                  "invalid marisa CSR index size: expected {}, got {}",
                  expected_index_bytes,
                  index_size);
    }
    if (static_cast<size_t>(offsets_size) % sizeof(uint32_t) != 0 ||
        static_cast<size_t>(offsets_size) / sizeof(uint32_t) >
            std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "invalid marisa CSR offsets size {}",
                  offsets_size);
    }
    return {.complete = true,
            .index_bytes = expected_index_bytes,
            .offsets_bytes = static_cast<size_t>(offsets_size)};
}

void
AddObservedBytes(size_t& total, size_t value) {
    if (value > std::numeric_limits<size_t>::max() - total) {
        ThrowInfo(DataFormatBroken, "marisa artifact byte size overflows");
    }
    total += value;
}

std::shared_ptr<const MarisaIndexStorage>
LoadState(storage::FileSource& source, const storage::LoadOptions& opts) {
    if (ParseNested(opts.params)) {
        ThrowInfo(DataTypeInvalid,
                  "marisa indexes support row-domain strings only");
    }
    // Nullability does not change the physical layout: null rows are always
    // represented by the historical -1 str-id sentinel. Parse the legacy knob
    // here so malformed runtime metadata is rejected rather than ignored.
    (void)GetValueFromConfigOrFallback<bool>(opts.params, "nullable", false);
    const auto value_type = ParseValueType(opts.params);

    if (!source.HasEntry(MARISA_TRIE_INDEX) ||
        !source.HasEntry(MARISA_STR_IDS)) {
        ThrowInfo(DataFormatBroken,
                  "marisa artifact requires {} and {} entries",
                  MARISA_TRIE_INDEX,
                  MARISA_STR_IDS);
    }

    const auto effective = ResolveLoadOptions(opts);
    const auto staging_root =
        effective.enable_mmap ? effective.mmap_dir
                              : std::filesystem::temp_directory_path().string();
    // On every failure, storage (and therefore the trie) is destroyed before
    // this owner removes the directory backing a mapped trie.
    auto staging = storage::LocalDirectory::CreateOwned(
        staging_root, "marisa_XXXXXX", "marisa");
    auto storage = std::make_shared<MarisaIndexStorage>();
    storage->value_type = value_type;

    std::string trie_path;
    std::string str_ids_path;
    if (effective.enable_mmap) {
        auto paths = source.ReadEntriesToLocalDir(
            {MARISA_TRIE_INDEX, MARISA_STR_IDS}, staging->Path());
        if (paths.size() != 2) {
            ThrowInfo(DataFormatBroken,
                      "marisa source returned {} paths for two entries",
                      paths.size());
        }
        if (!staging->Owns(paths[0]) || !staging->Owns(paths[1])) {
            ThrowInfo(DataFormatBroken,
                      "marisa source returned a path outside its staging "
                      "directory");
        }
        trie_path = paths[0];
        str_ids_path = paths[1];
    } else {
        trie_path =
            (std::filesystem::path(staging->Path()) / MARISA_TRIE_INDEX)
                .string();
        source.ReadEntryToLocalFile(MARISA_TRIE_INDEX, trie_path);
    }

    const auto trie_bytes = storage::LocalFileSize(
        trie_path, "failed to determine marisa entry size for");
    if (trie_bytes == 0) {
        ThrowInfo(DataFormatBroken, "marisa trie entry is empty");
    }
    storage->trie = OpenTrie(trie_path, effective.enable_mmap);
    const auto num_keys = storage->trie->num_keys();
    if (num_keys >= std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "marisa trie key count {} exceeds uint32_t CSR capacity",
                  num_keys);
    }
    storage->csr_num_keys = num_keys;
    const auto csr_presence = ReadCsrPresence(source, num_keys);
    size_t observed_bytes = trie_bytes;

    if (!effective.enable_mmap) {
        auto str_ids = ReadEntryVector<int64_t>(
            source, MARISA_STR_IDS, staging->Path());
        ValidateStrIds(str_ids->data(), str_ids->size(), num_keys);

        std::shared_ptr<std::vector<uint32_t>> csr_index;
        std::shared_ptr<std::vector<uint32_t>> csr_offsets;
        if (csr_presence.complete) {
            csr_index = ReadEntryVector<uint32_t>(source,
                                                  MARISA_CSR_INDEX,
                                                  staging->Path(),
                                                  csr_presence.index_bytes);
            csr_offsets = ReadEntryVector<uint32_t>(source,
                                                    MARISA_CSR_OFFSETS,
                                                    staging->Path(),
                                                    csr_presence.offsets_bytes);
            ValidateCsr(str_ids->data(),
                        str_ids->size(),
                        num_keys,
                        csr_index->data(),
                        csr_index->size(),
                        csr_offsets->data(),
                        csr_offsets->size());
        } else {
            BuildCsr(str_ids->data(),
                     str_ids->size(),
                     num_keys,
                     csr_index,
                     csr_offsets);
        }

        storage->str_ids_owner = std::move(str_ids);
        storage->csr_index_owner = std::move(csr_index);
        storage->csr_offsets_owner = std::move(csr_offsets);
        storage->str_ids = storage->str_ids_owner->data();
        storage->str_ids_size = storage->str_ids_owner->size();
        storage->csr_index = storage->csr_index_owner->data();
        storage->csr_offsets = storage->csr_offsets_owner->data();
        return storage;
    }

    const auto str_ids_bytes = storage::LocalFileSize(
        str_ids_path, "failed to determine marisa entry size for");
    if (str_ids_bytes % sizeof(int64_t) != 0) {
        ThrowInfo(DataFormatBroken,
                  "invalid {} size: expected a multiple of {}, got {}",
                  MARISA_STR_IDS,
                  sizeof(int64_t),
                  str_ids_bytes);
    }
    AddObservedBytes(observed_bytes, str_ids_bytes);
    auto str_ids_mapping = MapReadOnly(str_ids_path, str_ids_bytes);
    const auto* str_ids =
        reinterpret_cast<const int64_t*>(str_ids_mapping.Data());
    const auto str_ids_count = str_ids_bytes / sizeof(int64_t);
    ValidateStrIds(str_ids, str_ids_count, num_keys);

    storage::MappedRegionGuard csr_mapping;
    size_t csr_bytes = 0;
    size_t csr_index_bytes = 0;
    std::shared_ptr<std::vector<uint32_t>> csr_index_owner;
    std::shared_ptr<std::vector<uint32_t>> csr_offsets_owner;
    const uint32_t* csr_index = nullptr;
    const uint32_t* csr_offsets = nullptr;

    if (csr_presence.complete) {
        const auto csr_path =
            (std::filesystem::path(staging->Path()) / "marisa.csr")
                .string();
        source.ReadEntriesToLocalFile({MARISA_CSR_INDEX, MARISA_CSR_OFFSETS},
                                      csr_path);
        csr_bytes = storage::LocalFileSize(
            csr_path, "failed to determine marisa entry size for");
        if (csr_presence.index_bytes > std::numeric_limits<size_t>::max() -
                                           csr_presence.offsets_bytes ||
            csr_bytes !=
                csr_presence.index_bytes + csr_presence.offsets_bytes) {
            ThrowInfo(DataFormatBroken,
                      "combined marisa CSR size changed while loading");
        }
        AddObservedBytes(observed_bytes, csr_bytes);
        csr_index_bytes = csr_presence.index_bytes;
        csr_mapping = MapReadOnly(csr_path, csr_bytes);
        csr_index = reinterpret_cast<const uint32_t*>(csr_mapping.Data());
        csr_offsets = reinterpret_cast<const uint32_t*>(csr_mapping.Data() +
                                                        csr_index_bytes);
        ValidateCsr(str_ids,
                    str_ids_count,
                    num_keys,
                    csr_index,
                    num_keys + 1,
                    csr_offsets,
                    (csr_bytes - csr_index_bytes) / sizeof(uint32_t));
    } else {
        BuildCsr(str_ids,
                 str_ids_count,
                 num_keys,
                 csr_index_owner,
                 csr_offsets_owner);
        csr_index = csr_index_owner->data();
        csr_offsets = csr_offsets_owner->data();
        storage->csr_index_owner = csr_index_owner;
        storage->csr_offsets_owner = csr_offsets_owner;
    }

    // The shared owner is installed before either mapping guard is released.
    // A failed allocation therefore leaves both guards and staging intact.
    storage->mmap_owner =
        std::make_shared<MarisaMmapOwner>(str_ids_mapping.Data(),
                                          str_ids_bytes,
                                          csr_mapping.Data(),
                                          csr_bytes,
                                          staging);
    str_ids_mapping.Release();
    csr_mapping.Release();
    storage->str_ids = storage->mmap_owner->StrIds();
    storage->str_ids_size = str_ids_count;
    storage->csr_index =
        csr_presence.complete ? storage->mmap_owner->Csr() : csr_index;
    storage->csr_offsets =
        csr_presence.complete
            ? reinterpret_cast<const uint32_t*>(
                  reinterpret_cast<const char*>(storage->mmap_owner->Csr()) +
                  csr_index_bytes)
            : csr_offsets;
    storage->file_backed_bytes = observed_bytes;
    return storage;
}

}  // namespace

ReaderCaps
MarisaIndexLoader::DeriveCaps(const Config& index_meta) {
    if (ParseNested(index_meta)) {
        ThrowInfo(DataTypeInvalid,
                  "marisa indexes support row-domain strings only");
    }
    return DeriveJsonProjectedCaps(families::kMarisa,
                                   index_meta,
                                   ReaderCaps{.predicate = true,
                                              .pattern_match = true,
                                              .value_lookup = true,
                                              .cheap_value_lookup = true});
}

IIndexReaderBasePtr
MarisaIndexLoader::Open(storage::FileSource& source,
                        const storage::LoadOptions& opts) {
    auto projection = PrepareJsonProjectedOpen(families::kMarisa, source, opts);
    auto storage = LoadState(source, opts);
    auto reader = std::make_unique<MarisaIndexReader>(std::move(storage));
    return FinishJsonProjectedOpen(
        std::move(projection), source, std::move(reader));
}

namespace {

const bool kMarisaLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<MarisaIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
