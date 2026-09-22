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

#include "folly/coro/WithCancellation.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LocalFileIOPool.h"
#include "common/OpContext.h"
#include "index/scalar/marisa/MarisaIndexLoader.h"
#include "index/IndexLoaderFactory.h"
#include "index/PackedIndexLoad.h"
#include "index/LegacyIndexLoad.h"
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

/** @brief Resolved local storage preferences for this load. */
struct EffectiveLoadOptions {
    bool enable_mmap{false};
    std::string mmap_dir;
};

// Resolve explicit load options and legacy config fallbacks for local
// staging.
EffectiveLoadOptions
ResolveLoadOptions(const storage::LoadOptions& opts) {
    EffectiveLoadOptions result;
    result.enable_mmap =
        opts.enable_mmap ||
        GetValueFromConfigOrFallback<bool>(opts.params, ENABLE_MMAP, false) ||
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

// Stage an entry and check size/alignment before allocating the typed vector.
template <typename T>
folly::coro::Task<std::shared_ptr<std::vector<T>>>
ReadEntryVector(bool use_async,
                const storage::LoadOptions& opts,
                storage::FileSource& source,
                std::string_view entry_name,
                const std::string& staging_dir,
                std::optional<size_t> expected_bytes = std::nullopt) {
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    auto run_io = [&]() -> folly::coro::Task<std::shared_ptr<std::vector<T>>> {
        if (!source.HasEntry(entry_name)) {
            ThrowInfo(DataFormatBroken,
                      "marisa artifact entry {} is missing",
                      entry_name);
        }

        auto path =
            (std::filesystem::path(staging_dir) / std::string(entry_name))
                .string();
        LocalEntryGuard local(std::move(path));
        co_await source.ReadEntryToLocalFileAsync(
            entry_name, local.Path(), use_async);

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
        co_return result;
    };
    if (!use_async)
        co_return co_await run_io();
    co_return co_await folly::coro::co_withExecutor(
        storage::ResolveAsyncLoadExecutor(
            storage::LocalFileIOPool::GetInstance().GetExecutor(), priority),
        run_io());
}

// Map a nonempty local file with RAII rollback; zero bytes produce an empty
// guard.
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

// Open an already-staged trie in heap or mmap mode and translate MARISA
// failures.
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
        // Preserve allocation failures separately. Other MARISA failures use
        // the staged-payload classification supplied to ClassifyMarisaError.
        ThrowInfo(
            ClassifyMarisaError(error, DataFormatBroken, DataFormatBroken),
            "invalid marisa trie entry: {}",
            error.what());
    }
}

// Check the uint32 row domain and every key ID, allowing the historical null
// sentinel.
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

// Reconstruct key-to-row CSR from legacy row-to-key IDs, excluding null rows.
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

// Cross-check CSR ranges, ordered row IDs and coverage against the row-to-key
// mapping.
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

/** @brief Presence and byte sizes of the packed key-to-row CSR sidecars. */
struct CsrPresence {
    bool complete{false};
    size_t index_bytes{0};
    size_t offsets_bytes{0};
};

// Accumulate retained file bytes without overflowing the reader resource
// estimate.
void
AddObservedBytes(size_t& total, size_t value) {
    if (value > std::numeric_limits<size_t>::max() - total) {
        ThrowInfo(DataFormatBroken, "marisa artifact byte size overflows");
    }
    total += value;
}

// Load a legacy trie and row IDs; rebuild CSR and retain staging only for
// mmap readers.
folly::coro::Task<std::shared_ptr<const MarisaIndexStorage>>
LoadState(bool use_async,
          storage::FileSource& source,
          const storage::LoadOptions& opts) {
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
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
    // On every failure, storage (and therefore the trie) is destroyed before
    // this owner removes the directory backing a mapped trie.
    std::shared_ptr<storage::LocalDirectory> staging;
    {
        auto local_io = [&] {
            const auto staging_root =
                effective.enable_mmap
                    ? effective.mmap_dir
                    : std::filesystem::temp_directory_path().string();

            staging = storage::LocalDirectory::CreateOwned(
                staging_root, "marisa_XXXXXX", "marisa");
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(local_io, priority);
        } else {
            local_io();
        }
    }
    auto storage = std::make_shared<MarisaIndexStorage>();
    storage->value_type = value_type;

    std::string trie_path;
    std::string str_ids_path;
    if (effective.enable_mmap) {
        const std::vector<std::string> entries{MARISA_TRIE_INDEX,
                                               MARISA_STR_IDS};
        auto paths = co_await source.ReadEntriesToLocalDirAsync(
            entries, staging->Path(), use_async);
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
        trie_path = (std::filesystem::path(staging->Path()) / MARISA_TRIE_INDEX)
                        .string();
        co_await source.ReadEntryToLocalFileAsync(
            MARISA_TRIE_INDEX, trie_path, use_async);
    }

    size_t trie_bytes = 0;
    {
        auto local_io = [&] {
            trie_bytes = storage::LocalFileSize(
                trie_path, "failed to determine marisa entry size for");
            if (trie_bytes == 0) {
                ThrowInfo(DataFormatBroken, "marisa trie entry is empty");
            }
            storage->trie = OpenTrie(trie_path, effective.enable_mmap);
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(local_io, priority);
        } else {
            local_io();
        }
    }
    const auto num_keys = storage->trie->num_keys();
    if (num_keys >= std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "marisa trie key count {} exceeds uint32_t CSR capacity",
                  num_keys);
    }
    storage->csr_num_keys = num_keys;
    size_t observed_bytes = trie_bytes;

    if (!effective.enable_mmap) {
        auto str_ids = (co_await ReadEntryVector<int64_t>(
            use_async, opts, source, MARISA_STR_IDS, staging->Path()));
        ValidateStrIds(str_ids->data(), str_ids->size(), num_keys);

        std::shared_ptr<std::vector<uint32_t>> csr_index;
        std::shared_ptr<std::vector<uint32_t>> csr_offsets;

        BuildCsr(
            str_ids->data(), str_ids->size(), num_keys, csr_index, csr_offsets);

        storage->str_ids_owner = std::move(str_ids);
        storage->csr_index_owner = std::move(csr_index);
        storage->csr_offsets_owner = std::move(csr_offsets);
        storage->str_ids = storage->str_ids_owner->data();
        storage->str_ids_size = storage->str_ids_owner->size();
        storage->csr_index = storage->csr_index_owner->data();
        storage->csr_offsets = storage->csr_offsets_owner->data();
        co_return storage;
    }

    size_t str_ids_bytes = 0;
    storage::MappedRegionGuard str_ids_mapping;
    {
        auto local_io = [&] {
            str_ids_bytes = storage::LocalFileSize(
                str_ids_path, "failed to determine marisa entry size for");
            if (str_ids_bytes % sizeof(int64_t) != 0) {
                ThrowInfo(DataFormatBroken,
                          "invalid {} size: expected a multiple of {}, got {}",
                          MARISA_STR_IDS,
                          sizeof(int64_t),
                          str_ids_bytes);
            }
            AddObservedBytes(observed_bytes, str_ids_bytes);
            str_ids_mapping = MapReadOnly(str_ids_path, str_ids_bytes);
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(local_io, priority);
        } else {
            local_io();
        }
    }
    const auto* str_ids =
        reinterpret_cast<const int64_t*>(str_ids_mapping.Data());
    const auto str_ids_count = str_ids_bytes / sizeof(int64_t);
    ValidateStrIds(str_ids, str_ids_count, num_keys);

    std::shared_ptr<std::vector<uint32_t>> csr_index_owner;
    std::shared_ptr<std::vector<uint32_t>> csr_offsets_owner;
    const uint32_t* csr_index = nullptr;
    const uint32_t* csr_offsets = nullptr;

    BuildCsr(
        str_ids, str_ids_count, num_keys, csr_index_owner, csr_offsets_owner);
    csr_index = csr_index_owner->data();
    csr_offsets = csr_offsets_owner->data();
    storage->csr_index_owner = csr_index_owner;
    storage->csr_offsets_owner = csr_offsets_owner;

    // Install the shared owner before releasing the row-id mapping guard;
    // allocation failure leaves the mapping and staging intact.
    storage->mmap_owner = std::make_shared<MarisaMmapOwner>(
        str_ids_mapping.Data(), str_ids_bytes, nullptr, 0, staging);
    str_ids_mapping.Release();
    storage->str_ids = storage->mmap_owner->StrIds();
    storage->str_ids_size = str_ids_count;
    storage->csr_index = csr_index;
    storage->csr_offsets = csr_offsets;
    storage->file_backed_bytes = observed_bytes;
    co_return storage;
}

/**
 * @brief Per-load payload owners and decoding state, retained by IndexLoadPlan.
 */
struct PackedMarisaState {
    std::shared_ptr<storage::LocalDirectory> directory;
    EffectiveLoadOptions effective;
    DataType value_type{DataType::VARCHAR};
    JsonProjectedOpenPlan projection;
    CsrPresence csr;
    size_t num_keys{0};
    size_t trie_bytes{0};
    size_t str_ids_bytes{0};
    size_t csr_offsets_file_offset{0};
    size_t csr_file_bytes{0};
    std::string trie_path;
    std::string str_ids_path;
    std::string csr_path;
    std::shared_ptr<std::vector<int64_t>> str_ids;
    std::shared_ptr<std::vector<uint32_t>> csr_index;
    std::shared_ptr<std::vector<uint32_t>> csr_offsets;
};

// Read nonnegative persisted integer metadata without accepting signed
// wraparound.
uint64_t
ReadPackedMarisaInteger(const nlohmann::json& metadata, std::string_view key) {
    const auto found = metadata.find(key);
    if (found == metadata.end() ||
        (!found->is_number_unsigned() &&
         (!found->is_number_integer() || found->get<int64_t>() < 0))) {
        ThrowInfo(
            DataFormatBroken, "invalid marisa V3 integer metadata {}", key);
    }
    return found->get<uint64_t>();
}

// Allocate each final heap vector once; the async reader writes directly to it.
template <typename T>
std::shared_ptr<std::vector<T>>
PlanMarisaVector(IndexLoadPlan& plan, const std::string& name, size_t bytes) {
    AssertInfo(
        bytes % sizeof(T) == 0, "unaligned marisa vector target {}", name);
    auto target = std::make_shared<std::vector<T>>(bytes / sizeof(T));
    plan.entries.push_back(
        {name,
         storage::MemoryEntryTarget{
             target, reinterpret_cast<uint8_t*>(target->data()), bytes}});
    return target;
}

}  // namespace

folly::coro::Task<std::unique_ptr<IndexLoader>>
MarisaIndexLoader::Open(IndexOpenRequest request) {
    return OpenIndexLoader(
        std::move(request),
        [](OpenedIndexInput input, storage::LoadOptions options)
            -> folly::coro::Task<std::unique_ptr<IndexLoader>> {
            auto loader = std::unique_ptr<MarisaIndexLoader>(
                new MarisaIndexLoader(std::move(input), std::move(options)));
            (void)DeriveCaps(loader->options_.params);
            co_return loader;
        });
}

folly::coro::Task<IIndexReaderBasePtr>
MarisaIndexLoader::Load(milvus::OpContext* context) {
    if (auto* packed = std::get_if<PackedIndexSource>(&input_)) {
        return RunPackedIndexLoad(
            *packed, options_, &PlanPacked, &FinishPacked, context);
    }
    return RunLegacyLoad(
        std::get<LegacyIndexSource>(input_), options_, &LoadLegacy, context);
}

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

folly::coro::Task<IIndexReaderBasePtr>
MarisaIndexLoader::LoadLegacy(storage::FileSource& source,
                              const storage::LoadOptions& opts,
                              bool use_async) {
    auto projection = PrepareJsonProjectedOpen(families::kMarisa, source, opts);
    auto storage = (co_await LoadState(use_async, source, opts));
    auto reader = std::make_unique<MarisaIndexReader>(std::move(storage));
    co_return (co_await FinishJsonProjectedOpenAsync(
        use_async, std::move(projection), source, std::move(reader)));
}

IndexLoadPlan
MarisaIndexLoader::PlanPacked(const storage::IndexEntryDirectory& directory,
                              const nlohmann::json& metadata,
                              const storage::LoadOptions& opts) {
    if (ParseNested(opts.params)) {
        ThrowInfo(DataTypeInvalid,
                  "marisa indexes support row-domain strings only");
    }
    (void)GetValueFromConfigOrFallback<bool>(opts.params, "nullable", false);
    auto state = std::make_shared<PackedMarisaState>();
    IndexLoadPlan plan;
    plan.load_context = state;
    state->value_type = ParseValueType(opts.params);
    state->effective = ResolveLoadOptions(opts);
    if (!directory.HasEntry(MARISA_TRIE_INDEX) ||
        !directory.HasEntry(MARISA_STR_IDS)) {
        ThrowInfo(DataFormatBroken,
                  "marisa artifact requires trie and str-id entries");
    }
    state->trie_bytes = directory.At(MARISA_TRIE_INDEX).plaintext_size;
    state->str_ids_bytes = directory.At(MARISA_STR_IDS).plaintext_size;
    if (state->trie_bytes == 0 || state->str_ids_bytes % sizeof(int64_t) != 0 ||
        state->str_ids_bytes / sizeof(int64_t) >
            std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataFormatBroken, "invalid marisa trie or str-id size");
    }
    const bool has_index = directory.HasEntry(MARISA_CSR_INDEX);
    const bool has_offsets = directory.HasEntry(MARISA_CSR_OFFSETS);
    const bool has_version = metadata.contains(kCsrFormatVersionMeta);
    const bool has_keys = metadata.contains(kCsrNumKeysMeta);
    if (has_index || has_offsets || has_version || has_keys) {
        if (!(has_index && has_offsets && has_version && has_keys)) {
            ThrowInfo(DataFormatBroken, "incomplete marisa V3 CSR side data");
        }
        const auto version =
            ReadPackedMarisaInteger(metadata, kCsrFormatVersionMeta);
        const auto keys = ReadPackedMarisaInteger(metadata, kCsrNumKeysMeta);
        if (version != kCsrFormatVersion) {
            ThrowInfo(
                Unsupported, "unsupported marisa CSR version {}", version);
        }
        if (keys >= std::numeric_limits<uint32_t>::max() ||
            keys > state->str_ids_bytes / sizeof(int64_t) ||
            keys > std::numeric_limits<size_t>::max() / sizeof(uint32_t) - 1) {
            ThrowInfo(DataFormatBroken, "invalid marisa CSR key count");
        }
        state->num_keys = static_cast<size_t>(keys);
        state->csr = {
            .complete = true,
            .index_bytes = directory.At(MARISA_CSR_INDEX).plaintext_size,
            .offsets_bytes = directory.At(MARISA_CSR_OFFSETS).plaintext_size};
        if (state->csr.index_bytes !=
                (state->num_keys + 1) * sizeof(uint32_t) ||
            state->csr.offsets_bytes % sizeof(uint32_t) != 0 ||
            state->csr.offsets_bytes / sizeof(uint32_t) >
                state->str_ids_bytes / sizeof(int64_t) ||
            state->csr.index_bytes >
                std::numeric_limits<size_t>::max() - state->csr.offsets_bytes) {
            ThrowInfo(DataFormatBroken, "invalid marisa CSR entry sizes");
        }
    }
    state->projection =
        PreparePackedJsonProjectedOpen(kFamily,
                                       directory,
                                       metadata,
                                       opts,
                                       plan,
                                       state->str_ids_bytes / sizeof(int64_t));
    const auto root = state->effective.enable_mmap
                          ? state->effective.mmap_dir
                          : std::filesystem::temp_directory_path().string();
    state->directory =
        storage::LocalDirectory::CreateOwned(root, "marisa_XXXXXX", "marisa");
    state->trie_path = state->directory->Path() + "/" + MARISA_TRIE_INDEX;
    auto trie_file = std::make_shared<storage::IndexFileTarget>(
        state->trie_path, state->trie_bytes, state->effective.enable_mmap);
    plan.entries.reserve(plan.entries.size() + (state->csr.complete ? 4 : 2));
    plan.entries.push_back(
        {MARISA_TRIE_INDEX,
         storage::FileEntryTarget{trie_file, 0, state->trie_bytes}});
    if (state->effective.enable_mmap) {
        state->str_ids_path = state->directory->Path() + "/" + MARISA_STR_IDS;
        auto ids_file = std::make_shared<storage::IndexFileTarget>(
            state->str_ids_path, state->str_ids_bytes, true);
        plan.entries.push_back(
            {MARISA_STR_IDS,
             storage::FileEntryTarget{ids_file, 0, state->str_ids_bytes}});
        if (state->csr.complete) {
            state->csr_path = state->directory->Path() + "/marisa.csr";
            const auto mask = storage::FileWriter::ALIGNMENT_MASK;
            if (state->csr.index_bytes >
                std::numeric_limits<size_t>::max() - mask) {
                ThrowInfo(DataFormatBroken, "marisa CSR alignment overflow");
            }
            state->csr_offsets_file_offset =
                (state->csr.index_bytes + mask) & ~mask;
            if (state->csr.offsets_bytes > std::numeric_limits<size_t>::max() -
                                               state->csr_offsets_file_offset) {
                ThrowInfo(DataFormatBroken, "marisa padded CSR size overflow");
            }
            state->csr_file_bytes =
                state->csr_offsets_file_offset + state->csr.offsets_bytes;
            auto csr_file = std::make_shared<storage::IndexFileTarget>(
                state->csr_path, state->csr_file_bytes, true);
            plan.entries.push_back(
                {MARISA_CSR_INDEX,
                 storage::FileEntryTarget{
                     csr_file, 0, state->csr_offsets_file_offset}});
            plan.entries.push_back(
                {MARISA_CSR_OFFSETS,
                 storage::FileEntryTarget{csr_file,
                                          state->csr_offsets_file_offset,
                                          state->csr.offsets_bytes}});
        }
    } else {
        state->str_ids = PlanMarisaVector<int64_t>(
            plan, MARISA_STR_IDS, state->str_ids_bytes);
        if (state->csr.complete) {
            state->csr_index = PlanMarisaVector<uint32_t>(
                plan, MARISA_CSR_INDEX, state->csr.index_bytes);
            state->csr_offsets = PlanMarisaVector<uint32_t>(
                plan, MARISA_CSR_OFFSETS, state->csr.offsets_bytes);
        }
    }
    return plan;
}

folly::coro::Task<IIndexReaderBasePtr>
MarisaIndexLoader::FinishPacked(IndexLoadPlan& plan,
                                const storage::LoadOptions& opts,
                                bool use_async) {
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    const auto& state =
        std::any_cast<const std::shared_ptr<PackedMarisaState>&>(
            plan.load_context);
    AssertInfo(state != nullptr, "marisa packed load context is null");
    // Context retains the directory until the trie and both mapping guards
    // have either transferred to the reader or unwound on failure.
    auto result = std::make_shared<MarisaIndexStorage>();
    result->value_type = state->value_type;
    storage::MappedRegionGuard ids_mapping;
    storage::MappedRegionGuard csr_mapping;
    auto local_io = [&] {
        result->trie = OpenTrie(state->trie_path, state->effective.enable_mmap);
        if (state->effective.enable_mmap) {
            ids_mapping =
                MapReadOnly(state->str_ids_path, state->str_ids_bytes);
            if (state->csr.complete)
                csr_mapping =
                    MapReadOnly(state->csr_path, state->csr_file_bytes);
        }
    };
    if (use_async) {
        co_await storage::RunLocalFileIOAsync(local_io, priority);
    } else {
        local_io();
    }

    const auto keys = result->trie->num_keys();
    if (keys >= std::numeric_limits<uint32_t>::max() ||
        (state->csr.complete && keys != state->num_keys)) {
        ThrowInfo(DataFormatBroken, "marisa CSR key count disagrees with trie");
    }
    result->csr_num_keys = keys;
    const auto rows = state->str_ids_bytes / sizeof(int64_t);
    if (state->effective.enable_mmap) {
        result->str_ids = reinterpret_cast<const int64_t*>(ids_mapping.Data());
    } else {
        result->str_ids_owner = state->str_ids;
        result->str_ids = result->str_ids_owner->data();
    }
    result->str_ids_size = rows;
    ValidateStrIds(result->str_ids, rows, keys);
    size_t csr_bytes = 0;
    if (state->csr.complete) {
        csr_bytes = state->csr.index_bytes + state->csr.offsets_bytes;
        if (state->effective.enable_mmap) {
            csr_bytes = state->csr_file_bytes;
            result->csr_index =
                reinterpret_cast<const uint32_t*>(csr_mapping.Data());
            result->csr_offsets = reinterpret_cast<const uint32_t*>(
                csr_mapping.Data() + state->csr_offsets_file_offset);
        } else {
            result->csr_index_owner = state->csr_index;
            result->csr_offsets_owner = state->csr_offsets;
            result->csr_index = result->csr_index_owner->data();
            result->csr_offsets = result->csr_offsets_owner->data();
        }
        ValidateCsr(result->str_ids,
                    rows,
                    keys,
                    result->csr_index,
                    keys + 1,
                    result->csr_offsets,
                    state->csr.offsets_bytes / sizeof(uint32_t));
    } else {
        std::shared_ptr<std::vector<uint32_t>> csr_index;
        std::shared_ptr<std::vector<uint32_t>> csr_offsets;
        BuildCsr(result->str_ids, rows, keys, csr_index, csr_offsets);
        result->csr_index_owner = std::move(csr_index);
        result->csr_offsets_owner = std::move(csr_offsets);
        result->csr_index = result->csr_index_owner->data();
        result->csr_offsets = result->csr_offsets_owner->data();
    }
    if (state->effective.enable_mmap) {
        result->mmap_owner =
            std::make_shared<MarisaMmapOwner>(ids_mapping.Data(),
                                              state->str_ids_bytes,
                                              csr_mapping.Data(),
                                              csr_bytes,
                                              state->directory);
        ids_mapping.Release();
        csr_mapping.Release();
        result->file_backed_bytes = state->trie_bytes;
        AddObservedBytes(result->file_backed_bytes, state->str_ids_bytes);
        AddObservedBytes(result->file_backed_bytes, csr_bytes);
    }
    co_return FinishPackedJsonProjectedOpen(
        std::move(state->projection),
        std::make_unique<MarisaIndexReader>(std::move(result)));
}

namespace {

const bool kMarisaLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<MarisaIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
