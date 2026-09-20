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

#include "index/LoadResource.h"

#include <algorithm>
#include "yaml-cpp/yaml.h"
#include "storage/LocalFileIOPool.h"
#include "index/vector/VectorDiskLoader.h"
#include "index/vector/KnowhereEngine.h"
#include <cstring>
#include <limits>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/ResourceUsageUtils.h"
#include "index/scalar/spatial/RTreeIndexReader.h"
#include "index/scalar/spatial/RTreeSerialization.h"
#include <boost/geometry/index/detail/rtree/utilities/view.hpp>
#include "index/Utils.h"
#include "index/vector/VectorLoadResource.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "storage/EntryStreamUtils.h"
#include "storage/LoadAdmissionController.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/PluginLoader.h"

#include "folly/coro/BlockingWait.h"
#include "folly/coro/WithCancellation.h"
#include "index/IndexLoadUtils.h"
#include "index/PackedIndexLoad.h"
#include "index/IndexTypeAdapter.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/AsyncIndexEntryReader.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LoadOverheadController.h"
#include "storage/FileWriter.h"
#include "roaring/roaring.hh"

namespace milvus::index {
namespace {
using detail::SaturatingAdd;
using detail::SaturatingMul;

uint64_t
RTreeHeapBytes(int64_t num_rows) {
    using View = boost::geometry::index::detail::rtree::utilities::view<
        rtree_detail::RTree>;
    using Node = typename View::members_holder::node;
    const auto min_elements =
        rtree_detail::RTree::parameters_type{}.get_min_elements();
    // Boost's archive reader enforces minimum occupancy on non-root nodes.
    // Bound each level from the maximum leaf-value count, plus the root.
    uint64_t nodes = 1;
    for (uint64_t level =
             static_cast<uint64_t>(std::max<int64_t>(0, num_rows)) /
             min_elements;
         level != 0;
         level /= min_elements) {
        nodes = SaturatingAdd(nodes, level);
    }
    const auto objects =
        sizeof(RTreeQueryEngine) + sizeof(RTreeIndexState) +
        sizeof(std::vector<size_t>) +
        sizeof(std::vector<std::shared_ptr<const RTreeQueryEngine>>) +
        sizeof(std::shared_ptr<const RTreeQueryEngine>);
    return SaturatingAdd(objects, SaturatingMul(nodes, sizeof(Node)));
}
// Bounds for the synchronous encrypted entry-stream implementation.
struct EntryStreamLoadInfo {
    bool encrypted{false};
    uint64_t total_transient_bytes{0};
    uint64_t max_task_transient_bytes{0};
};

uint64_t
ScalarIndexStreamMemoryOverhead(
    uint64_t index_size_in_bytes,
    int32_t scalar_version,
    bool encrypted,
    bool file_stream,
    const std::optional<EntryStreamLoadInfo>& stream_load_info = std::nullopt) {
    if (index_size_in_bytes == 0) {
        return 0;
    }
    if (scalar_version < 3) {
        return index_size_in_bytes;
    }
    size_t total_transient_bytes;
    size_t max_task_transient_bytes;
    if (encrypted && stream_load_info.has_value()) {
        total_transient_bytes = stream_load_info->total_transient_bytes;
        max_task_transient_bytes = stream_load_info->max_task_transient_bytes;
    } else {
        total_transient_bytes = milvus::storage::EntryStreamTransientBytes(
            index_size_in_bytes, encrypted);
        max_task_transient_bytes = milvus::storage::EntryStreamTransientBytes(
            milvus::storage::MaxEntryStreamTaskBytes(), encrypted);

        // Without a concrete encrypted directory there is no trustworthy
        // ciphertext task bound. When the runtime budget is disabled, keep
        // the conservative whole-stream fallback instead of applying the
        // executor bound.
        if (encrypted && milvus::storage::LoadAdmissionController::GetInstance()
                                 .CapacityBytes() == 0) {
            return total_transient_bytes;
        }
    }

    if (file_stream && !encrypted) {
        total_transient_bytes = milvus::SaturatingMultiply(
            total_transient_bytes,
            milvus::storage::kFileStreamBufferMultiplier);
        max_task_transient_bytes = milvus::SaturatingMultiply(
            max_task_transient_bytes,
            milvus::storage::kFileStreamBufferMultiplier);
    }

    // File-aware estimates must remain valid when a later reload observes a
    // larger transient budget or executor. Grouped loads are capped by the
    // current Group policy; request-local loads reserve this stable full sum.
    if (stream_load_info.has_value()) {
        return total_transient_bytes;
    }

    return milvus::storage::EntryStreamMaxTransientBytes(
        total_transient_bytes, max_task_transient_bytes);
}

uint64_t
BitsetBytes(int64_t num_rows) {
    if (num_rows <= 0) {
        return 0;
    }
    return (static_cast<uint64_t>(num_rows) + 7) / 8;
}

uint64_t
AlignUp(uint64_t size, uint64_t alignment) {
    if (alignment == 0 || size == 0) {
        return size;
    }
    if (size > std::numeric_limits<uint64_t>::max() - (alignment - 1)) {
        return std::numeric_limits<uint64_t>::max();
    }
    return ((size + alignment - 1) / alignment) * alignment;
}

uint64_t
ValidityBitmapBytes(int64_t num_rows) {
    return AlignUp(BitsetBytes(num_rows), sizeof(uint64_t));
}

uint64_t
BitmapMmapFrozenBufferBytes(int64_t num_rows, uint64_t index_size_in_bytes) {
    constexpr uint64_t kBitmapFrozenAlignment = 32;
    constexpr uint64_t kValuesPerContainer = uint64_t{1} << 16;
    constexpr uint64_t kMaxValues = uint64_t{1} << 32;
    // CRoaring 3.0: array/bitset payloads need at most two bytes per row;
    // even unoptimized run containers have at most ceil(rows / 2) four-byte
    // runs. Use the full uint32 domain when the caller has no row count.
    const auto rows =
        num_rows > 0 ? std::min(static_cast<uint64_t>(num_rows), kMaxValues)
                     : kMaxValues;
    const auto containers =
        (rows + kValuesPerContainer - 1) / kValuesPerContainer;
    const auto payload_bytes =
        std::min(index_size_in_bytes, 2 * AlignUp(rows, 2));
    // Per-container pointers, keys, typecodes, headers and small run-container
    // capacity slack fit in 64 bytes; frozen metadata is smaller still.
    constexpr uint64_t kContainerOverhead = 64;
    const auto posting_bytes =
        AlignUp(SaturatingAdd(
                    payload_bytes,
                    sizeof(roaring::Roaring) + containers * kContainerOverhead),
                kBitmapFrozenAlignment);
    // Decoded Roaring and frozen output coexist. Growing the reusable output
    // can briefly retain its old allocation too; each output allocation holds
    // at most one batch prefix plus the largest bitmap.
    return SaturatingAdd(SaturatingMultiply(posting_bytes, uint64_t{3}),
                         uint64_t{2 * kBitmapFrozenBatchBytes});
}

uint64_t
SortLegacyAuxBytes(int64_t num_rows) {
    if (num_rows <= 0) {
        return 0;
    }
    auto rows = static_cast<uint64_t>(num_rows);
    if (rows > (std::numeric_limits<uint64_t>::max() - BitsetBytes(num_rows)) /
                   sizeof(int32_t)) {
        return std::numeric_limits<uint64_t>::max();
    }
    return rows * sizeof(int32_t) + BitsetBytes(num_rows);
}

uint64_t
MarisaLegacyCsrBytes(int64_t num_rows, uint64_t arrays_per_row) {
    if (num_rows <= 0) {
        return 0;
    }

    auto rows = static_cast<uint64_t>(num_rows);
    auto max_rows =
        (std::numeric_limits<uint64_t>::max() / sizeof(uint32_t) - 1) /
        arrays_per_row;
    if (rows > max_rows) {
        return std::numeric_limits<uint64_t>::max();
    }

    // Fallback CSR has csr_offsets_ <= num_rows and csr_index_ <= num_rows + 1.
    // During rebuild, fill_offsets also holds a temporary write_pos copy.
    return (arrays_per_row * rows + 1) * sizeof(uint32_t);
}

IndexType
HybridInternalIndexTypeToIndexType(ScalarIndexType type) {
    switch (type) {
        case ScalarIndexType::BITMAP:
            return BITMAP_INDEX_TYPE;
        case ScalarIndexType::STLSORT:
            return ASCENDING_SORT;
        case ScalarIndexType::MARISA:
            return MARISA_TRIE;
        case ScalarIndexType::INVERTED:
            return INVERTED_INDEX_TYPE;
        default:
            return "";
    }
}

// Inspect the legacy standalone type file. An unavailable or invalid type
// preserves the historical fallback to the generic HYBRID estimate.
IndexType
ResolveLegacyHybridIndexType(const std::vector<std::string>& index_files,
                             const storage::FileManagerContext& context) {
    if (!context.Valid()) {
        return {};
    }
    const auto file =
        std::find_if(index_files.begin(), index_files.end(), [](const auto& f) {
            return std::string_view(f).substr(f.find_last_of('/') + 1) ==
                   INDEX_TYPE;
        });
    if (file == index_files.end()) {
        return {};
    }
    try {
        storage::MemFileManagerImpl manager(context);
        auto data = manager.LoadIndexToMemory(
            {*file}, proto::common::LoadPriority::HIGH);
        BinarySet binary_set;
        AssembleIndexDatas(data, binary_set);
        const auto type = binary_set.GetByName(INDEX_TYPE);
        if (type == nullptr || type->data == nullptr ||
            type->size != sizeof(uint8_t)) {
            LOG_WARN(
                "invalid legacy hybrid index_type file {}, fallback to hybrid "
                "estimate",
                *file);
            return {};
        }
        return HybridInternalIndexTypeToIndexType(
            static_cast<ScalarIndexType>(type->data[0]));
    } catch (const std::exception& e) {
        LOG_WARN(
            "failed to resolve legacy hybrid internal index type from {}, "
            "fallback to hybrid estimate: {}",
            *file,
            e.what());
        return {};
    }
}

LoadResourceRequest
ScalarIndexLoadResourceWithOverhead(
    DataType field_type,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    uint64_t stream_memory_overhead) {
    const auto& index_type = index_params.at("index_type");
    LoadResourceRequest request{};
    request.has_raw_data = false;

    if (index_type == milvus::index::ASCENDING_SORT) {
        // Old V3 sort files do not have idx_to_offsets and valid_bitset
        // entries, so LoadEntries rebuilds them into heap memory.
        auto legacy_aux_bytes = SortLegacyAuxBytes(num_rows);
        if (mmap_enable) {
            // V3 streaming: chunks streamed to disk + mmap. The index data is
            // not heap-resident, but legacy metadata may be heap-resident.
            auto resident_bytes = legacy_aux_bytes;
            request.final_memory_cost = resident_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(resident_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        } else {
            // V3 streaming: pre-allocate target, stream into it
            request.final_memory_cost =
                SaturatingAdd(index_size_in_bytes, legacy_aux_bytes);
            request.final_disk_cost = 0;
            request.max_memory_cost = SaturatingAdd(request.final_memory_cost,
                                                    stream_memory_overhead);
            request.max_disk_cost = 0;
        }
        request.has_raw_data = true;
    } else if (index_type == milvus::index::MARISA_TRIE ||
               index_type == milvus::index::MARISA_TRIE_UPPER) {
        if (mmap_enable) {
            // V3 streaming: trie, str_ids, and persisted CSR are mmap'd.
            // Old V3 files do not have CSR entries, so LoadEntries rebuilds
            // CSR into heap vectors. Estimate a conservative legacy upper
            // bound because resource estimation cannot inspect entries here.
            auto legacy_csr_resident_bytes = MarisaLegacyCsrBytes(num_rows, 2);
            auto legacy_csr_peak_bytes = MarisaLegacyCsrBytes(num_rows, 3);
            request.final_memory_cost = legacy_csr_resident_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(legacy_csr_peak_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        } else {
            // V3 streaming: trie via temp file + read, str_ids pre-allocated
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = 0;
            request.max_memory_cost =
                SaturatingAdd(index_size_in_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;  // trie temp file
        }
        request.has_raw_data = true;
    } else if (index_type == milvus::index::INVERTED_INDEX_TYPE ||
               index_type == milvus::index::NGRAM_INDEX_TYPE) {
        // Sealed nullable Tantivy indexes materialize an immutable validity
        // bitmap on the heap. The estimator cannot know whether a sidecar
        // contains nulls, so reserve the conservative full bitmap.
        auto validity_bitmap_bytes = ValidityBitmapBytes(num_rows);
        if (mmap_enable) {
            request.final_memory_cost = validity_bitmap_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost = milvus::SaturatingAdd(
                stream_memory_overhead, validity_bitmap_bytes);
        } else {
            const auto resident_bytes = milvus::SaturatingAdd(
                index_size_in_bytes, validity_bitmap_bytes);
            request.final_memory_cost = resident_bytes;
            request.final_disk_cost = 0;
            request.max_memory_cost =
                std::max(resident_bytes, stream_memory_overhead);
        }
        request.max_disk_cost = index_size_in_bytes;
        request.has_raw_data = false;
    } else if (index_type == milvus::index::RTREE_INDEX_TYPE) {
        request.final_memory_cost = SaturatingAdd(
            RTreeHeapBytes(num_rows),
            SaturatingMul(static_cast<uint64_t>(std::max<int64_t>(0, num_rows)),
                          sizeof(size_t)));
        request.final_disk_cost = 0;
        request.max_memory_cost = SaturatingAdd(
            request.final_memory_cost,
            SaturatingAdd(stream_memory_overhead,
                          sizeof(rtree_serialization_detail::FileReadBuffer)));
        request.max_disk_cost = index_size_in_bytes;
        request.has_raw_data = false;
    } else if (index_type == milvus::index::FMINDEX_INDEX_TYPE) {
        // FM-index is a single flat blob. A LoadView (mmap) load views every
        // serialized array in place — wavelet words, sampled bitvector,
        // sampled-SA values, doc boundaries; nothing is heap-copied. What the
        // load DOES rebuild on the heap is the rank directories plus small
        // derived vectors and the wrapper's null bitmap. At the default
        // fm_block_bytes=64 the wavelet rank directory is roughly 1/8 of its
        // packed words, so index_size can substantially overstate steady heap
        // usage, especially when per-row document boundaries dominate the
        // blob. Keep index_size as the deliberately conservative pre-load
        // admission/peak bound; after LoadView succeeds FMIndex replaces the
        // cache cell's memory_bytes with its measured resident heap while
        // retaining file_bytes.
        if (mmap_enable) {
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(index_size_in_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        } else {
            // Deserialize MOVES the reader entry buffer into owned_blob_ (no
            // intermediate copies), so the resident set is the owned blob (~1x)
            // plus the rebuilt rank directories (~1x blob): both steady and
            // peak are ~2x. (Without the move overload the peak was ~4x from
            // three simultaneous full copies.)
            request.final_memory_cost =
                SaturatingMultiply(index_size_in_bytes, uint64_t{2});
            request.final_disk_cost = 0;
            request.max_memory_cost =
                SaturatingMultiply(index_size_in_bytes, uint64_t{2});
            request.max_disk_cost = 0;
        }
        request.has_raw_data = false;
    } else if (index_type == milvus::index::BITMAP_INDEX_TYPE) {
        if (mmap_enable) {
            // V3 streaming: stream to temp file (mmap'd), then MMapIndexData
            // converts one bitmap at a time to frozen format. The conversion
            // still allocates a per-bitmap heap buffer, so reserve for the
            // largest plausible bitmap in addition to stream buffers.
            auto resident_bytes = BitsetBytes(num_rows);
            auto frozen_buffer_bytes =
                BitmapMmapFrozenBufferBytes(num_rows, index_size_in_bytes);
            request.final_memory_cost = resident_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost = SaturatingAdd(
                SaturatingAdd(resident_bytes, stream_memory_overhead),
                frozen_buffer_bytes);
            request.max_disk_cost = SaturatingMultiply(
                index_size_in_bytes, uint64_t{2});  // temp + final
        } else {
            // V3 streaming: pre-allocate buffer + deserialize
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = 0;
            request.max_memory_cost = std::max(
                SaturatingMultiply(index_size_in_bytes, uint64_t{2}),
                SaturatingAdd(index_size_in_bytes, stream_memory_overhead));
            request.max_disk_cost = 0;
        }

        request.has_raw_data = false;
    } else if (index_type == milvus::index::HYBRID_INDEX_TYPE) {
        request.final_memory_cost = index_size_in_bytes;
        request.final_disk_cost = index_size_in_bytes;
        request.max_memory_cost =
            SaturatingMultiply(index_size_in_bytes, uint64_t{2});
        request.max_disk_cost = index_size_in_bytes;
        request.has_raw_data = false;
    } else {
        LOG_ERROR(
            "invalid index type to estimate scalar index load resource: {}",
            index_type);
        return LoadResourceRequest{0, 0, 0, 0, false};
    }
    request.has_raw_data =
        CanUseIndexRawDataForField(field_type, request.has_raw_data);
    return request;
}

}  // namespace
bool
CanUseIndexRawDataForField(DataType field_type, bool has_raw_data) {
    return has_raw_data && field_type != DataType::ARRAY &&
           field_type != DataType::VECTOR_ARRAY && field_type != DataType::JSON;
}

LoadResourceRequest
IndexLoadResource(DataType field_type,
                  DataType element_type,
                  IndexVersion index_version,
                  uint64_t index_size_in_bytes,
                  const std::map<std::string, std::string>& index_params,
                  bool mmap_enable,
                  int64_t num_rows,
                  int64_t dim) {
    return IsVectorDataType(field_type)
               ? VecIndexLoadResource(field_type,
                                      element_type,
                                      index_version,
                                      index_size_in_bytes,
                                      index_params,
                                      mmap_enable,
                                      num_rows,
                                      dim)
               : ScalarIndexLoadResource(field_type,
                                         index_version,
                                         index_size_in_bytes,
                                         index_params,
                                         mmap_enable,
                                         num_rows);
}

LoadResourceRequest
ScalarIndexLoadResource(DataType field_type,
                        IndexVersion index_version,
                        uint64_t index_size_in_bytes,
                        const std::map<std::string, std::string>& index_params,
                        bool mmap_enable,
                        int64_t num_rows) {
    auto config = milvus::index::ParseConfigFromIndexParams(index_params);

    auto index_type_it = index_params.find("index_type");
    AssertInfo(index_type_it != index_params.end(), "index type is empty");
    const std::string& index_type = index_type_it->second;

    auto scalar_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    // Without file metadata, plugin state provides the compatibility fallback.
    auto encrypted_stream =
        scalar_version >= 3 &&
        milvus::storage::PluginLoader::GetInstance().getCipherPlugin() !=
            nullptr;
    auto file_stream = index_type == milvus::index::INVERTED_INDEX_TYPE ||
                       index_type == milvus::index::NGRAM_INDEX_TYPE ||
                       index_type == milvus::index::RTREE_INDEX_TYPE;
    auto stream_memory_overhead = ScalarIndexStreamMemoryOverhead(
        index_size_in_bytes, scalar_version, encrypted_stream, file_stream);

    return ScalarIndexLoadResourceWithOverhead(field_type,
                                               index_size_in_bytes,
                                               index_params,
                                               mmap_enable,
                                               num_rows,
                                               stream_memory_overhead);
}

namespace {
// Envelope inspection measures decoded bytes and admitted scratch, rather than
// treating compressed object size as the in-memory load peak.
LoadResourceRequest
LegacyScalarLoadResource(DataType field_type,
                         uint64_t index_size,
                         const std::map<std::string, std::string>& index_params,
                         bool mmap_enable,
                         int64_t num_rows,
                         const std::vector<std::string>& paths,
                         const storage::FileManagerContext& context) {
    auto estimate = [&]() -> folly::coro::Task<LoadResourceRequest> {
        auto params = index_params;
        auto type = params.at(INDEX_TYPE);
        const bool directory_layout =
            type == HYBRID_INDEX_TYPE || type == INVERTED_INDEX_TYPE ||
            type == NGRAM_INDEX_TYPE || type == RTREE_INDEX_TYPE;
        storage::LoadOptions options;
        options.params = ParseConfigFromIndexParams(params);
        auto source = co_await storage::V1RemoteSource::OpenAsync(
            context,
            paths,
            options,
            storage::ArtifactStoragePath::Index,
            directory_layout ? storage::V1SourceLayout::DiskFiles
                             : storage::V1SourceLayout::MemoryEntries);
        if (type == HYBRID_INDEX_TYPE) {
            const auto family = co_await ResolveLoadFamilyAsync(
                families::kHybrid, *source, options.params);
            for (const auto child : {ScalarIndexType::BITMAP,
                                     ScalarIndexType::STLSORT,
                                     ScalarIndexType::MARISA,
                                     ScalarIndexType::INVERTED}) {
                if (FamilyFromScalarIndexType(child) == family) {
                    type = HybridInternalIndexTypeToIndexType(child);
                    break;
                }
            }
            params[INDEX_TYPE] = type;
        }
        const auto names = source->EntryNames();
        const auto bytes = co_await source->InspectLoadBytesAsync(names);
        const auto persisted = std::max(index_size, bytes.payload);
        auto rows = num_rows;
        if (field_type == DataType::ARRAY) {
            rows = std::max<int64_t>(
                rows,
                std::min<uint64_t>(bytes.payload,
                                   std::numeric_limits<int64_t>::max()));
        }
        uint64_t cardinality = 0;
        if (type == BITMAP_INDEX_TYPE) {
            const auto encoded =
                co_await source->ReadEntryAsync(BITMAP_INDEX_META);
            try {
                const auto meta =
                    YAML::Load(std::string(encoded.begin(), encoded.end()));
                const auto count = meta[BITMAP_INDEX_NUM_ROWS].as<uint64_t>();
                cardinality = meta[BITMAP_INDEX_LENGTH].as<uint64_t>();
                if (count > uint64_t{std::numeric_limits<int64_t>::max()}) {
                    ThrowInfo(DataFormatBroken,
                              "legacy bitmap row count exceeds int64");
                }
                rows = std::max(rows, static_cast<int64_t>(count));
            } catch (const YAML::Exception& error) {
                ThrowInfo(DataFormatBroken,
                          "invalid legacy bitmap metadata: {}",
                          error.what());
            }
            // Low-cardinality bitmaps stay resident even when mmap is requested.
            if (cardinality <= DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND) {
                mmap_enable = false;
            }
        }
        auto request = ScalarIndexLoadResourceWithOverhead(
            field_type, persisted, params, mmap_enable, rows, 0);
        if (type == BITMAP_INDEX_TYPE) {
            auto resident = SaturatingAdd(ValidityBitmapBytes(rows),
                                          SaturatingMul(cardinality, 128));
            if (cardinality <= DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND) {
                resident = SaturatingAdd(
                    resident,
                    SaturatingMul(ValidityBitmapBytes(rows), cardinality));
            }
            if (GetValueFromConfig<bool>(options.params, ENABLE_OFFSET_CACHE)
                    .value_or(false)) {
                resident = SaturatingAdd(
                    resident,
                    SaturatingMul(uint64_t{std::max<int64_t>(0, rows)},
                                  sizeof(void*)));
            }
            request.final_memory_cost =
                std::max(request.final_memory_cost, resident);
        }
        if (!mmap_enable &&
            (type == MARISA_TRIE || type == MARISA_TRIE_UPPER)) {
            request.final_memory_cost = SaturatingAdd(
                request.final_memory_cost, MarisaLegacyCsrBytes(rows, 2));
        }
        std::vector<std::string> sidecars;
        for (const auto& name : names) {
            if (name == INDEX_NULL_OFFSET ||
                name == INDEX_NON_EXIST_OFFSET_FILE_NAME ||
                name == "ngram_avg_row_size") {
                sidecars.push_back(name);
            }
        }
        const auto sidecar_bytes =
            co_await source->InspectLoadBytesAsync(sidecars);
        if (field_type == DataType::JSON) {
            request.final_memory_cost =
                SaturatingAdd(request.final_memory_cost,
                              SaturatingAdd(sidecar_bytes.payload,
                                            ValidityBitmapBytes(rows)));
        } else if (type == INVERTED_INDEX_TYPE || type == NGRAM_INDEX_TYPE) {
            request.final_memory_cost =
                SaturatingAdd(request.final_memory_cost, sidecar_bytes.payload);
        }
        auto retained = sidecar_bytes.payload;
        if (type == BITMAP_INDEX_TYPE && !mmap_enable) {
            retained = bytes.payload;
        }
        const auto scratch = SaturatingAdd(
            bytes.transient,
            SaturatingAdd(
                bytes.directory,
                SaturatingAdd(retained,
                              uint64_t{storage::FileWriter::MAX_BUFFER_SIZE})));
        request.max_memory_cost = SaturatingAdd(
            std::max(request.max_memory_cost, request.final_memory_cost),
            scratch);
        // Numeric sorted heap loads also stage their input before decoding.
        request.max_disk_cost = std::max(request.max_disk_cost, persisted);
        if (field_type == DataType::ARRAY) {
            request.has_raw_data = false;
        }
        co_return request;
    };
    return folly::coro::blockingWait(folly::coro::co_withExecutor(
        storage::ResolveAsyncLoadExecutor(
            storage::LocalFileIOPool::GetInstance().GetExecutor(),
            proto::common::LoadPriority::HIGH),
        estimate()));
}
}  // namespace

LoadResourceRequest
LegacyVectorFileLoadResource(LoadResourceRequest request,
                             bool disk_family,
                             const storage::LoadOptions& options,
                             const std::vector<std::string>& paths,
                             const storage::FileManagerContext& context) {
    auto estimate = [&]() -> folly::coro::Task<LoadResourceRequest> {
        auto source = co_await storage::V1RemoteSource::OpenAsync(
            context,
            paths,
            options,
            storage::ArtifactStoragePath::Index,
            disk_family ? storage::V1SourceLayout::DiskFiles
                        : storage::V1SourceLayout::MemoryEntries);
        const auto names =
            disk_family ? VectorDiskLoader::AsyncEntryNames(*source, options)
                        : source->EntryNames();
        const auto bytes = co_await source->InspectLoadBytesAsync(names);
        // BinarySet payloads coexist with native Deserialize state. For native
        // stream backends, names contains only eagerly prepared sidecars.
        const bool memory_mmap =
            !disk_family && options.enable_mmap &&
            KnowhereMmapSupported(
                options.params.at(INDEX_TYPE).get<std::string>());
        const auto retained = memory_mmap ? uint64_t{0} : bytes.payload;
        const auto scratch = SaturatingAdd(
            bytes.transient,
            SaturatingAdd(
                bytes.directory,
                SaturatingAdd(retained,
                              uint64_t{storage::FileWriter::MAX_BUFFER_SIZE})));
        request.max_memory_cost = SaturatingAdd(
            std::max(request.max_memory_cost, request.final_memory_cost),
            scratch);
        co_return request;
    };
    return folly::coro::blockingWait(folly::coro::co_withExecutor(
        storage::ResolveAsyncLoadExecutor(
            storage::LocalFileIOPool::GetInstance().GetExecutor(),
            proto::common::LoadPriority::HIGH),
        estimate()));
}

ScalarIndexLoadResources
ScalarIndexFileLoadResource(
    DataType field_type,
    uint64_t index_size,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& context,
    bool is_index_file) {
    const auto version =
        GetValueFromConfig<int32_t>(ParseConfigFromIndexParams(index_params),
                                    SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    const bool use_async_load = context.use_async_load.value_or(
        segcore::storagev2translator::StorageV2AsyncLoadEnabled());
    if (version < 3 && index_params.at(INDEX_TYPE) != FMINDEX_INDEX_TYPE) {
        if (use_async_load && context.Valid() && !index_files.empty()) {
            return {LegacyScalarLoadResource(field_type,
                                             index_size,
                                             index_params,
                                             mmap_enable,
                                             num_rows,
                                             index_files,
                                             context),
                    std::nullopt};
        }
        auto resolved_params = index_params;
        if (resolved_params.at(INDEX_TYPE) == HYBRID_INDEX_TYPE) {
            auto type = ResolveLegacyHybridIndexType(index_files, context);
            if (!type.empty()) {
                resolved_params[INDEX_TYPE] = std::move(type);
            }
        }
        return {ScalarIndexLoadResource(field_type,
                                        0,
                                        index_size,
                                        resolved_params,
                                        mmap_enable,
                                        num_rows),
                std::nullopt};
    }
    auto reader = InspectPackedIndexFile(index_files, context, is_index_file);
    return PackedScalarIndexLoadResource(field_type,
                                         index_size,
                                         index_params,
                                         mmap_enable,
                                         num_rows,
                                         index_files,
                                         *reader,
                                         use_async_load);
}

ScalarIndexLoadResources
PackedScalarIndexLoadResource(
    DataType field_type,
    uint64_t index_size,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::vector<std::string>& index_files,
    const storage::AsyncIndexEntryReader& reader,
    bool use_async_load) {
    const auto version =
        GetValueFromConfig<int32_t>(ParseConfigFromIndexParams(index_params),
                                    SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    const auto& directory = reader.Directory();
    const auto& metadata = reader.IndexMeta();
    auto resolved_params = index_params;
    if (resolved_params.at("index_type") == HYBRID_INDEX_TYPE) {
        auto config = ParseConfigFromIndexParams(index_params);
        config[INDEX_FILES] = index_files;
        const auto family =
            ResolvePackedLoadFamily(families::kHybrid, metadata, config);
        IndexType resolved;
        for (auto candidate : {ScalarIndexType::BITMAP,
                               ScalarIndexType::STLSORT,
                               ScalarIndexType::MARISA,
                               ScalarIndexType::INVERTED}) {
            if (FamilyFromScalarIndexType(candidate) == family) {
                resolved = HybridInternalIndexTypeToIndexType(candidate);
                break;
            }
        }
        AssertInfo(
            !resolved.empty(), "Unknown async hybrid index type {}", family);
        resolved_params["index_type"] = resolved;
    }
    const auto& type = resolved_params.at(INDEX_TYPE);
    const bool file_stream = type == INVERTED_INDEX_TYPE ||
                             type == NGRAM_INDEX_TYPE ||
                             type == RTREE_INDEX_TYPE;
    const bool may_write_files =
        use_async_load && (mmap_enable || file_stream || type == MARISA_TRIE ||
                           type == MARISA_TRIE_UPPER);
    EntryStreamLoadInfo legacy_stream;
    uint64_t total_transient = 0;
    uint64_t max_task = 0;
    for (const auto& entry : directory.Entries()) {
        if (const auto* encrypted =
                std::get_if<storage::EncryptedEntrySource>(&entry.source)) {
            legacy_stream.encrypted = true;
            for (const auto& slice : encrypted->slices) {
                if (!use_async_load) {
                    const auto bytes = SaturatingAdd(
                        slice.remote_bytes,
                        SaturatingMultiply(slice.plaintext_bytes, uint64_t{2}));
                    legacy_stream.total_transient_bytes = SaturatingAdd(
                        legacy_stream.total_transient_bytes, bytes);
                    legacy_stream.max_task_transient_bytes =
                        std::max(legacy_stream.max_task_transient_bytes, bytes);
                    continue;
                }
                auto bytes = SaturatingAdd(
                    SaturatingMultiply(slice.remote_bytes, uint64_t{2}),
                    slice.plaintext_bytes);
                if (may_write_files) {
                    bytes = SaturatingAdd(
                        bytes,
                        SaturatingAdd(
                            slice.plaintext_bytes,
                            uint64_t{2 * storage::FileWriter::ALIGNMENT_MASK}));
                }
                total_transient = SaturatingAdd(total_transient, bytes);
                max_task = std::max(max_task, bytes);
            }
        } else if (use_async_load) {
            const auto slice_size = storage::DefaultStreamSliceSize();
            auto bytes = entry.plaintext_size;
            auto task_bytes = std::min<uint64_t>(bytes, slice_size);
            if (may_write_files && bytes != 0) {
                const auto slices = 1 + (bytes - 1) / slice_size;
                bytes = SaturatingAdd(
                    SaturatingMultiply(bytes, uint64_t{2}),
                    SaturatingMultiply(
                        slices,
                        uint64_t{2 * storage::FileWriter::ALIGNMENT_MASK}));
                task_bytes = SaturatingAdd(
                    SaturatingMultiply(task_bytes, uint64_t{2}),
                    uint64_t{2 * storage::FileWriter::ALIGNMENT_MASK});
            }
            total_transient = SaturatingAdd(total_transient, bytes);
            max_task = std::max(max_task, task_bytes);
        }
    }
    // Estimates survive admission refreshes, including removal of all limits.
    // The shared overhead group applies the live global bound where eligible.
    const auto read_peak = use_async_load ? total_transient
                                          : ScalarIndexStreamMemoryOverhead(
                                                index_size,
                                                std::max(version, 3),
                                                legacy_stream.encrypted,
                                                file_stream,
                                                legacy_stream);
    uint64_t staging_bytes = 0;
    uint64_t bitmap_resident_bytes = 0;
    if ((type == INVERTED_INDEX_TYPE || type == NGRAM_INDEX_TYPE) &&
        directory.HasEntry("index_null_offset")) {
        // The whole sidecar survives until FinalizeSealed builds validity.
        staging_bytes = directory.At("index_null_offset").plaintext_size;
    } else if (type == BITMAP_INDEX_TYPE) {
        const auto cardinality =
            ReadRequiredIndexMeta<size_t>(metadata, BITMAP_INDEX_LENGTH);
        num_rows =
            ReadRequiredIndexMeta<int64_t>(metadata, BITMAP_INDEX_NUM_ROWS);
        const auto validity_bytes = ValidityBitmapBytes(num_rows);
        const bool uses_bitsets =
            cardinality <= DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND;
        const bool loads_to_mmap = mmap_enable && !uses_bitsets;
        if (!loads_to_mmap) {
            staging_bytes = directory.At(BITMAP_INDEX_DATA).plaintext_size;
            mmap_enable = false;
            // Keep the existing serialized-size allowance for keys/postings.
            // Validity and low-cardinality dense postings are additional resident
            // allocations, even when a non-nullable file has no validity entry.
            bitmap_resident_bytes = validity_bytes;
            if (uses_bitsets) {
                bitmap_resident_bytes = SaturatingAdd(
                    bitmap_resident_bytes,
                    SaturatingMultiply(validity_bytes,
                                       static_cast<uint64_t>(cardinality)));
            }
        }
        if (!use_async_load && directory.HasEntry(BITMAP_INDEX_VALID_BITSET)) {
            staging_bytes = SaturatingAdd(
                staging_bytes,
                directory.At(BITMAP_INDEX_VALID_BITSET).plaintext_size);
        }
    }
    // Async packed bitmaps are read into their final allocation. Synchronous
    // loaders still retain a packed sidecar while constructing TargetBitmap.
    if (!use_async_load && type == FMINDEX_INDEX_TYPE &&
        directory.HasEntry("fm_index_null_bitmap")) {
        staging_bytes = SaturatingAdd(
            staging_bytes, directory.At("fm_index_null_bitmap").plaintext_size);
    }
    if (!use_async_load && type == ASCENDING_SORT &&
        metadata.contains("version") && directory.HasEntry("valid_bitset")) {
        staging_bytes = SaturatingAdd(
            staging_bytes, directory.At("valid_bitset").plaintext_size);
    }
    auto request = ScalarIndexLoadResourceWithOverhead(field_type,
                                                       index_size,
                                                       resolved_params,
                                                       mmap_enable,
                                                       num_rows,
                                                       read_peak);
    request.final_memory_cost =
        SaturatingAdd(request.final_memory_cost, bitmap_resident_bytes);
    if (type == BITMAP_INDEX_TYPE && mmap_enable) {
        request.max_memory_cost = SaturatingAdd(
            request.max_memory_cost, storage::FileWriter::MAX_BUFFER_SIZE);
    }
    if (type == RTREE_INDEX_TYPE) {
        request.final_memory_cost =
            SaturatingAdd(RTreeHeapBytes(num_rows),
                          directory.HasEntry("index_null_offset")
                              ? directory.At("index_null_offset").plaintext_size
                              : uint64_t{0});
    }
    if (field_type == DataType::JSON) {
        const auto non_exist_bytes =
            directory.HasEntry(INDEX_NON_EXIST_OFFSET_FILE_NAME)
                ? directory.At(INDEX_NON_EXIST_OFFSET_FILE_NAME).plaintext_size
                : uint64_t{0};
        request.final_memory_cost = SaturatingAdd(
            request.final_memory_cost,
            SaturatingAdd(non_exist_bytes, ValidityBitmapBytes(num_rows)));
    }
    request.max_memory_cost =
        std::max(request.max_memory_cost,
                 SaturatingAdd(request.final_memory_cost,
                               SaturatingAdd(staging_bytes, read_peak)));
    // The translator pins the load mode for the lifetime of this cell.
    // Ordered sync prefetch retains completed buffers beyond worker execution;
    // only its direct-to-file streams have worker-bounded scratch lifetimes.
    const bool can_share =
        (use_async_load || file_stream) && staging_bytes == 0 &&
        type != BITMAP_INDEX_TYPE && type != RTREE_INDEX_TYPE &&
        request.max_memory_cost - request.final_memory_cost <= read_peak;
    if (!use_async_load) {
        max_task = legacy_stream.encrypted
                       ? legacy_stream.max_task_transient_bytes
                       : SaturatingMultiply(
                             storage::MaxEntryStreamTaskBytes(),
                             file_stream ? storage::kFileStreamBufferMultiplier
                                         : size_t{1});
    }
    std::optional<cachinglayer::LoadingOverheadConfig> overhead;
    // Shared reservations require the selected route to lease all scratch.
    if (can_share) {
        auto& controller = storage::LoadMemoryOverheadController::GetInstance();
        auto memory_group = controller.GetOrCreate();
        AssertInfo(max_task <= static_cast<uint64_t>(
                                   std::numeric_limits<int64_t>::max()),
                   "Async scalar task estimate exceeds resource policy range");
        overhead = cachinglayer::LoadingOverheadConfig{
            cachinglayer::LoadingOverheadGroupBinding{
                std::move(memory_group), static_cast<int64_t>(max_task)},
            std::nullopt};
    }
    return {request, std::move(overhead)};
}

}  // namespace milvus::index
