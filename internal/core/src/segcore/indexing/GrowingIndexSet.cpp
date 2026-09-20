// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/indexing/GrowingIndexSet.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <mutex>
#include <span>
#include <string>
#include <utility>

#include "common/EasyAssert.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/growing/GrowingVectorSource.h"
#include "index/growing/KnowhereGrowingVectorIndex.h"
#include "index/growing/RTreeGrowingSpatialIndex.h"
#include "index/growing/TantivyGrowingTextIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/comp/knowhere_config.h"
#include "knowhere/version.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/IndexConfigGenerator.h"
#include "segcore/InsertRecord.h"
#include "storage/MmapManager.h"

namespace milvus::segcore {
namespace {

constexpr int64_t kGrowingTextCommitIntervalMs = 200;

template <typename T>
class ChunkedGrowingVectorSource final
    : public index::GrowingVectorSource<T> {
 public:
    ChunkedGrowingVectorSource(
        std::shared_ptr<ChunkVectorBase<T>> storage,
        int64_t rows_per_chunk,
        int64_t elements_per_row)
        : storage_(std::move(storage)),
          rows_per_chunk_(rows_per_chunk),
          elements_per_row_(elements_per_row) {
        AssertInfo(storage_ != nullptr && rows_per_chunk_ > 0 &&
                       elements_per_row_ > 0,
                   "invalid growing vector column source");
    }

    std::span<const T>
    ContiguousRows(int64_t physical_begin,
                   int64_t row_count) const override {
        ValidateRange(physical_begin, row_count);
        if (row_count == 0) {
            return {};
        }
        const auto first_chunk = physical_begin / rows_per_chunk_;
        const auto last_chunk =
            (physical_begin + row_count - 1) / rows_per_chunk_;
        if (first_chunk != last_chunk) {
            return {};
        }

        const auto snapshot = storage_->acquire();
        AssertInfo(first_chunk < snapshot.count,
                   "growing vector source range starts in missing chunk {}",
                   first_chunk);
        const auto chunk_offset = physical_begin % rows_per_chunk_;
        AssertInfo(chunk_offset + row_count <= ChunkRows(snapshot, first_chunk),
                   "growing vector source range [{}, {}) exceeds chunk {}",
                   physical_begin,
                   physical_begin + row_count,
                   first_chunk);
        const auto* chunk = static_cast<const T*>(
            storage_->get_chunk_data(snapshot, first_chunk));
        return {chunk + chunk_offset * elements_per_row_,
                ElementCount(row_count)};
    }

    void
    CopyRows(int64_t physical_begin,
             int64_t row_count,
             T* output) const override {
        ValidateRange(physical_begin, row_count);
        AssertInfo(row_count == 0 || output != nullptr,
                   "growing vector source copy output is null");
        const auto snapshot = storage_->acquire();
        int64_t copied = 0;
        while (copied < row_count) {
            const auto physical = physical_begin + copied;
            const auto chunk_id = physical / rows_per_chunk_;
            const auto chunk_offset = physical % rows_per_chunk_;
            AssertInfo(chunk_id < snapshot.count,
                       "growing vector source copy reaches missing chunk {}",
                       chunk_id);
            const auto chunk_rows = ChunkRows(snapshot, chunk_id);
            const auto count =
                std::min(row_count - copied, chunk_rows - chunk_offset);
            AssertInfo(count > 0,
                       "growing vector source copy has an empty chunk {}",
                       chunk_id);
            const auto* chunk = static_cast<const T*>(
                storage_->get_chunk_data(snapshot, chunk_id));
            std::copy_n(chunk + chunk_offset * elements_per_row_,
                        ElementCount(count),
                        output + copied * elements_per_row_);
            copied += count;
        }
    }

    const T*
    Row(int64_t physical_offset) const override {
        ValidateRange(physical_offset, 1);
        const auto snapshot = storage_->acquire();
        const auto chunk_id = physical_offset / rows_per_chunk_;
        const auto chunk_offset = physical_offset % rows_per_chunk_;
        AssertInfo(chunk_id < snapshot.count &&
                       chunk_offset < ChunkRows(snapshot, chunk_id),
                   "growing vector source row {} is unavailable",
                   physical_offset);
        const auto* chunk = static_cast<const T*>(
            storage_->get_chunk_data(snapshot, chunk_id));
        return chunk + chunk_offset * elements_per_row_;
    }

 private:
    static void
    ValidateRange(int64_t physical_begin, int64_t row_count) {
        AssertInfo(physical_begin >= 0 && row_count >= 0 &&
                       physical_begin <=
                           std::numeric_limits<int64_t>::max() - row_count,
                   "invalid growing vector source begin {}, row count {}",
                   physical_begin,
                   row_count);
    }

    size_t
    ElementCount(int64_t row_count) const {
        AssertInfo(
            static_cast<uint64_t>(row_count) <=
                std::numeric_limits<size_t>::max() /
                    static_cast<uint64_t>(elements_per_row_),
            "growing vector source element count overflows size_t");
        return static_cast<size_t>(row_count) *
               static_cast<size_t>(elements_per_row_);
    }

    int64_t
    ChunkRows(const ChunkSnapshot& snapshot, int64_t chunk_id) const {
        const auto elements = storage_->get_chunk_size(snapshot, chunk_id);
        AssertInfo(elements >= 0 && elements % elements_per_row_ == 0,
                   "growing vector source chunk {} has {} elements, not a "
                   "multiple of {} elements per row",
                   chunk_id,
                   elements,
                   elements_per_row_);
        return elements / elements_per_row_;
    }

    std::shared_ptr<ChunkVectorBase<T>> storage_;
    const int64_t rows_per_chunk_;
    const int64_t elements_per_row_;
};

template <typename EngineType, typename Trait>
std::shared_ptr<const index::GrowingVectorSource<
    index::GrowingVectorStorageType<EngineType>>>
MakeVectorSource(const VectorBase* raw) {
    using StorageType = index::GrowingVectorStorageType<EngineType>;
    const auto* typed = dynamic_cast<const ConcurrentVector<Trait>*>(raw);
    AssertInfo(typed != nullptr,
               "growing vector column has an incompatible physical type");
    return std::make_shared<ChunkedGrowingVectorSource<StorageType>>(
        typed->share_chunk_storage(),
        typed->get_size_per_chunk(),
        typed->elements_per_row());
}

std::string
UniqueFieldId(int64_t segment_id, FieldId field_id) {
    return std::to_string(segment_id) + "_" +
           std::to_string(field_id.get());
}

FieldIndexCapability
MakeCapability(FieldId field_id,
               index::ReaderCaps caps,
               const index::IGrowingIndex* owner) {
    AssertInfo(owner != nullptr,
               "cannot construct an empty growing index for field {}",
               field_id.get());
    return FieldIndexCapability(
        field_id,
        {IndexCapabilityEntry{
            .key = IndexKey{field_id, IndexIdentity::SegmentLocal(0)},
            .family = owner->Family(),
            .value_type = owner->ValueType(),
            .caps = caps,
        }});
}

}  // namespace

// Resolved against the live build pool size -- the cpu budget of index
// building -- and clamped to [1, pool size] because knowhere declares
// num_build_thread without a range and hands it to omp_set_num_threads, and
// this config is refreshable.
int64_t
ResolveGrowingBuildThreadNum(const SegcoreConfig& segcore_config) {
    const auto rate = segcore_config.get_growing_index_build_thread_rate();
    if (!std::isfinite(rate) || rate <= 0.0F) {
        return 1;
    }
    const auto pool_size = static_cast<int64_t>(
        knowhere::KnowhereConfig::GetBuildThreadPoolSize());
    if (pool_size <= 1) {
        return 1;
    }
    // Cap the rate before scaling so the product can never overflow llround.
    const auto capped_rate = std::min(static_cast<double>(rate), 1.0);
    const auto thread_num = static_cast<int64_t>(
        std::llround(capped_rate * static_cast<double>(pool_size)));
    return std::clamp<int64_t>(thread_num, 1, pool_size);
}

GrowingIndexSet::Appender::Appender(
    FieldId field_id,
    index::ReaderCaps reader_caps,
    std::unique_ptr<index::IGrowingIndex> value,
    bool source_backed_value)
    : caps(reader_caps),
      owner(std::move(value)),
      source_backed(source_backed_value),
      capability(MakeCapability(field_id, reader_caps, owner.get())) {
}

void
GrowingIndexSet::Initialize(const Schema& schema,
                            const IndexMetaPtr& index_meta,
                            const SegcoreConfig& segcore_config,
                            int64_t segment_id,
                            const InsertRecordGrowing& insert_record) {
    AppenderMap staged;
    for (const auto& [field_id, field_meta] : schema.get_fields()) {
        auto appender = StageAppender(field_meta,
                                      index_meta,
                                      segcore_config,
                                      segment_id,
                                      insert_record.get_data_base(field_id));
        if (appender.has_value()) {
            staged.emplace(field_id, std::move(*appender));
        }
    }
    RegisterBatch(std::move(staged));
}

std::optional<GrowingIndexSet::Appender>
GrowingIndexSet::StageAppender(const FieldMeta& field_meta,
                               const IndexMetaPtr& index_meta,
                               const SegcoreConfig& segcore_config,
                               int64_t segment_id,
                               const VectorBase* field_raw_data) const {
    if (IsStringDataType(field_meta.get_data_type()) &&
        field_meta.enable_match()) {
        const auto unique_id = UniqueFieldId(segment_id, field_meta.get_id());
        const auto analyzer_params = field_meta.get_analyzer_params();
        return Appender(
            field_meta.get_id(),
            index::ReaderCaps{.text_match = true},
            std::make_unique<index::TantivyGrowingTextIndex>(
                unique_id.c_str(),
                "milvus_tokenizer",
                analyzer_params.c_str(),
                field_meta.get_data_type(),
                kGrowingTextCommitIntervalMs));
    }

    if (field_meta.get_data_type() == DataType::GEOMETRY &&
        segcore_config.get_enable_interim_segment_index() &&
        index_meta != nullptr && index_meta->GetIndexMaxRowCount() > 0 &&
        index_meta->HasField(field_meta.get_id())) {
        return Appender(
            field_meta.get_id(),
            index::ReaderCaps{.spatial = true, .exact = false},
            std::make_unique<index::RTreeGrowingSpatialIndex>(
                segcore_config.get_chunk_rows()));
    }

    const auto data_type = field_meta.get_data_type();
    const bool supported_vector =
        data_type == DataType::VECTOR_FLOAT ||
        data_type == DataType::VECTOR_FLOAT16 ||
        data_type == DataType::VECTOR_BFLOAT16 ||
        data_type == DataType::VECTOR_SPARSE_U32_F32;
    const bool growing_mmap_enabled = storage::MmapManager::GetInstance()
                                          .GetMmapConfig()
                                          .GetEnableGrowingMmap();
    if (!supported_vector ||
        !segcore_config.get_enable_interim_segment_index() ||
        growing_mmap_enabled || index_meta == nullptr ||
        index_meta->GetIndexMaxRowCount() <= 0 ||
        !index_meta->HasField(field_meta.get_id())) {
        return std::nullopt;
    }

    const auto& field_index_meta =
        index_meta->GetFieldIndexMeta(field_meta.get_id());
    if (field_index_meta.IsFlatIndex()) {
        return std::nullopt;
    }
    AssertInfo(field_raw_data != nullptr,
               "growing vector field {} has no raw column",
               field_meta.get_id().get());

    VecIndexConfig config(index_meta->GetIndexMaxRowCount(),
                          field_index_meta,
                          segcore_config,
                          SegmentType::Growing,
                          IsSparseFloatVectorDataType(data_type));
    auto build_params = config.GetBuildBaseParams(data_type);
    const int64_t dim = IsSparseFloatVectorDataType(data_type)
                            ? 0
                            : field_meta.get_dim();
    if (dim > 0) {
        build_params[knowhere::meta::DIM] = std::to_string(dim);
    }
    build_params[knowhere::meta::NUM_BUILD_THREAD] =
        std::to_string(ResolveGrowingBuildThreadNum(segcore_config));
    const bool source_backed =
        config.GetIndexType() == knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR;
    // #52361: honour the configured interim index target engine version
    // instead of pinning the growing index to knowhere's current version.
    const auto version = segcore_config.get_interim_index_version();

    // The knob is refreshable, so resolve it again before every knowhere
    // Build/Add instead of freezing the value stamped above. SegcoreConfig
    // state is process-global (every member is `inline static`), so reading
    // the default instance observes the same values as `segcore_config`
    // without retaining a reference to the caller's object.
    std::function<int64_t()> build_thread_num = [] {
        return ResolveGrowingBuildThreadNum(SegcoreConfig::default_config());
    };

    auto make_owner = [&]<typename EngineType>(
                          std::shared_ptr<const index::GrowingVectorSource<
                              index::GrowingVectorStorageType<EngineType>>>
                              source) -> std::unique_ptr<index::IGrowingIndex> {
        return std::make_unique<index::KnowhereGrowingVectorIndex<EngineType>>(
            data_type,
            config.GetIndexType(),
            config.GetMetricType(),
            version,
            dim,
            config.GetBuildThreshold(),
            build_params,
            config.GetSearchBaseParams(),
            std::move(source),
            source_backed,
            build_thread_num);
    };

    std::unique_ptr<index::IGrowingIndex> owner;
    switch (data_type) {
        case DataType::VECTOR_FLOAT:
            owner = make_owner.operator()<float>(
                MakeVectorSource<float, FloatVector>(field_raw_data));
            break;
        case DataType::VECTOR_FLOAT16:
            owner = make_owner.operator()<float16>(
                MakeVectorSource<float16, Float16Vector>(field_raw_data));
            break;
        case DataType::VECTOR_BFLOAT16:
            owner = make_owner.operator()<bfloat16>(
                MakeVectorSource<bfloat16, BFloat16Vector>(field_raw_data));
            break;
        case DataType::VECTOR_SPARSE_U32_F32:
            owner = make_owner.operator()<sparse_u32_f32>(
                MakeVectorSource<sparse_u32_f32, SparseFloatVector>(
                    field_raw_data));
            break;
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported growing vector owner type {}",
                      data_type);
    }

    return Appender(
        field_meta.get_id(), {}, std::move(owner), source_backed);
}

void
GrowingIndexSet::RegisterBatch(AppenderMap staged) {
    if (staged.empty()) {
        return;
    }

    std::vector<FieldId> keys;
    keys.reserve(staged.size());
    for (const auto& [field_id, appender] : staged) {
        AssertInfo(appender.owner != nullptr,
                   "cannot register an empty growing index for field {}",
                   field_id.get());
        keys.push_back(field_id);
    }

    std::unique_lock lock(mutex_);
    for (const auto& field_id : keys) {
        AssertInfo(appenders_.find(field_id) == appenders_.end(),
                   "growing index already exists for field {}",
                   field_id.get());
    }

    try {
        appenders_.merge(staged);
        AssertInfo(staged.empty(),
                   "growing index registration left staged entries");
    } catch (...) {
        // FieldId compares its int64 payload and cannot throw. Node extraction
        // and reinsertion reuse storage allocated before appenders_ changed.
        for (const auto& field_id : keys) {
            auto node = appenders_.extract(field_id);
            if (!node.empty()) {
                staged.insert(std::move(node));
            }
        }
        throw;
    }
}

bool
GrowingIndexSet::Has(FieldId field_id) const {
    std::shared_lock lock(mutex_);
    return appenders_.find(field_id) != appenders_.end();
}

bool
GrowingIndexSet::CanReleaseVectorColumn(FieldId field_id,
                                        int64_t visible_row_end) const {
    std::shared_lock lock(mutex_);
    auto it = appenders_.find(field_id);
    if (it == appenders_.end() || !it->second.owner ||
        it->second.source_backed) {
        return false;
    }
    auto pin = it->second.owner->PinSnapshot();
    if (!pin || pin.CoveredRowEnd() < visible_row_end) {
        return false;
    }
    const auto* reader =
        dynamic_cast<const index::IVectorReader*>(&pin.Reader());
    return reader != nullptr && reader->HasRawData();
}

void
GrowingIndexSet::FlushAll() {
    std::shared_lock lock(mutex_);
    for (auto& [field_id, appender] : appenders_) {
        Flush(appender, field_id);
    }
}

void
GrowingIndexSet::Flush(Appender& appender, FieldId field_id) {
    AssertInfo(appender.owner != nullptr,
               "growing field {} has no index owner to flush",
               field_id.get());
    appender.owner->Flush();
}

FieldIndexCapability
GrowingIndexSet::Capability(FieldId field_id) const {
    std::shared_lock lock(mutex_);
    auto it = appenders_.find(field_id);
    if (it == appenders_.end() || !it->second.owner) {
        return FieldIndexCapability(field_id);
    }

    return it->second.capability;
}

}  // namespace milvus::segcore
