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

#include "index/vector/VectorIndexReader.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/CheckedInputArithmetic.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"
#include "cachinglayer/CacheSlot.h"
#include "index/Meta.h"
#include "index/vector/RangeSearchParams.h"
#include "index/vector/VectorDiskAnnUtils.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorReaderUtils.h"
#include "index/vector/VectorReaderValidation.h"
#include "index/vector/VectorTypeUtils.h"
#include "knowhere/comp/index_param.h"

namespace milvus::index {
namespace {

void
ValidateDiskAnnBeamwidth(const KnowhereEngine& engine, uint32_t beamwidth) {
    if (engine.KnowhereIndexType() != knowhere::IndexEnum::INDEX_DISKANN) {
        return;
    }
    vector_disk_ann::ValidateBeamwidth(beamwidth);
}

[[noreturn]] void
ThrowSearchError(const char* operation,
                 const knowhere::expected<knowhere::DataSetPtr>& result,
                 const knowhere::Json* config = nullptr) {
    const auto status = result.error();
    if (config == nullptr) {
        ThrowInfo(KnowhereStatusToErrorCode(status),
                  "failed to {}: status {} ({}), detail: {}",
                  operation,
                  static_cast<int>(status),
                  knowhere::Status2String(status),
                  result.what());
    }
    ThrowInfo(KnowhereStatusToErrorCode(status),
              "failed to {}: config={} status {} ({}), detail: {}",
              operation,
              milvus::EscapeBraces(config->dump()),
              static_cast<int>(status),
              knowhere::Status2String(status),
              result.what());
}

void
FillRegularSearchResult(const DatasetPtr& native_result,
                        const detail::VectorQueryShape& shape,
                        int64_t topk,
                        size_t result_count,
                        SearchResult& result) {
    detail::ValidateRegularSearchResult(
        native_result, shape, topk, result_count);
    const auto* ids = native_result->GetIds();
    const auto* distances = native_result->GetDistance();
    const auto id_bytes =
        CheckedKnowhereBytes(result_count, sizeof(int64_t), "search id");
    const auto distance_bytes =
        CheckedKnowhereBytes(result_count, sizeof(float), "search distance");

    result.seg_offsets_.resize(result_count);
    result.distances_.resize(result_count);
    result.total_nq_ = shape.logical_nq;
    result.unity_topK_ = topk;
    if (result_count > 0) {
        milvus::fastmem::FastMemcpy(result.seg_offsets_.data(), ids, id_bytes);
        milvus::fastmem::FastMemcpy(
            result.distances_.data(), distances, distance_bytes);
    }
}

knowhere::MutableBitmapView
ValidityTarget(TargetBitmapView view) {
    if (view.size() == 0) {
        return {};
    }
    const auto intra = view.offset() % 8;
    AssertInfo(view.size() <= std::numeric_limits<size_t>::max() - intra,
               "validity window overflows");
    const auto span = intra + view.size();
    // Pass only the bytes addressed by this window, not an inferred capacity.
    auto* bytes = reinterpret_cast<uint8_t*>(view.data());
    AssertInfo(bytes != nullptr, "validity window data is null");
    return {bytes + view.offset() / 8, span / 8 + (span % 8 != 0), intra};
}

bool
EngineHasValidity(const KnowhereEngine& engine) {
    const auto result = engine.native_index.HasValidity();
    if (!result.has_value()) {
        ThrowInfo(KnowhereStatusToErrorCode(result.error()),
                  "failed to inspect nullable validity: {}",
                  result.what());
    }
    return result.value();
}

bool
EngineIsRowValid(const KnowhereEngine& engine, int64_t logical_offset) {
    const auto result = engine.native_index.IsRowValid(logical_offset);
    if (!result.has_value()) {
        ThrowInfo(KnowhereStatusToErrorCode(result.error()),
                  "failed to read row validity: {}",
                  result.what());
    }
    return result.value();
}

}  // namespace

VectorIndex::VectorIndex(KnowhereEngine engine)
    : VectorIndex(std::make_shared<const KnowhereEngine>(std::move(engine))) {
}

VectorIndex::VectorIndex(std::shared_ptr<const KnowhereEngine> engine)
    : engine_(std::move(engine)) {
    const auto& owned =
        std::get<std::shared_ptr<const KnowhereEngine>>(engine_);
    AssertInfo(owned != nullptr, "vector index requires an engine");
    physical_count_ =
        IsAllNullNullable(owned->native_index) || owned->IsEmptyEmbListIndex()
            ? 0
            : owned->native_index.Count();
}

VectorIndex::VectorIndex(
    std::shared_ptr<cachinglayer::CacheSlot<KnowhereEngine>> slot,
    DataType physical_type,
    IndexType index_type,
    MetricType metric,
    std::optional<uint32_t> disk_ann_beamwidth)
    : engine_(CachedEngine{std::move(slot),
                           physical_type,
                           std::move(index_type),
                           std::move(metric)}),
      backend_(disk_ann_beamwidth ? Backend::Disk : Backend::Memory),
      disk_ann_beamwidth_(disk_ann_beamwidth.value_or(8)) {
    const auto& cached = std::get<CachedEngine>(engine_);
    AssertInfo(cached.slot != nullptr, "vector index requires a cache slot");
    if (disk_ann_beamwidth &&
        cached.index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        vector_disk_ann::ValidateBeamwidth(*disk_ann_beamwidth);
    }
}

VectorIndex::VectorIndex(uint32_t disk_ann_beamwidth, KnowhereEngine engine)
    : VectorIndex(std::move(engine)) {
    backend_ = Backend::Disk;
    disk_ann_beamwidth_ = disk_ann_beamwidth;
    ValidateDiskAnnBeamwidth(
        *std::get<std::shared_ptr<const KnowhereEngine>>(engine_),
        disk_ann_beamwidth_);
}

VectorIndex::VectorIndex(KnowhereEngine engine,
                         int64_t logical_count,
                         int64_t physical_count,
                         knowhere::Json search_defaults)
    : engine_(std::make_shared<const KnowhereEngine>(std::move(engine))),
      backend_(Backend::Memory),
      physical_count_(physical_count),
      logical_count_(logical_count),
      growing_search_defaults_(std::move(search_defaults)) {
    const auto& owned =
        *std::get<std::shared_ptr<const KnowhereEngine>>(engine_);
    AssertInfo(physical_count >= 0,
               "growing vector reader physical count {} is negative",
               physical_count);
    AssertInfo(logical_count_ >= physical_count,
               "growing vector reader logical count {} is below its physical "
               "count {}",
               logical_count_,
               physical_count);
    if (physical_count > 0) {
        const auto engine_count = owned.native_index.Count();
        AssertInfo(physical_count <= engine_count,
                   "growing vector reader physical count {} exceeds engine "
                   "count {}",
                   physical_count,
                   engine_count);
    }
    // The shared IdMap keeps growing behind this generation, so only the
    // frozen prefix may be compared against it.
    const auto logical_count_result = owned.native_index.LogicalCount();
    if (!logical_count_result.has_value()) {
        ThrowInfo(KnowhereStatusToErrorCode(logical_count_result.error()),
                  "failed to read growing logical count: {}",
                  logical_count_result.what());
    }
    AssertInfo(!EngineHasValidity(owned) ||
                   static_cast<int64_t>(logical_count_result.value()) >=
                       logical_count_,
               "growing vector reader logical count {} exceeds the published "
               "id map row domain {}",
               logical_count_,
               logical_count_result.value());
}

IndexMetadata
VectorIndex::Metadata() const {
    const auto* cached = std::get_if<CachedEngine>(&engine_);
    const auto type =
        cached ? cached->physical_type
               : std::get<std::shared_ptr<const KnowhereEngine>>(engine_)
                     ->PhysicalType();
    return {ReaderCaps{}, Domain::Row, type, physical_count_};
}

std::shared_ptr<const KnowhereEngine>
VectorIndex::PinEngine(milvus::OpContext* op_ctx) const {
    if (const auto* resident =
            std::get_if<std::shared_ptr<const KnowhereEngine>>(&engine_))
        return *resident;
    const auto& cached = std::get<CachedEngine>(engine_);
    std::shared_ptr<const KnowhereEngine> engine;
    if (const auto* resident = cached.slot->TryBorrowCell(0)) {
        engine = {cached.slot, resident};
    } else {
        auto pin =
            cachinglayer::SemiInlineGet(cached.slot->PinCells(op_ctx, {0}));
        const auto* cell = pin->get_ith_cell(0);
        engine = {std::move(pin), cell};
    }
    AssertInfo(engine && engine->PhysicalType() == cached.physical_type,
               "cached vector engine disagrees with metadata");
    return engine;
}

void
VectorIndex::CancelWarmup() const {
    if (const auto* cached = std::get_if<CachedEngine>(&engine_))
        cached->slot->CancelWarmup();
}

IIndexReaderBasePtr
VectorIndex::CreateReader(milvus::OpContext* op_ctx) const {
    return IIndexReaderBasePtr(
        new VectorIndexReader(shared_from_this(), op_ctx));
}

void
VectorIndexReader::Prefetch() const {
    (void)GetEngine();
}

VectorIndexReader::VectorIndexReader(KnowhereEngine engine)
    : VectorIndexReader(std::make_shared<VectorIndex>(std::move(engine)),
                        nullptr) {
}

VectorIndexReader::VectorIndexReader(uint32_t beamwidth, KnowhereEngine engine)
    : VectorIndexReader(
          std::make_shared<VectorIndex>(beamwidth, std::move(engine)),
          nullptr) {
}

VectorIndexReader::VectorIndexReader(KnowhereEngine engine,
                                     int64_t logical_count,
                                     int64_t physical_count,
                                     knowhere::Json search_defaults)
    : VectorIndexReader(
          std::make_shared<VectorIndex>(std::move(engine),
                                        logical_count,
                                        physical_count,
                                        std::move(search_defaults)),
          nullptr) {
}

VectorIndexReader::VectorIndexReader(std::shared_ptr<const VectorIndex> index,
                                     milvus::OpContext* op_ctx)
    : index_(std::move(index)), op_ctx_(op_ctx) {
}

ReaderCaps
VectorIndexReader::Caps() const {
    // ReaderCaps is scalar-shaped and does not describe raw-vector or refine
    // support; the vector methods retain their runtime capability checks.
    return {};
}

Domain
VectorIndexReader::CoordDomain() const {
    // Inventory is row keyed. Embedding-list element IDs are mapped back to
    // rows by the query consumer using that query's offsets.
    return Domain::Row;
}

int64_t
VectorIndexReader::Count() const {
    if (index_->physical_count_)
        return *index_->physical_count_;
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    return IsEmptyEngine(engine) ? 0 : engine.native_index.Count();
}

DataType
VectorIndexReader::ValueType() const {
    return index_->Metadata().value_type;
}

void
VectorIndexReader::Search(const DatasetPtr& dataset,
                          const VectorSearchParams& params,
                          const BitsetView& bitset,
                          SearchResult& result) const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    if (!IsGrowingGeneration()) {
        if (index_->backend_ == Backend::Disk) {
            SearchDisk(engine, dataset, params, bitset, op_ctx_, result);
        } else {
            SearchMemory(engine, dataset, params, bitset, op_ctx_, result);
        }
        return;
    }
    if (index_->physical_count_ == 0) {
        FillEmptySearchResult(engine, dataset, params, result);
        return;
    }

    TargetBitmap prefix_storage;
    const auto bounded = BoundSearchBitset(bitset, prefix_storage);
    const auto effective = EffectiveSearchParams(engine, params);
    SearchMemory(engine, dataset, effective, bounded, op_ctx_, result);
}

knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
VectorIndexReader::Iterators(const DatasetPtr& dataset,
                             const knowhere::Json& json,
                             const BitsetView& bitset) const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine.PhysicalType(), engine.Dim());
    if (IsEmptyEngine(engine)) {
        return detail::MakeEmptyVectorIterators(shape.logical_nq_size);
    }
    const auto bounded = BoundIteratorBitset(bitset);
    auto result =
        engine.native_index.AnnIterator(dataset, json, bounded, false, op_ctx_);
    if (!result.has_value()) {
        return result;
    }
    // Native iterator destruction may still use the engine and its file owner.
    struct IteratorOwner {
        std::shared_ptr<const KnowhereEngine> engine;
        knowhere::IndexNode::IteratorPtr iterator;
    };
    for (auto& iterator : result.value()) {
        if (iterator == nullptr) {
            continue;
        }
        auto* raw = iterator.get();
        auto owner = std::make_shared<IteratorOwner>(
            IteratorOwner{engine_pin, std::move(iterator)});
        iterator = knowhere::IndexNode::IteratorPtr(std::move(owner), raw);
    }
    return result;
}

bool
VectorIndexReader::RefineEnabled() const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    return !IsEmptyEngine(engine) && engine.native_index.IsIndexRefineEnabled();
}

bool
VectorIndexReader::HasRawData() const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    return IsEmptyEngine(engine) ||
           engine.native_index.HasRawData(engine.Metric());
}

std::vector<uint8_t>
VectorIndexReader::GetVector(const DatasetPtr& dataset) const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    auto reject_sparse = [&]() -> std::vector<uint8_t> {
        ThrowInfo(Unsupported,
                  index_->backend_ == Backend::Disk
                      ? "dense vector retrieval is not supported for a sparse "
                        "disk index"
                      : "dense vector retrieval is not supported for a sparse "
                        "index");
    };
    if (engine.PhysicalType() == DataType::VECTOR_SPARSE_U32_F32) {
        return reject_sparse();
    }
    ValidateGenerationIds(engine, dataset, "get vector");
    const auto request = detail::ValidateIdRequest(dataset, "get vector");
    if (request.rows_size == 0) {
        return {};
    }
    if (IsAllNullNullable(engine.native_index)) {
        ThrowInfo(UnexpectedError,
                  "failed to get vector, this nullable vector index contains "
                  "no valid vectors");
    }
    return DispatchPhysicalVectorDataType(
        engine.PhysicalType(),
        [&]<typename T>() -> std::vector<uint8_t> {
            if constexpr (std::is_same_v<T, sparse_u32_f32>) {
                return reject_sparse();
            } else {
                return detail::RetrieveDenseVectors<T>(engine, dataset);
            }
        },
        [&]() -> std::vector<uint8_t> {
            ThrowInfo(DataTypeInvalid,
                      "invalid physical vector data type {}",
                      engine.PhysicalType());
        });
}

std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
VectorIndexReader::GetSparseVector(const DatasetPtr& dataset) const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    if (index_->backend_ == Backend::Disk) {
        ThrowInfo(Unsupported,
                  "sparse vector retrieval is not supported for disk indexes");
    }
    if (engine.PhysicalType() != DataType::VECTOR_SPARSE_U32_F32) {
        ThrowInfo(Unsupported,
                  "sparse vector retrieval is not supported for a dense "
                  "index");
    }

    ValidateGenerationIds(engine, dataset, "get sparse vector");
    const auto request =
        detail::ValidateIdRequest(dataset, "get sparse vector");
    if (request.rows_size == 0) {
        return nullptr;
    }
    if (IsAllNullNullable(engine.native_index)) {
        ThrowInfo(UnexpectedError,
                  "failed to get vector, this nullable vector index contains "
                  "no valid vectors");
    }
    auto retrieved = engine.native_index.GetVectorByIds(dataset);
    if (!retrieved.has_value()) {
        detail::ThrowRetrievalError("get sparse vector", retrieved);
    }
    const auto& result = retrieved.value();
    if (result == nullptr) {
        ThrowInfo(KnowhereError,
                  "knowhere returned a null sparse-vector dataset");
    }
    if (result->GetRows() != request.rows) {
        ThrowInfo(KnowhereError,
                  "knowhere sparse-vector row count {} disagrees with "
                  "requested count {}",
                  result->GetRows(),
                  request.rows);
    }
    if (!result->GetIsSparse()) {
        ThrowInfo(KnowhereError,
                  "knowhere sparse-vector result is not marked sparse");
    }
    const auto* tensor =
        static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
            result->GetTensor());
    if (tensor == nullptr) {
        ThrowInfo(KnowhereError, "knowhere sparse-vector result has no tensor");
    }

    // Detach only after all validation. If SetTensor throws, the dataset still
    // owns the SparseRow[]; afterwards unique_ptr construction is non-throwing.
    result->SetTensor(nullptr);
    return std::unique_ptr<
        const knowhere::sparse::SparseRow<SparseValueType>[]>(tensor);
}

MetricType
VectorIndexReader::Metric() const {
    if (const auto* cached =
            std::get_if<VectorIndex::CachedEngine>(&index_->engine_)) {
        return cached->metric;
    }
    const auto& engine =
        *std::get<std::shared_ptr<const KnowhereEngine>>(index_->engine_);
    return engine.Metric();
}

IndexType
VectorIndexReader::KnowhereIndexType() const {
    if (const auto* cached =
            std::get_if<VectorIndex::CachedEngine>(&index_->engine_)) {
        return cached->index_type;
    }
    const auto& engine =
        *std::get<std::shared_ptr<const KnowhereEngine>>(index_->engine_);
    return engine.KnowhereIndexType();
}

int64_t
VectorIndexReader::Dim() const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    return engine.Dim();
}

knowhere::Json
VectorIndexReader::PrepareSearchParams(const VectorSearchParams& params) const {
    if (!IsGrowingGeneration()) {
        return PrepareVectorSearchParams(params);
    }
    const auto& engine_pin = GetEngine();
    return PrepareVectorSearchParams(
        EffectiveSearchParams(*engine_pin, params));
}

bool
VectorIndexReader::HasValidData() const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    return EngineHasValidity(engine);
}

int64_t
VectorIndexReader::ValidCount() const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    // A growing generation reports the vector count it froze, not the live
    // count of the shared engine.
    if (IsGrowingGeneration()) {
        return index_->physical_count_.value();
    }
    const auto result = engine.native_index.ValidCount();
    if (!result.has_value()) {
        ThrowInfo(KnowhereStatusToErrorCode(result.error()),
                  "failed to read valid count: {}",
                  result.what());
    }
    return static_cast<int64_t>(result.value());
}

bool
VectorIndexReader::IsRowValid(int64_t logical_offset) const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    if (IsGrowingGeneration() &&
        (logical_offset < 0 || logical_offset >= index_->logical_count_)) {
        return false;
    }
    return EngineIsRowValid(engine, logical_offset);
}

void
VectorIndexReader::ApplyValidDataByRange(int64_t logical_offset,
                                         int64_t count,
                                         TargetBitmapView valid_result) const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    AssertInfo(count >= 0 && valid_result.size() == static_cast<size_t>(count),
               "validity range result size mismatch");
    const auto status =
        engine.native_index.IntersectValidity(logical_offset,
                                              static_cast<size_t>(count),
                                              ValidityTarget(valid_result));
    if (status != knowhere::Status::success) {
        ThrowInfo(KnowhereStatusToErrorCode(status),
                  "failed to apply validity range: {}",
                  knowhere::Status2String(status));
    }
    if (IsGrowingGeneration()) {
        for (int64_t i = 0; i < count; ++i) {
            const auto row = static_cast<uint64_t>(logical_offset) +
                             static_cast<uint64_t>(i);
            if (row >= static_cast<uint64_t>(index_->logical_count_)) {
                valid_result[i] = false;
            }
        }
    }
}

void
VectorIndexReader::ApplyValidDataByOffsets(
    const int64_t* offsets,
    int64_t count,
    TargetBitmapView valid_result) const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    AssertInfo(count >= 0 && valid_result.size() == static_cast<size_t>(count),
               "validity offset result size mismatch");
    const auto status = engine.native_index.IntersectValidityAt(
        offsets, static_cast<size_t>(count), ValidityTarget(valid_result));
    if (status != knowhere::Status::success) {
        ThrowInfo(KnowhereStatusToErrorCode(status),
                  "failed to apply validity offsets: {}",
                  knowhere::Status2String(status));
    }
    if (IsGrowingGeneration()) {
        for (int64_t i = 0; i < count; ++i) {
            if (offsets[i] < 0 || offsets[i] >= index_->logical_count_) {
                valid_result[i] = false;
            }
        }
    }
}

knowhere::expected<knowhere::DataSetPtr>
VectorIndexReader::CalcDistByIDs(const knowhere::DataSetPtr& query_dataset,
                                 const BitsetView& bitset,
                                 const int64_t* labels,
                                 size_t labels_len,
                                 bool is_cosine) const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    ValidateGenerationIds(engine, labels, labels_len, "calculate distance");
    return engine.native_index.CalcDistByIDs(
        query_dataset, bitset, labels, labels_len, is_cosine, op_ctx_);
}

std::pair<std::vector<uint8_t>, std::vector<size_t>>
VectorIndexReader::GetEmbListByIds(const DatasetPtr& dataset,
                                   const std::string& metric_type) const {
    const auto& engine_pin = GetEngine();
    const auto& engine = *engine_pin;
    using Result = std::pair<std::vector<uint8_t>, std::vector<size_t>>;
    return DispatchPhysicalVectorDataType(
        engine.PhysicalType(),
        [&]<typename T>() -> Result {
            if constexpr (std::is_same_v<T, sparse_u32_f32>) {
                ThrowInfo(Unsupported,
                          "sparse vectors are not supported as embedding-list "
                          "elements");
            } else {
                return detail::RetrieveEmbeddingLists<T>(
                    engine, dataset, metric_type);
            }
        },
        [&]() -> Result {
            ThrowInfo(DataTypeInvalid,
                      "invalid physical vector data type {}",
                      engine.PhysicalType());
        });
}

bool
VectorIndexReader::IsGrowingGeneration() const {
    return index_->growing_search_defaults_.has_value();
}

VectorSearchParams
VectorIndexReader::EffectiveSearchParams(
    const KnowhereEngine& engine, const VectorSearchParams& params) const {
    auto effective = params;
    effective.metric_type_ = engine.Metric();
    effective.search_params_ = *index_->growing_search_defaults_;
    for (const auto* key : kGrowingQueryOverrides) {
        if (params.search_params_.contains(key)) {
            effective.search_params_[key] = params.search_params_.at(key);
        }
    }
    if (engine.Metric() == knowhere::metric::BM25) {
        effective.search_params_[knowhere::meta::BM25_AVGDL] =
            params.search_params_.at(knowhere::meta::BM25_AVGDL);
    }
    return effective;
}

void
VectorIndexReader::FillEmptySearchResult(const KnowhereEngine& engine,
                                         const DatasetPtr& dataset,
                                         const VectorSearchParams& params,
                                         SearchResult& result) const {
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine.PhysicalType(), engine.Dim());
    const auto result_count = detail::CheckedResultCount(shape, params.topk_);
    result.seg_offsets_.assign(result_count, INVALID_SEG_OFFSET);
    result.distances_.assign(result_count, 0.0F);
    result.total_nq_ = shape.logical_nq;
    result.unity_topK_ = params.topk_;
}

void
VectorIndexReader::SearchMemory(const KnowhereEngine& engine,
                                const DatasetPtr& dataset,
                                const VectorSearchParams& params,
                                const BitsetView& bitset,
                                milvus::OpContext* op_ctx,
                                SearchResult& result) const {
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine.PhysicalType(), engine.Dim());
    const auto topk = params.topk_;
    const auto result_count = detail::CheckedResultCount(shape, topk);
    auto search_conf = PrepareVectorSearchParams(params);

    if (IsEmptyEngine(engine)) {
        result.seg_offsets_.assign(result_count, INVALID_SEG_OFFSET);
        result.distances_.assign(result_count, 0.0F);
        result.total_nq_ = shape.logical_nq;
        result.unity_topK_ = topk;
        return;
    }

    auto final = [&] {
        if (CheckAndUpdateKnowhereRangeSearchParam(
                params, topk, engine.Metric(), search_conf)) {
            milvus::tracer::AddEvent("start_knowhere_index_range_search");
            auto search = engine.native_index.RangeSearch(
                dataset, search_conf, bitset, op_ctx);
            milvus::tracer::AddEvent("finish_knowhere_index_range_search");
            if (!search.has_value()) {
                const auto status = search.error();
                ThrowInfo(KnowhereStatusToErrorCode(status),
                          "failed to range search: status {} ({}), detail: {}",
                          static_cast<int>(status),
                          knowhere::Status2String(status),
                          search.what());
            }
            auto regenerated = milvus::ReGenRangeSearchResult(
                search.value(), topk, shape.logical_nq, engine.Metric());
            milvus::tracer::AddEvent("finish_ReGenRangeSearchResult");
            return regenerated;
        }

        milvus::tracer::AddEvent("start_knowhere_index_search");
        auto search =
            engine.native_index.Search(dataset, search_conf, bitset, op_ctx);
        milvus::tracer::AddEvent("finish_knowhere_index_search");
        if (!search.has_value()) {
            const auto status = search.error();
            ThrowInfo(KnowhereStatusToErrorCode(status),
                      "failed to search: config={} status {} ({}), detail: {}",
                      milvus::EscapeBraces(search_conf.dump()),
                      static_cast<int>(status),
                      knowhere::Status2String(status),
                      search.what());
        }
        return search.value();
    }();

    FillRegularSearchResult(final, shape, topk, result_count, result);
}

void
VectorIndexReader::SearchDisk(const KnowhereEngine& engine,
                              const DatasetPtr& dataset,
                              const VectorSearchParams& params,
                              const BitsetView& bitset,
                              milvus::OpContext* op_ctx,
                              SearchResult& result) const {
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine.PhysicalType(), engine.Dim());
    if (params.metric_type_ != engine.Metric()) {
        ThrowInfo(MetricTypeInvalid,
                  "vector query metric {} disagrees with index metric {}",
                  params.metric_type_,
                  engine.Metric());
    }
    const auto topk = params.topk_;
    const auto result_count = detail::CheckedResultCount(shape, topk);
    auto search_config = PrepareVectorSearchParams(params);

    if (IsEmptyEngine(engine)) {
        result.seg_offsets_.assign(result_count, INVALID_SEG_OFFSET);
        result.distances_.assign(result_count, 0.0F);
        result.total_nq_ = shape.logical_nq;
        result.unity_topK_ = topk;
        return;
    }

    if (engine.KnowhereIndexType() == knowhere::IndexEnum::INDEX_DISKANN) {
        ValidateDiskAnnBeamwidth(engine, index_->disk_ann_beamwidth_);
        if (params.search_params_.contains(DISK_ANN_QUERY_LIST)) {
            search_config[DISK_ANN_SEARCH_LIST_SIZE] =
                params.search_params_.at(DISK_ANN_QUERY_LIST);
        }
        search_config[DISK_ANN_QUERY_BEAMWIDTH] =
            static_cast<int32_t>(index_->disk_ann_beamwidth_);
        search_config[DISK_ANN_PQ_CODE_BUDGET] = 0.0;
    }

    auto final = [&] {
        if (CheckAndUpdateKnowhereRangeSearchParam(
                params, topk, engine.Metric(), search_config)) {
            auto search = engine.native_index.RangeSearch(
                dataset, search_config, bitset, op_ctx);
            if (!search.has_value()) {
                ThrowSearchError("range search", search);
            }
            return milvus::ReGenRangeSearchResult(
                search.value(), topk, shape.logical_nq, engine.Metric());
        }

        auto search =
            engine.native_index.Search(dataset, search_config, bitset, op_ctx);
        if (!search.has_value()) {
            ThrowSearchError("search", search, &search_config);
        }
        return search.value();
    }();

    FillRegularSearchResult(final, shape, topk, result_count, result);
}

BitsetView
VectorIndexReader::BoundSearchBitset(const BitsetView& bitset,
                                     TargetBitmap& storage) const {
    if (!IsGrowingGeneration()) {
        return bitset;
    }

    // Bitsets are public-row keyed (#50524): knowhere projects them onto the
    // backend id domain through the IdMap, and every backend id whose public
    // row falls outside this bitmap is filtered. Bounding in logical space is
    // therefore what keeps rows appended after publication out of the top-k.
    const auto prefix = CheckedInputSize(index_->logical_count_,
                                         "growing vector logical row count");
    BitsetView bounded;
    if (bitset.empty()) {
        storage = TargetBitmap(prefix, false);
        bounded = BitsetView(storage);
    } else if (!bitset.has_out_ids() && bitset.size() <= prefix) {
        bounded = bitset;
    } else {
        // A mapped view's out_ids array covers its original domain.
        // Materialize it before a live engine can return newer IDs beyond that
        // borrowed array.
        storage = TargetBitmap(prefix, true);
        const auto copy_count = std::min(prefix, bitset.size());
        for (size_t i = 0; i < copy_count; ++i) {
            storage[i] = bitset.test(static_cast<int64_t>(i));
        }
        bounded = BitsetView(storage);
    }
    // While an Add is in flight a live engine already holds the new backend
    // ids but has not advanced its count, so knowhere sees no ids beyond the
    // bitmap and skips the boundary filter unless it is required explicitly.
    bounded.set_require_id_boundary(true);
    return bounded;
}

BitsetView
VectorIndexReader::BoundIteratorBitset(const BitsetView& bitset) const {
    if (!IsGrowingGeneration()) {
        return bitset;
    }

    const auto prefix = CheckedInputSize(index_->logical_count_,
                                         "growing vector logical row count");
    AssertInfo(!bitset.empty(),
               "growing vector iterators require a caller-owned logical "
               "prefix bitmap");
    AssertInfo(!bitset.has_out_ids(),
               "growing vector iterators require a materialized logical "
               "prefix bitmap");
    AssertInfo(bitset.size() <= prefix,
               "growing vector iterator bitmap size {} exceeds frozen "
               "logical row count {}",
               bitset.size(),
               prefix);
    auto bounded = bitset;
    // Same in-flight Add window as BoundSearchBitset.
    bounded.set_require_id_boundary(true);
    return bounded;
}

void
VectorIndexReader::ValidateGenerationIds(const KnowhereEngine& engine,
                                         const DatasetPtr& dataset,
                                         const char* operation) const {
    if (!IsGrowingGeneration()) {
        return;
    }
    const auto request = detail::ValidateIdRequest(dataset, operation);
    ValidateGenerationIds(engine, request.ids, request.rows_size, operation);
}

void
VectorIndexReader::ValidateGenerationIds(const KnowhereEngine& engine,
                                         const int64_t* ids,
                                         size_t count,
                                         const char* operation) const {
    if (!IsGrowingGeneration()) {
        return;
    }
    AssertInfo(count == 0 || ids != nullptr,
               "{} ids are null for {} entries",
               operation,
               count);
    for (size_t i = 0; i < count; ++i) {
        AssertInfo(ids[i] >= 0 && ids[i] < index_->logical_count_,
                   "{} id {} is outside the frozen logical prefix [0, {})",
                   operation,
                   ids[i],
                   index_->logical_count_);
        AssertInfo(EngineIsRowValid(engine, ids[i]),
                   "{} id {} is a null row of this nullable vector index",
                   operation,
                   ids[i]);
    }
}

bool
VectorIndexReader::IsEmptyEngine(const KnowhereEngine& engine) const {
    if (IsGrowingGeneration()) {
        return index_->physical_count_ == 0;
    }
    return IsAllNullNullable(engine.native_index) ||
           engine.IsEmptyEmbListIndex();
}

}  // namespace milvus::index
