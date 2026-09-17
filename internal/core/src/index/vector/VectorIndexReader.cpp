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
#include <memory>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"
#include "index/Meta.h"
#include "index/vector/RangeSearchParams.h"
#include "index/vector/VectorReaderUtils.h"
#include "index/vector/VectorReaderValidation.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/segcore_error_code.h"

namespace milvus::index {
namespace {

constexpr uint32_t kMinDiskAnnBeamwidth = 1;
constexpr uint32_t kMaxDiskAnnBeamwidth = 128;

void
ValidateDiskAnnBeamwidth(const KnowhereEngine& engine, uint32_t beamwidth) {
    if (engine.KnowhereIndexType() != knowhere::IndexEnum::INDEX_DISKANN) {
        return;
    }
    if (beamwidth < kMinDiskAnnBeamwidth || beamwidth > kMaxDiskAnnBeamwidth) {
        ThrowInfo(ConfigInvalid,
                  "DiskANN beamwidth {} is outside [{}, {}]",
                  beamwidth,
                  kMinDiskAnnBeamwidth,
                  kMaxDiskAnnBeamwidth);
    }
}

[[noreturn]] void
ThrowSearchError(const char* operation,
                 const knowhere::expected<knowhere::DataSetPtr>& result,
                 const knowhere::Json* config = nullptr) {
    const auto status = result.error();
    if (config == nullptr) {
        ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                  "failed to {}: status {} ({}), detail: {}",
                  operation,
                  static_cast<int>(status),
                  knowhere::Status2String(status),
                  result.what());
    }
    ThrowInfo(knowhere::ToSegcoreErrorCode(status),
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
        detail::CheckedSystemBytes(result_count, sizeof(int64_t), "search id");
    const auto distance_bytes = detail::CheckedSystemBytes(
        result_count, sizeof(float), "search distance");

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

}  // namespace

VectorIndexReader::VectorIndexReader(KnowhereEngine engine,
                                     VectorValidData valid)
    : engine_(std::move(engine)),
      valid_(std::move(valid)),
      physical_count_((valid_.Enabled() && valid_.ValidCount() == 0) ||
                              engine_.IsEmptyEmbListIndex()
                          ? 0
                          : engine_.native_index.Count()) {
}

VectorIndexReader::VectorIndexReader(
    uint32_t disk_ann_beamwidth,
    KnowhereEngine engine,
    VectorValidData valid)
    : engine_(std::move(engine)),
      valid_(std::move(valid)),
      backend_(Backend::Disk),
      disk_ann_beamwidth_(disk_ann_beamwidth),
      physical_count_((valid_.Enabled() && valid_.ValidCount() == 0) ||
                              engine_.IsEmptyEmbListIndex()
                          ? 0
                          : engine_.native_index.Count()) {
    ValidateDiskAnnBeamwidth(engine_, disk_ann_beamwidth_);
}

VectorIndexReader::VectorIndexReader(KnowhereEngine engine,
                                     VectorValidData valid,
                                     int64_t physical_count,
                                     knowhere::Json search_defaults)
    : engine_(std::move(engine)),
      valid_(std::move(valid)),
      backend_(Backend::Memory),
      physical_count_(physical_count),
      growing_search_defaults_(std::move(search_defaults)) {
    AssertInfo(physical_count_ >= 0,
               "growing vector reader physical count {} is negative",
               physical_count_);
    if (physical_count_ > 0) {
        const auto engine_count = engine_.native_index.Count();
        AssertInfo(physical_count_ == engine_count,
                   "growing vector reader physical count {} disagrees with "
                   "engine count {}",
                   physical_count_,
                   engine_count);
    }
    AssertInfo(!valid_.Enabled() || valid_.ValidCount() == physical_count_,
               "growing vector reader physical count {} disagrees with "
               "validity count {}",
               physical_count_,
               valid_.ValidCount());
}

cachinglayer::ResourceUsage
VectorIndexReader::CellByteSize() const {
    // Knowhere exposes no stable owned memory/file breakdown here. Zero means
    // unavailable, not a measured empty footprint.
    return {};
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
    return IsEmptyEngine() ? 0 : physical_count_;
}

DataType
VectorIndexReader::ValueType() const {
    return engine_.PhysicalType();
}

int64_t
VectorIndexReader::MemoryUsage() const {
    // Native resident accounting is unavailable; zero is not a measurement.
    return 0;
}

void
VectorIndexReader::Search(const DatasetPtr& dataset,
                          const VectorSearchParams& params,
                          const BitsetView& bitset,
                          milvus::OpContext* op_ctx,
                          SearchResult& result) const {
    if (!IsGrowingGeneration()) {
        if (backend_ == Backend::Disk) {
            SearchDisk(dataset, params, bitset, op_ctx, result);
        } else {
            SearchMemory(dataset, params, bitset, op_ctx, result);
        }
        return;
    }
    if (physical_count_ == 0) {
        FillEmptySearchResult(dataset, params, result);
        return;
    }

    TargetBitmap prefix_storage;
    const auto bounded = BoundSearchBitset(bitset, prefix_storage);
    const auto effective = EffectiveSearchParams(params);
    SearchMemory(dataset, effective, bounded, op_ctx, result);
}

knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
VectorIndexReader::Iterators(const DatasetPtr& dataset,
                             const knowhere::Json& json,
                             const BitsetView& bitset,
                             milvus::OpContext* op_ctx) const {
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine_.PhysicalType(), engine_.Dim());
    if (IsEmptyEngine() ||
        (IsGrowingGeneration() && physical_count_ == 0)) {
        return detail::MakeEmptyVectorIterators(shape.logical_nq_size);
    }
    const auto bounded = BoundIteratorBitset(bitset);
    return engine_.native_index.AnnIterator(
        dataset, json, bounded, false, op_ctx);
}

bool
VectorIndexReader::RefineEnabled() const {
    return !IsEmptyEngine() && engine_.native_index.IsIndexRefineEnabled();
}

bool
VectorIndexReader::HasRawData() const {
    return IsEmptyEngine() || engine_.native_index.HasRawData(engine_.Metric());
}

std::vector<uint8_t>
VectorIndexReader::GetVector(const DatasetPtr& dataset) const {
    if (engine_.PhysicalType() == DataType::VECTOR_SPARSE_U32_F32) {
        ThrowInfo(Unsupported,
                  backend_ == Backend::Disk
                      ? "dense vector retrieval is not supported for a sparse "
                        "disk index"
                      : "dense vector retrieval is not supported for a sparse "
                        "index");
    }
    ValidatePhysicalIds(dataset, "get vector");
    switch (engine_.PhysicalType()) {
        case DataType::VECTOR_FLOAT:
            return detail::RetrieveDenseVectors<float>(engine_, dataset);
        case DataType::VECTOR_BINARY:
            return detail::RetrieveDenseVectors<bin1>(engine_, dataset);
        case DataType::VECTOR_FLOAT16:
            return detail::RetrieveDenseVectors<float16>(engine_, dataset);
        case DataType::VECTOR_BFLOAT16:
            return detail::RetrieveDenseVectors<bfloat16>(engine_, dataset);
        case DataType::VECTOR_INT8:
            return detail::RetrieveDenseVectors<int8>(engine_, dataset);
        default:
            break;
    }
    ThrowInfo(DataTypeInvalid,
              "invalid physical vector data type {}",
              engine_.PhysicalType());
}

std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
VectorIndexReader::GetSparseVector(const DatasetPtr& dataset) const {
    if (backend_ == Backend::Disk) {
        ThrowInfo(Unsupported,
                  "sparse vector retrieval is not supported for disk indexes");
    }
    if (engine_.PhysicalType() != DataType::VECTOR_SPARSE_U32_F32) {
        ThrowInfo(Unsupported,
                  "sparse vector retrieval is not supported for a dense "
                  "index");
    }

    ValidatePhysicalIds(dataset, "get sparse vector");
    const auto request = detail::ValidateIdRequest(dataset, "get sparse vector");
    if (request.rows_size == 0) {
        return nullptr;
    }

    auto retrieved = engine_.native_index.GetVectorByIds(dataset);
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
        ThrowInfo(KnowhereError,
                  "knowhere sparse-vector result has no tensor");
    }

    // Detach only after all validation. If SetTensor throws, the dataset still
    // owns the SparseRow[]; afterwards unique_ptr construction is non-throwing.
    result->SetTensor(nullptr);
    return std::unique_ptr<
        const knowhere::sparse::SparseRow<SparseValueType>[]>(tensor);
}

MetricType
VectorIndexReader::Metric() const {
    return engine_.Metric();
}

IndexType
VectorIndexReader::KnowhereIndexType() const {
    return engine_.KnowhereIndexType();
}

int64_t
VectorIndexReader::Dim() const {
    return engine_.Dim();
}

knowhere::Json
VectorIndexReader::PrepareSearchParams(
    const VectorSearchParams& params) const {
    if (!IsGrowingGeneration()) {
        return PrepareVectorSearchParams(params);
    }
    return PrepareVectorSearchParams(EffectiveSearchParams(params));
}

bool
VectorIndexReader::HasValidData() const {
    return valid_.Enabled();
}

int64_t
VectorIndexReader::ValidCount() const {
    return valid_.ValidCount();
}

bool
VectorIndexReader::IsRowValid(int64_t logical_offset) const {
    return valid_.IsRowValid(logical_offset);
}

int64_t
VectorIndexReader::PhysicalOffset(int64_t logical_offset) const {
    return valid_.PhysicalOffset(logical_offset);
}

int64_t
VectorIndexReader::LogicalOffset(int64_t physical_offset) const {
    return valid_.LogicalOffset(physical_offset);
}

const milvus::OffsetMapping&
VectorIndexReader::OffsetMapping() const {
    return valid_.Mapping();
}

knowhere::expected<knowhere::DataSetPtr>
VectorIndexReader::CalcDistByIDs(const knowhere::DataSetPtr& query_dataset,
                                 const BitsetView& bitset,
                                 const int64_t* labels,
                                 size_t labels_len,
                                 bool is_cosine,
                                 milvus::OpContext* op_ctx) const {
    ValidatePhysicalIds(labels, labels_len, "calculate distance");
    return engine_.native_index.CalcDistByIDs(
        query_dataset, bitset, labels, labels_len, is_cosine, op_ctx);
}

std::pair<std::vector<uint8_t>, std::vector<size_t>>
VectorIndexReader::GetEmbListByIds(const DatasetPtr& dataset,
                                   const std::string& metric_type) const {
    switch (engine_.PhysicalType()) {
        case DataType::VECTOR_FLOAT:
            return detail::RetrieveEmbeddingLists<float>(
                engine_, dataset, metric_type);
        case DataType::VECTOR_BINARY:
            return detail::RetrieveEmbeddingLists<bin1>(
                engine_, dataset, metric_type);
        case DataType::VECTOR_FLOAT16:
            return detail::RetrieveEmbeddingLists<float16>(
                engine_, dataset, metric_type);
        case DataType::VECTOR_BFLOAT16:
            return detail::RetrieveEmbeddingLists<bfloat16>(
                engine_, dataset, metric_type);
        case DataType::VECTOR_INT8:
            return detail::RetrieveEmbeddingLists<int8>(
                engine_, dataset, metric_type);
        case DataType::VECTOR_SPARSE_U32_F32:
            ThrowInfo(Unsupported,
                      "sparse vectors are not supported as embedding-list "
                      "elements");
        default:
            ThrowInfo(DataTypeInvalid,
                      "invalid physical vector data type {}",
                      engine_.PhysicalType());
    }
}

bool
VectorIndexReader::IsGrowingGeneration() const {
    return growing_search_defaults_.has_value();
}

VectorSearchParams
VectorIndexReader::EffectiveSearchParams(
    const VectorSearchParams& params) const {
    auto effective = params;
    effective.metric_type_ = engine_.Metric();
    effective.search_params_ = *growing_search_defaults_;
    for (const auto* key : kGrowingQueryOverrides) {
        if (params.search_params_.contains(key)) {
            effective.search_params_[key] = params.search_params_.at(key);
        }
    }
    if (engine_.Metric() == knowhere::metric::BM25) {
        effective.search_params_[knowhere::meta::BM25_AVGDL] =
            params.search_params_.at(knowhere::meta::BM25_AVGDL);
    }
    return effective;
}

void
VectorIndexReader::FillEmptySearchResult(const DatasetPtr& dataset,
                                         const VectorSearchParams& params,
                                         SearchResult& result) const {
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine_.PhysicalType(), engine_.Dim());
    const auto result_count = detail::CheckedResultCount(shape, params.topk_);
    result.seg_offsets_.assign(result_count, INVALID_SEG_OFFSET);
    result.distances_.assign(result_count, 0.0F);
    result.total_nq_ = shape.logical_nq;
    result.unity_topK_ = params.topk_;
}

void
VectorIndexReader::SearchMemory(const DatasetPtr& dataset,
                                const VectorSearchParams& params,
                                const BitsetView& bitset,
                                milvus::OpContext* op_ctx,
                                SearchResult& result) const {
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine_.PhysicalType(), engine_.Dim());
    const auto topk = params.topk_;
    const auto result_count = detail::CheckedResultCount(shape, topk);
    auto search_conf = PrepareVectorSearchParams(params);

    if (IsEmptyEngine()) {
        result.seg_offsets_.assign(result_count, INVALID_SEG_OFFSET);
        result.distances_.assign(result_count, 0.0F);
        result.total_nq_ = shape.logical_nq;
        result.unity_topK_ = topk;
        return;
    }

    auto final = [&] {
        if (CheckAndUpdateKnowhereRangeSearchParam(
                params, topk, engine_.Metric(), search_conf)) {
            milvus::tracer::AddEvent("start_knowhere_index_range_search");
            auto search = engine_.native_index.RangeSearch(
                dataset, search_conf, bitset, op_ctx);
            milvus::tracer::AddEvent("finish_knowhere_index_range_search");
            if (!search.has_value()) {
                const auto status = search.error();
                ThrowInfo(
                    knowhere::ToSegcoreErrorCode(status),
                    "failed to range search: status {} ({}), detail: {}",
                    static_cast<int>(status),
                    knowhere::Status2String(status),
                    search.what());
            }
            auto regenerated = milvus::ReGenRangeSearchResult(
                search.value(), topk, shape.logical_nq, engine_.Metric());
            milvus::tracer::AddEvent("finish_ReGenRangeSearchResult");
            return regenerated;
        }

        milvus::tracer::AddEvent("start_knowhere_index_search");
        auto search =
            engine_.native_index.Search(dataset, search_conf, bitset, op_ctx);
        milvus::tracer::AddEvent("finish_knowhere_index_search");
        if (!search.has_value()) {
            const auto status = search.error();
            ThrowInfo(knowhere::ToSegcoreErrorCode(status),
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
VectorIndexReader::SearchDisk(const DatasetPtr& dataset,
                              const VectorSearchParams& params,
                              const BitsetView& bitset,
                              milvus::OpContext* op_ctx,
                              SearchResult& result) const {
    const auto shape = detail::ValidateQueryDataset(
        dataset, engine_.PhysicalType(), engine_.Dim());
    if (params.metric_type_ != engine_.Metric()) {
        ThrowInfo(MetricTypeInvalid,
                  "vector query metric {} disagrees with index metric {}",
                  params.metric_type_,
                  engine_.Metric());
    }
    const auto topk = params.topk_;
    const auto result_count = detail::CheckedResultCount(shape, topk);
    auto search_config = PrepareVectorSearchParams(params);

    if (IsEmptyEngine()) {
        result.seg_offsets_.assign(result_count, INVALID_SEG_OFFSET);
        result.distances_.assign(result_count, 0.0F);
        result.total_nq_ = shape.logical_nq;
        result.unity_topK_ = topk;
        return;
    }

    if (engine_.KnowhereIndexType() == knowhere::IndexEnum::INDEX_DISKANN) {
        ValidateDiskAnnBeamwidth(engine_, disk_ann_beamwidth_);
        if (params.search_params_.contains(DISK_ANN_QUERY_LIST)) {
            search_config[DISK_ANN_SEARCH_LIST_SIZE] =
                params.search_params_.at(DISK_ANN_QUERY_LIST);
        }
        search_config[DISK_ANN_QUERY_BEAMWIDTH] =
            static_cast<int32_t>(disk_ann_beamwidth_);
        search_config[DISK_ANN_PQ_CODE_BUDGET] = 0.0;
    }

    auto final = [&] {
        if (CheckAndUpdateKnowhereRangeSearchParam(
                params, topk, engine_.Metric(), search_config)) {
            auto search = engine_.native_index.RangeSearch(
                dataset, search_config, bitset, op_ctx);
            if (!search.has_value()) {
                ThrowSearchError("range search", search);
            }
            return milvus::ReGenRangeSearchResult(
                search.value(), topk, shape.logical_nq, engine_.Metric());
        }

        auto search = engine_.native_index.Search(
            dataset, search_config, bitset, op_ctx);
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

    const auto prefix = detail::CheckedInputSize(
        physical_count_, "growing vector physical count");
    if (bitset.empty()) {
        storage = TargetBitmap(prefix, false);
        return BitsetView(storage);
    }
    if (!bitset.has_out_ids() && bitset.size() <= prefix) {
        return bitset;
    }

    // A mapped view's out_ids array covers its original domain. Materialize it
    // before a live engine can return newer IDs beyond that borrowed array.
    storage = TargetBitmap(prefix, true);
    const auto copy_count = std::min(prefix, bitset.size());
    for (size_t i = 0; i < copy_count; ++i) {
        storage[i] = bitset.test(static_cast<int64_t>(i));
    }
    return BitsetView(storage);
}

BitsetView
VectorIndexReader::BoundIteratorBitset(const BitsetView& bitset) const {
    if (!IsGrowingGeneration()) {
        return bitset;
    }

    const auto prefix = detail::CheckedInputSize(
        physical_count_, "growing vector physical count");
    AssertInfo(!bitset.empty(),
               "growing vector iterators require a caller-owned physical "
               "prefix bitmap");
    AssertInfo(!bitset.has_out_ids(),
               "growing vector iterators require a materialized physical "
               "prefix bitmap");
    AssertInfo(bitset.size() <= prefix,
               "growing vector iterator bitmap size {} exceeds frozen "
               "physical count {}",
               bitset.size(),
               prefix);
    return bitset;
}

void
VectorIndexReader::ValidatePhysicalIds(const DatasetPtr& dataset,
                                       const char* operation) const {
    if (!IsGrowingGeneration()) {
        return;
    }
    const auto request = detail::ValidateIdRequest(dataset, operation);
    ValidatePhysicalIds(request.ids, request.rows_size, operation);
}

void
VectorIndexReader::ValidatePhysicalIds(const int64_t* ids,
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
        AssertInfo(ids[i] >= 0 && ids[i] < physical_count_,
                   "{} id {} is outside frozen physical prefix [0, {})",
                   operation,
                   ids[i],
                   physical_count_);
    }
}

bool
VectorIndexReader::IsEmptyEngine() const {
    return (valid_.Enabled() && valid_.ValidCount() == 0) ||
           engine_.IsEmptyEmbListIndex();
}

}  // namespace milvus::index
