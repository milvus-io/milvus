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

#include "index/growing/KnowhereGrowingVectorIndex.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "index/Families.h"
#include "index/vector/VectorIndexReader.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "log/Log.h"

namespace milvus::index {
namespace {

template <typename T>
constexpr bool kSparseVector = std::is_same_v<T, sparse_u32_f32>;

int64_t
CheckedLogicalRows(size_t rows) {
    AssertInfo(rows <=
                   static_cast<size_t>(std::numeric_limits<int64_t>::max()),
               "growing vector logical row count exceeds int64");
    return static_cast<int64_t>(rows);
}

template <typename T>
size_t
SourceElementCount(int64_t physical_rows, int64_t dense_dim) {
    AssertInfo(physical_rows >= 0,
               "growing vector physical row count is negative");
    const auto rows = static_cast<size_t>(physical_rows);
    if constexpr (kSparseVector<T>) {
        return rows;
    } else {
        AssertInfo(dense_dim > 0,
                   "growing dense vector dimension must be positive");
        const auto dim = static_cast<size_t>(dense_dim);
        AssertInfo(rows <= std::numeric_limits<size_t>::max() / dim,
                   "growing vector element count overflows size_t");
        return rows * dim;
    }
}

template <typename T>
int64_t
SparseDimension(std::span<const GrowingVectorStorageType<T>> values) {
    if constexpr (!kSparseVector<T>) {
        return 0;
    } else {
        int64_t result = 0;
        for (const auto& row : values) {
            result = std::max(result, row.dim());
        }
        return result;
    }
}

template <typename T, typename Consume>
void
WithSourceRows(
    const std::shared_ptr<
        const GrowingVectorSource<GrowingVectorStorageType<T>>>& source,
    int64_t physical_begin,
    int64_t physical_rows,
    int64_t dense_dim,
    Consume&& consume) {
    AssertInfo(source != nullptr, "growing vector source is null");
    AssertInfo(physical_begin >= 0 && physical_rows > 0,
               "invalid growing vector source range [{}, {})",
               physical_begin,
               physical_begin + physical_rows);
    const auto expected = SourceElementCount<T>(physical_rows, dense_dim);
    auto contiguous = source->ContiguousRows(physical_begin, physical_rows);
    if (!contiguous.empty()) {
        AssertInfo(contiguous.size() == expected,
                   "growing vector contiguous source returned {} elements, "
                   "expected {}",
                   contiguous.size(),
                   expected);
        consume(contiguous);
        return;
    }

    std::vector<GrowingVectorStorageType<T>> copied(expected);
    source->CopyRows(physical_begin, physical_rows, copied.data());
    consume(std::span<const GrowingVectorStorageType<T>>(copied.data(),
                                                         copied.size()));
}

}  // namespace

template <typename T>
KnowhereGrowingVectorIndex<T>::KnowhereGrowingVectorIndex(
    DataType value_type,
    IndexType index_type,
    MetricType metric_type,
    IndexVersion version,
    int64_t dim,
    int64_t build_threshold,
    knowhere::Json build_params,
    knowhere::Json search_defaults,
    std::shared_ptr<
        const GrowingVectorSource<GrowingVectorStorageType<T>>> source,
    bool retain_source_as_data_view,
    std::function<int64_t()> build_thread_num)
    : value_type_(value_type),
      index_type_(std::move(index_type)),
      metric_type_(std::move(metric_type)),
      version_(version),
      dim_(dim),
      build_threshold_(build_threshold),
      build_params_(std::move(build_params)),
      search_defaults_(std::move(search_defaults)),
      retain_source_as_data_view_(retain_source_as_data_view),
      build_thread_num_(std::move(build_thread_num)),
      source_(std::move(source)) {
    constexpr auto physical_type = PhysicalVectorDataType<T>();
    AssertInfo(value_type_ == physical_type,
               "growing vector value type {} disagrees with physical type {}",
               value_type_,
               physical_type);
    AssertInfo(source_ != nullptr, "growing vector source is null");
    AssertInfo(build_threshold_ > 0,
               "growing vector build threshold must be positive");
    if constexpr (kSparseVector<T>) {
        AssertInfo(dim_ >= 0,
                   "growing sparse vector dimension is negative");
    } else {
        AssertInfo(dim_ > 0,
                   "growing dense vector dimension must be positive");
    }
    engine_.emplace(CreateEngine());
}

template <typename T>
KnowhereEngine
KnowhereGrowingVectorIndex<T>::CreateEngine() const {
    KnowhereEngine engine = [&]() {
        if (!retain_source_as_data_view_) {
            return KnowhereEngine(PhysicalVectorDataType<T>(),
                                  DataType::NONE,
                                  index_type_,
                                  metric_type_,
                                  version_,
                                  /*use_knowhere_build_pool=*/true);
        }
        AssertInfo(source_ != nullptr,
                   "source-backed growing vector engine lost its source");
        auto source = source_;
        knowhere::ViewDataOp view_data =
            [source = std::move(source)](size_t physical_offset) {
                return static_cast<const void*>(
                    source->Row(static_cast<int64_t>(physical_offset)));
            };
        return KnowhereEngine(PhysicalVectorDataType<T>(),
                              DataType::NONE,
                              index_type_,
                              metric_type_,
                              version_,
                              std::move(view_data),
                              /*use_knowhere_build_pool=*/true);
    }();
    engine.SetDim(dim_);
    return engine;
}

template <typename T>
knowhere::Json
KnowhereGrowingVectorIndex<T>::BuildConfig() const {
    if (!build_thread_num_) {
        return build_params_;
    }
    auto config = build_params_;
    config[knowhere::meta::NUM_BUILD_THREAD] =
        std::to_string(build_thread_num_());
    return config;
}

template <typename T>
std::vector<uint8_t>
KnowhereGrowingVectorIndex<T>::MaterializeValidity(
    int64_t logical_begin, int64_t logical_count) const {
    AssertInfo(logical_begin >= 0 && logical_count >= 0,
               "invalid growing vector validity range [{}, +{})",
               logical_begin,
               logical_count);
    const auto count = static_cast<size_t>(logical_count);
    std::vector<uint8_t> bitmap(GetValidDataBitmapSize(count), 0);
    if (logical_count == 0) {
        return bitmap;
    }

    // The physical -> logical spine is strictly increasing, so one forward
    // walk over the physical window covering this logical range sets exactly
    // the valid public rows.
    auto physical = validity_.ValidCountBelow(logical_begin);
    const auto physical_end =
        validity_.ValidCountBelow(logical_begin + logical_count);
    while (physical < physical_end) {
        const auto view = validity_.GetPhysicalToLogicalIds(
            physical, physical_end - physical);
        AssertInfo(!view.empty(),
                   "growing vector validity spine has no entry at physical "
                   "offset {}",
                   physical);
        for (int64_t i = 0; i < view.count; ++i) {
            const auto row = static_cast<int64_t>(view.data[i]) - logical_begin;
            AssertInfo(row >= 0 && row < logical_count,
                       "growing vector validity spine returned public row {} "
                       "outside [{}, +{})",
                       view.data[i],
                       logical_begin,
                       logical_count);
            bitmap[static_cast<size_t>(row) >> 3] |=
                static_cast<uint8_t>(1U << (static_cast<size_t>(row) & 7U));
        }
        physical += view.count;
    }
    return bitmap;
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::BuildFromSource(int64_t physical_count,
                                               int64_t logical_count) {
    AssertInfo(physical_count == build_threshold_,
               "growing vector cold build count {} disagrees with threshold {}",
               physical_count,
               build_threshold_);
    auto next = engine_.has_value() ? std::move(*engine_) : CreateEngine();
    engine_.reset();

    // A nullable growing index hands its public-row validity to knowhere's
    // append-only IdMap (#50524); the cold build publishes the logical prefix
    // that covers the first `physical_count` stored vectors.
    const bool nullable = nullable_.value_or(false);
    std::vector<uint8_t> validity_bitmap;
    if (nullable) {
        validity_bitmap = MaterializeValidity(0, logical_count);
        next.native_index.SetIdMapType(knowhere::IdMap::Type::GROWING);
    }
    const auto build_config = BuildConfig();

    WithSourceRows<T>(
        source_,
        0,
        physical_count,
        dim_,
        [&](std::span<const GrowingVectorStorageType<T>> values) {
            const auto dataset_dim =
                kSparseVector<T> ? SparseDimension<T>(values) : dim_;
            next.SetDim(dataset_dim);
            auto dataset =
                knowhere::GenDataSet(physical_count, dataset_dim, values.data());
            if constexpr (kSparseVector<T>) {
                dataset->SetIsSparse(true);
            }
            if (nullable) {
                dataset->SetIdMapData(knowhere::IdMapData::FromValidBitmap(
                    validity_bitmap.data(),
                    static_cast<size_t>(logical_count)));
            }
            const auto status = next.native_index.Build(
                dataset, build_config, next.UseBuildPool());
            if (status != knowhere::Status::success) {
                ThrowInfo(KnowhereBuildStatusToErrorCode(status),
                          "failed to build growing vector index: status {} ({})",
                          static_cast<int>(status),
                          knowhere::Status2String(status));
            }
        });
    next.SetDim(next.native_index.Dim());
    engine_.emplace(std::move(next));
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::AddBatch(
    const GrowingVectorStorageType<T>* values,
    int64_t physical_count,
    int64_t dim,
    const uint8_t* validity_bitmap,
    int64_t logical_count) {
    AssertInfo(built_ && engine_.has_value(),
               "cannot append an unbuilt growing vector engine");
    AssertInfo(physical_count >= 0,
               "growing vector append physical count {} is negative",
               physical_count);
    AssertInfo(physical_count == 0 || values != nullptr,
               "growing vector append has no physical values");
    const bool nullable = validity_bitmap != nullptr;
    // A nullable batch must reach knowhere even when every row is null: the
    // IdMap's public row domain has to advance with the segment, and an armed
    // IdMap rejects any batch that carries no id-map input.
    AssertInfo(nullable || physical_count > 0,
               "non-nullable growing vector append has no physical values");
    auto dataset = knowhere::GenDataSet(
        physical_count, dim, physical_count == 0 ? nullptr : values);
    if constexpr (kSparseVector<T>) {
        dataset->SetIsSparse(true);
    }
    if (nullable) {
        engine_->native_index.SetIdMapType(knowhere::IdMap::Type::GROWING);
        dataset->SetIdMapData(knowhere::IdMapData::FromValidBitmap(
            validity_bitmap, static_cast<size_t>(logical_count)));
    }
    const auto status = engine_->native_index.Add(
        dataset, BuildConfig(), engine_->UseBuildPool());
    if (status != knowhere::Status::success) {
        ThrowInfo(KnowhereBuildStatusToErrorCode(status),
                  "failed to append growing vector index: status {} ({})",
                  static_cast<int>(status),
                  knowhere::Status2String(status));
    }
    if constexpr (kSparseVector<T>) {
        engine_->SetDim(engine_->native_index.Dim());
    }
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::AddFromSource(int64_t physical_begin,
                                             int64_t physical_count,
                                             int64_t logical_begin,
                                             int64_t logical_count) {
    const bool nullable = nullable_.value_or(false);
    if (physical_count == 0 && (!nullable || logical_count == 0)) {
        return;
    }
    std::vector<uint8_t> validity_bitmap;
    if (nullable) {
        validity_bitmap = MaterializeValidity(logical_begin, logical_count);
    }
    if (physical_count == 0) {
        AddBatch(nullptr, 0, dim_, validity_bitmap.data(), logical_count);
        return;
    }
    WithSourceRows<T>(
        source_,
        physical_begin,
        physical_count,
        dim_,
        [&](std::span<const GrowingVectorStorageType<T>> values) {
            const auto dataset_dim =
                kSparseVector<T> ? SparseDimension<T>(values) : dim_;
            AddBatch(values.data(),
                     physical_count,
                     dataset_dim,
                     nullable ? validity_bitmap.data() : nullptr,
                     logical_count);
        });
}

template <typename T>
bool
KnowhereGrowingVectorIndex<T>::TryBuildAccepted() {
    AssertInfo(!built_ && accepted_physical_count_ >= build_threshold_,
               "invalid growing vector cold-build state");
    // Public row prefix that covers the first build_threshold_ stored vectors.
    // A nullable field compacts null rows away, so the two counts differ.
    int64_t build_logical_count = build_threshold_;
    if (nullable_.value_or(false)) {
        const auto last_logical =
            validity_.GetLogicalOffset(build_threshold_ - 1);
        AssertInfo(last_logical >= 0,
                   "growing vector cold build cannot resolve the public row "
                   "of physical offset {}",
                   build_threshold_ - 1);
        build_logical_count = last_logical + 1;
    }
    AssertInfo(build_logical_count <= accepted_row_end_,
               "growing vector cold build public prefix {} exceeds the "
               "accepted row end {}",
               build_logical_count,
               accepted_row_end_);
    try {
        BuildFromSource(build_threshold_, build_logical_count);
    } catch (const std::exception& error) {
        LOG_WARN(
            "growing vector cold build failed; retaining complete raw source "
            "for fallback and retry: index_type={}, threshold={}, "
            "accepted_row_end={}, accepted_physical_count={}, error={}",
            index_type_,
            build_threshold_,
            accepted_row_end_,
            accepted_physical_count_,
            error.what());
        engine_.reset();
        return false;
    } catch (...) {
        LOG_WARN(
            "growing vector cold build failed with a non-standard exception; "
            "retaining complete raw source for fallback and retry: "
            "index_type={}, threshold={}, accepted_row_end={}, "
            "accepted_physical_count={}",
            index_type_,
            build_threshold_,
            accepted_row_end_,
            accepted_physical_count_);
        engine_.reset();
        return false;
    }

    built_ = true;
    try {
        AddFromSource(build_threshold_,
                      accepted_physical_count_ - build_threshold_,
                      build_logical_count,
                      accepted_row_end_ - build_logical_count);
    } catch (...) {
        poison_ = std::current_exception();
        throw;
    }
    if (!retain_source_as_data_view_) {
        source_.reset();
    }
    return true;
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::PublishAccepted() {
    AssertInfo(built_ && engine_.has_value(),
               "cannot publish an unbuilt growing vector engine");
    // The reader shares the live engine -- including its append-only IdMap --
    // but freezes this generation's public row prefix and vector count.
    auto reader = std::make_unique<VectorIndexReader>(*engine_,
                                                      accepted_row_end_,
                                                      accepted_physical_count_,
                                                      search_defaults_);
    PublishSnapshot(std::move(reader), accepted_row_end_);
    publication_pending_ = false;
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::CommitIfNeeded() {
    std::lock_guard<std::mutex> lock(writer_mutex_);
    RethrowIfPoisoned();
    if (publication_pending_) {
        PublishAccepted();
    }
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::Flush() {
    std::lock_guard<std::mutex> lock(writer_mutex_);
    RethrowIfPoisoned();
    if (publication_pending_) {
        PublishAccepted();
        return;
    }
    if (built_ || accepted_physical_count_ < build_threshold_) {
        return;
    }
    if (!TryBuildAccepted()) {
        // The complete compact source remains available. Cold-build failure
        // leaves no published engine and queries continue through raw fallback.
        return;
    }
    publication_pending_ = true;
    PublishAccepted();
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::Append(int64_t row_begin,
                                      const VectorBatch<
                                          GrowingVectorStorageType<T>>& batch) {
    std::lock_guard<std::mutex> lock(writer_mutex_);
    RethrowIfPoisoned();

    const int64_t logical_rows = CheckedLogicalRows(batch.row_count);
    AssertInfo(row_begin >= 0 &&
                   logical_rows <=
                       std::numeric_limits<int64_t>::max() - row_begin,
               "invalid growing vector logical range [{}, +{})",
               row_begin,
               logical_rows);
    const int64_t row_end = row_begin + logical_rows;
    const bool nullable = batch.valid != nullptr;
    if (nullable_.has_value()) {
        AssertInfo(*nullable_ == nullable,
                   "growing vector nullability changed between batches");
    }

    int64_t batch_physical = logical_rows;
    if (nullable) {
        batch_physical = static_cast<int64_t>(
            std::count(batch.valid, batch.valid + logical_rows, true));
    }
    AssertInfo(batch_physical == 0 || batch.values != nullptr,
               "growing vector batch has {} physical rows but no values",
               batch_physical);
    if constexpr (kSparseVector<T>) {
        AssertInfo(batch.dim >= 0,
                   "growing sparse vector batch dimension is negative");
        if (batch_physical > 0) {
            const auto values = std::span<const GrowingVectorStorageType<T>>(
                batch.values, static_cast<size_t>(batch_physical));
            const auto observed_dim = SparseDimension<T>(values);
            AssertInfo(batch.dim >= observed_dim,
                       "growing sparse vector batch dimension {} is below "
                       "observed {}",
                       batch.dim,
                       observed_dim);
        }
    } else {
        AssertInfo(batch.dim == dim_,
                   "growing vector batch dimension {} disagrees with {}",
                   batch.dim,
                   dim_);
    }

    // A pump may replay any wholly accepted immutable subrange. Only a replay
    // reaching the accepted end needs to retry an interrupted publication.
    if (row_end <= accepted_row_end_) {
        if (row_end == accepted_row_end_ && publication_pending_) {
            PublishAccepted();
        }
        return;
    }
    AssertInfo(row_begin == accepted_row_end_,
               "growing vector append partially overlaps or skips accepted "
               "rows: got [{}, {}), accepted end {}",
               row_begin,
               row_end,
               accepted_row_end_);
    if (publication_pending_) {
        PublishAccepted();
    }

    AssertInfo(batch_physical <=
                   std::numeric_limits<int64_t>::max() -
                       accepted_physical_count_,
               "growing vector physical row count overflows int64");
    const int64_t next_physical =
        accepted_physical_count_ + batch_physical;

    // Mapping publication precedes any engine mutation. Reserve failure leaves
    // both states unchanged; after this returns, an Add failure poisons the
    // owner so the unpublished mapping suffix can never be exposed.
    validity_.Append(batch.valid,
                     logical_rows,
                     accepted_row_end_,
                     accepted_physical_count_);
    if (!nullable_.has_value()) {
        nullable_ = nullable;
    }

    if (!built_) {
        // Before the first engine exists, raw data and the append-only mapping
        // are the recoverable source of truth. Accept the complete range first
        // so a failed cold build can be retried from all historical rows.
        accepted_row_end_ = row_end;
        accepted_physical_count_ = next_physical;
        if (accepted_physical_count_ < build_threshold_ ||
            !TryBuildAccepted()) {
            return;
        }
    } else {
        try {
            // A nullable batch always reaches knowhere so the IdMap's public
            // row domain advances with the segment, even for an all-null tail.
            if (batch_physical > 0 || nullable) {
                const auto add_dim =
                    kSparseVector<T> ? batch.dim : dim_;
                std::vector<uint8_t> validity_bitmap;
                if (nullable) {
                    validity_bitmap =
                        PackValidityBitmap(batch.valid, logical_rows);
                }
                AddBatch(batch.values,
                         batch_physical,
                         add_dim,
                         nullable ? validity_bitmap.data() : nullptr,
                         logical_rows);
            }
        } catch (...) {
            poison_ = std::current_exception();
            throw;
        }
        accepted_row_end_ = row_end;
        accepted_physical_count_ = next_physical;
    }

    publication_pending_ = true;
    PublishAccepted();
}

template <typename T>
void
KnowhereGrowingVectorIndex<T>::RethrowIfPoisoned() const {
    if (poison_ != nullptr) {
        std::rethrow_exception(poison_);
    }
}

template <typename T>
DataType
KnowhereGrowingVectorIndex<T>::ValueType() const {
    return value_type_;
}

template <typename T>
std::string
KnowhereGrowingVectorIndex<T>::Family() const {
    return families::kVectorMem;
}

template class KnowhereGrowingVectorIndex<float>;
template class KnowhereGrowingVectorIndex<float16>;
template class KnowhereGrowingVectorIndex<bfloat16>;
template class KnowhereGrowingVectorIndex<sparse_u32_f32>;

}  // namespace milvus::index
