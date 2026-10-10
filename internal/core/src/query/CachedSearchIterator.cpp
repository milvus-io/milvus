// Copyright (C) 2019-2024 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <math.h>
#include <algorithm>
#include <exception>
#include <iterator>
#include <limits>
#include <memory>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Utils.h"
#include "common/VectorArray.h"
#include "index/Utils.h"
#include "index/VectorIndex.h"
#include "knowhere/expected.h"
#include "knowhere/sparse_utils.h"
#include "mmap/ChunkedColumnInterface.h"
#include "nlohmann/json.hpp"
#include "query/CachedSearchIterator.h"
#include "query/SearchBruteForce.h"
#include "query/Utils.h"
#include "query/helper.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/SegmentInterface.h"
#include "segcore/Utils.h"

namespace milvus::query {

CachedSearchIterator::PrimaryKeyGetter
CachedSearchIterator::MakePrimaryKeyGetter(
    const segcore::SegmentInternalInterface& segment,
    milvus::OpContext* op_context,
    SearchResult& search_result) {
    auto schema = segment.get_schema_snapshot();
    auto pk_field = schema->get_primary_field_id();
    AssertInfo(pk_field.has_value(), "Iterator segment has no primary key");
    return [&segment, pk_field = pk_field.value(), op_context, &search_result](
               const std::vector<int64_t>& offsets) {
        milvus::OpContext local_context;
        if (op_context != nullptr) {
            local_context.cancellation_token = op_context->cancellation_token;
            local_context.runtime_load_priority =
                op_context->runtime_load_priority;
        }
        auto data = segment.bulk_subscript(
            &local_context, pk_field, offsets.data(), offsets.size());
        std::vector<PkType> pks(offsets.size());
        segcore::ParsePksFromFieldData(pks, *data);
        search_result.search_storage_cost_.scanned_remote_bytes +=
            local_context.storage_usage.scanned_cold_bytes.load();
        search_result.search_storage_cost_.scanned_total_bytes +=
            local_context.storage_usage.scanned_total_bytes.load();
        return pks;
    };
}

CachedSearchIterator::RawVectorGetter
CachedSearchIterator::MakeRawVectorGetter(
    const segcore::SegmentInternalInterface& segment,
    FieldId field_id,
    milvus::OpContext* op_context,
    SearchResult& search_result) {
    return [&segment, field_id, op_context, &search_result](
               const std::vector<int64_t>& offsets) {
        milvus::OpContext local_context;
        if (op_context != nullptr) {
            local_context.cancellation_token = op_context->cancellation_token;
            local_context.runtime_load_priority =
                op_context->runtime_load_priority;
        }
        auto data = segment.bulk_subscript(
            &local_context, field_id, offsets.data(), offsets.size());
        search_result.search_storage_cost_.scanned_remote_bytes +=
            local_context.storage_usage.scanned_cold_bytes.load();
        search_result.search_storage_cost_.scanned_total_bytes +=
            local_context.storage_usage.scanned_total_bytes.load();
        return data;
    };
}

CachedSearchIterator::CachedSearchIterator(
    const dataset::SearchDataset& query_ds,
    int64_t row_count,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    DataType data_type,
    RawVectorGetter vector_getter,
    PrimaryKeyGetter pk_getter,
    milvus::OpContext* op_context)
    : nq_(query_ds.num_queries),
      pk_getter_(std::move(pk_getter)),
      op_context_(op_context),
      exact_row_count_(row_count),
      exact_bitset_(bitset) {
    AssertInfo(
        query_ds.query_data != nullptr && query_ds.query_offsets == nullptr,
        "Strict iterator requires a plain query vector");
    AssertInfo(nq_ == 1 && row_count >= 0 && vector_getter,
               "Invalid strict iterator corpus scan");
    AssertInfo(
        bitset.empty() || bitset.size() >= static_cast<size_t>(row_count),
        "Strict iterator filter is smaller than the frozen row count");
    if (query_ds.metric_type == knowhere::metric::BM25) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Strict iterator requires frozen BM25 statistics; live IDF "
                  "and avgdl cannot be paginated");
    }
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        AssertInfo(query_ds.metric_type == knowhere::metric::IP,
                   "Strict sparse iterator supports only IP");
    }
    Init(search_info);
    AssertInfo(search_info.iterator_v2_info_->cursor_version == 2,
               "Corpus iterator requires a strict PK cursor");
    auto full_info = search_info;
    full_info.round_decimal_ = -1;
    if (full_info.search_params_.is_null()) {
        full_info.search_params_ = knowhere::Json::object();
    }
    // Bounds are applied after scoring, so the scorer must return every valid
    // row rather than range-search output or a top-B subset.
    full_info.search_params_.erase(RADIUS);
    full_info.search_params_.erase(RANGE_FILTER);
    exact_candidates_ = [query_ds,
                         data_type,
                         vector_getter = std::move(vector_getter),
                         full_info,
                         index_info,
                         op_context](
                            const std::vector<int64_t>& offsets) mutable {
        auto data = vector_getter(offsets);
        AssertInfo(data != nullptr,
                   "Strict iterator vector getter returned no data");
        AssertInfo(
            data->valid_data_size() == 0 ||
                data->valid_data_size() == static_cast<int>(offsets.size()),
            "Strict iterator vector validity size mismatch");
        std::vector<int64_t> valid_offsets;
        valid_offsets.reserve(offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            if (data->valid_data_size() == 0 || data->valid_data(i)) {
                valid_offsets.push_back(offsets[i]);
            }
        }
        std::vector<ScoredOffset> results;
        if (valid_offsets.empty()) {
            return results;
        }
        results.reserve(valid_offsets.size());
        const auto& vectors = data->vectors();
        if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            using Row = knowhere::sparse::SparseRow<SparseValueType>;
            const auto& rows = vectors.sparse_float_vector().contents();
            AssertInfo(rows.size() == static_cast<int>(valid_offsets.size()),
                       "Strict iterator sparse payload size mismatch");
            auto computer = knowhere::sparse::GetDocValueOriginalComputer<
                SparseValueType>();
            const auto& query = *static_cast<const Row*>(query_ds.query_data);
            for (size_t i = 0; i < valid_offsets.size(); ++i) {
                const auto& bytes = rows.Get(i);
                AssertInfo(bytes.size() % Row::element_size() == 0,
                           "Strict iterator received malformed sparse data");
                Row row(
                    bytes.size() / Row::element_size(),
                    reinterpret_cast<uint8_t*>(const_cast<char*>(bytes.data())),
                    false);
                // Sparse top-k helpers discard zero/negative scores. Direct
                // scoring retains them, including an empty query's zero ties.
                results.emplace_back(valid_offsets[i],
                                     query.dot(row, computer));
            }
            return results;
        }
        const void* raw_data = nullptr;
        size_t payload_size = 0;
        switch (data_type) {
            case DataType::VECTOR_FLOAT:
                raw_data = vectors.float_vector().data().data();
                payload_size =
                    vectors.float_vector().data_size() * sizeof(float);
                break;
            case DataType::VECTOR_FLOAT16:
                raw_data = vectors.float16_vector().data();
                payload_size = vectors.float16_vector().size();
                break;
            case DataType::VECTOR_BFLOAT16:
                raw_data = vectors.bfloat16_vector().data();
                payload_size = vectors.bfloat16_vector().size();
                break;
            case DataType::VECTOR_INT8:
                raw_data = vectors.int8_vector().data();
                payload_size = vectors.int8_vector().size();
                break;
            case DataType::VECTOR_BINARY:
                raw_data = vectors.binary_vector().data();
                payload_size = vectors.binary_vector().size();
                break;
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "Strict iterator cannot score vector type {}",
                          data_type);
        }
        AssertInfo(payload_size == valid_offsets.size() *
                                       GetDataTypeSize(data_type, query_ds.dim),
                   "Strict iterator vector payload size mismatch");
        auto block_query = query_ds;
        block_query.topk = valid_offsets.size();
        block_query.round_decimal = -1;
        full_info.topk_ = block_query.topk;
        dataset::RawDataset raw{0,
                                query_ds.dim,
                                static_cast<int64_t>(valid_offsets.size()),
                                raw_data};
        auto scored = BruteForceSearch(block_query,
                                       raw,
                                       full_info,
                                       index_info,
                                       BitsetView{},
                                       data_type,
                                       DataType::NONE,
                                       op_context);
        std::vector<bool> seen(valid_offsets.size(), false);
        for (size_t i = 0; i < valid_offsets.size(); ++i) {
            auto local = scored.get_offsets()[i];
            AssertInfo(local >= 0 &&
                           local < static_cast<int64_t>(valid_offsets.size()) &&
                           !seen[local],
                       "Strict iterator scorer did not return every vector");
            seen[local] = true;
            results.emplace_back(valid_offsets[local],
                                 scored.get_distances()[i]);
        }
        return results;
    };
}

// For sealed segment with vector index
CachedSearchIterator::CachedSearchIterator(
    const milvus::index::VectorIndex& index,
    const knowhere::DataSetPtr& query_ds,
    const SearchInfo& search_info,
    const BitsetView& bitset,
    milvus::OpContext* op_context,
    PrimaryKeyGetter pk_getter)
    : pk_getter_(std::move(pk_getter)), op_context_(op_context) {
    if (query_ds == nullptr) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Query dataset is nullptr, cannot initialize iterator");
    }
    auto offsets =
        query_ds->Get<const size_t*>(knowhere::meta::EMB_LIST_OFFSET);
    if (offsets != nullptr) {
        if (search_info.iterator_v2_info_.has_value() &&
            search_info.iterator_v2_info_->cursor_version == 2) {
            ThrowInfo(
                ErrorCode::Unsupported,
                "Strict iterator cursor does not support embedding lists");
        }
        nq_ = query_ds->Get<int64_t>(knowhere::meta::NQ);
        AssertInfo(nq_ > 0, "embedding list query count is missing");
        auto total_vectors = static_cast<size_t>(query_ds->GetRows());
        AssertInfo(offsets[nq_] == total_vectors,
                   "embedding list query offsets are inconsistent with "
                   "flattened rows: nq={}, terminal_offset={}, rows={}",
                   nq_,
                   offsets[nq_],
                   total_vectors);
    } else {
        nq_ = query_ds->GetRows();
    }
    Init(search_info);

    if (search_info.iterator_v2_info_->cursor_version == 2) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Strict iterator requires a corpus vector getter, not ANN "
                  "enumeration");
    }

    auto search_json = index.PrepareSearchParams(search_info);
    index::CheckAndUpdateKnowhereRangeSearchParam(
        search_info, batch_size_, index.GetMetricType(), search_json);

    auto expected_iterators =
        index.VectorIterators(query_ds, search_json, bitset, op_context);
    if (expected_iterators.has_value()) {
        iterators_ = std::move(expected_iterators.value());
    } else {
        // Route the knowhere status through the shared mapper so the
        // retriability verdict survives (e.g. malloc_error -> retriable
        // MemAllocateFailed) instead of collapsing to UnexpectedError.
        ThrowInfo(KnowhereStatusToErrorCode(expected_iterators.error()),
                  "Failed to create iterators from index: {}",
                  expected_iterators.what());
    }
}

void
CachedSearchIterator::AppendChunkIterators(
    const dataset::SearchDataset& query_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const dataset::RawDataset& raw_data,
    const BitsetView& bitset,
    const milvus::DataType& data_type) {
    auto expected_iterators = GetBruteForceSearchIterators(
        query_ds, raw_data, search_info, index_info, bitset, data_type);
    if (expected_iterators.has_value()) {
        auto& chunk_iterators = expected_iterators.value();
        iterators_.insert(iterators_.end(),
                          std::make_move_iterator(chunk_iterators.begin()),
                          std::make_move_iterator(chunk_iterators.end()));
        ++num_chunks_;
        return;
    }
    // Route the knowhere status through the shared mapper so the retriability
    // verdict survives (e.g. malloc_error -> retriable MemAllocateFailed)
    // instead of collapsing to UnexpectedError.
    ThrowInfo(KnowhereStatusToErrorCode(expected_iterators.error()),
              "Failed to create brute-force iterators: {}",
              expected_iterators.what());
}

void
CachedSearchIterator::InitializeChunkedIterators(
    const dataset::SearchDataset& query_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    const milvus::DataType& data_type,
    const GetChunkDataFunc& get_chunk_data) {
    int64_t offset = 0;
    chunked_heaps_.resize(nq_);
    const auto source_chunks = num_chunks_;
    num_chunks_ = 0;
    for (int64_t chunk_id = 0; chunk_id < static_cast<int64_t>(source_chunks);
         ++chunk_id) {
        auto chunk = get_chunk_data(chunk_id, offset);
        AppendChunkIterators(query_ds,
                             search_info,
                             index_info,
                             chunk.raw_data,
                             chunk.bitset,
                             data_type);
        offset += chunk.raw_data.num_raw_data;
    }
}

// For growing segment with chunked data, BF
CachedSearchIterator::CachedSearchIterator(
    const dataset::SearchDataset& query_ds,
    const segcore::VectorBase* vec_data,
    const ChunkSnapshot& chunks,
    const int64_t row_count,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    const milvus::DataType& data_type,
    PrimaryKeyGetter pk_getter,
    milvus::OpContext* op_context)
    : pk_getter_(std::move(pk_getter)), op_context_(op_context) {
    if (vec_data == nullptr) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Vector data is nullptr, cannot initialize iterator");
    }

    if (row_count <= 0) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Number of rows is 0, cannot initialize iterator");
    }

    const int64_t vec_size_per_chunk = vec_data->get_size_per_chunk();
    const int64_t source_chunks = upper_div(row_count, vec_size_per_chunk);
    nq_ = query_ds.num_queries;
    Init(search_info);

    // VECTOR_ARRAY element-level search: growing stores each row as a
    // separate VectorArray with its own backing allocation, so we must
    // flatten per-chunk into a contiguous buffer that knowhere can read.
    // struct_element_offsets_ != nullptr is the element-level signal (multi-search-
    // multi emb-list iterator is rejected upstream, so we don't branch on
    // it here).
    const bool is_element_level =
        search_info.struct_element_offsets_ != nullptr;
    if (is_element_level) {
        chunk_buffers_.reserve(source_chunks);
    }

    iterators_.reserve(nq_ * source_chunks);
    chunked_heaps_.resize(nq_);
    num_chunks_ = 0;
    const auto& offset_mapping = vec_data->get_offset_mapping();
    const bool has_offset_mapping = offset_mapping.IsEnabled() &&
                                    !is_element_level &&
                                    data_type != DataType::VECTOR_ARRAY;
    int64_t element_offset = 0;

    for (int64_t chunk_id = 0; chunk_id < source_chunks; ++chunk_id) {
        // Read through the caller's snapshot, not the live container: the
        // generation behind `chunks` is pinned and cannot be reclaimed under
        // us. There is no PinWrapper here (unlike the sealed path) because the
        // snapshot itself is the pin, and the caller holds it for at least as
        // long as this iterator lives.
        const void* chunk_data = vec_data->get_chunk_data(chunks, chunk_id);
        const int64_t row_begin = chunk_id * vec_size_per_chunk;
        const int64_t row_end =
            std::min(row_count, (chunk_id + 1) * vec_size_per_chunk);
        auto range_begin = row_begin;
        while (range_begin < row_end) {
            int64_t chunk_size = row_end - range_begin;
            const void* range_data = chunk_data;
            OffsetMappingIdView id_view;
            if (has_offset_mapping) {
                id_view = offset_mapping.GetPhysicalToLogicalIds(range_begin,
                                                                 chunk_size);
                AssertInfo(!id_view.empty(),
                           "empty id map view for non-empty BF iterator range");
                chunk_size = id_view.count;
                range_data = AdvanceVectorDataPointer(chunk_data,
                                                      data_type,
                                                      query_ds.dim,
                                                      range_begin - row_begin);
            }
            auto chunk_bitset = AttachOffsetMappingIds(bitset, id_view);

            dataset::RawDataset raw_data;
            if (!is_element_level) {
                raw_data = {range_begin, query_ds.dim, chunk_size, range_data};
            } else {
                auto va_ptr = reinterpret_cast<const VectorArray*>(range_data);
                int64_t total_bytes = 0;
                int64_t total_elements = 0;
                for (int64_t i = 0; i < chunk_size; ++i) {
                    total_bytes += va_ptr[i].byte_size();
                    total_elements += va_ptr[i].physical_length();
                }
                auto buf = std::make_unique<uint8_t[]>(total_bytes);
                auto* ptr = buf.get();
                for (int64_t i = 0; i < chunk_size; ++i) {
                    milvus::fastmem::FastMemcpy(
                        ptr, va_ptr[i].data(), va_ptr[i].byte_size());
                    ptr += va_ptr[i].byte_size();
                }
                const void* flat_data = buf.get();
                chunk_buffers_.emplace_back(std::move(buf));
                raw_data = {
                    element_offset, query_ds.dim, total_elements, flat_data};
                element_offset += total_elements;
            }
            AppendChunkIterators(query_ds,
                                 search_info,
                                 index_info,
                                 raw_data,
                                 chunk_bitset,
                                 data_type);
            range_begin += chunk_size;
        }
    }
}

// For sealed segment with chunked data, BF
CachedSearchIterator::CachedSearchIterator(
    ChunkedColumnInterface* column,
    const dataset::SearchDataset& query_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    const milvus::DataType& data_type,
    PrimaryKeyGetter pk_getter,
    milvus::OpContext* op_context)
    : pk_getter_(std::move(pk_getter)), op_context_(op_context) {
    if (column == nullptr) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Column is nullptr, cannot initialize iterator");
    }

    num_chunks_ = column->num_chunks();
    nq_ = query_ds.num_queries;
    Init(search_info);

    iterators_.reserve(nq_ * num_chunks_);
    pin_wrappers_.reserve(num_chunks_);

    InitializeChunkedIterators(
        query_ds,
        search_info,
        index_info,
        bitset,
        data_type,
        [this, column, &query_ds, &search_info, &bitset](
            int64_t chunk_id, int64_t physical_begin) {
            auto* load_context =
                search_info.iterator_v2_info_->cursor_version == 2 ? op_context_
                                                                   : nullptr;
            auto pw = column->DataOfChunk(load_context, chunk_id)
                          .transform<const void*>([](const auto& x) {
                              return static_cast<const void*>(x);
                          });
            int64_t chunk_size = column->chunk_row_nums(chunk_id);
            const auto& offset_mapping = column->GetOffsetMapping();
            const bool has_offset_mapping =
                offset_mapping.IsEnabled() &&
                search_info.struct_element_offsets_ == nullptr;
            if (has_offset_mapping) {
                chunk_size = column->GetValidCountInChunk(chunk_id);
            }
            // For element-level search on vector array field, chunk_size
            // must be the element count in this chunk, not the row count.
            if (search_info.struct_element_offsets_ != nullptr) {
                auto elem_offsets_pw =
                    column->VectorArrayOffsets(load_context, chunk_id);
                chunk_size = elem_offsets_pw.get()[chunk_size];
            }
            // pw guarantees chunk_data is kept alive.
            auto chunk_data = pw.get();
            pin_wrappers_.emplace_back(std::move(pw));
            BitsetView chunk_bitset = bitset;
            if (has_offset_mapping) {
                chunk_bitset = AttachOffsetMappingIds(
                    bitset,
                    offset_mapping.GetPhysicalToLogicalIds(physical_begin,
                                                           chunk_size));
            }
            return ChunkSearchData{
                {physical_begin, query_ds.dim, chunk_size, chunk_data},
                chunk_bitset};
        });
}

void
CachedSearchIterator::NextBatch(const SearchInfo& search_info,
                                SearchResult& search_result) {
    if (search_info.iterator_v2_info_.has_value() &&
        search_info.iterator_v2_info_->cursor_version == 2) {
        ValidateSearchInfo(search_info);
        AssertInfo(exact_candidates_,
                   "Strict iterator requires complete corpus scoring");
    }
    if (iterators_.empty() && !exact_candidates_) {
        search_result.iterator_pk_cursor_executed_ =
            search_info.iterator_v2_info_->cursor_version == 2;
        return;
    }

    if (!exact_candidates_ && iterators_.size() != nq_ * num_chunks_) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Iterator size mismatch, expect %d, but got %d",
                  nq_ * num_chunks_,
                  iterators_.size());
    }

    ValidateSearchInfo(search_info);

    search_result.total_nq_ = nq_;
    search_result.unity_topK_ = batch_size_;
    search_result.seg_offsets_.resize(nq_ * batch_size_);
    search_result.distances_.resize(nq_ * batch_size_);

    for (size_t query_idx = 0; query_idx < nq_; ++query_idx) {
        auto rst = search_info.iterator_v2_info_->cursor_version == 2
                       ? GetPkOrderedResults(search_info)
                       : GetBatchedNextResults(query_idx, search_info);
        WriteSingleQuerySearchResult(search_result, query_idx, rst);
    }
    search_result.iterator_pk_cursor_executed_ =
        search_info.iterator_v2_info_->cursor_version == 2;
}

std::vector<CachedSearchIterator::DisIdPair>
CachedSearchIterator::GetPkOrderedResults(const SearchInfo& search_info) {
    AssertInfo(pk_getter_, "Strict iterator cursor requires a PK getter");
    const auto& cursor = search_info.iterator_v2_info_.value();
    const auto last_bound = ConvertIncomingDistance(cursor.last_bound);
    AssertInfo(last_bound.has_value() == cursor.last_pk.has_value(),
               "Strict iterator cursor requires both score and primary key");
    const auto radius = ConvertIncomingDistance(
        index::GetValueFromConfig<float>(search_info.search_params_, RADIUS));
    const auto range_filter =
        ConvertIncomingDistance(index::GetValueFromConfig<float>(
            search_info.search_params_, RANGE_FILTER));

    struct Candidate {
        DisIdPair result;
        PkType pk;
    };
    auto better = [](const Candidate& left, const Candidate& right) {
        if (left.result.first != right.result.first) {
            return left.result.first < right.result.first;
        }
        if (left.pk != right.pk) {
            return left.pk < right.pk;
        }
        return left.result.second < right.result.second;
    };
    // The heap holds at most one output batch, with its worst row at the top.
    std::priority_queue<Candidate, std::vector<Candidate>, decltype(better)>
        selected(better);
    constexpr size_t pk_read_batch_size = 256;
    std::vector<DisIdPair> pending;
    std::vector<int64_t> offsets;
    pending.reserve(pk_read_batch_size);
    offsets.reserve(pk_read_batch_size);
    const std::string pk_read_operation = "Strict iterator PK read";
    const std::string scan_operation = "Strict iterator scan";
    auto select_pending = [&] {
        if (pending.empty()) {
            return;
        }
        segcore::CheckCancellation(op_context_, -1, pk_read_operation);
        auto pks = pk_getter_(offsets);
        AssertInfo(pks.size() == pending.size(),
                   "PK getter returned {} keys for {} iterator candidates",
                   pks.size(),
                   pending.size());
        for (size_t i = 0; i < pending.size(); ++i) {
            if (last_bound.has_value()) {
                AssertInfo(pks[i].index() == cursor.last_pk->index(),
                           "Iterator cursor PK type does not match segment");
                if (pending[i].first == last_bound.value() &&
                    pks[i] <= cursor.last_pk.value()) {
                    continue;
                }
            }
            Candidate candidate{pending[i], std::move(pks[i])};
            if (selected.size() < static_cast<size_t>(batch_size_)) {
                selected.push(std::move(candidate));
            } else if (better(candidate, selected.top())) {
                selected.pop();
                selected.push(std::move(candidate));
            }
        }
        pending.clear();
        offsets.clear();
    };

    auto consume = [&](const ScoredOffset& next) {
        auto result = ConvertIteratorResult(next);
        if (result.second < 0) {
            return;
        }
        AssertInfo(std::isfinite(result.first),
                   "Strict iterator returned a nonfinite score");
        if (!IsValid(result, std::nullopt, radius, range_filter) ||
            (last_bound.has_value() && result.first < last_bound.value())) {
            return;
        }
        pending.push_back(result);
        offsets.push_back(result.second);
        if (pending.size() == pk_read_batch_size) {
            select_pending();
        }
    };
    {
        std::vector<int64_t> eligible;
        eligible.reserve(pk_read_batch_size);
        auto score_block = [&] {
            if (!eligible.empty()) {
                segcore::CheckCancellation(op_context_, -1, scan_operation);
                for (const auto& result : exact_candidates_(eligible)) {
                    consume(result);
                }
                eligible.clear();
            }
        };
        for (int64_t offset = 0; offset < exact_row_count_; ++offset) {
            if (offset % pk_read_batch_size == 0) {
                segcore::CheckCancellation(op_context_, -1, scan_operation);
            }
            if (!exact_bitset_.empty() && exact_bitset_.test(offset)) {
                continue;
            }
            eligible.push_back(offset);
            if (eligible.size() == pk_read_batch_size) {
                score_block();
            }
        }
        score_block();
    }
    select_pending();
    std::vector<Candidate> candidates;
    candidates.reserve(selected.size());
    while (!selected.empty()) {
        candidates.push_back(selected.top());
        selected.pop();
    }
    std::sort(candidates.begin(), candidates.end(), better);
    std::vector<DisIdPair> result;
    result.reserve(batch_size_);
    for (const auto& candidate : candidates) {
        result.emplace_back(candidate.result.first * sign_,
                            candidate.result.second);
    }
    while (result.size() < static_cast<size_t>(batch_size_)) {
        result.emplace_back(std::numeric_limits<float>::infinity(), -1);
    }
    return result;
}

void
CachedSearchIterator::ValidateSearchInfo(const SearchInfo& search_info) {
    if (!search_info.iterator_v2_info_.has_value()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Iterator v2 SearchInfo is not set");
    }

    const auto& iterator_v2_info = search_info.iterator_v2_info_.value();
    if (iterator_v2_info.cursor_version == 2 &&
        (search_info.array_offsets_ != nullptr || search_info.has_group_by() ||
         search_info.iterative_filter_execution ||
         search_info.global_refine_enable_)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "Strict iterator cursor does not support element search, "
                  "group by, iterative filtering, or global refinement");
    }
    if (iterator_v2_info.batch_size != batch_size_) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Batch size mismatch, expect %d, but got %d",
                  batch_size_,
                  iterator_v2_info.batch_size);
    }
}

std::optional<CachedSearchIterator::DisIdPair>
CachedSearchIterator::GetNextValidResult(
    const size_t iterator_idx,
    const std::optional<float>& last_bound,
    const std::optional<float>& radius,
    const std::optional<float>& range_filter) {
    auto& iterator = iterators_[iterator_idx];
    while (true) {
        auto has_next = iterator->HasNext();
        if (!has_next.has_value()) {
            // knowhere already classified this (OOM, disk read); route it
            // through the mapper so a transient failure stays retriable
            // instead of collapsing into UnexpectedError(2001).
            ThrowInfo(KnowhereStatusToErrorCode(has_next.error()),
                      "knowhere iterator HasNext failed: {}",
                      has_next.what());
        }
        if (!has_next.value()) {
            break;
        }
        auto next = iterator->Next();
        if (!next.has_value()) {
            ThrowInfo(KnowhereStatusToErrorCode(next.error()),
                      "knowhere iterator Next failed: {}",
                      next.what());
        }
        auto result = ConvertIteratorResult(next.value());
        if (IsValid(result, last_bound, radius, range_filter)) {
            return result;
        }
    }
    return std::nullopt;
}

// TODO: Optimize this method
void
CachedSearchIterator::MergeChunksResults(
    size_t query_idx,
    const std::optional<float>& last_bound,
    const std::optional<float>& radius,
    const std::optional<float>& range_filter,
    std::vector<DisIdPair>& rst) {
    auto& heap = chunked_heaps_[query_idx];

    if (heap.empty()) {
        for (size_t chunk_id = 0; chunk_id < num_chunks_; ++chunk_id) {
            const size_t iterator_idx = query_idx + chunk_id * nq_;
            if (auto next_result = GetNextValidResult(
                    iterator_idx, last_bound, radius, range_filter);
                next_result.has_value()) {
                heap.emplace(iterator_idx, next_result.value());
            }
        }
    }

    while (!heap.empty() && rst.size() < batch_size_) {
        const auto [iterator_idx, cur_rst] = heap.top();
        heap.pop();

        // last_bound may change between NextBatch calls, discard any invalid results
        if (!IsValid(cur_rst, last_bound, radius, range_filter)) {
            continue;
        }
        rst.emplace_back(cur_rst);

        if (auto next_result = GetNextValidResult(
                iterator_idx, last_bound, radius, range_filter);
            next_result.has_value()) {
            heap.emplace(iterator_idx, next_result.value());
        }
    }
}

std::vector<CachedSearchIterator::DisIdPair>
CachedSearchIterator::GetBatchedNextResults(size_t query_idx,
                                            const SearchInfo& search_info) {
    auto last_bound = ConvertIncomingDistance(
        search_info.iterator_v2_info_.value().last_bound);
    auto radius = ConvertIncomingDistance(
        index::GetValueFromConfig<float>(search_info.search_params_, RADIUS));
    auto range_filter =
        ConvertIncomingDistance(index::GetValueFromConfig<float>(
            search_info.search_params_, RANGE_FILTER));

    std::vector<DisIdPair> rst;
    rst.reserve(batch_size_);

    if (num_chunks_ == 1) {
        auto& iterator = iterators_[query_idx];
        while (rst.size() < batch_size_) {
            auto has_next = iterator->HasNext();
            if (!has_next.has_value()) {
                ThrowInfo(KnowhereStatusToErrorCode(has_next.error()),
                          "knowhere iterator HasNext failed: {}",
                          has_next.what());
            }
            if (!has_next.value()) {
                break;
            }
            auto next = iterator->Next();
            if (!next.has_value()) {
                ThrowInfo(KnowhereStatusToErrorCode(next.error()),
                          "knowhere iterator Next failed: {}",
                          next.what());
            }
            auto result = ConvertIteratorResult(next.value());
            if (IsValid(result, last_bound, radius, range_filter)) {
                rst.emplace_back(result);
            }
        }
    } else {
        MergeChunksResults(query_idx, last_bound, radius, range_filter, rst);
    }
    std::sort(rst.begin(), rst.end());
    if (sign_ == -1) {
        std::for_each(rst.begin(), rst.end(), [this](DisIdPair& x) {
            x.first = x.first * sign_;
        });
    }
    while (rst.size() < batch_size_) {
        rst.emplace_back(1.0f / 0.0f, -1);
    }
    return rst;
}

void
CachedSearchIterator::WriteSingleQuerySearchResult(
    SearchResult& search_result,
    const size_t idx,
    std::vector<DisIdPair>& rst) {
    std::transform(rst.begin(),
                   rst.end(),
                   search_result.distances_.begin() + idx * batch_size_,
                   [](const DisIdPair& x) { return x.first; });

    std::transform(rst.begin(),
                   rst.end(),
                   search_result.seg_offsets_.begin() + idx * batch_size_,
                   [](const DisIdPair& x) { return x.second; });
}

void
CachedSearchIterator::Init(const SearchInfo& search_info) {
    if (!search_info.iterator_v2_info_.has_value()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Iterator v2 info is not set, cannot initialize iterator");
    }

    const auto& iterator_v2_info = search_info.iterator_v2_info_.value();
    if (iterator_v2_info.batch_size == 0) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Batch size is 0, cannot initialize iterator");
    }
    batch_size_ = iterator_v2_info.batch_size;
    if (iterator_v2_info.cursor_version == 2) {
        ValidateSearchInfo(search_info);
        AssertInfo(pk_getter_, "Strict iterator cursor requires a PK getter");
    }

    if (search_info.metric_type_.empty()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Metric type is empty, cannot initialize iterator");
    }
    if (PositivelyRelated(search_info.metric_type_)) {
        sign_ = -1;
    } else {
        sign_ = 1;
    }

    if (nq_ == 0) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Number of queries is 0, cannot initialize iterator");
    }

    // disable multi-query for now
    if (nq_ > 1) {
        ThrowInfo(
            ErrorCode::UnexpectedError,
            "Number of queries is greater than 1, cannot initialize iterator");
    }
}

}  // namespace milvus::query
