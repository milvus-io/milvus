// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <string.h>
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "SearchOnGrowing.h"
#include "cachinglayer/CacheSlot.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/FieldMeta.h"
#include "common/IndexMeta.h"
#include "common/OffsetMapping.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/VectorArray.h"
#include "common/protobuf_utils.h"
#include "exec/operator/Utils.h"
#include "index/Index.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "query/CachedSearchIterator.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnIndex.h"
#include "query/SubSearchResult.h"
#include "query/Utils.h"
#include "query/helper.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/FieldIndexing.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegmentGrowingImpl.h"

namespace milvus::query {

namespace {

std::atomic<bool>&
AfterChunkSnapshotHookEnabled() {
    static std::atomic<bool> enabled{false};
    return enabled;
}

std::mutex&
AfterChunkSnapshotHookMutex() {
    static std::mutex mutex;
    return mutex;
}

SearchOnGrowingAfterChunkSnapshotHook&
AfterChunkSnapshotHookSlot() {
    static SearchOnGrowingAfterChunkSnapshotHook hook;
    return hook;
}

void
RunAfterChunkSnapshotHookForTest() {
    if (!AfterChunkSnapshotHookEnabled().load(std::memory_order_acquire)) {
        return;
    }

    SearchOnGrowingAfterChunkSnapshotHook hook;
    {
        std::lock_guard lock(AfterChunkSnapshotHookMutex());
        hook = AfterChunkSnapshotHookSlot();
    }
    if (hook) {
        hook();
    }
}

}  // namespace

void
SetSearchOnGrowingAfterChunkSnapshotHookForTest(
    SearchOnGrowingAfterChunkSnapshotHook hook) {
    std::lock_guard lock(AfterChunkSnapshotHookMutex());
    AfterChunkSnapshotHookSlot() = std::move(hook);
    AfterChunkSnapshotHookEnabled().store(
        static_cast<bool>(AfterChunkSnapshotHookSlot()),
        std::memory_order_release);
}

void
FloatSegmentIndexSearch(const segcore::SegmentGrowingImpl& segment,
                        const SearchInfo& info,
                        const void* query_data,
                        int64_t num_queries,
                        Timestamp timestamp,
                        const BitsetView& bitset,
                        milvus::OpContext* op_context,
                        SearchResult& search_result) {
    auto schema = segment.get_schema_snapshot();
    auto& indexing_record = segment.get_indexing_record();

    auto vecfield_id = info.field_id_;
    auto& field = (*schema)[vecfield_id];
    auto is_sparse = field.get_data_type() == DataType::VECTOR_SPARSE_U32_F32;
    // TODO(SPARSE): see todo in PlanImpl.h::PlaceHolder.
    auto dim = is_sparse ? 0 : field.get_dim();

    AssertInfo(IsVectorDataType(field.get_data_type()),
               "[FloatSearch]Field data type isn't VECTOR_FLOAT, "
               "VECTOR_FLOAT16, VECTOR_BFLOAT16 or VECTOR_SPARSE_U32_F32");
    dataset::SearchDataset search_dataset{info.metric_type_,
                                          num_queries,
                                          info.topk_,
                                          info.round_decimal_,
                                          dim,
                                          query_data};
    if (indexing_record.is_in(vecfield_id)) {
        const auto& field_indexing =
            indexing_record.get_vec_field_indexing(vecfield_id);

        auto indexing = field_indexing.get_segment_indexing();
        SearchInfo search_conf = field_indexing.get_search_params(info);
        if (search_conf.active_count_ < 0) {
            // Direct callers do not have a plan node to freeze this value.
            // Resolve it before entering the index kernel so the same physical
            // prefix bound applies to tests and legacy call sites as well.
            search_conf.active_count_ = segment.get_active_count(timestamp);
        }
        auto vec_index = dynamic_cast<index::VectorIndex*>(indexing.get());
        SearchOnIndex(search_dataset,
                      *vec_index,
                      search_conf,
                      bitset,
                      op_context,
                      search_result,
                      is_sparse);
    }
}

void
SearchOnGrowing(const segcore::SegmentGrowingImpl& segment,
                const SearchInfo& info,
                const void* query_data,
                const size_t* query_offsets,
                int64_t num_queries,
                Timestamp timestamp,
                const BitsetView& bitset,
                milvus::OpContext* op_context,
                SearchResult& search_result) {
    auto schema = segment.get_schema_snapshot();
    auto& record = segment.get_insert_record();

    // step 1.1: get meta
    // step 1.2: get which vector field to search
    auto vecfield_id = info.field_id_;
    auto& field = (*schema)[vecfield_id];
    if (field.get_data_type() == DataType::VECTOR_ARRAY &&
        field.is_element_nullable()) {
        ThrowInfo(NotImplemented,
                  "search on element-nullable VECTOR_ARRAY fields is not "
                  "supported");
    }
    CheckBruteForceSearchParam(field, info);

    auto data_type = field.get_data_type();
    auto element_type = field.get_element_type();
    AssertInfo(IsVectorDataType(data_type),
               "[SearchOnGrowing]Data type isn't vector type");

    auto topk = info.topk_;
    auto metric_type = info.metric_type_;
    auto round_decimal = info.round_decimal_;

    // step 2: small indexing search
    if (segment.get_indexing_record().SyncDataWithIndex(field.get_id())) {
        AssertInfo(
            data_type != DataType::VECTOR_ARRAY,
            "vector array(embedding list) is not supported for growing segment "
            "indexing search");

        FloatSegmentIndexSearch(segment,
                                info,
                                query_data,
                                num_queries,
                                timestamp,
                                bitset,
                                op_context,
                                search_result);
    } else {
        SubSearchResult final_qr(num_queries, topk, metric_type, round_decimal);
        // TODO(SPARSE): see todo in PlanImpl.h::PlaceHolder.
        auto dim = field.get_data_type() == DataType::VECTOR_SPARSE_U32_F32
                       ? 0
                       : field.get_dim();
        dataset::SearchDataset search_dataset{metric_type,
                                              num_queries,
                                              topk,
                                              round_decimal,
                                              dim,
                                              query_data,
                                              query_offsets};
        int32_t current_chunk_id = 0;

        // get index params for bm25 and minhash brute force.
        // A field added by add_function_field is absent from the growing
        // record's index_meta_ snapshot, so guard with has_field_index_meta:
        // BM25 k1/b are delivered through the plan, MinHash falls back to
        // defaults for the brief window before the segment is reloaded.
        std::map<std::string, std::string> index_info;
        if ((metric_type == knowhere::metric::BM25 ||
             metric_type == knowhere::metric::MHJACCARD) &&
            segment.get_indexing_record().has_field_index_meta(vecfield_id)) {
            index_info = segment.get_indexing_record()
                             .get_field_index_meta(vecfield_id)
                             .GetIndexParams();
        }

        // step 3: brute force search where small indexing is unavailable
        auto vec_ptr = record.get_data_base(vecfield_id);

        // Direct/legacy callers do not carry the plan-frozen active_count.
        // Resolve their fallback BEFORE acquiring the chunk snapshot. If an
        // insert crosses a chunk boundary and completes its ack between the
        // two reads, either the bound sees the insert and the later snapshot
        // contains its chunk, or the bound stays on the old prefix and the
        // search never addresses the new chunk. Acquiring in the opposite
        // order can pair an old snapshot with a newer bound and make both BF
        // and iterator-v2 walk past snapshot.count.
        const int64_t plan_bound = info.active_count_ >= 0
                                       ? info.active_count_
                                       : segment.get_active_count(timestamp);

        // Pin the chunk generation once, up front, and read every chunk
        // through it. try_remove_chunks may swap the column's storage out at
        // any moment once the interim index owns the raw data; this snapshot
        // is what keeps the buffers this scan walks -- and the buffers the
        // brute-force iterators keep pointing into after this function
        // returns -- alive. Which ROWS are visible is a separate question,
        // answered by the plan-frozen bound above.
        //
        // The snapshot also replaces the segment-wide chunk lock this branch
        // used to hold for its whole scan. That lock existed to keep
        // reclamation from running between the SyncDataWithIndex check above
        // and the scan; holding it also made try_remove_chunks' try_lock fail
        // under query load, so reclamation was skipped and only retried on the
        // next insert. Pinning the generation makes the scan safe on its own,
        // and reclamation now always proceeds.
        const auto chunks = vec_ptr->acquire_chunks();
        RunAfterChunkSnapshotHookForTest();
        if (chunks.empty()) {
            // Either the column holds nothing yet, or try_remove_chunks
            // reclaimed it after the check above. Reclamation is gated on
            // HasRawData, which is itself gated on SyncDataWithIndex
            // (FieldIndexing.h), so re-asking distinguishes the two -- and
            // when it says yes, the index owns the raw data and can answer.
            if (segment.get_indexing_record().SyncDataWithIndex(
                    field.get_id())) {
                AssertInfo(data_type != DataType::VECTOR_ARRAY,
                           "vector array(embedding list) is not supported for "
                           "growing segment indexing search");
                return FloatSegmentIndexSearch(segment,
                                               info,
                                               query_data,
                                               num_queries,
                                               timestamp,
                                               bitset,
                                               op_context,
                                               search_result);
            }
            FillEmptySearchResult(search_result, num_queries, info.topk_);
            return;
        }
        const auto& offset_mapping = vec_ptr->get_offset_mapping();
        const bool is_element_level_search =
            data_type == DataType::VECTOR_ARRAY &&
            info.array_offsets_ != nullptr;
        search_result.element_level_ = is_element_level_search;
        const auto has_offset_mapping =
            offset_mapping.IsEnabled() && !is_element_level_search;

        BitsetView search_bitset = bitset;

        // An empty BitsetView means "no filter", NOT "zero rows": its size() is
        // 0 and clamping to it would zero the bound and make the search return
        // an empty result. Only a populated bitset carries a row count worth
        // clamping to.
        //
        // Element-level search must not clamp either: its bitset lives in
        // ELEMENT space (RowBitsetToElementBitset), while plan_bound counts
        // ROWS. A nullable embedding-list column stores null rows as empty
        // arrays, so the flattened element count can run BELOW the row count,
        // and min() would silently drop the tail rows from the scan. Knowhere
        // already bounds element ids by the element bitset's size.
        const int64_t logical_bound =
            (bitset.empty() || is_element_level_search)
                ? plan_bound
                : std::min(int64_t(bitset.size()), plan_bound);

        // Nullable vector fields store only non-null rows, so the chunk walk
        // below runs in the mapping's physical row space. Convert the bound
        // whenever the mapping is enabled -- element-level search included:
        // it skips the bitset and result-offset transforms above (null rows
        // contribute no elements, so element ids agree between the logical
        // and physical flattening), but it still walks compacted physical
        // rows, and a logical bound would run past them. Convert instead of
        // querying the mapping's current size, so every branch enforces the
        // same MVCC bound.
        const int64_t active_count =
            offset_mapping.IsEnabled()
                ? offset_mapping.ValidCountBelow(logical_bound)
                : logical_bound;

        // Check for nullable vector field with all null values
        if (active_count == 0) {
            // All vectors are null, return empty result
            FillEmptySearchResult(search_result, num_queries, info.topk_);
            return;
        }

        // Element-level search (embedding-search-embedding): knowhere sees
        // a scalar vector type and the per-chunk size must be measured in
        // elements. Compute this before the iterator_v2 branch so both
        // paths share the substitution. Emb-list (multi-search-multi)
        // iterator is rejected by the proxy; the assert below is
        // defense-in-depth.
        const auto iter_data_type =
            is_element_level_search ? element_type : data_type;
        const bool use_vector_iterator = milvus::exec::UseVectorIterator(info);

        if (info.iterator_v2_info_.has_value()) {
            AssertInfo(iter_data_type != DataType::VECTOR_ARRAY,
                       "embedding list (multi-search-multi) iterator is not "
                       "supported on vector array fields");

            // Hand the iterator the snapshot this function pinned, so it
            // reads the same generation the scan below would have. Reading the
            // live container instead would assert once try_remove_chunks has
            // reclaimed it -- reclamation no longer waits for a chunk lock.
            CachedSearchIterator cached_iter(search_dataset,
                                             vec_ptr,
                                             chunks,
                                             active_count,
                                             info,
                                             index_info,
                                             search_bitset,
                                             iter_data_type);
            cached_iter.NextBatch(info, search_result);
            FinalizeVectorSearchOffsets(search_result,
                                        info.array_offsets_.get());
            // The iterator is consumed and destroyed above, so nothing borrows
            // the chunks past this point today. Pin the generation anyway: the
            // brute-force branch below has to, and a future change that lets
            // iterator-v2 state outlive this call would otherwise reintroduce
            // a dangling read silently.
            search_result.resource_pins_.emplace_back(chunks.storage);
            return;
        }

        auto vec_size_per_chunk = vec_ptr->get_size_per_chunk();
        auto max_chunk = upper_div(active_count, vec_size_per_chunk);

        // Track cumulative element offset for element-level search.
        // begin_id must be the cumulative element count (not row offset),
        // because ArrayOffsets maps global element IDs to row IDs.
        int64_t cumulative_element_offset = 0;
        int bf_chunk_count = 0;

        std::vector<size_t> offsets;
        for (int chunk_id = current_chunk_id; chunk_id < max_chunk;
             ++chunk_id) {
            auto chunk_data = vec_ptr->get_chunk_data(chunks, chunk_id);

            auto row_begin = chunk_id * vec_size_per_chunk;
            auto row_end =
                std::min(active_count, (chunk_id + 1) * vec_size_per_chunk);
            auto range_begin = row_begin;
            while (range_begin < row_end) {
                auto size_per_chunk = row_end - range_begin;
                const void* range_data = chunk_data;
                OffsetMappingIdView id_view;
                if (has_offset_mapping) {
                    id_view = offset_mapping.GetPhysicalToLogicalIds(
                        range_begin, size_per_chunk);
                    AssertInfo(!id_view.empty(),
                               "empty id map view for non-empty BF range");
                    size_per_chunk = id_view.count;
                    range_data = AdvanceVectorDataPointer(
                        chunk_data, data_type, dim, range_begin - row_begin);
                }
                auto chunk_bitset =
                    AttachOffsetMappingIds(search_bitset, id_view);

                query::dataset::RawDataset sub_data;
                std::unique_ptr<uint8_t[]> buf = nullptr;
                if (data_type != DataType::VECTOR_ARRAY) {
                    sub_data = query::dataset::RawDataset{
                        range_begin, dim, size_per_chunk, range_data};
                } else {
                    // TODO(SpadeA): For VectorArray(Embedding List), data is
                    // discreted stored in FixedVector which means we will copy the
                    // data to a contiguous memory buffer. This is inefficient and
                    // will be optimized in the future.
                    auto vec_ptr =
                        reinterpret_cast<const VectorArray*>(range_data);
                    auto size = 0;
                    for (int i = 0; i < size_per_chunk; ++i) {
                        size += vec_ptr[i].byte_size();
                    }

                    buf = std::make_unique<uint8_t[]>(size);

                    if (is_element_level_search) {
                        auto count = 0;
                        auto ptr = buf.get();
                        for (int i = 0; i < size_per_chunk; ++i) {
                            milvus::fastmem::FastMemcpy(
                                ptr, vec_ptr[i].data(), vec_ptr[i].byte_size());
                            ptr += vec_ptr[i].byte_size();
                            count += vec_ptr[i].physical_length();
                        }
                        sub_data = query::dataset::RawDataset{
                            cumulative_element_offset, dim, count, buf.get()};
                        cumulative_element_offset += count;
                    } else {
                        offsets.clear();
                        offsets.reserve(size_per_chunk + 1);
                        offsets.push_back(0);

                        auto offset = 0;
                        auto ptr = buf.get();
                        for (int i = 0; i < size_per_chunk; ++i) {
                            milvus::fastmem::FastMemcpy(
                                ptr, vec_ptr[i].data(), vec_ptr[i].byte_size());
                            ptr += vec_ptr[i].byte_size();

                            offset += vec_ptr[i].physical_length();
                            offsets.push_back(offset);
                        }
                        sub_data = query::dataset::RawDataset{range_begin,
                                                              dim,
                                                              size_per_chunk,
                                                              buf.get(),
                                                              offsets.data()};
                    }
                }

                if (use_vector_iterator) {
                    AssertInfo(
                        iter_data_type != DataType::VECTOR_ARRAY,
                        "vector array(embedding list) is not supported for "
                        "vector iterator");

                    if (buf != nullptr) {
                        search_result.chunk_buffers_.emplace_back(
                            std::move(buf));
                    }

                    auto sub_qr = PackBruteForceSearchIteratorsIntoSubResult(
                        search_dataset,
                        sub_data,
                        info,
                        index_info,
                        chunk_bitset,
                        iter_data_type);
                    final_qr.merge(sub_qr);
                } else {
                    auto sub_qr = BruteForceSearch(search_dataset,
                                                   sub_data,
                                                   info,
                                                   index_info,
                                                   chunk_bitset,
                                                   iter_data_type,
                                                   element_type,
                                                   op_context);
                    final_qr.merge(sub_qr);
                }
                range_begin += size_per_chunk;
                ++bf_chunk_count;
            }
        }
        if (use_vector_iterator) {
            bool larger_is_closer = PositivelyRelated(info.metric_type_);
            search_result.AssembleChunkVectorIterators(
                num_queries,
                bf_chunk_count,
                final_qr.chunk_iterators(),
                larger_is_closer);
            // Knowhere's brute-force iterators retain raw pointers into the
            // chunk storage and are consumed after SearchOnGrowing returns.
            // Hand the snapshot's reference to the SearchResult so the
            // generation this scan read outlives this function: reclamation
            // proceeds immediately for the segment, and the old collection
            // dies with the last reference, on whichever thread that is.
            search_result.resource_pins_.emplace_back(chunks.storage);
        } else {
            // See FinalizeVectorSearchOffsets for the rationale:
            // element-level and row-level remapping are mutually exclusive.
            if (info.array_offsets_ != nullptr) {
                auto [seg_offsets, elem_indicies] =
                    final_qr.convert_to_element_offsets(
                        info.array_offsets_.get());
                search_result.seg_offsets_ = std::move(seg_offsets);
                search_result.element_indices_ = std::move(elem_indicies);
                search_result.element_level_ = true;
            } else {
                search_result.seg_offsets_ =
                    std::move(final_qr.mutable_offsets());
            }
            search_result.distances_ = std::move(final_qr.mutable_distances());
        }
        search_result.unity_topK_ = topk;
        search_result.total_nq_ = num_queries;
    }
}

}  // namespace milvus::query
