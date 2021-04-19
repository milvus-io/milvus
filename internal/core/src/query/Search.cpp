#include "Search.h"
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include "segcore/Reduce.h"

#include <faiss/utils/distances.h>
#include "utils/tools.h"

namespace milvus::query {
static faiss::ConcurrentBitsetPtr
create_bitmap_view(std::optional<const BitmapSimple*> bitmaps_opt, int64_t chunk_id) {
    if (!bitmaps_opt.has_value()) {
        return nullptr;
    }
    auto& bitmaps = *bitmaps_opt.value();
    auto& src_vec = bitmaps.at(chunk_id);
    auto dst = std::make_shared<faiss::ConcurrentBitset>(src_vec.size());
    boost::to_block_range(src_vec, dst->mutable_data());
    return dst;
}

using namespace segcore;
Status
QueryBruteForceImpl(const SegmentSmallIndex& segment,
                    const query::QueryInfo& info,
                    const float* query_data,
                    int64_t num_queries,
                    Timestamp timestamp,
                    std::optional<const BitmapSimple*> bitmaps_opt,
                    QueryResult& results) {
    auto& record = segment.get_insert_record();
    auto& schema = segment.get_schema();
    auto& indexing_record = segment.get_indexing_record();
    // step 1: binary search to find the barrier of the snapshot
    auto ins_barrier = get_barrier(record, timestamp);
    auto max_chunk = upper_div(ins_barrier, DefaultElementPerChunk);
    // auto del_barrier = get_barrier(deleted_record_, timestamp);

#if 0
    auto bitmap_holder = get_deleted_bitmap(del_barrier, timestamp, ins_barrier);
    Assert(bitmap_holder);
    auto bitmap = bitmap_holder->bitmap_ptr;
#endif

    // step 2.1: get meta
    // step 2.2: get which vector field to search
    auto vecfield_offset_opt = schema.get_offset(info.field_id_);
    Assert(vecfield_offset_opt.has_value());
    auto vecfield_offset = vecfield_offset_opt.value();
    auto& field = schema[vecfield_offset];
    auto vec_ptr = record.get_vec_entity<float>(vecfield_offset);

    Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();
    auto topK = info.topK_;
    auto total_count = topK * num_queries;
    // TODO: optimize

    // step 3: small indexing search
    std::vector<int64_t> final_uids(total_count, -1);
    std::vector<float> final_dis(total_count, std::numeric_limits<float>::max());

    auto max_indexed_id = indexing_record.get_finished_ack();
    const auto& indexing_entry = indexing_record.get_indexing(vecfield_offset);
    auto search_conf = indexing_entry.get_search_conf(topK);

    for (int chunk_id = 0; chunk_id < max_indexed_id; ++chunk_id) {
        auto indexing = indexing_entry.get_indexing(chunk_id);
        auto src_data = vec_ptr->get_chunk(chunk_id).data();
        auto dataset = knowhere::GenDataset(num_queries, dim, src_data);
        auto bitmap_view = create_bitmap_view(bitmaps_opt, chunk_id);
        auto ans = indexing->Query(dataset, search_conf, bitmap_view);
        auto dis = ans->Get<float*>(milvus::knowhere::meta::DISTANCE);
        auto uids = ans->Get<int64_t*>(milvus::knowhere::meta::IDS);
        merge_into(num_queries, topK, final_dis.data(), final_uids.data(), dis, uids);
    }

    // step 4: brute force search where small indexing is unavailable
    for (int chunk_id = max_indexed_id; chunk_id < max_chunk; ++chunk_id) {
        std::vector<int64_t> buf_uids(total_count, -1);
        std::vector<float> buf_dis(total_count, std::numeric_limits<float>::max());

        faiss::float_maxheap_array_t buf = {(size_t)num_queries, (size_t)topK, buf_uids.data(), buf_dis.data()};
        auto src_data = vec_ptr->get_chunk(chunk_id).data();
        auto nsize =
            chunk_id != max_chunk - 1 ? DefaultElementPerChunk : ins_barrier - chunk_id * DefaultElementPerChunk;
        auto bitmap_view = create_bitmap_view(bitmaps_opt, chunk_id);
        faiss::knn_L2sqr(query_data, src_data, dim, num_queries, nsize, &buf, bitmap_view);
        merge_into(num_queries, topK, final_dis.data(), final_uids.data(), buf_dis.data(), buf_uids.data());
    }

    // step 5: convert offset to uids
    for (auto& id : final_uids) {
        if (id == -1) {
            continue;
        }
        id = record.uids_[id];
    }

    results.result_ids_ = std::move(final_uids);
    results.result_distances_ = std::move(final_dis);
    results.topK_ = topK;
    results.num_queries_ = num_queries;

    return Status::OK();
}
}  // namespace milvus::query
