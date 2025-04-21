#include "segcore/storagev1translator/InsertRecordTranslator.h"

#include <memory>
#include <vector>
#include <string>

#include "fmt/core.h"

#include "cachinglayer/Utils.h"
#include "common/ChunkWriter.h"
#include "common/Types.h"
#include "common/SystemProperty.h"
#include "segcore/Utils.h"
#include "storage/ThreadPools.h"

namespace milvus::segcore::storagev1translator {

InsertRecordTranslator::InsertRecordTranslator(
    int64_t segment_id,
    DataType data_type,
    FieldDataInfo field_data_info,
    SchemaPtr schema,
    bool is_sorted_by_pk,
    std::vector<std::string> insert_files,
    ChunkedSegmentSealedImpl* chunked_segment)
    : segment_id_(segment_id),
      data_type_(data_type),
      key_(fmt::format("seg_{}_ir_f_{}", segment_id, field_data_info.field_id)),
      field_data_info_(field_data_info),
      schema_(schema),
      is_sorted_by_pk_(is_sorted_by_pk),
      insert_files_(insert_files),
      chunked_segment_(chunked_segment),
      meta_(milvus::cachinglayer::StorageType::MEMORY) {
}

size_t
InsertRecordTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
InsertRecordTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return 0;
}

milvus::cachinglayer::ResourceUsage
InsertRecordTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    return {0, 0};
}

const std::string&
InsertRecordTranslator::key() const {
    return key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::segcore::InsertRecord<true>>>>
InsertRecordTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    AssertInfo(cids.size() == 1 && cids[0] == 0,
               "InsertRecordTranslator only supports single cell");
    FieldId fid = FieldId(field_data_info_.field_id);
    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    // TODO(tiered storage 4): we should phase out this thread pool and use folly executor.
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    pool.Submit(LoadArrowReaderFromRemote,
                insert_files_,
                field_data_info_.arrow_reader_channel);
    LOG_INFO("segment {} submits load field {} task to thread pool",
             segment_id_,
             field_data_info_.field_id);
    auto num_rows = field_data_info_.row_count;
    AssertInfo(milvus::SystemProperty::Instance().IsSystem(fid),
               "system field is not system field");
    auto system_field_type =
        milvus::SystemProperty::Instance().GetSystemFieldType(fid);
    AssertInfo(system_field_type == SystemFieldType::Timestamp,
               "system field is not timestamp");
    std::vector<Timestamp> timestamps(num_rows);
    int64_t offset = 0;
    FieldMeta field_meta(
        FieldName(""), FieldId(0), DataType::INT64, false, std::nullopt);

    std::shared_ptr<milvus::ArrowDataWrapper> r;
    while (field_data_info_.arrow_reader_channel->pop(r)) {
        arrow::ArrayVector array_vec = read_single_column_batches(r->reader);
        auto chunk = create_chunk(field_meta, 1, array_vec);
        auto chunk_ptr = static_cast<FixedWidthChunk*>(chunk.get());
        std::copy_n(static_cast<const Timestamp*>(chunk_ptr->Span().data()),
                    chunk_ptr->Span().row_count(),
                    timestamps.data() + offset);
        offset += chunk_ptr->Span().row_count();
    }

    TimestampIndex index;
    auto min_slice_length = num_rows < 4096 ? 1 : 4096;
    auto meta =
        GenerateFakeSlices(timestamps.data(), num_rows, min_slice_length);
    index.set_length_meta(std::move(meta));
    // todo ::opt to avoid copy timestamps from field data
    index.build_with(timestamps.data(), num_rows);

    std::unique_ptr<milvus::segcore::InsertRecord<true>> ir =
        std::make_unique<milvus::segcore::InsertRecord<true>>(*schema_,
                                                              MAX_ROW_COUNT);

    // use special index
    AssertInfo(ir->timestamps_.empty(), "already exists");
    ir->timestamps_.set_data_raw(0, timestamps.data(), timestamps.size());
    ir->timestamp_index_ = std::move(index);
    AssertInfo(ir->timestamps_.num_chunk() == 1,
               "num chunk not equal to 1 for sealed segment");
    chunked_segment_->stats_.mem_size += sizeof(Timestamp) * num_rows;

    auto pk_field_id = schema_->get_primary_field_id();
    AssertInfo(pk_field_id.has_value(),
               "primary key field not found in schema");
    auto pk_field_meta = schema_->operator[](pk_field_id.value());

    // set pks to offset
    if (!is_sorted_by_pk_) {
        AssertInfo(ir->empty_pks(), "already exists");
        auto it = chunked_segment_->fields_.find(pk_field_id.value());
        AssertInfo(it != chunked_segment_->fields_.end(),
                   "primary key field not found in segment");
        ir->insert_pks(pk_field_meta.get_data_type(), it->second.get());
        ir->seal_pks();
    }
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::segcore::InsertRecord<true>>>>
        cells;
    cells.emplace_back(0, std::move(ir));
    return cells;
}

}  // namespace milvus::segcore::storagev1translator
