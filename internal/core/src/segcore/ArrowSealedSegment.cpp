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

#include "segcore/ArrowSealedSegment.h"

#include <arrow/array.h>
#include <arrow/type.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "segcore/Utils.h"

namespace milvus::segcore {

namespace {

std::string
ExpectedTypeName(arrow::Type::type type) {
    switch (type) {
        case arrow::Type::INT64:
            return "int64";
        case arrow::Type::STRING:
            return "string";
        default:
            return "arrow type " + std::to_string(static_cast<int>(type));
    }
}

arrow::Type::type
ExpectedArrowType(DataType type) {
    switch (type) {
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return arrow::Type::INT64;
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::TEXT:
            return arrow::Type::STRING;
        default:
            throw std::invalid_argument("unsupported Arrow POC field type " +
                                        std::to_string(static_cast<int>(type)));
    }
}

void
ValidateArrowType(const arrow::DataType& type,
                  arrow::Type::type expected,
                  std::string_view field_name) {
    if (type.id() != expected) {
        throw std::invalid_argument("Arrow field '" + std::string(field_name) +
                                    "' has type " + type.ToString() +
                                    ", expected " + ExpectedTypeName(expected));
    }
}

[[noreturn]] void
Unsupported(std::string_view method) {
    throw std::runtime_error("ArrowSealedSegment POC does not implement " +
                             std::string(method));
}

int64_t
ArrowArrayDataBytes(const std::shared_ptr<arrow::ArrayData>& data) {
    if (data == nullptr) {
        return 0;
    }

    int64_t bytes = 0;
    for (const auto& buffer : data->buffers) {
        if (buffer != nullptr) {
            bytes += buffer->size();
        }
    }
    for (const auto& child : data->child_data) {
        bytes += ArrowArrayDataBytes(child);
    }
    return bytes;
}

int64_t
ArrowArrayBytes(const std::shared_ptr<arrow::Array>& array) {
    if (array == nullptr) {
        return 0;
    }
    return ArrowArrayDataBytes(array->data());
}

int64_t
RecordBatchBytes(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (batch == nullptr) {
        return 0;
    }

    int64_t bytes = 0;
    for (int i = 0; i < batch->num_columns(); ++i) {
        bytes += ArrowArrayBytes(batch->column(i));
    }
    return std::max<int64_t>(bytes, 1);
}

}  // namespace

struct ArrowSealedSegment::RecordBatchCell {
    explicit RecordBatchCell(std::shared_ptr<arrow::RecordBatch> batch)
        : batch_(std::move(batch)), byte_size_(RecordBatchBytes(batch_)) {
    }

    cachinglayer::ResourceUsage
    CellByteSize() const {
        return {byte_size_, 0};
    }

    const std::shared_ptr<arrow::RecordBatch>&
    batch() const {
        return batch_;
    }

 private:
    std::shared_ptr<arrow::RecordBatch> batch_;
    int64_t byte_size_;
};

struct ArrowSealedSegment::RecordBatchCacheState {
    RecordBatchCacheState(
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
        std::shared_ptr<std::atomic<int64_t>> latency_us)
        : batches_(std::move(batches)), latency_us_(std::move(latency_us)) {
        load_counts_.reserve(batches_.size());
        for (size_t i = 0; i < batches_.size(); ++i) {
            load_counts_.push_back(std::make_shared<std::atomic<size_t>>(0));
        }
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::vector<std::shared_ptr<std::atomic<size_t>>> load_counts_;
    std::shared_ptr<std::atomic<int64_t>> latency_us_;
};

class ArrowRecordBatchTranslator final
    : public cachinglayer::Translator<ArrowSealedSegment::RecordBatchCell> {
 public:
    ArrowRecordBatchTranslator(
        int64_t segment_id,
        int64_t column_group_id,
        std::shared_ptr<ArrowSealedSegment::RecordBatchCacheState> state)
        : state_(std::move(state)),
          key_("arrow-segment-" + std::to_string(segment_id) +
               "-column-group-" + std::to_string(column_group_id) +
               "-record-batches"),
          meta_(cachinglayer::StorageType::MEMORY,
                cachinglayer::CellIdMappingMode::IDENTICAL,
                cachinglayer::CellDataType::SCALAR_FIELD,
                CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                true) {
    }

    size_t
    num_cells() const override {
        return state_->batches_.size();
    }

    cachinglayer::cid_t
    cell_id_of(cachinglayer::uid_t uid) const override {
        return uid;
    }

    std::pair<cachinglayer::ResourceUsage, cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(cachinglayer::cid_t cid) const override {
        return {{RecordBatchBytes(state_->batches_.at(cid)), 0}, {0, 0}};
    }

    const std::string&
    key() const override {
        return key_;
    }

    cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

    int64_t
    cells_storage_bytes(
        const std::vector<cachinglayer::cid_t>& cids) const override {
        int64_t bytes = 0;
        for (auto cid : cids) {
            bytes += RecordBatchBytes(state_->batches_.at(cid));
        }
        return bytes;
    }

    std::vector<std::pair<cachinglayer::cid_t,
                          std::unique_ptr<ArrowSealedSegment::RecordBatchCell>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<cachinglayer::cid_t>& cids) override {
        std::vector<
            std::pair<cachinglayer::cid_t,
                      std::unique_ptr<ArrowSealedSegment::RecordBatchCell>>>
            result;
        result.reserve(cids.size());
        for (auto cid : cids) {
            auto latency_us =
                state_->latency_us_->load(std::memory_order_relaxed);
            if (latency_us > 0) {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(latency_us));
            }
            state_->load_counts_.at(cid)->fetch_add(1,
                                                    std::memory_order_relaxed);
            result.emplace_back(
                cid,
                std::make_unique<ArrowSealedSegment::RecordBatchCell>(
                    state_->batches_.at(cid)));
        }
        return result;
    }

 private:
    std::shared_ptr<ArrowSealedSegment::RecordBatchCacheState> state_;
    std::string key_;
    cachinglayer::Meta meta_;
};

ArrowSealedSegment::ArrowSealedSegment(
    SchemaPtr schema,
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
    int64_t segment_id)
    : schema_(std::move(schema)),
      backing_batches_(std::move(batches)),
      segment_id_(segment_id),
      simulated_load_latency_us_(std::make_shared<std::atomic<int64_t>>(0)),
      insert_record_(*schema_, 1),
      timestamps_(1) {
    if (schema_ == nullptr) {
        throw std::invalid_argument("ArrowSealedSegment requires a schema");
    }
    if (backing_batches_.empty()) {
        throw std::invalid_argument(
            "ArrowSealedSegment requires at least one RecordBatch");
    }
    BuildFieldMap();
}

std::shared_ptr<ArrowSealedSegment>
ArrowSealedSegment::FromRecordBatches(
    SchemaPtr schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    int64_t segment_id) {
    return std::make_shared<ArrowSealedSegment>(
        std::move(schema), batches, segment_id);
}

int64_t
ArrowSealedSegment::row_count() const {
    return row_count_;
}

std::shared_ptr<arrow::Schema>
ArrowSealedSegment::arrow_schema() const {
    return arrow_schema_;
}

void
ArrowSealedSegment::SetSimulatedLoadLatency(std::chrono::milliseconds latency) {
    simulated_load_latency_us_->store(
        std::chrono::duration_cast<std::chrono::microseconds>(latency).count(),
        std::memory_order_relaxed);
}

bool
ArrowSealedSegment::EvictArrowRecordBatch(int64_t column_group_id,
                                          int64_t row_stripe_id) const {
    if (column_group_id < 0 ||
        column_group_id >= static_cast<int64_t>(column_groups_.size())) {
        throw std::out_of_range("Arrow column group id out of range");
    }
    const auto& slot = column_groups_[column_group_id].slot;
    if (row_stripe_id < 0 ||
        row_stripe_id >= static_cast<int64_t>(slot->num_cells())) {
        throw std::out_of_range("Arrow row stripe id out of range");
    }
    return slot->ManualEvict(row_stripe_id);
}

size_t
ArrowSealedSegment::ArrowRecordBatchLoadCount(int64_t column_group_id,
                                              int64_t row_stripe_id) const {
    if (column_group_id < 0 ||
        column_group_id >= static_cast<int64_t>(column_groups_.size())) {
        throw std::out_of_range("Arrow column group id out of range");
    }
    const auto& state = column_groups_[column_group_id].state;
    if (row_stripe_id < 0 ||
        row_stripe_id >= static_cast<int64_t>(state->load_counts_.size())) {
        throw std::out_of_range("Arrow row stripe id out of range");
    }
    return state->load_counts_.at(row_stripe_id)
        ->load(std::memory_order_relaxed);
}

PinWrapper<std::shared_ptr<arrow::RecordBatch>>
ArrowSealedSegment::PinArrowRecordBatchForTest(milvus::OpContext* op_ctx,
                                               int64_t column_group_id,
                                               int64_t row_stripe_id) const {
    auto pinned = PinRecordBatch(op_ctx, column_group_id, row_stripe_id);
    return PinWrapper<std::shared_ptr<arrow::RecordBatch>>(
        pinned.accessor, std::move(pinned.batch));
}

int64_t
ArrowSealedSegment::ArrowColumnGroupCountForTest() const {
    return column_groups_.size();
}

int64_t
ArrowSealedSegment::ArrowFieldColumnGroupForTest(FieldId field_id) const {
    auto iter = field_to_location_.find(field_id);
    if (iter == field_to_location_.end()) {
        throw std::invalid_argument("Arrow field id " +
                                    std::to_string(field_id.get()) +
                                    " does not exist");
    }
    return iter->second.column_group_id;
}

size_t
ArrowSealedSegment::ArrowBatchIteratorCreatedCountForTest() const {
    return arrow_batch_iterator_created_count_.load(
        std::memory_order_relaxed);
}

ArrowSealedSegment::RecordBatchIterator::RecordBatchIterator(
    const ArrowSealedSegment* segment,
    milvus::OpContext* op_ctx,
    Projection field_ids,
    CandidateSelection input)
    : segment_(segment),
      op_ctx_(op_ctx),
      field_ids_(std::move(field_ids)),
      next_row_offset_(input.row_begin()),
      row_end_(input.row_end()) {
    if (segment_ == nullptr) {
        throw std::invalid_argument("RecordBatchIterator requires a segment");
    }
    if (input.row_begin() < 0 || input.row_end() > segment_->row_count_) {
        throw std::out_of_range("Arrow candidate selection is outside segment");
    }
    for (auto field_id : field_ids_) {
        if (!segment_->is_field_exist(field_id)) {
            throw std::invalid_argument("Arrow field id " +
                                        std::to_string(field_id.get()) +
                                        " does not exist");
        }
    }
}

bool
ArrowSealedSegment::RecordBatchIterator::HasNext() const {
    return next_row_offset_ < row_end_;
}

ArrowSealedSegment::BatchView
ArrowSealedSegment::RecordBatchIterator::Next() {
    if (!HasNext()) {
        throw std::out_of_range("Arrow batch iterator is exhausted");
    }

    std::vector<FieldId> output_fields = field_ids_;
    if (output_fields.empty()) {
        for (const auto& column_group : segment_->column_groups_) {
            output_fields.insert(output_fields.end(),
                                 column_group.field_ids.begin(),
                                 column_group.field_ids.end());
        }
    }
    if (output_fields.empty()) {
        throw std::invalid_argument("Arrow batch iterator has no fields");
    }

    const auto location = segment_->LocateOffset(next_row_offset_);
    const auto row_stripe_id = location.chunk_id;
    const auto local_offset = location.local_offset;
    const auto row_stripe_end =
        segment_->rows_until_batch_.at(row_stripe_id + 1);
    const auto output_row_count =
        std::min(row_end_, row_stripe_end) - next_row_offset_;

    std::vector<std::shared_ptr<cachinglayer::CellAccessor<RecordBatchCell>>>
        accessors;
    std::unordered_map<int64_t, std::shared_ptr<arrow::RecordBatch>> batches;
    for (auto field_id : output_fields) {
        const auto& location = segment_->field_to_location_.at(field_id);
        if (batches.find(location.column_group_id) != batches.end()) {
            continue;
        }
        auto pinned = segment_->PinRecordBatch(
            op_ctx_, location.column_group_id, row_stripe_id);
        accessors.push_back(std::move(pinned.accessor));
        batches.emplace(location.column_group_id, std::move(pinned.batch));
    }

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    fields.reserve(output_fields.size());
    columns.reserve(output_fields.size());
    for (auto field_id : output_fields) {
        const auto& location = segment_->field_to_location_.at(field_id);
        const auto& batch = batches.at(location.column_group_id);
        fields.push_back(batch->schema()->field(location.column_index));
        columns.push_back(batch->column(location.column_index)
                              ->Slice(local_offset, output_row_count));
    }
    auto batch = arrow::RecordBatch::Make(
        std::make_shared<arrow::Schema>(std::move(fields)),
        output_row_count,
        std::move(columns));
    const auto row_begin = next_row_offset_;
    next_row_offset_ += output_row_count;

    return {row_begin,
            batch->num_rows(),
            PinWrapper<std::shared_ptr<arrow::RecordBatch>>(
                std::move(accessors), std::move(batch))};
}

bool
ArrowSealedSegment::CanUseArrowBatchIterator(const Projection& fields) const {
    if (fields.empty()) {
        return true;
    }
    for (auto field_id : fields) {
        if (!is_field_exist(field_id)) {
            return false;
        }
        auto field_type = schema_->operator[](field_id).get_data_type();
        try {
            (void)ExpectedArrowType(field_type);
        } catch (const std::invalid_argument&) {
            return false;
        }
    }
    return true;
}

CandidateSelection
ArrowSealedSegment::Prune(const PrunePredicate& predicate,
                          CandidateSelection input) const {
    (void)predicate;
    return input;
}

std::unique_ptr<ArrowBatchIterator>
ArrowSealedSegment::Iterate(
    milvus::OpContext* op_ctx,
    Projection fields,
    CandidateSelection input) const {
    arrow_batch_iterator_created_count_.fetch_add(
        1, std::memory_order_relaxed);
    return std::unique_ptr<ArrowBatchIterator>(
        new RecordBatchIterator(this, op_ctx, std::move(fields), input));
}

void
ArrowSealedSegment::LoadIndex(LoadIndexInfo& info) {
    Unsupported("LoadIndex");
}

void
ArrowSealedSegment::LoadSegmentMeta(
    const milvus::proto::segcore::LoadSegmentMeta& meta) {
    Unsupported("LoadSegmentMeta");
}

void
ArrowSealedSegment::DropIndex(const FieldId field_id) {
    Unsupported("DropIndex");
}

void
ArrowSealedSegment::DropJSONIndex(const FieldId field_id,
                                  const std::string& nested_path) {
    Unsupported("DropJSONIndex");
}

void
ArrowSealedSegment::DropFieldData(const FieldId field_id) {
    Unsupported("DropFieldData");
}

void
ArrowSealedSegment::AddFieldDataInfoForSealed(
    const LoadFieldDataInfo& field_data_info) {
    Unsupported("AddFieldDataInfoForSealed");
}

void
ArrowSealedSegment::RemoveFieldFile(const FieldId field_id) {
    Unsupported("RemoveFieldFile");
}

void
ArrowSealedSegment::ClearData() {
    arrow_schema_.reset();
    backing_batches_.clear();
    rows_until_batch_.clear();
    row_count_ = 0;
    field_to_location_.clear();
    column_groups_.clear();
}

std::unique_ptr<DataArray>
ArrowSealedSegment::get_vector(milvus::OpContext* op_ctx,
                               FieldId field_id,
                               const int64_t* ids,
                               int64_t count) const {
    Unsupported("get_vector");
}

void
ArrowSealedSegment::LoadTextIndex(
    milvus::OpContext* op_ctx,
    std::shared_ptr<milvus::proto::indexcgo::LoadTextIndexInfo> info_proto) {
    Unsupported("LoadTextIndex");
}

InsertRecord<true>&
ArrowSealedSegment::get_insert_record() {
    return insert_record_;
}

void
ArrowSealedSegment::LoadJsonStats(FieldId field_id,
                                  std::shared_ptr<index::JsonKeyStats> stats) {
    Unsupported("LoadJsonStats");
}

void
ArrowSealedSegment::RemoveJsonStats(FieldId field_id) {
    Unsupported("RemoveJsonStats");
}

PinWrapper<index::NgramInvertedIndex*>
ArrowSealedSegment::GetNgramIndex(milvus::OpContext* op_ctx,
                                  FieldId field_id) const {
    Unsupported("GetNgramIndex");
}

PinWrapper<index::NgramInvertedIndex*>
ArrowSealedSegment::GetNgramIndexForJson(milvus::OpContext* op_ctx,
                                         FieldId field_id,
                                         const std::string& nested_path) const {
    Unsupported("GetNgramIndexForJson");
}

bool
ArrowSealedSegment::Contain(const PkType& pk) const {
    auto pk_field = schema_->get_primary_field_id();
    if (!pk_field.has_value() || !is_field_exist(pk_field.value())) {
        return false;
    }

    auto field_type = schema_->operator[](pk_field.value()).get_data_type();
    if (field_type == DataType::INT64) {
        auto pk_value = std::get_if<int64_t>(&pk);
        if (pk_value == nullptr) {
            return false;
        }
        for (int64_t chunk_id = 0; chunk_id < num_chunk_data(pk_field.value());
             ++chunk_id) {
            auto location = field_to_location_.at(pk_field.value());
            auto pinned =
                PinRecordBatch(nullptr, location.column_group_id, chunk_id);
            auto array = std::static_pointer_cast<arrow::Int64Array>(
                GetFieldArray(*pinned.batch, pk_field.value()));
            for (int64_t i = 0; i < array->length(); ++i) {
                if (!array->IsNull(i) && array->Value(i) == *pk_value) {
                    return true;
                }
            }
        }
        return false;
    }

    auto pk_value = std::get_if<std::string>(&pk);
    if (pk_value == nullptr) {
        return false;
    }
    for (int64_t chunk_id = 0; chunk_id < num_chunk_data(pk_field.value());
         ++chunk_id) {
        auto location = field_to_location_.at(pk_field.value());
        auto pinned =
            PinRecordBatch(nullptr, location.column_group_id, chunk_id);
        auto array = std::static_pointer_cast<arrow::StringArray>(
            GetFieldArray(*pinned.batch, pk_field.value()));
        for (int64_t i = 0; i < array->length(); ++i) {
            if (!array->IsNull(i) && array->GetView(i) == *pk_value) {
                return true;
            }
        }
    }
    return false;
}

size_t
ArrowSealedSegment::GetMemoryUsageInBytes() const {
    int64_t bytes = 0;
    for (const auto& column_group : column_groups_) {
        const auto& slot = column_group.slot;
        for (cachinglayer::cid_t cid = 0;
             cid < static_cast<cachinglayer::cid_t>(slot->num_cells());
             ++cid) {
            bytes += slot->size_of_cell(cid).memory_bytes;
        }
    }
    return bytes;
}

int64_t
ArrowSealedSegment::get_row_count() const {
    return row_count_;
}

const Schema&
ArrowSealedSegment::get_schema() const {
    return *schema_;
}

int64_t
ArrowSealedSegment::get_deleted_count() const {
    return 0;
}

SegcoreError
ArrowSealedSegment::Delete(int64_t size,
                           const IdArray* pks,
                           const Timestamp* timestamps) {
    return SegcoreError::success();
}

void
ArrowSealedSegment::LoadDeletedRecord(const LoadDeletedRecordInfo& info) {
    Unsupported("LoadDeletedRecord");
}

void
ArrowSealedSegment::LoadFieldData(const LoadFieldDataInfo& info,
                                  milvus::OpContext* op_ctx) {
    Unsupported("LoadFieldData");
}

int64_t
ArrowSealedSegment::get_segment_id() const {
    return segment_id_;
}

bool
ArrowSealedSegment::HasRawData(int64_t field_id) const {
    return is_field_exist(FieldId(field_id));
}

bool
ArrowSealedSegment::HasFieldData(FieldId field_id) const {
    return is_field_exist(field_id);
}

bool
ArrowSealedSegment::is_nullable(FieldId field_id) const {
    return schema_->operator[](field_id).is_nullable();
}

void
ArrowSealedSegment::CreateTextIndex(FieldId field_id,
                                    milvus::OpContext* op_ctx) {
    Unsupported("CreateTextIndex");
}

void
ArrowSealedSegment::BulkGetJsonData(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const std::function<void(milvus::Json, size_t, bool)>& fn,
    const int64_t* offsets,
    int64_t count) const {
    Unsupported("BulkGetJsonData");
}

void
ArrowSealedSegment::LazyCheckSchema(SchemaPtr sch) {
    schema_ = std::move(sch);
    BuildFieldMap();
}

void
ArrowSealedSegment::Reopen(SchemaPtr sch) {
    LazyCheckSchema(std::move(sch));
}

void
ArrowSealedSegment::Reopen(
    milvus::OpContext* op_ctx,
    const milvus::proto::segcore::SegmentLoadInfo& new_load_info) {
    Unsupported("Reopen(SegmentLoadInfo)");
}

void
ArrowSealedSegment::Load(milvus::tracer::TraceContext& trace_ctx,
                         milvus::OpContext* op_ctx) {
}

std::shared_ptr<const IArrayOffsets>
ArrowSealedSegment::GetArrayOffsets(FieldId field_id) const {
    return nullptr;
}

bool
ArrowSealedSegment::HasIndex(FieldId field_id) const {
    return false;
}

bool
ArrowSealedSegment::is_chunked() const {
    return true;
}

DataType
ArrowSealedSegment::GetFieldDataType(FieldId fieldId) const {
    return schema_->operator[](fieldId).get_data_type();
}

void
ArrowSealedSegment::vector_search(SearchInfo& search_info,
                                  const void* query_data,
                                  const size_t* query_offsets,
                                  int64_t query_count,
                                  Timestamp timestamp,
                                  const BitsetView& bitset,
                                  milvus::OpContext* op_context,
                                  SearchResult& output) const {
    Unsupported("vector_search");
}

void
ArrowSealedSegment::mask_with_delete(BitsetTypeView& bitset,
                                     int64_t ins_barrier,
                                     Timestamp timestamp) const {
}

int64_t
ArrowSealedSegment::num_chunk_data(FieldId field_id) const {
    auto iter = field_to_location_.find(field_id);
    if (iter == field_to_location_.end()) {
        throw std::invalid_argument("Arrow field id " +
                                    std::to_string(field_id.get()) +
                                    " does not exist");
    }
    return column_groups_[iter->second.column_group_id].slot->num_cells();
}

int64_t
ArrowSealedSegment::num_rows_until_chunk(FieldId field_id,
                                         int64_t chunk_id) const {
    if (!is_field_exist(field_id)) {
        throw std::invalid_argument("Arrow field id " +
                                    std::to_string(field_id.get()) +
                                    " does not exist");
    }
    if (chunk_id < 0 ||
        chunk_id >= static_cast<int64_t>(rows_until_batch_.size())) {
        throw std::out_of_range("Arrow chunk id out of range");
    }
    return rows_until_batch_[chunk_id];
}

void
ArrowSealedSegment::mask_with_timestamps(BitsetTypeView& bitset_chunk,
                                         Timestamp timestamp,
                                         Timestamp collection_ttl) const {
}

int64_t
ArrowSealedSegment::num_chunk(FieldId field_id) const {
    return num_chunk_data(field_id);
}

int64_t
ArrowSealedSegment::chunk_size(FieldId field_id, int64_t chunk_id) const {
    if (!is_field_exist(field_id)) {
        throw std::invalid_argument("Arrow field id " +
                                    std::to_string(field_id.get()) +
                                    " does not exist");
    }
    if (chunk_id < 0 ||
        chunk_id + 1 >= static_cast<int64_t>(rows_until_batch_.size())) {
        throw std::out_of_range("Arrow chunk id out of range");
    }
    return rows_until_batch_[chunk_id + 1] - rows_until_batch_[chunk_id];
}

std::pair<int64_t, int64_t>
ArrowSealedSegment::get_chunk_by_offset(FieldId field_id,
                                        int64_t offset) const {
    if (!is_field_exist(field_id)) {
        throw std::invalid_argument("Arrow field id " +
                                    std::to_string(field_id.get()) +
                                    " does not exist");
    }
    auto location = LocateOffset(offset);
    return {location.chunk_id, location.local_offset};
}

int64_t
ArrowSealedSegment::size_per_chunk() const {
    return row_count_;
}

int64_t
ArrowSealedSegment::get_active_count(Timestamp ts) const {
    return row_count_;
}

Timestamp
ArrowSealedSegment::get_max_timestamp() const {
    return 0;
}

void
ArrowSealedSegment::search_ids(BitsetType& bitset,
                               const IdArray& id_array) const {
    bitset.set();
    auto pk_field = schema_->get_primary_field_id();
    if (!pk_field.has_value()) {
        return;
    }
    for (int64_t offset = 0; offset < row_count_; ++offset) {
        bool found = false;
        if (id_array.has_int_id()) {
            auto location = LocateOffset(offset);
            auto field_location = field_to_location_.at(pk_field.value());
            auto pinned = PinRecordBatch(
                nullptr, field_location.column_group_id, location.chunk_id);
            auto array = std::static_pointer_cast<arrow::Int64Array>(
                GetFieldArray(*pinned.batch, pk_field.value()));
            if (!array->IsNull(location.local_offset)) {
                auto value = array->Value(location.local_offset);
                for (auto id : id_array.int_id().data()) {
                    if (value == id) {
                        found = true;
                        break;
                    }
                }
            }
        }
        bitset.set(offset, !found);
    }
}

std::pair<std::vector<OffsetMap::OffsetType>, bool>
ArrowSealedSegment::find_first_n(int64_t limit,
                                 const BitsetTypeView& bitset) const {
    if (limit == Unlimited || limit == NoLimit) {
        limit = static_cast<int64_t>(bitset.size());
    }

    std::vector<OffsetMap::OffsetType> offsets;
    offsets.reserve(std::min<int64_t>(limit, bitset.size()));
    auto result = bitset.find_first(false);
    while (result.has_value() && static_cast<int64_t>(offsets.size()) < limit) {
        offsets.push_back(result.value());
        result = bitset.find_next(result.value(), false);
    }
    return {offsets, result.has_value()};
}

std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
ArrowSealedSegment::find_first_n_element(
    int64_t limit,
    const BitsetTypeView& element_bitset,
    const IArrayOffsets* array_offsets,
    const std::optional<QueryIteratorCursor>& cursor) const {
    Unsupported("find_first_n_element");
}

bool
ArrowSealedSegment::is_mmap_field(FieldId field_id) const {
    return false;
}

bool
ArrowSealedSegment::is_field_exist(FieldId field_id) const {
    return field_to_location_.find(field_id) != field_to_location_.end();
}

void
ArrowSealedSegment::bulk_subscript(milvus::OpContext* op_ctx,
                                   SystemFieldType system_type,
                                   const int64_t* seg_offsets,
                                   int64_t count,
                                   void* output) const {
    Unsupported("bulk_subscript(system field)");
}

void
ArrowSealedSegment::bulk_subscript(milvus::OpContext* op_ctx,
                                   FieldId field_id,
                                   DataType data_type,
                                   const int64_t* seg_offsets,
                                   int64_t count,
                                   void* data,
                                   TargetBitmap& valid_map,
                                   bool small_int_raw_type) const {
    ValidateFieldType(field_id, data_type);
    auto field_type = schema_->operator[](field_id).get_data_type();
    const auto field_location = field_to_location_.at(field_id);
    valid_map.set();

    if (field_type == DataType::INT64 || field_type == DataType::TIMESTAMPTZ) {
        auto* output = static_cast<int64_t*>(data);
        for (int64_t i = 0; i < count; ++i) {
            auto location = LocateOffset(seg_offsets[i]);
            auto pinned = PinRecordBatch(
                op_ctx, field_location.column_group_id, location.chunk_id);
            auto array = std::static_pointer_cast<arrow::Int64Array>(
                GetFieldArray(*pinned.batch, field_id));
            if (array->IsNull(location.local_offset)) {
                valid_map.set(i, false);
                output[i] = 0;
            } else {
                output[i] = array->Value(location.local_offset);
            }
        }
        return;
    }

    if (field_type == DataType::VARCHAR || field_type == DataType::STRING ||
        field_type == DataType::TEXT) {
        auto* output = static_cast<std::string*>(data);
        for (int64_t i = 0; i < count; ++i) {
            auto location = LocateOffset(seg_offsets[i]);
            auto pinned = PinRecordBatch(
                op_ctx, field_location.column_group_id, location.chunk_id);
            auto array = std::static_pointer_cast<arrow::StringArray>(
                GetFieldArray(*pinned.batch, field_id));
            if (array->IsNull(location.local_offset)) {
                valid_map.set(i, false);
                output[i].clear();
            } else {
                output[i] = array->GetString(location.local_offset);
            }
        }
        return;
    }

    Unsupported("bulk_subscript(field)");
}

std::unique_ptr<DataArray>
ArrowSealedSegment::bulk_subscript(milvus::OpContext* op_ctx,
                                   FieldId field_id,
                                   const int64_t* seg_offsets,
                                   int64_t count) const {
    auto field_type = schema_->operator[](field_id).get_data_type();
    TargetBitmap valid_map(count);
    if (field_type == DataType::INT64 || field_type == DataType::TIMESTAMPTZ) {
        FixedVector<int64_t> values(count);
        bulk_subscript(op_ctx,
                       field_id,
                       field_type,
                       seg_offsets,
                       count,
                       values.data(),
                       valid_map);
        auto data_array = std::make_unique<DataArray>();
        data_array->set_field_id(field_id.get());
        CreateScalarDataArray(*data_array, count, field_type, field_type, true);
        auto* valid_data = data_array->mutable_valid_data()->mutable_data();
        auto* long_data = field_type == DataType::TIMESTAMPTZ
                              ? data_array->mutable_scalars()
                                    ->mutable_timestamptz_data()
                                    ->mutable_data()
                                    ->mutable_data()
                              : data_array->mutable_scalars()
                                    ->mutable_long_data()
                                    ->mutable_data()
                                    ->mutable_data();
        for (int64_t i = 0; i < count; ++i) {
            valid_data[i] = valid_map[i];
            long_data[i] = values[i];
        }
        return data_array;
    }

    if (field_type == DataType::VARCHAR || field_type == DataType::STRING ||
        field_type == DataType::TEXT) {
        FixedVector<std::string> values(count);
        bulk_subscript(op_ctx,
                       field_id,
                       field_type,
                       seg_offsets,
                       count,
                       values.data(),
                       valid_map);
        auto data_array = std::make_unique<DataArray>();
        data_array->set_field_id(field_id.get());
        CreateScalarDataArray(*data_array, count, field_type, field_type, true);
        auto* valid_data = data_array->mutable_valid_data()->mutable_data();
        auto* string_data = data_array->mutable_scalars()
                                ->mutable_string_data()
                                ->mutable_data();
        for (int64_t i = 0; i < count; ++i) {
            valid_data[i] = valid_map[i];
            (*string_data)[i] = values[i];
        }
        return data_array;
    }

    Unsupported("bulk_subscript(DataArray)");
}

std::unique_ptr<DataArray>
ArrowSealedSegment::bulk_subscript(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const int64_t* seg_offsets,
    int64_t count,
    const std::vector<std::string>& dynamic_field_names) const {
    Unsupported("bulk_subscript(dynamic fields)");
}

void
ArrowSealedSegment::pk_range(milvus::OpContext* op_ctx,
                             proto::plan::OpType op,
                             const PkType& pk,
                             BitsetTypeView& bitset) const {
    Unsupported("pk_range");
}

void
ArrowSealedSegment::pk_binary_range(milvus::OpContext* op_ctx,
                                    const PkType& lower_pk,
                                    bool lower_inclusive,
                                    const PkType& upper_pk,
                                    bool upper_inclusive,
                                    BitsetTypeView& bitset) const {
    Unsupported("pk_binary_range");
}

PinWrapper<SpanBase>
ArrowSealedSegment::chunk_data_impl(milvus::OpContext* op_ctx,
                                    FieldId field_id,
                                    int64_t chunk_id) const {
    auto field_type = schema_->operator[](field_id).get_data_type();
    if (field_type != DataType::INT64 && field_type != DataType::TIMESTAMPTZ) {
        Unsupported("chunk_data_impl(non-int64)");
    }

    const auto field_location = field_to_location_.at(field_id);
    auto pinned =
        PinRecordBatch(op_ctx, field_location.column_group_id, chunk_id);
    auto array = std::static_pointer_cast<arrow::Int64Array>(
        GetFieldArray(*pinned.batch, field_id));
    if (array->null_count() == 0) {
        return PinWrapper<SpanBase>(
            pinned.accessor,
            SpanBase(array->raw_values(), array->length(), sizeof(int64_t)));
    }

    auto validity = std::make_shared<FixedVector<bool>>(
        BuildValidity(*array, 0, array->length()));
    return PinWrapper<SpanBase>(std::make_pair(pinned.accessor, validity),
                                SpanBase(array->raw_values(),
                                         validity->data(),
                                         array->length(),
                                         sizeof(int64_t)));
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
ArrowSealedSegment::chunk_string_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    const auto field_location = field_to_location_.at(field_id);
    auto pinned =
        PinRecordBatch(op_ctx, field_location.column_group_id, chunk_id);
    auto array = std::static_pointer_cast<arrow::StringArray>(
        GetFieldArray(*pinned.batch, field_id));
    auto start = offset_len.has_value() ? offset_len->first : 0;
    auto length = offset_len.has_value() ? offset_len->second : array->length();
    FixedVector<bool> valid = BuildValidity(*array, start, length);
    std::vector<std::string_view> views;
    views.reserve(length);
    for (int64_t i = 0; i < length; ++i) {
        auto local = start + i;
        if (array->IsNull(local)) {
            views.emplace_back();
        } else {
            views.emplace_back(array->GetView(local));
        }
    }
    return PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
        pinned.accessor, std::make_pair(std::move(views), std::move(valid)));
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
ArrowSealedSegment::chunk_array_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    Unsupported("chunk_array_view_impl");
}

PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
ArrowSealedSegment::chunk_vector_array_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    Unsupported("chunk_vector_array_view_impl");
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
ArrowSealedSegment::chunk_string_views_by_offsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    const auto field_location = field_to_location_.at(field_id);
    auto pinned =
        PinRecordBatch(op_ctx, field_location.column_group_id, chunk_id);
    auto array = std::static_pointer_cast<arrow::StringArray>(
        GetFieldArray(*pinned.batch, field_id));
    FixedVector<bool> valid(offsets.size(), true);
    std::vector<std::string_view> views;
    views.reserve(offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        auto local = offsets[i];
        if (array->IsNull(local)) {
            valid[i] = false;
            views.emplace_back();
        } else {
            views.emplace_back(array->GetView(local));
        }
    }
    return PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
        pinned.accessor, std::make_pair(std::move(views), std::move(valid)));
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
ArrowSealedSegment::chunk_array_views_by_offsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    Unsupported("chunk_array_views_by_offsets");
}

void
ArrowSealedSegment::check_search(const query::Plan* plan) const {
    Assert(plan);
}

const ConcurrentVector<Timestamp>&
ArrowSealedSegment::get_timestamps() const {
    return timestamps_;
}

ArrowSealedSegment::PinnedRecordBatch
ArrowSealedSegment::PinRecordBatch(milvus::OpContext* op_ctx,
                                   int64_t column_group_id,
                                   int64_t row_stripe_id) const {
    if (column_group_id < 0 ||
        column_group_id >= static_cast<int64_t>(column_groups_.size())) {
        throw std::out_of_range("Arrow column group id out of range");
    }
    const auto& slot = column_groups_[column_group_id].slot;
    if (row_stripe_id < 0 ||
        row_stripe_id >= static_cast<int64_t>(slot->num_cells())) {
        throw std::out_of_range("Arrow row stripe id out of range");
    }
    auto accessor =
        cachinglayer::SemiInlineGet(slot->PinCells(op_ctx, {row_stripe_id}));
    auto cell = accessor->get_cell_of(row_stripe_id);
    AssertInfo(cell != nullptr,
               "Arrow RecordBatch cache cell is null, column_group_id: {}, "
               "row_stripe_id: {}",
               column_group_id,
               row_stripe_id);
    return {std::move(accessor), cell->batch()};
}

std::shared_ptr<arrow::Array>
ArrowSealedSegment::GetFieldArray(const arrow::RecordBatch& batch,
                                  FieldId field_id) const {
    auto iter = field_to_location_.find(field_id);
    if (iter == field_to_location_.end()) {
        throw std::invalid_argument("Arrow field id " +
                                    std::to_string(field_id.get()) +
                                    " does not exist");
    }
    return batch.column(iter->second.column_index);
}

ArrowSealedSegment::ChunkLocation
ArrowSealedSegment::LocateOffset(int64_t offset) const {
    if (offset < 0 || offset >= row_count_) {
        throw std::out_of_range("row offset " + std::to_string(offset) +
                                " is outside segment row count " +
                                std::to_string(row_count_));
    }

    for (int64_t chunk_id = 0;
         chunk_id + 1 < static_cast<int64_t>(rows_until_batch_.size());
         ++chunk_id) {
        const auto row_base = rows_until_batch_[chunk_id];
        const auto row_end = rows_until_batch_[chunk_id + 1];
        if (offset < row_end) {
            return {chunk_id, offset - row_base};
        }
    }

    throw std::out_of_range("row offset " + std::to_string(offset) +
                            " is not present in Arrow RecordBatch stripes");
}

void
ArrowSealedSegment::BuildFieldMap() {
    field_to_location_.clear();
    column_groups_.clear();
    rows_until_batch_.clear();
    row_count_ = 0;

    if (backing_batches_.empty() || backing_batches_.front() == nullptr) {
        throw std::invalid_argument(
            "ArrowSealedSegment requires at least one non-null RecordBatch");
    }
    arrow_schema_ = backing_batches_.front()->schema();
    if (arrow_schema_ == nullptr) {
        throw std::invalid_argument("Arrow RecordBatch requires a schema");
    }

    rows_until_batch_.reserve(backing_batches_.size() + 1);
    rows_until_batch_.push_back(0);
    for (size_t i = 0; i < backing_batches_.size(); ++i) {
        const auto& batch = backing_batches_[i];
        if (batch == nullptr) {
            throw std::invalid_argument("Arrow RecordBatch " +
                                        std::to_string(i) + " is null");
        }
        if (!batch->schema()->Equals(*arrow_schema_, false)) {
            throw std::invalid_argument(
                "Arrow RecordBatch schemas differ across row stripes");
        }
        row_count_ += batch->num_rows();
        rows_until_batch_.push_back(row_count_);
    }

    std::unordered_map<std::string, std::pair<FieldId, DataType>>
        fields_by_name;
    for (const auto& [field_id, field_meta] : *schema_) {
        fields_by_name.emplace(
            field_meta.get_name().get(),
            std::make_pair(field_id, field_meta.get_data_type()));
    }

    for (int field_index = 0; field_index < arrow_schema_->num_fields();
         ++field_index) {
        const auto& arrow_field = arrow_schema_->field(field_index);
        auto field_iter = fields_by_name.find(arrow_field->name());
        if (field_iter == fields_by_name.end()) {
            continue;
        }
        auto field_id = field_iter->second.first;
        auto field_type = field_iter->second.second;

        ValidateArrowType(*arrow_field->type(),
                          ExpectedArrowType(field_type),
                          arrow_field->name());
        std::vector<std::shared_ptr<arrow::RecordBatch>> group_batches;
        group_batches.reserve(backing_batches_.size());
        for (const auto& batch : backing_batches_) {
            ValidateArrowType(*batch->column(field_index)->type(),
                              ExpectedArrowType(field_type),
                              arrow_field->name());
            group_batches.push_back(
                arrow::RecordBatch::Make(arrow::schema({arrow_field}),
                                         batch->num_rows(),
                                         {batch->column(field_index)}));
        }

        const auto column_group_id =
            static_cast<int64_t>(column_groups_.size());
        auto state = std::make_shared<RecordBatchCacheState>(
            group_batches, simulated_load_latency_us_);
        std::unique_ptr<cachinglayer::Translator<RecordBatchCell>> translator =
            std::make_unique<ArrowRecordBatchTranslator>(
                segment_id_, column_group_id, state);
        auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot(
            std::move(translator));

        field_to_location_.emplace(field_id, FieldLocation{column_group_id, 0});
        column_groups_.push_back(
            ColumnGroup{{field_id}, std::move(group_batches), slot, state});
    }
}

void
ArrowSealedSegment::ValidateFieldType(FieldId field_id,
                                      DataType data_type) const {
    auto field_type = schema_->operator[](field_id).get_data_type();
    if (field_type != data_type) {
        throw std::invalid_argument("field type mismatch for field " +
                                    std::to_string(field_id.get()));
    }
}

std::string
ArrowSealedSegment::FieldName(FieldId field_id) const {
    return schema_->operator[](field_id).get_name().get();
}

FixedVector<bool>
ArrowSealedSegment::BuildValidity(const arrow::Array& array,
                                  int64_t start,
                                  int64_t length) const {
    FixedVector<bool> valid(length, true);
    if (array.null_count() == 0) {
        return valid;
    }
    for (int64_t i = 0; i < length; ++i) {
        valid[i] = !array.IsNull(start + i);
    }
    return valid;
}

}  // namespace milvus::segcore
