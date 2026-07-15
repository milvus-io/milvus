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
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/FieldMeta.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/bson_view.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnInterface.h"
#include "mmap/VortexColumnGroup.h"
#include "milvus-storage/format/vortex/vortex_format_reader.h"
#include "milvus-storage/format/vortex/vortex_planner.h"

namespace arrow {
class Array;
class RecordBatchReader;
class Schema;
class Table;
}  // namespace arrow

namespace milvus {

class VortexDataScanCursor;
class VortexRowIdScanCursor;

class VortexColumn final : public ChunkedColumnInterface {
    friend class VortexDataScanCursor;
    friend class VortexRowIdScanCursor;

 public:
    using FileInfo = VortexColumnFileInfo;

    VortexColumn(FieldId field_id,
                 FieldMeta field_meta,
                 std::shared_ptr<milvus_storage::api::Properties> properties,
                 std::shared_ptr<VortexColumnGroup> column_group,
                 std::optional<size_t> data_byte_size = std::nullopt);

    ~VortexColumn() override;

    void
    ManualEvictCache() const override;

    void
    CancelWarmup() override;

    bool
    IsInMultiFieldColumnGroup() const override;

    LocalFormat
    GetLocalFormat() const override {
        return LocalFormat::Vortex;
    }

    bool
    SupportsScanPushdown(const ScanOptions& options) const override;

    bool
    IsNullable() const override;

    size_t
    NumRows() const override;

    int64_t
    num_chunks() const override;

    size_t
    DataByteSize() const override;

    int64_t
    chunk_row_nums(int64_t chunk_id) const override;

    PinWrapper<const char*>
    DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const override;

    bool
    IsValid(milvus::OpContext* op_ctx, size_t offset) const override;

    void
    BulkIsValid(milvus::OpContext* op_ctx,
                std::function<void(bool, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override;

    void
    PrefetchChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) const override;

    bool
    CellsLoaded(const int64_t* offsets, int64_t count) const override;

    PinWrapper<SpanBase>
    Span(milvus::OpContext* op_ctx, int64_t chunk_id) const override;

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(milvus::OpContext* op_ctx,
                int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override;

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(milvus::OpContext* op_ctx,
               int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len =
                   std::nullopt) const override;

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    VectorArrayViews(milvus::OpContext* op_ctx,
                     int64_t chunk_id,
                     std::optional<std::pair<int64_t, int64_t>> offset_len =
                         std::nullopt) const override;

    PinWrapper<const size_t*>
    VectorArrayOffsets(milvus::OpContext* op_ctx,
                       int64_t chunk_id) const override;

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViewsByOffsets(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const override;

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        const FixedVector<int32_t>& offsets) const override;

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override;

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) const override;

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override;

    std::vector<PinWrapper<Chunk*>>
    GetAllChunks(milvus::OpContext* op_ctx) const override;

    void
    ApplyValidDataInChunk(milvus::OpContext* op_ctx,
                          int64_t chunk_id,
                          int64_t offset,
                          int64_t size,
                          TargetBitmapView valid_result) const override;

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override;

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override;

    void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) override;

    void
    BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                         void* dst,
                         const int64_t* offsets,
                         int64_t count,
                         bool small_int_raw_type = false) override;

    void
    BulkVectorValueAt(milvus::OpContext* op_ctx,
                      void* dst,
                      const int64_t* offsets,
                      int64_t count,
                      int64_t dim) override;

    void
    BulkRawStringAt(milvus::OpContext* op_ctx,
                    std::function<void(std::string_view, size_t, bool)> fn,
                    const int64_t* offsets = nullptr,
                    int64_t count = 0) const override;

    void
    BulkRawJsonAt(milvus::OpContext* op_ctx,
                  std::function<void(Json, size_t, bool)> fn,
                  const int64_t* offsets,
                  int64_t count) const override;

    void
    BulkRawBsonAt(milvus::OpContext* op_ctx,
                  std::function<void(BsonView, uint32_t, uint32_t)> fn,
                  const uint32_t* row_offsets,
                  const uint32_t* value_offsets,
                  int64_t count) const override;

    void
    BulkArrayAt(milvus::OpContext* op_ctx,
                std::function<void(const ArrayView&, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override;

    void
    BulkVectorArrayAt(milvus::OpContext* op_ctx,
                      std::function<void(VectorFieldProto&&, size_t)> fn,
                      const int64_t* offsets,
                      int64_t count) const override;

    ScanResult
    Scan(milvus::OpContext* op_ctx, const ScanOptions& options) const override;

    struct TakeResult {
        std::vector<std::shared_ptr<Chunk>> chunks;
        std::vector<int64_t> offsets;
    };

    TakeResult
    TakeOwn(milvus::OpContext* op_ctx,
            const int64_t* offsets,
            int64_t count) const;

    std::shared_ptr<TakeResult>
    Take(milvus::OpContext* op_ctx,
         const int64_t* offsets,
         int64_t count) const;

 private:
    std::optional<DataType>
    GetDefaultScanDataType() const override;

    struct FileState {
        std::string path;
        std::shared_ptr<milvus_storage::vortex::VortexPlanner> planner;
        std::shared_ptr<milvus_storage::vortex::VortexFormatReader> reader;
        std::shared_ptr<
            cachinglayer::CacheSlot<milvus_storage::vortex::VortexCellGuard>>
            slot;
        int64_t rows = 0;
    };

    struct ArrowTakeResult;
    struct ArrowStringViewHolder;
    class ArrowStringLikeColumn;

    static std::shared_ptr<arrow::Schema>
    MakeProjectedArrowSchema(const std::shared_ptr<arrow::Schema>& schema,
                             const std::string& field_name);

    static std::optional<std::string>
    VortexCompareOp(proto::plan::OpType op_type);

    static std::string
    QuoteSqlIdentifier(std::string_view value);

    static std::string
    QuoteSqlStringLiteral(std::string_view value);

    static std::optional<std::string>
    VortexLiteral(DataType data_type, const proto::plan::GenericValue& value);

    std::optional<std::string>
    BuildVortexPredicate(const ScanOptions& options) const;

    static milvus_storage::api::Properties
    MakeVortexReaderProperties(
        const std::shared_ptr<milvus_storage::api::Properties>& properties);

    std::shared_ptr<milvus_storage::vortex::VortexPlanner>
    MakeFilePlanner(const VortexColumnGroup::FileState& group_file) const;

    std::shared_ptr<milvus_storage::vortex::VortexFormatReader>
    BuildFileReader(const VortexColumnGroup::FileState& group_file) const;

    void
    ValidateFileState(const FileState& state,
                      const VortexColumnGroup::FileState& group_file) const;

    FileState
    BuildFileState(const VortexColumnGroup::FileState& group_file) const;

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    BuildStringViewsFromArrow(
        const ArrowStringLikeColumn& column,
        std::optional<std::pair<int64_t, int64_t>> offset_len,
        bool emit_valid) const;

    void
    CheckChunkId(int64_t chunk_id) const;

    std::shared_ptr<
        cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>
    PinPlanCells(milvus::OpContext* op_ctx,
                 const FileState& file,
                 const std::vector<uint64_t>& cell_ids) const;

    milvus_storage::vortex::VortexPlan
    PlanRowRange(const FileState& file,
                 uint64_t row_start,
                 uint64_t row_end,
                 const std::string& predicate = "") const;

    milvus_storage::vortex::VortexPlan
    PlanOffsets(const FileState& file,
                const std::vector<int64_t>& offsets) const;

    std::shared_ptr<Chunk>
    ChunkFromTable(const std::shared_ptr<arrow::Table>& table) const;

    std::shared_ptr<Chunk>
    MaterializeChunk(milvus::OpContext* op_ctx,
                     int64_t chunk_id,
                     std::optional<std::pair<int64_t, int64_t>> offset_len =
                         std::nullopt) const;

    PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>
    OpenDataScanForFile(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        int64_t start_offset,
                        int64_t length) const;

    PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>
    OpenRowIdScanForFile(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         int64_t start_offset,
                         int64_t length,
                         const std::string& predicate) const;

    std::pair<std::shared_ptr<ArrowStringViewHolder>,
              std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    ScanStringLikeViewsFromFile(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const;

    std::shared_ptr<Chunk>
    TakeFromFile(milvus::OpContext* op_ctx,
                 int64_t chunk_id,
                 const std::vector<int64_t>& offsets) const;

    ArrowTakeResult
    TakeArrowFromFile(milvus::OpContext* op_ctx,
                      int64_t chunk_id,
                      const std::vector<int64_t>& offsets) const;

    std::pair<std::shared_ptr<ArrowStringViewHolder>,
              std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    TakeStringLikeViews(milvus::OpContext* op_ctx,
                        const int64_t* offsets,
                        int64_t count) const;

    template <typename ArrowArrayT,
              typename SrcT,
              typename RawDstT,
              typename WidenDstT>
    void
    CopyArrowPrimitiveValues(
        void* dst,
        const std::shared_ptr<arrow::Table>& table,
        const std::vector<std::vector<int64_t>>& original_indices_by_unique,
        bool small_int_raw_type) const;

    template <typename ArrowArrayT,
              typename SrcT,
              typename RawDstT,
              typename WidenDstT>
    void
    BulkPrimitiveValueAtFromArrow(milvus::OpContext* op_ctx,
                                  void* dst,
                                  const int64_t* offsets,
                                  int64_t count,
                                  bool small_int_raw_type) const;

    void
    BulkStringLikeAt(
        milvus::OpContext* op_ctx,
        const std::function<void(std::string_view, size_t, bool)>& fn,
        const int64_t* offsets,
        int64_t count) const;

    FieldId field_id_;
    FieldMeta field_meta_;
    DataType data_type_;
    std::string field_name_;
    milvus_storage::api::Properties local_format_properties_;
    std::shared_ptr<VortexColumnGroup> column_group_;
    size_t data_byte_size_ = 0;
    std::vector<FileState> files_;
    std::vector<int64_t> num_rows_until_chunk_;
    int64_t num_rows_ = 0;
};

}  // namespace milvus
