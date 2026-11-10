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

#include <arrow/api.h>
#include <filesystem>
#include <memory>

#include "DataGen.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/segment/segment_writer.h"
#include "milvus-storage/transaction/transaction.h"
#include "segcore/default_fs.h"
#include "storage/loon_ffi/util.h"

namespace milvus::test {

// Write supplied test rows through the storage writer without constructing a
// query segment. This keeps storage/read regressions independent of segcore's
// growing-segment representation and raw-data ownership.
inline arrow::Result<std::string>
WriteStorageV3TestRecord(const SchemaPtr& schema,
                         const InsertRecordProto& record,
                         const std::vector<int64_t>& row_ids,
                         const std::vector<Timestamp>& timestamps,
                         const std::string& segment_path,
                         const std::string& column_group_pattern = "",
                         int64_t batch_rows = 1024) {
    arrow::FieldVector fields;
    arrow::ArrayVector columns;
    arrow::Int64Builder row_builder, timestamp_builder;
    ARROW_RETURN_NOT_OK(row_builder.AppendValues(row_ids));
    for (auto timestamp : timestamps) {
        ARROW_RETURN_NOT_OK(timestamp_builder.Append(timestamp));
    }
    ARROW_ASSIGN_OR_RAISE(auto rows, row_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto times, timestamp_builder.Finish());
    fields.push_back(arrow::field("0", arrow::int64(), false));
    fields.push_back(arrow::field("1", arrow::int64(), false));
    columns.push_back(std::move(rows));
    columns.push_back(std::move(times));

    milvus_storage::segment::SegmentWriterConfig config;
    config.segment_path = segment_path;
    auto writer_properties =
        std::make_shared<milvus_storage::api::Properties>();
    config.properties = writer_properties;
    auto& props = *writer_properties;
    milvus_storage::api::SetValue(
        props, PROPERTY_FS_STORAGE_TYPE, LOON_FS_TYPE_LOCAL);
    milvus_storage::api::SetValue(
        props, PROPERTY_FS_ROOT_PATH, kLoonLocalFSRootPath);
    milvus_storage::api::SetValue(
        props, PROPERTY_WRITER_FORMAT, LOON_FORMAT_PARQUET);
    milvus_storage::api::SetValue(props,
                                  PROPERTY_WRITER_POLICY,
                                  column_group_pattern.empty()
                                      ? LOON_COLUMN_GROUP_POLICY_SINGLE
                                      : LOON_COLUMN_GROUP_POLICY_SCHEMA_BASED);
    if (!column_group_pattern.empty()) {
        milvus_storage::api::SetValue(props,
                                      PROPERTY_WRITER_SCHEMA_BASE_PATTERNS,
                                      column_group_pattern.c_str());
    }
    for (const auto& data : record.fields_data()) {
        const auto& meta = (*schema)[FieldId(data.field_id())];
        auto field_data = segcore::CreateFieldDataFromDataArray(
            record.num_rows(), &data, meta);
        auto wrapper = storage::ConvertFieldDataToArrowDataWrapper(field_data);
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(wrapper->reader->ReadNext(&batch));
        auto column = batch->column(0);
        fields.push_back(arrow::field(std::to_string(data.field_id()),
                                      column->type(),
                                      meta.is_nullable()));
        columns.push_back(std::move(column));
        if (meta.get_data_type() == DataType::TEXT) {
            milvus_storage::lob_column::LobColumnConfig lob;
            lob.field_id = data.field_id();
            lob.lob_base_path =
                (std::filesystem::path(segment_path).parent_path() / "lobs" /
                 std::to_string(data.field_id()))
                    .string();
            lob.properties = writer_properties;
            config.lob_columns.emplace(data.field_id(), std::move(lob));
        }
    }
    auto arrow_schema = arrow::schema(std::move(fields));
    auto batch = arrow::RecordBatch::Make(
        arrow_schema, record.num_rows(), std::move(columns));
    ARROW_RETURN_NOT_OK(batch->ValidateFull());
    auto fs = segcore::GetDefaultArrowFileSystem();
    ARROW_ASSIGN_OR_RAISE(auto writer,
                          milvus_storage::segment::SegmentWriter::Create(
                              fs, arrow_schema, config));
    for (int64_t offset = 0; offset < batch->num_rows(); offset += batch_rows) {
        ARROW_RETURN_NOT_OK(writer->Write(batch->Slice(offset, batch_rows)));
    }
    ARROW_ASSIGN_OR_RAISE(auto output, writer->Close());
    ARROW_ASSIGN_OR_RAISE(
        auto txn,
        milvus_storage::api::transaction::Transaction::Open(fs, segment_path));
    txn->AppendFiles(*output.column_groups);
    for (const auto& lob : output.lob_files) {
        txn->AddLobFile(lob);
    }
    ARROW_ASSIGN_OR_RAISE(auto version, txn->Commit());
    return "{\"base_path\":\"" + segment_path +
           "\",\"ver\":" + std::to_string(version) + "}";
}

}  // namespace milvus::test
