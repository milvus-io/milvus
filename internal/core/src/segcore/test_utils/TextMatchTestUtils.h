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

#pragma once

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "common/FieldData.h"
#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "common/Utils.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "index/contracts/query/INullReader.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/PlanProto.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentGrowingImpl.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/Util.h"
#include "storage/artifact/LocalDirectory.h"
#include "test_utils/Constants.h"
#include "test_utils/GenExprProto.h"

namespace milvus::segcore::text_test {

inline storage::StorageConfig
get_default_local_storage_config() {
    storage::StorageConfig config;
    config.storage_type = "local";
    config.root_path = TestRemotePath;
    return config;
}

struct TextTestData {
    SchemaPtr schema_;
    std::vector<int64_t> row_ids_;
    std::vector<Timestamp> timestamps_;
    std::shared_ptr<InsertRecordProto> owner;
    InsertRecordProto* raw_{};
    std::shared_ptr<storage::LocalDirectory> binlogs;
};

inline TextTestData
DataGen(SchemaPtr schema, int64_t rows, uint64_t = 0, int64_t offset = 0) {
    TextTestData result;
    result.schema_ = schema;
    result.row_ids_.resize(rows);
    result.timestamps_.resize(rows);
    std::iota(result.row_ids_.begin(), result.row_ids_.end(), offset);
    std::iota(result.timestamps_.begin(), result.timestamps_.end(), offset);
    result.owner = std::make_shared<InsertRecordProto>();
    result.raw_ = result.owner.get();
    result.raw_->set_num_rows(rows);
    for (const auto id : schema->get_field_ids()) {
        if (id.get() < START_USER_FIELDID) continue;
        const auto& meta = (*schema)[id];
        auto* field = result.raw_->add_fields_data();
        field->set_field_id(id.get());
        field->set_type(static_cast<proto::schema::DataType>(meta.get_data_type()));
        if (meta.get_data_type() == DataType::INT64) {
            for (const auto value : result.row_ids_) field->mutable_scalars()->mutable_long_data()->add_data(value);
        } else if (meta.get_data_type() == DataType::TIMESTAMPTZ) {
            for (const auto value : result.row_ids_) field->mutable_scalars()->mutable_timestamptz_data()->add_data(value);
        } else if (IsStringDataType(meta.get_data_type())) {
            for (int64_t row = 0; row < rows; ++row) field->mutable_scalars()->mutable_string_data()->add_data("");
        } else {
            AssertInfo(meta.get_data_type() == DataType::VECTOR_FLOAT, "unsupported text fixture field {}", meta.get_data_type());
            field->mutable_vectors()->set_dim(meta.get_dim());
            for (int64_t value = 0; value < rows * meta.get_dim(); ++value) field->mutable_vectors()->mutable_float_vector()->add_data(0.0F);
        }
        if (meta.is_nullable()) {
            for (int64_t row = 0; row < rows; ++row) field->mutable_scalars()->add_valid_data(true);
        }
    }
    result.binlogs = storage::LocalDirectory::CreateOwned(
        TestRemotePath, "text-consumer-binlogs-XXXXXX", "text consumer binlogs");
    return result;
}

inline void
AddBinlog(LoadFieldDataInfo& info,
           const std::string& root,
           int64_t field_id,
           const std::vector<FieldDataPtr>& batches,
           const storage::ChunkManagerPtr& manager) {
    std::vector<std::string> paths;
    std::vector<int64_t> rows;
    std::vector<int64_t> bytes;
    int64_t total = 0;
    paths.reserve(batches.size());
    rows.reserve(batches.size());
    bytes.reserve(batches.size());
    for (size_t batch = 0; batch < batches.size(); ++batch) {
        const auto path = root + "/" + std::to_string(field_id) + "/" + std::to_string(batch);
        auto payload = std::make_shared<storage::PayloadReader>(batches[batch]);
        storage::InsertData insert(payload);
        insert.SetFieldDataMeta(storage::FieldDataMeta{1, 2, 3, field_id});
        auto serialized = insert.serialize_to_remote_file();
        manager->Write(path, serialized.data(), serialized.size());
        paths.push_back(path);
        rows.push_back(batches[batch]->get_num_rows());
        bytes.push_back(serialized.size());
        total += rows.back();
    }
    info.field_infos.emplace(field_id, FieldBinlogInfo{field_id, total, rows, bytes, false, "", paths});
}

inline LoadFieldDataInfo
PrepareInsertBinlog(int64_t, int64_t, int64_t,
                     const TextTestData& data,
                     const storage::ChunkManagerPtr& manager) {
    LoadFieldDataInfo info;
    auto add_int64 = [&](int64_t id, const auto& values) {
        auto field = storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
        field->FillFieldData(values.data(), values.size());
        AddBinlog(info, data.binlogs->Path(), id, {field}, manager);
    };
    add_int64(RowFieldID.get(), data.row_ids_);
    add_int64(TimestampFieldID.get(), data.timestamps_);
    for (const auto& field : data.raw_->fields_data()) {
        const auto& meta = (*data.schema_)[FieldId(field.field_id())];
        const auto rows = data.row_ids_.size();
        auto native = storage::CreateFieldData(meta.get_data_type(), DataType::NONE,
                                                meta.is_nullable(), meta.is_vector() ? meta.get_dim() : 1);
        auto fill = [&](const void* values) {
            if (!meta.is_nullable()) {
                native->FillFieldData(values, rows);
                return;
            }
            const auto& valid = GetFieldDataRowValidData(field);
            AssertInfo(valid.size() == rows, "text fixture validity row count mismatch");
            std::vector<uint8_t> packed((rows + 7) / 8, 0);
            for (size_t row = 0; row < rows; ++row) {
                if (valid[row]) packed[row / 8] |= uint8_t{1} << (row % 8);
            }
            native->FillFieldData(values, packed.data(), rows, 0);
        };
        if (meta.get_data_type() == DataType::INT64) {
            fill(field.scalars().long_data().data().data());
        } else if (meta.get_data_type() == DataType::TIMESTAMPTZ) {
            fill(field.scalars().timestamptz_data().data().data());
        } else if (IsStringDataType(meta.get_data_type())) {
            const auto& strings = field.scalars().string_data().data();
            std::vector<std::string> values(strings.begin(), strings.end());
            fill(values.data());
        } else {
            AssertInfo(meta.get_data_type() == DataType::VECTOR_FLOAT, "unsupported text binlog fixture field");
            fill(field.vectors().float_vector().data().data());
        }
        AddBinlog(info, data.binlogs->Path(), field.field_id(), {native}, manager);
    }
    return info;
}

inline std::unique_ptr<SegmentSealed>
CreateSealedWithFieldDataLoaded(const SchemaPtr& schema, const TextTestData& data) {
    auto manager = storage::CreateChunkManager(get_default_local_storage_config());
    auto info = PrepareInsertBinlog(1, 2, 3, data, manager);
    auto segment = CreateSealedSegment(schema, empty_index_meta);
    segment->LoadFieldData(info);
    return segment;
}

inline IndexPin
PinSealedText(const SegmentInternalInterface& segment, FieldId field) {
    const auto capability = segment.IndexCapability(field);
    for (const auto& entry : capability.entries()) {
        if (entry.caps.text_match) return segment.PinIndex(nullptr, entry.key);
    }
    return {};
}

inline const index::ITextMatchReader&
TextReader(const index::IIndexReaderBase& base) {
    const auto* reader = dynamic_cast<const index::ITextMatchReader*>(&base);
    AssertInfo(reader != nullptr, "text fixture selected a reader without text capability");
    return *reader;
}

inline void
ExpectOnlyTextMatchHit(const index::ITextMatchReader& reader,
                        const std::string& term,
                        int64_t expected,
                        int64_t rows) {
    const auto hits = reader.MatchQuery(term, 1);
    ASSERT_EQ(hits.size(), rows);
    for (int64_t row = 0; row < rows; ++row) EXPECT_EQ(hits[row], row == expected) << term << "/" << row;
}
inline SchemaPtr
GenTestSchema(std::map<std::string, std::string> params = {},
              bool nullable = false) {
    auto schema = std::make_shared<Schema>();
    {
        FieldMeta f(FieldName("pk"),
                    FieldId(100),
                    DataType::INT64,
                    false,
                    std::nullopt);
        schema->AddField(std::move(f));
        schema->set_primary_field_id(FieldId(100));
    }
    {
        FieldMeta f(FieldName("str"),
                    FieldId(101),
                    DataType::VARCHAR,
                    65536,
                    nullable,
                    true,
                    true,
                    params,
                    std::nullopt);
        schema->AddField(std::move(f));
    }
    {
        FieldMeta f(FieldName("fvec"),
                    FieldId(102),
                    DataType::VECTOR_FLOAT,
                    16,
                    knowhere::metric::L2,
                    false,
                    std::nullopt);
        schema->AddField(std::move(f));
    }
    return schema;
}

inline std::shared_ptr<milvus::plan::FilterBitsNode>
GetMatchExpr(SchemaPtr schema,
             const std::string& query,
             proto::plan::OpType op,
             int64_t slop = 0) {
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);

    auto unary_range_expr = test::GenUnaryRangeExpr(op, query);
    unary_range_expr->set_allocated_column_info(column_info);
    auto generic_for_slop = milvus::test::GenGenericValue(slop);
    unary_range_expr->add_extra_values()->CopyFrom(*generic_for_slop);
    delete generic_for_slop;
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = query::ProtoParser(schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);
    return parsed;
};

inline std::shared_ptr<milvus::plan::FilterBitsNode>
GetNotMatchExpr(SchemaPtr schema,
                const std::string& query,
                proto::plan::OpType op,
                int64_t slop = 0) {
    const auto& str_meta = schema->operator[](FieldName("str"));
    proto::plan::GenericValue val;
    val.set_string_val(query);
    std::vector<proto::plan::GenericValue> extra_values;
    auto generic_for_slop = milvus::test::GenGenericValue(slop);
    extra_values.push_back(*generic_for_slop);
    delete generic_for_slop;
    auto child_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        milvus::expr::ColumnInfo(str_meta.get_id(), DataType::VARCHAR),
        op,
        val,
        extra_values);
    auto expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    return parsed;
};

inline std::shared_ptr<milvus::plan::FilterBitsNode>
GetFuzzyMatchExpr(SchemaPtr schema,
                  const std::string& query,
                  int64_t max_edit_distance) {
    // For TextMatchFuzzy the single extra value carries the max edit distance,
    // reusing the slot phrase match uses for slop.
    return GetMatchExpr(
        schema, query, proto::plan::OpType::TextMatchFuzzy, max_edit_distance);
};

inline std::shared_ptr<milvus::plan::FilterBitsNode>
GetFuzzyMatchExprNoDistance(SchemaPtr schema, const std::string& query) {
    // A fuzzy plan with no extra value at all, to reach the executor's
    // "max_edit_distance is required" guard.
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr =
        test::GenUnaryRangeExpr(proto::plan::OpType::TextMatchFuzzy, query);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);
    auto parser = query::ProtoParser(schema);
    auto typed_expr = parser.ParseExprs(*expr);
    return std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  typed_expr);
};

}  // namespace milvus::segcore::text_test
