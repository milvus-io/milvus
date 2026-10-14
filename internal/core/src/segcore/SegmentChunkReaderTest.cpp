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

#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "exec/QueryContext.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentChunkReader.h"

namespace milvus::segcore {

namespace {
class SchemaContractSnapshot : public SegmentReadSnapshot {
 public:
    explicit SchemaContractSnapshot(SchemaPtr schema)
        : schema_(std::move(schema)) {
    }

    int64_t
    chunk_size(FieldId, int64_t, OpContext* = nullptr) const override {
        return 0;
    }

    int64_t
    num_rows_until_chunk(FieldId,
                         int64_t,
                         OpContext* = nullptr) const override {
        return 0;
    }

    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId, int64_t, OpContext* = nullptr) const override {
        return {0, 0};
    }

    int64_t
    num_chunk_data(FieldId, OpContext* = nullptr) const override {
        return 0;
    }

    int64_t
    get_row_count() const override {
        return 0;
    }

    std::shared_ptr<ChunkedColumnInterface>
    GetDataScanResources(FieldId) const override {
        return nullptr;
    }

    std::shared_ptr<const IArrayOffsets>
    GetArrayOffsets(FieldId) const override {
        return nullptr;
    }

    SchemaPtr
    get_schema_snapshot() const override {
        return schema_;
    }

 private:
    SchemaPtr schema_;
};
}  // namespace

TEST(SegmentChunkReader, SchemaUsesBoundSnapshot) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateSealedSegment(schema);
    auto captured_schema = std::make_shared<Schema>(*schema);
    auto snapshot = std::make_shared<SchemaContractSnapshot>(captured_schema);
    SegmentChunkReader reader(nullptr, segment.get(), 0);
    exec::QueryContext context(
        "schema_snapshot", segment.get(), 0, MAX_TIMESTAMP);

    EXPECT_EQ(reader.GetSchemaSnapshot(), schema);
    EXPECT_EQ(context.get_schema_snapshot(), schema);
    reader.SetSnapshot(snapshot.get());
    context.set_read_snapshot(snapshot);
    EXPECT_EQ(reader.GetSchemaSnapshot(), captured_schema);
    EXPECT_EQ(context.get_schema_snapshot(), captured_schema);
}

TEST(SegmentChunkReader, MissingSnapshotSchemaDoesNotUseLiveSchema) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateSealedSegment(schema);
    auto snapshot = std::make_shared<SchemaContractSnapshot>(nullptr);
    SegmentChunkReader reader(nullptr, segment.get(), 0);
    exec::QueryContext context(
        "missing_schema", segment.get(), 0, MAX_TIMESTAMP);
    reader.SetSnapshot(snapshot.get());
    context.set_read_snapshot(snapshot);

    auto expect_missing_schema = [](auto&& get_schema) {
        try {
            get_schema();
            FAIL() << "expected missing snapshot schema to fail";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
        }
    };
    expect_missing_schema([&] { return reader.GetSchemaSnapshot(); });
    expect_missing_schema([&] { return context.get_schema_snapshot(); });
}

TEST(SegmentChunkReader, NumericVariantMismatchIsSystemError) {
    const data_access_type value = int64_t{7};

    try {
        (void)get_from_variant<int32_t>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

TEST(SegmentChunkReader, StringVariantMismatchIsSystemError) {
    const data_access_type value = true;

    try {
        (void)get_from_variant<std::string>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

}  // namespace milvus::segcore
