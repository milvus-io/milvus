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

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "common/Consts.h"
#include "common/Schema.h"
#include "mmap/ChunkedColumnGroup.h"
#include "pb/segcore.pb.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/Constants.h"
#include "test_utils/ManifestTestUtil.h"
#include "test_utils/TmpPath.h"

namespace milvus::segcore {
namespace {

SchemaPtr
CreateColumnPolicyProjectionSchema(FieldId& sync_field, FieldId& async_field) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    sync_field = schema->AddDebugField("sync_field", DataType::INT64);
    async_field = schema->AddDebugField("async_field", DataType::INT64);
    schema->set_primary_field_id(pk);

    auto schema_proto = schema->ToProto();
    for (auto& field : *schema_proto.mutable_fields()) {
        if (field.fieldid() != sync_field.get() &&
            field.fieldid() != async_field.get()) {
            continue;
        }

        const bool is_sync_field = field.fieldid() == sync_field.get();
        auto* warmup = field.add_type_params();
        warmup->set_key(WARMUP_KEY);
        warmup->set_value(is_sync_field ? "sync" : "async");
        auto* evictable = field.add_type_params();
        evictable->set_key(EVICTABLE_KEY);
        evictable->set_value(is_sync_field ? "true" : "false");
    }
    return Schema::ParseFrom(schema_proto);
}

std::shared_ptr<ProxyChunkColumn>
GetProxyColumn(const ChunkedSegmentSealedImpl::RuntimeResourceState& runtime,
               FieldId field_id) {
    auto field = runtime.fields.find(field_id);
    if (field == runtime.fields.end()) {
        return nullptr;
    }
    return std::dynamic_pointer_cast<ProxyChunkColumn>(field->second);
}

TEST(ChunkedSegmentSealedStorageV3,
     SplitsColumnGroupByWarmupAndEvictablePolicy) {
    FieldId sync_field;
    FieldId async_field;
    auto schema = CreateColumnPolicyProjectionSchema(sync_field, async_field);

    test::TmpPath directory;
    auto base_path = directory.get().string();
    test::V3SegmentTestData test_data(
        schema, 1, 8, 1, TestLocalPath, base_path);

    const auto sync_column = std::to_string(sync_field.get());
    const auto async_column = std::to_string(async_field.get());
    bool fields_share_physical_group = false;
    for (const auto& column_group : *test_data.GetColumnGroups()) {
        fields_share_physical_group =
            std::find(column_group->columns.begin(),
                      column_group->columns.end(),
                      sync_column) != column_group->columns.end() &&
            std::find(column_group->columns.begin(),
                      column_group->columns.end(),
                      async_column) != column_group->columns.end();
        if (fields_share_physical_group) {
            break;
        }
    }
    ASSERT_TRUE(fields_share_physical_group);

    proto::segcore::SegmentLoadInfo load_info;
    load_info.set_collectionid(1);
    load_info.set_partitionid(2);
    load_info.set_segmentid(3);
    load_info.set_storageversion(STORAGE_V3);
    load_info.set_num_of_rows(test_data.TotalRows());
    load_info.set_manifest_path(test_data.ManifestPathJson());
    load_info.set_insert_channel("by-dev-rootcoord-dml_0_1v0");

    auto segment = CreateSealedSegment(
        schema, nullptr, -1, SegcoreConfig::default_config(), true);
    auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetLoadInfo(std::move(load_info));
    milvus::OpContext op_ctx;
    milvus::tracer::TraceContext trace_ctx;
    sealed->Load(trace_ctx, &op_ctx);

    auto runtime = sealed->TestCloneMutableRuntimeResourceState();
    auto sync_column_proxy = GetProxyColumn(*runtime, sync_field);
    auto async_column_proxy = GetProxyColumn(*runtime, async_field);
    ASSERT_NE(sync_column_proxy, nullptr);
    ASSERT_NE(async_column_proxy, nullptr);

    EXPECT_EQ(sync_column_proxy->TestCacheWarmupPolicy(),
              CacheWarmupPolicy::CacheWarmupPolicy_Sync);
    EXPECT_TRUE(sync_column_proxy->TestSupportEviction());
    EXPECT_EQ(async_column_proxy->TestCacheWarmupPolicy(),
              CacheWarmupPolicy::CacheWarmupPolicy_Async);
    EXPECT_FALSE(async_column_proxy->TestSupportEviction());
}

}  // namespace
}  // namespace milvus::segcore
