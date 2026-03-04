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

#include <gtest/gtest.h>
#include <stdint.h>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "NamedType/underlying_functionalities.hpp"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "knowhere/comp/index_param.h"
#include "pb/common.pb.h"
#include "pb/index_cgo_msg.pb.h"
#include "pb/segcore.pb.h"
#include "segcore/SegmentLoadInfo.h"
#include "segcore/Types.h"

using namespace milvus;
using namespace milvus::segcore;

class SegmentLoadInfoTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Setup schema with test fields (sequential field IDs starting from 100)
        schema_ = std::make_shared<Schema>();
        // Field IDs: 100=pk, 101=vec, 102=json, 103=varchar, 104-110=additional fields
        schema_->AddDebugField("pk", DataType::INT64);
        schema_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 128, knowhere::metric::L2);
        schema_->AddDebugField("json_field", DataType::JSON);
        schema_->AddDebugField("bm25_field", DataType::JSON);
        schema_->AddDebugField("group_field", DataType::INT64);
        schema_->AddDebugField("child_field1", DataType::FLOAT);
        schema_->AddDebugField("child_field2", DataType::FLOAT);
        schema_->AddDebugField("child_field3", DataType::FLOAT);
        schema_->AddDebugField("extra_field1", DataType::INT64);
        schema_->AddDebugField("extra_field2", DataType::FLOAT);
        schema_->AddDebugField("extra_field3", DataType::FLOAT);
        schema_->set_primary_field_id(FieldId(100));

        // Setup a basic SegmentLoadInfo proto
        proto_.set_segmentid(12345);
        proto_.set_partitionid(100);
        proto_.set_collectionid(200);
        proto_.set_dbid(1);
        proto_.set_num_of_rows(10000);
        proto_.set_flush_time(1234567890);
        proto_.set_readableversion(5);
        proto_.set_storageversion(1);
        proto_.set_is_sorted(true);
        proto_.set_insert_channel("test_channel");
        proto_.set_manifest_path("/path/to/manifest");
        proto_.set_priority(proto::common::LoadPriority::LOW);

        // Add compaction from
        proto_.add_compactionfrom(111);
        proto_.add_compactionfrom(222);

        // Add index info (field 101=vec, field 102=json_field)
        auto* index_info = proto_.add_index_infos();
        index_info->set_fieldid(101);
        index_info->set_indexid(1001);
        index_info->set_buildid(2001);
        index_info->set_index_version(1);
        index_info->add_index_file_paths("/path/to/index1");
        index_info->add_index_file_paths("/path/to/index2");
        // Add required index_type parameter for vector field
        auto* index_param1 = index_info->add_index_params();
        index_param1->set_key("index_type");
        index_param1->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

        auto* index_info2 = proto_.add_index_infos();
        index_info2->set_fieldid(102);
        index_info2->set_indexid(1002);
        index_info2->add_index_file_paths("/path/to/index3");
        // Add required index_type parameter for scalar field
        auto* index_param2 = index_info2->add_index_params();
        index_param2->set_key("index_type");
        index_param2->set_value(milvus::index::INVERTED_INDEX_TYPE);

        // Add binlog paths (field 101)
        auto* binlog = proto_.add_binlog_paths();
        binlog->set_fieldid(101);
        auto* log1 = binlog->add_binlogs();
        log1->set_log_path("/path/to/binlog1");
        log1->set_entries_num(500);
        auto* log2 = binlog->add_binlogs();
        log2->set_log_path("/path/to/binlog2");
        log2->set_entries_num(500);

        // Add column group binlog (field 104 with child fields 105, 106)
        auto* group_binlog = proto_.add_binlog_paths();
        group_binlog->set_fieldid(104);
        group_binlog->add_child_fields(105);
        group_binlog->add_child_fields(106);
        auto* group_log = group_binlog->add_binlogs();
        group_log->set_log_path("/path/to/group_binlog");
        group_log->set_entries_num(1000);

        // Add statslogs
        auto* statslog = proto_.add_statslogs();
        statslog->set_fieldid(101);
        auto* stat_log = statslog->add_binlogs();
        stat_log->set_log_path("/path/to/statslog");

        // Add deltalogs
        auto* deltalog = proto_.add_deltalogs();
        deltalog->set_fieldid(0);
        auto* delta_log = deltalog->add_binlogs();
        delta_log->set_log_path("/path/to/deltalog");

        // Add text stats
        auto& text_stats = (*proto_.mutable_textstatslogs())[101];
        text_stats.set_fieldid(101);
        text_stats.set_version(1);
        text_stats.add_files("/path/to/text_stats");

        // Add json key stats
        auto& json_stats = (*proto_.mutable_jsonkeystatslogs())[102];
        json_stats.set_fieldid(102);
        json_stats.set_version(1);

        // Add bm25 logs
        auto* bm25log = proto_.add_bm25logs();
        bm25log->set_fieldid(103);
        auto* bm25_binlog = bm25log->add_binlogs();
        bm25_binlog->set_log_path("/path/to/bm25log");
    }

    SchemaPtr schema_;
    proto::segcore::SegmentLoadInfo proto_;
};

TEST_F(SegmentLoadInfoTest, EmptyProtoConstructor) {
    proto::segcore::SegmentLoadInfo empty_proto;
    SegmentLoadInfo info(empty_proto, schema_);
    EXPECT_TRUE(info.IsEmpty());
    EXPECT_EQ(info.GetSegmentID(), 0);
    EXPECT_EQ(info.GetNumOfRows(), 0);
}

TEST_F(SegmentLoadInfoTest, ConstructFromProto) {
    SegmentLoadInfo info(proto_, schema_);

    EXPECT_FALSE(info.IsEmpty());
    EXPECT_EQ(info.GetSegmentID(), 12345);
    EXPECT_EQ(info.GetPartitionID(), 100);
    EXPECT_EQ(info.GetCollectionID(), 200);
    EXPECT_EQ(info.GetDbID(), 1);
    EXPECT_EQ(info.GetNumOfRows(), 10000);
    EXPECT_EQ(info.GetFlushTime(), 1234567890);
    EXPECT_EQ(info.GetReadableVersion(), 5);
    EXPECT_EQ(info.GetStorageVersion(), 1);
    EXPECT_TRUE(info.IsSorted());
    EXPECT_EQ(info.GetInsertChannel(), "test_channel");
    EXPECT_EQ(info.GetManifestPath(), "/path/to/manifest");
    EXPECT_TRUE(info.HasManifestPath());
    EXPECT_EQ(info.GetPriority(), proto::common::LoadPriority::LOW);
}

TEST_F(SegmentLoadInfoTest, MoveConstructor) {
    SegmentLoadInfo info1(proto_, schema_);
    SegmentLoadInfo info2(std::move(info1));

    EXPECT_EQ(info2.GetSegmentID(), 12345);
    EXPECT_EQ(info2.GetNumOfRows(), 10000);
}

TEST_F(SegmentLoadInfoTest, CopyAssignment) {
    SegmentLoadInfo info1(proto_, schema_);
    proto::segcore::SegmentLoadInfo empty_proto;
    SegmentLoadInfo info2(empty_proto, schema_);
    info2 = info1;

    EXPECT_EQ(info2.GetSegmentID(), 12345);
    EXPECT_EQ(info2.GetNumOfRows(), 10000);
}

TEST_F(SegmentLoadInfoTest, SetMethod) {
    proto::segcore::SegmentLoadInfo empty_proto;
    SegmentLoadInfo info(empty_proto, schema_);
    info.Set(proto_, schema_);

    EXPECT_EQ(info.GetSegmentID(), 12345);
    EXPECT_EQ(info.GetNumOfRows(), 10000);
}

TEST_F(SegmentLoadInfoTest, CompactionInfo) {
    SegmentLoadInfo info(proto_, schema_);

    EXPECT_TRUE(info.IsCompacted());
    EXPECT_EQ(info.GetCompactionFromCount(), 2);
    EXPECT_EQ(info.GetCompactionFrom()[0], 111);
    EXPECT_EQ(info.GetCompactionFrom()[1], 222);
}

TEST_F(SegmentLoadInfoTest, IndexInfo) {
    SegmentLoadInfo info(proto_, schema_);

    EXPECT_EQ(info.GetIndexInfoCount(), 2);
    EXPECT_TRUE(info.HasIndexInfo(FieldId(101)));
    EXPECT_TRUE(info.HasIndexInfo(FieldId(102)));
    EXPECT_FALSE(info.HasIndexInfo(FieldId(999)));

    auto index_infos = info.GetFieldIndexInfos(FieldId(101));
    EXPECT_EQ(index_infos.size(), 1);
    EXPECT_EQ(index_infos[0].index_id, 1001);

    auto indexed_fields = info.GetIndexedFieldIds();
    EXPECT_EQ(indexed_fields.size(), 2);
    EXPECT_TRUE(indexed_fields.count(FieldId(101)) > 0);
    EXPECT_TRUE(indexed_fields.count(FieldId(102)) > 0);

    // Test non-existent field
    auto empty_infos = info.GetFieldIndexInfos(FieldId(999));
    EXPECT_TRUE(empty_infos.empty());
}

TEST_F(SegmentLoadInfoTest, BinlogInfo) {
    SegmentLoadInfo info(proto_, schema_);

    EXPECT_EQ(info.GetBinlogPathCount(), 2);
    EXPECT_TRUE(info.HasBinlogPath(FieldId(101)));
    EXPECT_TRUE(info.HasBinlogPath(FieldId(104)));
    EXPECT_FALSE(info.HasBinlogPath(FieldId(999)));

    auto binlog = info.GetFieldBinlog(FieldId(101));
    EXPECT_NE(binlog, nullptr);
    EXPECT_EQ(binlog->binlogs_size(), 2);

    auto paths = info.GetFieldBinlogPaths(FieldId(101));
    EXPECT_EQ(paths.size(), 2);
    EXPECT_EQ(paths[0], "/path/to/binlog1");
    EXPECT_EQ(paths[1], "/path/to/binlog2");

    auto row_count = info.GetFieldBinlogRowCount(FieldId(101));
    EXPECT_EQ(row_count, 1000);

    // Test non-existent field
    auto empty_paths = info.GetFieldBinlogPaths(FieldId(999));
    EXPECT_TRUE(empty_paths.empty());

    auto zero_count = info.GetFieldBinlogRowCount(FieldId(999));
    EXPECT_EQ(zero_count, 0);
}

TEST_F(SegmentLoadInfoTest, ColumnGroup) {
    SegmentLoadInfo info(proto_, schema_);

    EXPECT_TRUE(info.IsColumnGroup(FieldId(104)));
    EXPECT_FALSE(info.IsColumnGroup(FieldId(101)));
    EXPECT_FALSE(info.IsColumnGroup(FieldId(999)));

    auto child_fields = info.GetChildFieldIds(FieldId(104));
    EXPECT_EQ(child_fields.size(), 2);
    EXPECT_EQ(child_fields[0], 105);
    EXPECT_EQ(child_fields[1], 106);

    auto empty_children = info.GetChildFieldIds(FieldId(101));
    EXPECT_TRUE(empty_children.empty());
}

TEST_F(SegmentLoadInfoTest, StatsAndDeltaLogs) {
    SegmentLoadInfo info(proto_, schema_);

    EXPECT_EQ(info.GetStatslogCount(), 1);
    EXPECT_EQ(info.GetDeltalogCount(), 1);

    const auto& statslog = info.GetStatslog(0);
    EXPECT_EQ(statslog.fieldid(), 101);

    const auto& deltalog = info.GetDeltalog(0);
    EXPECT_EQ(deltalog.fieldid(), 0);
}

TEST_F(SegmentLoadInfoTest, TextStats) {
    SegmentLoadInfo info(proto_, schema_);

    EXPECT_TRUE(info.HasTextStatsLog(101));
    EXPECT_FALSE(info.HasTextStatsLog(999));

    auto text_stats = info.GetTextStatsLog(101);
    EXPECT_NE(text_stats, nullptr);
    EXPECT_EQ(text_stats->fieldid(), 101);
    EXPECT_EQ(text_stats->version(), 1);

    auto null_stats = info.GetTextStatsLog(999);
    EXPECT_EQ(null_stats, nullptr);
}

TEST_F(SegmentLoadInfoTest, JsonKeyStats) {
    SegmentLoadInfo info(proto_, schema_);

    EXPECT_TRUE(info.HasJsonKeyStatsLog(102));
    EXPECT_FALSE(info.HasJsonKeyStatsLog(999));

    auto json_stats = info.GetJsonKeyStatsLog(102);
    EXPECT_NE(json_stats, nullptr);
    EXPECT_EQ(json_stats->fieldid(), 102);

    auto null_stats = info.GetJsonKeyStatsLog(999);
    EXPECT_EQ(null_stats, nullptr);
}

TEST_F(SegmentLoadInfoTest, Bm25Logs) {
    SegmentLoadInfo info(proto_, schema_);

    EXPECT_EQ(info.GetBm25logCount(), 1);
    const auto& bm25log = info.GetBm25log(0);
    EXPECT_EQ(bm25log.fieldid(), 103);
}

TEST_F(SegmentLoadInfoTest, UnderlyingProtoAccess) {
    SegmentLoadInfo info(proto_, schema_);

    const auto& proto = info.GetProto();
    EXPECT_EQ(proto.segmentid(), 12345);

    auto* mutable_proto = info.MutableProto();
    mutable_proto->set_segmentid(99999);
    info.RebuildCache();

    EXPECT_EQ(info.GetSegmentID(), 99999);
}

TEST_F(SegmentLoadInfoTest, EmptyManifestPath) {
    proto::segcore::SegmentLoadInfo empty_proto;
    empty_proto.set_segmentid(1);
    empty_proto.set_num_of_rows(100);

    SegmentLoadInfo info(empty_proto, schema_);
    EXPECT_FALSE(info.HasManifestPath());
    EXPECT_TRUE(info.GetManifestPath().empty());
}

TEST_F(SegmentLoadInfoTest, IndexWithoutFiles) {
    proto::segcore::SegmentLoadInfo test_proto;
    test_proto.set_segmentid(1);

    // Add index info without files - should be ignored in cache
    auto* index_info = test_proto.add_index_infos();
    index_info->set_fieldid(101);
    index_info->set_indexid(1001);
    // No index_file_paths added

    SegmentLoadInfo info(test_proto, schema_);

    // Index without files should not be in cache
    EXPECT_FALSE(info.HasIndexInfo(FieldId(101)));
    EXPECT_EQ(info.GetIndexInfoCount(), 1);  // Proto still has it
}

// ==================== GetLoadDiff Tests ====================

TEST_F(SegmentLoadInfoTest, GetLoadDiffWithIndexesOnly) {
    // Create info with only indexes (no binlogs, no manifest)
    proto::segcore::SegmentLoadInfo test_proto;
    test_proto.set_segmentid(100);
    test_proto.set_num_of_rows(1000);

    // Add two indexes
    auto* index1 = test_proto.add_index_infos();
    index1->set_fieldid(101);
    index1->set_indexid(1001);
    index1->add_index_file_paths("/path/to/index1");
    auto* index_param1 = index1->add_index_params();
    index_param1->set_key("index_type");
    index_param1->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

    auto* index2 = test_proto.add_index_infos();
    index2->set_fieldid(102);
    index2->set_indexid(1002);
    index2->add_index_file_paths("/path/to/index2");
    auto* index_param2 = index2->add_index_params();
    index_param2->set_key("index_type");
    index_param2->set_value(milvus::index::INVERTED_INDEX_TYPE);

    SegmentLoadInfo info(test_proto, schema_);
    auto diff = info.GetLoadDiff();
    std::cout << diff.ToString() << "\n";

    EXPECT_TRUE(diff.HasChanges());
    // Both indexes should be in indexes_to_load
    EXPECT_EQ(diff.indexes_to_load.size(), 2);
    EXPECT_TRUE(diff.indexes_to_load.count(FieldId(101)) > 0);
    EXPECT_TRUE(diff.indexes_to_load.count(FieldId(102)) > 0);
    EXPECT_EQ(diff.indexes_to_load[FieldId(101)].size(), 1);
    EXPECT_EQ(diff.indexes_to_load[FieldId(102)].size(), 1);

    // No drops or other changes
    EXPECT_TRUE(diff.indexes_to_drop.empty());
    EXPECT_TRUE(diff.binlogs_to_load.empty());
    EXPECT_TRUE(diff.field_data_to_drop.empty());
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffWithBinlogsOnly) {
    // Create info with only binlogs (no indexes, no manifest)
    proto::segcore::SegmentLoadInfo test_proto;
    test_proto.set_segmentid(100);
    test_proto.set_num_of_rows(1000);

    // Add binlog with child fields (field 104 with children 105, 106)
    auto* binlog = test_proto.add_binlog_paths();
    binlog->set_fieldid(104);
    binlog->add_child_fields(105);
    binlog->add_child_fields(106);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/binlog");
    log->set_entries_num(500);

    SegmentLoadInfo info(test_proto, schema_);
    auto diff = info.GetLoadDiff();
    std::cout << diff.ToString() << "\n";

    EXPECT_TRUE(diff.HasChanges());
    // Binlogs should be in binlogs_to_load
    EXPECT_EQ(diff.binlogs_to_load.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first.size(), 2);  // 2 child fields
    EXPECT_EQ(diff.binlogs_to_load[0].first[0].get(), 105);
    EXPECT_EQ(diff.binlogs_to_load[0].first[1].get(), 106);

    // No index changes
    EXPECT_TRUE(diff.indexes_to_load.empty());
    EXPECT_TRUE(diff.indexes_to_drop.empty());
    EXPECT_TRUE(diff.field_data_to_drop.empty());
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffWithIndexesAndBinlogs) {
    // Create info with both indexes and binlogs
    proto::segcore::SegmentLoadInfo test_proto;
    test_proto.set_segmentid(100);
    test_proto.set_num_of_rows(1000);

    // Add index
    auto* index = test_proto.add_index_infos();
    index->set_fieldid(101);
    index->set_indexid(1001);
    index->add_index_file_paths("/path/to/index");
    auto* index_param = index->add_index_params();
    index_param->set_key("index_type");
    index_param->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

    // Add binlog with child fields (field 104 with child 105)
    auto* binlog = test_proto.add_binlog_paths();
    binlog->set_fieldid(104);
    binlog->add_child_fields(105);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/binlog");
    log->set_entries_num(500);

    SegmentLoadInfo info(test_proto, schema_);
    auto diff = info.GetLoadDiff();
    std::cout << diff.ToString() << "\n";

    EXPECT_TRUE(diff.HasChanges());
    // Index should be in indexes_to_load
    EXPECT_EQ(diff.indexes_to_load.size(), 1);
    EXPECT_TRUE(diff.indexes_to_load.count(FieldId(101)) > 0);

    // Binlog should be in binlogs_to_load
    EXPECT_EQ(diff.binlogs_to_load.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first[0].get(), 105);

    // No drops
    EXPECT_TRUE(diff.indexes_to_drop.empty());
    EXPECT_TRUE(diff.field_data_to_drop.empty());
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffIgnoresIndexesWithoutFiles) {
    // Indexes without files should be ignored in GetLoadDiff
    proto::segcore::SegmentLoadInfo test_proto;
    test_proto.set_segmentid(100);
    test_proto.set_num_of_rows(1000);

    // Add index with files
    auto* index1 = test_proto.add_index_infos();
    index1->set_fieldid(101);
    index1->set_indexid(1001);
    index1->add_index_file_paths("/path/to/index");
    auto* index_param1 = index1->add_index_params();
    index_param1->set_key("index_type");
    index_param1->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

    // Add index without files - should be ignored
    auto* index2 = test_proto.add_index_infos();
    index2->set_fieldid(102);
    index2->set_indexid(1002);
    // No index_file_paths added

    SegmentLoadInfo info(test_proto, schema_);
    auto diff = info.GetLoadDiff();
    std::cout << diff.ToString() << "\n";

    EXPECT_TRUE(diff.HasChanges());
    // Only index with files should be in indexes_to_load
    EXPECT_EQ(diff.indexes_to_load.size(), 1);
    EXPECT_TRUE(diff.indexes_to_load.count(FieldId(101)) > 0);
    EXPECT_FALSE(diff.indexes_to_load.count(FieldId(102)) > 0);
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffWithMultipleIndexesPerField) {
    // A field can have multiple indexes (e.g., JSON field with multiple paths)
    proto::segcore::SegmentLoadInfo test_proto;
    test_proto.set_segmentid(100);
    test_proto.set_num_of_rows(1000);

    // Add two indexes for the same field
    auto* index1 = test_proto.add_index_infos();
    index1->set_fieldid(101);
    index1->set_indexid(1001);
    index1->add_index_file_paths("/path/to/index1");
    auto* index_param1 = index1->add_index_params();
    index_param1->set_key("index_type");
    index_param1->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

    auto* index2 = test_proto.add_index_infos();
    index2->set_fieldid(101);
    index2->set_indexid(1002);
    index2->add_index_file_paths("/path/to/index2");
    auto* index_param2 = index2->add_index_params();
    index_param2->set_key("index_type");
    index_param2->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

    SegmentLoadInfo info(test_proto, schema_);
    auto diff = info.GetLoadDiff();

    EXPECT_TRUE(diff.HasChanges());
    // Both indexes should be in indexes_to_load for the same field
    EXPECT_EQ(diff.indexes_to_load.size(), 1);
    EXPECT_TRUE(diff.indexes_to_load.count(FieldId(101)) > 0);
    EXPECT_EQ(diff.indexes_to_load[FieldId(101)].size(), 2);
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffWithMultipleBinlogGroups) {
    // Test with multiple binlog groups
    proto::segcore::SegmentLoadInfo test_proto;
    test_proto.set_segmentid(100);
    test_proto.set_num_of_rows(1000);

    // Add first binlog group (field 104 with children 105, 106)
    auto* binlog1 = test_proto.add_binlog_paths();
    binlog1->set_fieldid(104);
    binlog1->add_child_fields(105);
    binlog1->add_child_fields(106);
    auto* log1 = binlog1->add_binlogs();
    log1->set_log_path("/path/to/binlog1");
    log1->set_entries_num(500);

    // Add second binlog group (field 108 with child 109)
    auto* binlog2 = test_proto.add_binlog_paths();
    binlog2->set_fieldid(108);
    binlog2->add_child_fields(109);
    auto* log2 = binlog2->add_binlogs();
    log2->set_log_path("/path/to/binlog2");
    log2->set_entries_num(500);

    SegmentLoadInfo info(test_proto, schema_);
    auto diff = info.GetLoadDiff();

    EXPECT_TRUE(diff.HasChanges());
    // Both binlog groups should be in binlogs_to_load
    EXPECT_EQ(diff.binlogs_to_load.size(), 2);

    // Check first group
    EXPECT_EQ(diff.binlogs_to_load[0].first.size(), 2);
    // Check second group
    EXPECT_EQ(diff.binlogs_to_load[1].first.size(), 1);
}

// ==================== Legacy Format (v1) Tests ====================
// Test binlogs without child_fields (v1/legacy format where group_id == field_id)

TEST_F(SegmentLoadInfoTest, GetLoadDiffWithBinlogsLegacyFormat) {
    // Create info with binlogs without child_fields (legacy/v1 format)
    proto::segcore::SegmentLoadInfo test_proto;
    test_proto.set_segmentid(100);
    test_proto.set_num_of_rows(1000);

    // Add binlog WITHOUT child_fields - this is the legacy format
    auto* binlog = test_proto.add_binlog_paths();
    binlog->set_fieldid(101);
    // Note: no child_fields added - this triggers the legacy handling
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/binlog");
    log->set_entries_num(500);

    SegmentLoadInfo info(test_proto, schema_);
    auto diff = info.GetLoadDiff();
    std::cout << "Legacy format diff: " << diff.ToString() << "\n";

    EXPECT_TRUE(diff.HasChanges());
    // In legacy format, field_id itself is used as the child_id
    EXPECT_EQ(diff.binlogs_to_load.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first[0].get(),
              101);  // field_id as child_id

    // No index changes
    EXPECT_TRUE(diff.indexes_to_load.empty());
    EXPECT_TRUE(diff.indexes_to_drop.empty());
    EXPECT_TRUE(diff.field_data_to_drop.empty());
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffWithMultipleBinlogsLegacyFormat) {
    // Test multiple binlogs in legacy format
    proto::segcore::SegmentLoadInfo test_proto;
    test_proto.set_segmentid(100);
    test_proto.set_num_of_rows(1000);

    // Add first binlog without child_fields
    auto* binlog1 = test_proto.add_binlog_paths();
    binlog1->set_fieldid(101);
    auto* log1 = binlog1->add_binlogs();
    log1->set_log_path("/path/to/binlog1");
    log1->set_entries_num(500);

    // Add second binlog without child_fields
    auto* binlog2 = test_proto.add_binlog_paths();
    binlog2->set_fieldid(102);
    auto* log2 = binlog2->add_binlogs();
    log2->set_log_path("/path/to/binlog2");
    log2->set_entries_num(500);

    SegmentLoadInfo info(test_proto, schema_);
    auto diff = info.GetLoadDiff();

    EXPECT_TRUE(diff.HasChanges());
    EXPECT_EQ(diff.binlogs_to_load.size(), 2);
    EXPECT_EQ(diff.binlogs_to_load[0].first.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first[0].get(), 101);
    EXPECT_EQ(diff.binlogs_to_load[1].first.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[1].first[0].get(), 102);
}

TEST_F(SegmentLoadInfoTest, ComputeDiffWithBinlogsLegacyFormat) {
    // Test ComputeDiff between two SegmentLoadInfos using legacy format

    // Current: has field 101
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);
    auto* binlog1 = current_proto.add_binlog_paths();
    binlog1->set_fieldid(101);
    auto* log1 = binlog1->add_binlogs();
    log1->set_log_path("/path/to/binlog1");
    log1->set_entries_num(500);

    // New: has field 101 and 102
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_num_of_rows(1000);
    auto* new_binlog1 = new_proto.add_binlog_paths();
    new_binlog1->set_fieldid(101);
    auto* new_log1 = new_binlog1->add_binlogs();
    new_log1->set_log_path("/path/to/binlog1");
    new_log1->set_entries_num(500);

    auto* new_binlog2 = new_proto.add_binlog_paths();
    new_binlog2->set_fieldid(102);
    auto* new_log2 = new_binlog2->add_binlogs();
    new_log2->set_log_path("/path/to/binlog2");
    new_log2->set_entries_num(500);

    SegmentLoadInfo current_info(current_proto, schema_);
    SegmentLoadInfo new_info(new_proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);
    std::cout << "ComputeDiff legacy format: " << diff.ToString() << "\n";

    EXPECT_TRUE(diff.HasChanges());
    // Field 102 should be added
    EXPECT_EQ(diff.binlogs_to_load.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first[0].get(), 102);

    // No fields should be dropped or replaced
    EXPECT_TRUE(diff.field_data_to_drop.empty());
    EXPECT_TRUE(diff.binlogs_to_replace.empty());
}

TEST_F(SegmentLoadInfoTest, ComputeDiffDropFieldLegacyFormat) {
    // Test ComputeDiff when dropping a field in legacy format

    // Current: has fields 101 and 102
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);

    auto* binlog1 = current_proto.add_binlog_paths();
    binlog1->set_fieldid(101);
    auto* log1 = binlog1->add_binlogs();
    log1->set_log_path("/path/to/binlog1");
    log1->set_entries_num(500);

    auto* binlog2 = current_proto.add_binlog_paths();
    binlog2->set_fieldid(102);
    auto* log2 = binlog2->add_binlogs();
    log2->set_log_path("/path/to/binlog2");
    log2->set_entries_num(500);

    // New: only has field 101 (102 is dropped)
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_num_of_rows(1000);

    auto* new_binlog1 = new_proto.add_binlog_paths();
    new_binlog1->set_fieldid(101);
    auto* new_log1 = new_binlog1->add_binlogs();
    new_log1->set_log_path("/path/to/binlog1");
    new_log1->set_entries_num(500);

    SegmentLoadInfo current_info(current_proto, schema_);
    SegmentLoadInfo new_info(new_proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);

    EXPECT_TRUE(diff.HasChanges());
    // No new binlogs to load or replace
    EXPECT_TRUE(diff.binlogs_to_load.empty());
    EXPECT_TRUE(diff.binlogs_to_replace.empty());
    // Field 102 should be dropped
    EXPECT_EQ(diff.field_data_to_drop.size(), 1);
    EXPECT_TRUE(diff.field_data_to_drop.count(FieldId(102)) > 0);
}

TEST_F(SegmentLoadInfoTest, ComputeDiffMixedFormats) {
    // Test ComputeDiff with mixed formats: legacy and column group

    // Current: has field 101 (legacy) and column group 104 with child fields 105, 106
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);

    // Legacy format binlog
    auto* binlog1 = current_proto.add_binlog_paths();
    binlog1->set_fieldid(101);
    auto* log1 = binlog1->add_binlogs();
    log1->set_log_path("/path/to/binlog1");
    log1->set_entries_num(500);

    // Column group format binlog
    auto* binlog2 = current_proto.add_binlog_paths();
    binlog2->set_fieldid(104);
    binlog2->add_child_fields(105);
    binlog2->add_child_fields(106);
    auto* log2 = binlog2->add_binlogs();
    log2->set_log_path("/path/to/group_binlog");
    log2->set_entries_num(1000);

    // New: has all existing + new field 103 (legacy) + new field 107 in column group
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_num_of_rows(1000);

    auto* new_binlog1 = new_proto.add_binlog_paths();
    new_binlog1->set_fieldid(101);
    auto* new_log1 = new_binlog1->add_binlogs();
    new_log1->set_log_path("/path/to/binlog1");
    new_log1->set_entries_num(500);

    // New legacy field
    auto* new_binlog3 = new_proto.add_binlog_paths();
    new_binlog3->set_fieldid(103);
    auto* new_log3 = new_binlog3->add_binlogs();
    new_log3->set_log_path("/path/to/binlog3");
    new_log3->set_entries_num(500);

    // Updated column group with additional field
    auto* new_binlog2 = new_proto.add_binlog_paths();
    new_binlog2->set_fieldid(104);
    new_binlog2->add_child_fields(105);
    new_binlog2->add_child_fields(106);
    new_binlog2->add_child_fields(107);  // New field added to group
    auto* new_log2 = new_binlog2->add_binlogs();
    new_log2->set_log_path("/path/to/group_binlog_new");
    new_log2->set_entries_num(1500);

    SegmentLoadInfo current_info(current_proto, schema_);
    SegmentLoadInfo new_info(new_proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);
    std::cout << "Mixed format diff: " << diff.ToString() << "\n";

    EXPECT_TRUE(diff.HasChanges());
    // Field 103 (legacy) and field 107 (in column group) should be loaded
    EXPECT_EQ(diff.binlogs_to_load.size(), 2);

    // Fields 105, 106 were in current (group 104) and group 104 is not found
    // as a child_id key in current_fields, so they go to binlogs_to_replace
    EXPECT_EQ(diff.binlogs_to_replace.size(), 1);
    EXPECT_EQ(diff.binlogs_to_replace[0].first.size(), 2);
    std::set<int64_t> replace_field_ids;
    for (const auto& fid : diff.binlogs_to_replace[0].first) {
        replace_field_ids.insert(fid.get());
    }
    EXPECT_TRUE(replace_field_ids.count(105) > 0);
    EXPECT_TRUE(replace_field_ids.count(106) > 0);

    // No fields should be dropped
    EXPECT_TRUE(diff.field_data_to_drop.empty());
}

TEST_F(SegmentLoadInfoTest, ComputeDiffNoChangesLegacyFormat) {
    // Test ComputeDiff when there are no changes (same binlogs)
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    auto* binlog = proto.add_binlog_paths();
    binlog->set_fieldid(101);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/binlog");
    log->set_entries_num(500);

    SegmentLoadInfo current_info(proto, schema_);
    // calculate first diff to set default value fields
    auto diff = current_info.GetLoadDiff();
    SegmentLoadInfo new_info(proto, schema_);
    diff = current_info.ComputeDiff(new_info);

    EXPECT_FALSE(diff.HasChanges());
    EXPECT_TRUE(diff.binlogs_to_load.empty());
    EXPECT_TRUE(diff.binlogs_to_replace.empty());
    EXPECT_TRUE(diff.indexes_to_replace.empty());
    EXPECT_TRUE(diff.field_data_to_drop.empty());
}

// ==================== Default Value Filling Tests ====================

TEST_F(SegmentLoadInfoTest, ComputeDiffDefaultFieldsBasic) {
    // Test that fields without data sources are added to fields_to_fill_default
    // Schema has fields 100-110, but we only provide binlog for field 100 (pk)
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    // Only add binlog for pk field (100)
    auto* binlog = proto.add_binlog_paths();
    binlog->set_fieldid(100);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/pk_binlog");
    log->set_entries_num(1000);

    SegmentLoadInfo empty_info(milvus::proto::segcore::SegmentLoadInfo(),
                               schema_);
    SegmentLoadInfo new_info(proto, schema_);

    auto diff = empty_info.ComputeDiff(new_info);

    // Fields 101-110 should be in fields_to_fill_default (no data source)
    // Field 100 has binlog, so it should NOT be in fields_to_fill_default
    EXPECT_TRUE(diff.HasChanges());
    EXPECT_FALSE(diff.fields_to_fill_default.empty());

    // Check that field 100 is NOT in fields_to_fill_default
    for (const auto& field_id : diff.fields_to_fill_default) {
        EXPECT_NE(field_id.get(), 100);
    }

    // Fields 101-110 should be in fields_to_fill_default
    std::set<int64_t> expected_fields = {
        101, 102, 103, 104, 105, 106, 107, 108, 109, 110};
    std::set<int64_t> actual_fields;
    for (const auto& field_id : diff.fields_to_fill_default) {
        actual_fields.insert(field_id.get());
    }
    EXPECT_EQ(actual_fields, expected_fields);
}

TEST_F(SegmentLoadInfoTest, ComputeDiffDefaultFieldsSkipAlreadyFilled) {
    // Test that fields already filled with default values are skipped
    // on subsequent ComputeDiff calls
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    // Only add binlog for pk field (100)
    auto* binlog = proto.add_binlog_paths();
    binlog->set_fieldid(100);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/pk_binlog");
    log->set_entries_num(1000);

    // First diff: empty -> proto (simulates initial load)
    SegmentLoadInfo empty_info(milvus::proto::segcore::SegmentLoadInfo(),
                               schema_);
    SegmentLoadInfo first_new_info(proto, schema_);
    auto first_diff = empty_info.ComputeDiff(first_new_info);

    // Verify fields were added to fields_to_fill_default
    EXPECT_FALSE(first_diff.fields_to_fill_default.empty());
    size_t first_count = first_diff.fields_to_fill_default.size();

    // Second diff: first_new_info -> same proto (simulates reopen)
    // first_new_info should now have fields_filled_with_default_ populated
    SegmentLoadInfo second_new_info(proto, schema_);
    auto second_diff = first_new_info.ComputeDiff(second_new_info);

    // All previously filled fields should be skipped
    EXPECT_TRUE(second_diff.fields_to_fill_default.empty());

    // Verify that second_new_info inherited the filled status
    // by doing a third diff
    SegmentLoadInfo third_new_info(proto, schema_);
    auto third_diff = second_new_info.ComputeDiff(third_new_info);
    EXPECT_TRUE(third_diff.fields_to_fill_default.empty());
}

TEST_F(SegmentLoadInfoTest, ComputeDiffDefaultFieldsWithIndex) {
    // Test that fields with index (that has raw data) are NOT added
    // to fields_to_fill_default
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    // Add binlog for pk field (100)
    auto* binlog = proto.add_binlog_paths();
    binlog->set_fieldid(100);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/pk_binlog");
    log->set_entries_num(1000);

    // Add index for field 101 (vec field) - FLAT index has raw data
    auto* index_info = proto.add_index_infos();
    index_info->set_fieldid(101);
    index_info->set_indexid(1001);
    index_info->add_index_file_paths("/path/to/index");
    auto* index_param = index_info->add_index_params();
    index_param->set_key("index_type");
    index_param->set_value(knowhere::IndexEnum::INDEX_FAISS_IDMAP);

    SegmentLoadInfo empty_info(milvus::proto::segcore::SegmentLoadInfo(),
                               schema_);
    SegmentLoadInfo new_info(proto, schema_);
    auto diff = empty_info.ComputeDiff(new_info);

    // Field 101 should NOT be in fields_to_fill_default (has index with raw
    // data)
    for (const auto& field_id : diff.fields_to_fill_default) {
        EXPECT_NE(field_id.get(), 101);
    }

    // Field 101 should be in indexes_to_load
    EXPECT_TRUE(diff.indexes_to_load.find(FieldId(101)) !=
                diff.indexes_to_load.end());
}

TEST_F(SegmentLoadInfoTest, ComputeDiffDefaultFieldsNotInBinlogsToLoad) {
    // Test that fields being loaded via binlogs_to_load are NOT added
    // to fields_to_fill_default
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);

    // Current has only pk field
    auto* binlog1 = current_proto.add_binlog_paths();
    binlog1->set_fieldid(100);
    auto* log1 = binlog1->add_binlogs();
    log1->set_log_path("/path/to/pk_binlog");
    log1->set_entries_num(1000);

    // New has pk + field 101
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_num_of_rows(1000);

    auto* new_binlog1 = new_proto.add_binlog_paths();
    new_binlog1->set_fieldid(100);
    auto* new_log1 = new_binlog1->add_binlogs();
    new_log1->set_log_path("/path/to/pk_binlog");
    new_log1->set_entries_num(1000);

    auto* new_binlog2 = new_proto.add_binlog_paths();
    new_binlog2->set_fieldid(101);
    auto* new_log2 = new_binlog2->add_binlogs();
    new_log2->set_log_path("/path/to/field101_binlog");
    new_log2->set_entries_num(1000);

    SegmentLoadInfo current_info(current_proto, schema_);
    SegmentLoadInfo new_info(new_proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);

    // Field 101 should be in binlogs_to_load, NOT in fields_to_fill_default
    bool found_in_binlogs = false;
    for (const auto& [field_ids, binlog_ptr] : diff.binlogs_to_load) {
        for (const auto& fid : field_ids) {
            if (fid.get() == 101) {
                found_in_binlogs = true;
                break;
            }
        }
    }
    EXPECT_TRUE(found_in_binlogs);

    // Field 101 should NOT be in fields_to_fill_default
    for (const auto& field_id : diff.fields_to_fill_default) {
        EXPECT_NE(field_id.get(), 101);
    }
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffDefaultFields) {
    // Test GetLoadDiff includes fields_to_fill_default for initial load
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    // Only add binlog for pk field (100) and vec field (101)
    auto* binlog1 = proto.add_binlog_paths();
    binlog1->set_fieldid(100);
    auto* log1 = binlog1->add_binlogs();
    log1->set_log_path("/path/to/pk_binlog");
    log1->set_entries_num(1000);

    auto* binlog2 = proto.add_binlog_paths();
    binlog2->set_fieldid(101);
    auto* log2 = binlog2->add_binlogs();
    log2->set_log_path("/path/to/vec_binlog");
    log2->set_entries_num(1000);

    SegmentLoadInfo info(proto, schema_);
    auto diff = info.GetLoadDiff();

    // Fields 102-110 should be in fields_to_fill_default
    std::set<int64_t> expected_fields = {
        102, 103, 104, 105, 106, 107, 108, 109, 110};
    std::set<int64_t> actual_fields;
    for (const auto& field_id : diff.fields_to_fill_default) {
        actual_fields.insert(field_id.get());
    }
    EXPECT_EQ(actual_fields, expected_fields);
}

TEST_F(SegmentLoadInfoTest, LoadDiffToStringIncludesDefaultFields) {
    // Test that LoadDiff::ToString includes fields_to_fill_default
    LoadDiff diff;
    diff.fields_to_fill_default.push_back(FieldId(102));
    diff.fields_to_fill_default.push_back(FieldId(103));

    std::string str = diff.ToString();
    EXPECT_TRUE(str.find("fields_to_fill_default") != std::string::npos);
    EXPECT_TRUE(str.find("102") != std::string::npos);
    EXPECT_TRUE(str.find("103") != std::string::npos);
}

TEST_F(SegmentLoadInfoTest, LoadDiffHasChangesWithDefaultFields) {
    // Test that HasChanges returns true when only fields_to_fill_default
    // is non-empty
    LoadDiff diff;
    EXPECT_FALSE(diff.HasChanges());

    diff.fields_to_fill_default.push_back(FieldId(102));
    EXPECT_TRUE(diff.HasChanges());
}

// ==================== Text Index Tests ====================

// Helper: create a schema with varchar field that has enable_match=true
static SchemaPtr
CreateSchemaWithTextMatchField() {
    auto schema = std::make_shared<Schema>();
    // 100=pk, 101=vec, 102=varchar_match, 103=varchar_no_match
    schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 128, knowhere::metric::L2);

    // Varchar field with enable_match=true (field 102)
    std::map<std::string, std::string> params;
    schema->AddDebugVarcharField(FieldName("text_field"),
                                 DataType::VARCHAR,
                                 /*max_length=*/65535,
                                 /*nullable=*/false,
                                 /*enable_match=*/true,
                                 /*enable_analyzer=*/true,
                                 params,
                                 std::nullopt);

    // Varchar field without enable_match (field 103)
    schema->AddDebugField("plain_varchar", DataType::VARCHAR);

    schema->set_primary_field_id(FieldId(100));
    return schema;
}

TEST_F(SegmentLoadInfoTest, SetTextIndexCreatedAndHas) {
    // Test SetTextIndexCreated/HasTextIndexCreated tracking
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    SegmentLoadInfo info(proto, schema_);

    EXPECT_FALSE(info.HasTextIndexCreated(FieldId(101)));

    info.SetTextIndexCreated(FieldId(101));
    EXPECT_TRUE(info.HasTextIndexCreated(FieldId(101)));
    EXPECT_FALSE(info.HasTextIndexCreated(FieldId(102)));

    info.SetTextIndexCreated(FieldId(102));
    EXPECT_TRUE(info.HasTextIndexCreated(FieldId(102)));
}

TEST_F(SegmentLoadInfoTest, CreatedTextIndexesCopyConstructor) {
    // Test that created_text_indexes_ is preserved via copy constructor
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    SegmentLoadInfo info1(proto, schema_);
    info1.SetTextIndexCreated(FieldId(101));
    info1.SetTextIndexCreated(FieldId(102));

    // Copy constructor
    SegmentLoadInfo info2(info1);
    EXPECT_TRUE(info2.HasTextIndexCreated(FieldId(101)));
    EXPECT_TRUE(info2.HasTextIndexCreated(FieldId(102)));
    EXPECT_FALSE(info2.HasTextIndexCreated(FieldId(103)));
}

TEST_F(SegmentLoadInfoTest, CreatedTextIndexesMoveConstructor) {
    // Test that created_text_indexes_ is preserved via move constructor
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    SegmentLoadInfo info1(proto, schema_);
    info1.SetTextIndexCreated(FieldId(101));

    SegmentLoadInfo info2(std::move(info1));
    EXPECT_TRUE(info2.HasTextIndexCreated(FieldId(101)));
}

TEST_F(SegmentLoadInfoTest, CreatedTextIndexesCopyAssignment) {
    // Test that created_text_indexes_ is preserved via copy assignment
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    SegmentLoadInfo info1(proto, schema_);
    info1.SetTextIndexCreated(FieldId(101));

    proto::segcore::SegmentLoadInfo empty_proto;
    SegmentLoadInfo info2(empty_proto, schema_);
    info2 = info1;

    EXPECT_TRUE(info2.HasTextIndexCreated(FieldId(101)));
}

TEST_F(SegmentLoadInfoTest, CreatedTextIndexesMoveAssignment) {
    // Test that created_text_indexes_ is preserved via move assignment
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    SegmentLoadInfo info1(proto, schema_);
    info1.SetTextIndexCreated(FieldId(101));

    proto::segcore::SegmentLoadInfo empty_proto;
    SegmentLoadInfo info2(empty_proto, schema_);
    info2 = std::move(info1);

    EXPECT_TRUE(info2.HasTextIndexCreated(FieldId(101)));
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffTextIndexesFromStats) {
    // Test GetLoadDiff loads pre-built text indexes from textstatslogs
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_collectionid(200);
    proto.set_partitionid(300);
    proto.set_num_of_rows(1000);

    // Add a binlog to satisfy GetLoadDiff precondition
    auto* binlog = proto.add_binlog_paths();
    binlog->set_fieldid(100);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/pk_binlog");
    log->set_entries_num(1000);

    // Add text stats for field 101 (simulates pre-built text index)
    auto& text_stats = (*proto.mutable_textstatslogs())[101];
    text_stats.set_fieldid(101);
    text_stats.set_version(1);
    text_stats.set_buildid(5001);
    text_stats.set_memory_size(1024);
    text_stats.set_current_scalar_index_version(2);
    text_stats.add_files("/path/to/text_index_file1");
    text_stats.add_files("/path/to/text_index_file2");

    SegmentLoadInfo info(proto, schema_);
    auto diff = info.GetLoadDiff();

    // The text index should be in text_indexes_to_load
    EXPECT_EQ(diff.text_indexes_to_load.size(), 1);
    EXPECT_TRUE(diff.text_indexes_to_load.count(FieldId(101)) > 0);

    // Verify the converted LoadTextIndexInfo
    auto& load_info = diff.text_indexes_to_load[FieldId(101)];
    EXPECT_NE(load_info, nullptr);
    EXPECT_EQ(load_info->fieldid(), 101);
    EXPECT_EQ(load_info->version(), 1);
    EXPECT_EQ(load_info->buildid(), 5001);
    EXPECT_EQ(load_info->files_size(), 2);
    EXPECT_EQ(load_info->files(0), "/path/to/text_index_file1");
    EXPECT_EQ(load_info->files(1), "/path/to/text_index_file2");
    EXPECT_EQ(load_info->collectionid(), 200);
    EXPECT_EQ(load_info->partitionid(), 300);
    EXPECT_EQ(load_info->index_size(), 1024);
    EXPECT_EQ(load_info->current_scalar_index_version(), 2);
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffTextIndexesToCreate) {
    // Test GetLoadDiff creates text indexes for enable_match fields
    // without pre-built text indexes
    auto text_schema = CreateSchemaWithTextMatchField();

    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    // Add a binlog to satisfy GetLoadDiff precondition
    auto* binlog = proto.add_binlog_paths();
    binlog->set_fieldid(100);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/pk_binlog");
    log->set_entries_num(1000);

    // No text stats provided - the text_field (102) has enable_match
    // so it should be in text_indexes_to_create
    SegmentLoadInfo info(proto, text_schema);
    auto diff = info.GetLoadDiff();

    // text_field (102) should be in text_indexes_to_create
    EXPECT_TRUE(diff.text_indexes_to_create.count(FieldId(102)) > 0);
    // plain_varchar (103) should NOT be (no enable_match)
    EXPECT_TRUE(diff.text_indexes_to_create.count(FieldId(103)) == 0);
    // No text indexes to load (no stats)
    EXPECT_TRUE(diff.text_indexes_to_load.empty());
}

TEST_F(SegmentLoadInfoTest, GetLoadDiffTextIndexesPreBuiltSkipsCreate) {
    // When a pre-built text index exists, the field should NOT be
    // in text_indexes_to_create
    auto text_schema = CreateSchemaWithTextMatchField();

    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_collectionid(200);
    proto.set_partitionid(300);
    proto.set_num_of_rows(1000);

    // Add a binlog to satisfy GetLoadDiff precondition
    auto* binlog = proto.add_binlog_paths();
    binlog->set_fieldid(100);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/pk_binlog");
    log->set_entries_num(1000);

    // Add pre-built text index for field 102 (text_field)
    auto& text_stats = (*proto.mutable_textstatslogs())[102];
    text_stats.set_fieldid(102);
    text_stats.set_version(1);
    text_stats.set_buildid(5001);
    text_stats.add_files("/path/to/text_index");

    SegmentLoadInfo info(proto, text_schema);
    auto diff = info.GetLoadDiff();

    // Field 102 should be in text_indexes_to_load (pre-built)
    EXPECT_EQ(diff.text_indexes_to_load.size(), 1);
    EXPECT_TRUE(diff.text_indexes_to_load.count(FieldId(102)) > 0);

    // Field 102 should NOT be in text_indexes_to_create (has pre-built)
    EXPECT_TRUE(diff.text_indexes_to_create.count(FieldId(102)) == 0);
}

TEST_F(SegmentLoadInfoTest, ComputeDiffTextIndexNewTextStats) {
    // ComputeDiff: new_info has text stats that current doesn't -> load
    auto text_schema = CreateSchemaWithTextMatchField();

    // Current: no text stats
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);

    // New: has text stats for field 102
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_collectionid(200);
    new_proto.set_partitionid(300);
    new_proto.set_num_of_rows(1000);
    auto& text_stats = (*new_proto.mutable_textstatslogs())[102];
    text_stats.set_fieldid(102);
    text_stats.set_version(1);
    text_stats.set_buildid(5001);
    text_stats.add_files("/path/to/text_index");

    SegmentLoadInfo current_info(current_proto, text_schema);
    SegmentLoadInfo new_info(new_proto, text_schema);
    auto diff = current_info.ComputeDiff(new_info);

    // Field 102 should be in text_indexes_to_load
    EXPECT_EQ(diff.text_indexes_to_load.size(), 1);
    EXPECT_TRUE(diff.text_indexes_to_load.count(FieldId(102)) > 0);

    // Field 102 should NOT be in text_indexes_to_create (has pre-built in new)
    EXPECT_TRUE(diff.text_indexes_to_create.count(FieldId(102)) == 0);
}

TEST_F(SegmentLoadInfoTest, ComputeDiffTextIndexAlreadyLoaded) {
    // ComputeDiff: both current and new have same text stats -> no diff
    auto text_schema = CreateSchemaWithTextMatchField();

    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_collectionid(200);
    proto.set_partitionid(300);
    proto.set_num_of_rows(1000);

    // Add a binlog to satisfy GetLoadDiff precondition
    auto* binlog = proto.add_binlog_paths();
    binlog->set_fieldid(100);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/pk_binlog");
    log->set_entries_num(1000);

    auto& text_stats = (*proto.mutable_textstatslogs())[102];
    text_stats.set_fieldid(102);
    text_stats.set_version(1);
    text_stats.set_buildid(5001);
    text_stats.add_files("/path/to/text_index");

    SegmentLoadInfo current_info(proto, text_schema);
    // Calculate first diff to populate default fields etc.
    auto first_diff = current_info.GetLoadDiff();
    SegmentLoadInfo new_info(proto, text_schema);
    auto diff = current_info.ComputeDiff(new_info);

    // No text index changes - both have the same text stats
    EXPECT_TRUE(diff.text_indexes_to_load.empty());
    EXPECT_TRUE(diff.text_indexes_to_create.empty());
}

TEST_F(SegmentLoadInfoTest, ComputeDiffTextIndexCreatedFromRawData) {
    // ComputeDiff: current already created text index from raw data ->
    // should not be re-created or re-loaded
    auto text_schema = CreateSchemaWithTextMatchField();

    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);

    SegmentLoadInfo current_info(proto, text_schema);
    // Mark field 102 as having been created from raw data
    current_info.SetTextIndexCreated(FieldId(102));

    SegmentLoadInfo new_info(proto, text_schema);
    auto diff = current_info.ComputeDiff(new_info);

    // Field 102 should NOT be in text_indexes_to_create (already created)
    EXPECT_TRUE(diff.text_indexes_to_create.count(FieldId(102)) == 0);
    // Field 102 should NOT be in text_indexes_to_load (no stats in new)
    EXPECT_TRUE(diff.text_indexes_to_load.empty());
}

TEST_F(SegmentLoadInfoTest, ComputeDiffTextIndexCreateForUnindexed) {
    // ComputeDiff: enable_match field without pre-built index and not yet
    // created -> should be in text_indexes_to_create
    auto text_schema = CreateSchemaWithTextMatchField();

    // Current: no text stats, no created indexes
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);

    // New: also no text stats for field 102
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_num_of_rows(1000);

    SegmentLoadInfo current_info(current_proto, text_schema);
    SegmentLoadInfo new_info(new_proto, text_schema);
    auto diff = current_info.ComputeDiff(new_info);

    // Field 102 (enable_match=true) should be in text_indexes_to_create
    EXPECT_TRUE(diff.text_indexes_to_create.count(FieldId(102)) > 0);
    // Field 103 (no enable_match) should NOT be in text_indexes_to_create
    EXPECT_TRUE(diff.text_indexes_to_create.count(FieldId(103)) == 0);
}

TEST_F(SegmentLoadInfoTest, ConvertTextIndexStatsToLoadTextIndexInfo) {
    // Test the conversion of TextIndexStats to LoadTextIndexInfo
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_collectionid(200);
    proto.set_partitionid(300);
    proto.set_num_of_rows(1000);
    proto.set_priority(proto::common::LoadPriority::HIGH);

    proto::segcore::TextIndexStats text_stats;
    text_stats.set_fieldid(101);
    text_stats.set_version(3);
    text_stats.set_buildid(5001);
    text_stats.set_memory_size(2048);
    text_stats.set_current_scalar_index_version(2);
    text_stats.add_files("/path/to/file1");
    text_stats.add_files("/path/to/file2");
    text_stats.add_files("/path/to/file3");

    SegmentLoadInfo info(proto, schema_);
    auto load_info =
        info.ConvertTextIndexStatsToLoadTextIndexInfo(text_stats, FieldId(101));

    EXPECT_NE(load_info, nullptr);
    EXPECT_EQ(load_info->fieldid(), 101);
    EXPECT_EQ(load_info->version(), 3);
    EXPECT_EQ(load_info->buildid(), 5001);
    EXPECT_EQ(load_info->index_size(), 2048);
    EXPECT_EQ(load_info->current_scalar_index_version(), 2);
    EXPECT_EQ(load_info->files_size(), 3);
    EXPECT_EQ(load_info->files(0), "/path/to/file1");
    EXPECT_EQ(load_info->files(1), "/path/to/file2");
    EXPECT_EQ(load_info->files(2), "/path/to/file3");
    EXPECT_EQ(load_info->collectionid(), 200);
    EXPECT_EQ(load_info->partitionid(), 300);
    EXPECT_EQ(load_info->load_priority(), proto::common::LoadPriority::HIGH);
    // Schema should be populated
    EXPECT_TRUE(load_info->has_schema());
}

TEST_F(SegmentLoadInfoTest, LoadDiffHasChangesWithTextIndexesToLoad) {
    // Test that HasChanges returns true when text_indexes_to_load is non-empty
    LoadDiff diff;
    EXPECT_FALSE(diff.HasChanges());

    diff.text_indexes_to_load[FieldId(101)] =
        std::make_shared<proto::indexcgo::LoadTextIndexInfo>();
    EXPECT_TRUE(diff.HasChanges());
}

TEST_F(SegmentLoadInfoTest, LoadDiffHasChangesWithTextIndexesToCreate) {
    // Test that HasChanges returns true when text_indexes_to_create is non-empty
    LoadDiff diff;
    EXPECT_FALSE(diff.HasChanges());

    diff.text_indexes_to_create.insert(FieldId(102));
    EXPECT_TRUE(diff.HasChanges());
}

TEST_F(SegmentLoadInfoTest, LoadDiffToStringIncludesTextIndexes) {
    // Test that ToString includes text index fields
    LoadDiff diff;
    diff.text_indexes_to_load[FieldId(101)] =
        std::make_shared<proto::indexcgo::LoadTextIndexInfo>();
    diff.text_indexes_to_create.insert(FieldId(102));

    std::string str = diff.ToString();
    EXPECT_TRUE(str.find("text_indexes_to_load") != std::string::npos);
    EXPECT_TRUE(str.find("101") != std::string::npos);
    EXPECT_TRUE(str.find("text_indexes_to_create") != std::string::npos);
    EXPECT_TRUE(str.find("102") != std::string::npos);
}

TEST_F(SegmentLoadInfoTest, ComputeDiffTextIndexMultipleFields) {
    // Test with multiple text stats fields in new_info
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);

    // Current has text stats for field 101
    auto& current_stats = (*current_proto.mutable_textstatslogs())[101];
    current_stats.set_fieldid(101);
    current_stats.set_version(1);
    current_stats.add_files("/path/to/old_text_index");

    // New has text stats for fields 101 and 102
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_collectionid(200);
    new_proto.set_partitionid(300);
    new_proto.set_num_of_rows(1000);

    auto& new_stats1 = (*new_proto.mutable_textstatslogs())[101];
    new_stats1.set_fieldid(101);
    new_stats1.set_version(1);
    new_stats1.add_files("/path/to/old_text_index");

    auto& new_stats2 = (*new_proto.mutable_textstatslogs())[102];
    new_stats2.set_fieldid(102);
    new_stats2.set_version(1);
    new_stats2.set_buildid(5002);
    new_stats2.add_files("/path/to/new_text_index");

    SegmentLoadInfo current_info(current_proto, schema_);
    SegmentLoadInfo new_info(new_proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);

    // Only field 102 should be in text_indexes_to_load (101 already exists)
    EXPECT_EQ(diff.text_indexes_to_load.size(), 1);
    EXPECT_TRUE(diff.text_indexes_to_load.count(FieldId(102)) > 0);
    EXPECT_TRUE(diff.text_indexes_to_load.count(FieldId(101)) == 0);
}

// ==================== Replace Tests ====================
// Tests for indexes_to_replace, binlogs_to_replace, column_groups_to_replace

TEST_F(SegmentLoadInfoTest, ComputeDiffIndexReplace) {
    // When current has index for field 101 (index_id=1001) and new has
    // index for field 101 with different index_id (2001), it should go
    // to indexes_to_replace (not indexes_to_load)

    // Current: field 101 has index 1001
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);
    auto* cur_index = current_proto.add_index_infos();
    cur_index->set_fieldid(101);
    cur_index->set_indexid(1001);
    cur_index->add_index_file_paths("/path/to/old_index");
    auto* cur_param = cur_index->add_index_params();
    cur_param->set_key("index_type");
    cur_param->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

    // New: field 101 has different index 2001
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_num_of_rows(1000);
    auto* new_index = new_proto.add_index_infos();
    new_index->set_fieldid(101);
    new_index->set_indexid(2001);
    new_index->add_index_file_paths("/path/to/new_index");
    auto* new_param = new_index->add_index_params();
    new_param->set_key("index_type");
    new_param->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

    SegmentLoadInfo current_info(current_proto, schema_);
    SegmentLoadInfo new_info(new_proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);
    std::cout << "Index replace diff: " << diff.ToString() << "\n";

    EXPECT_TRUE(diff.HasChanges());
    // Field 101 should be in indexes_to_replace (already had index)
    EXPECT_EQ(diff.indexes_to_replace.size(), 1);
    EXPECT_TRUE(diff.indexes_to_replace.count(FieldId(101)) > 0);
    EXPECT_EQ(diff.indexes_to_replace[FieldId(101)].size(), 1);
    EXPECT_EQ(diff.indexes_to_replace[FieldId(101)][0].index_id, 2001);

    // Not in indexes_to_load
    EXPECT_TRUE(diff.indexes_to_load.empty());
    // Old index should be dropped
    EXPECT_EQ(diff.indexes_to_drop.size(), 1);
    EXPECT_TRUE(diff.indexes_to_drop.count(FieldId(101)) > 0);
}

TEST_F(SegmentLoadInfoTest, ComputeDiffIndexReplaceMixed) {
    // Mixed scenario: some indexes replaced, some new, some unchanged

    // Current: field 101 index_id=1001, field 102 index_id=1002
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);

    auto* cur_idx1 = current_proto.add_index_infos();
    cur_idx1->set_fieldid(101);
    cur_idx1->set_indexid(1001);
    cur_idx1->add_index_file_paths("/path/to/idx1");
    auto* cur_param1 = cur_idx1->add_index_params();
    cur_param1->set_key("index_type");
    cur_param1->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

    auto* cur_idx2 = current_proto.add_index_infos();
    cur_idx2->set_fieldid(102);
    cur_idx2->set_indexid(1002);
    cur_idx2->add_index_file_paths("/path/to/idx2");
    auto* cur_param2 = cur_idx2->add_index_params();
    cur_param2->set_key("index_type");
    cur_param2->set_value(milvus::index::INVERTED_INDEX_TYPE);

    // New: field 101 index_id=2001 (replace), field 102 index_id=1002 (same),
    //      field 103 index_id=3001 (new)
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_num_of_rows(1000);

    auto* new_idx1 = new_proto.add_index_infos();
    new_idx1->set_fieldid(101);
    new_idx1->set_indexid(2001);
    new_idx1->add_index_file_paths("/path/to/new_idx1");
    auto* new_param1 = new_idx1->add_index_params();
    new_param1->set_key("index_type");
    new_param1->set_value(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8);

    auto* new_idx2 = new_proto.add_index_infos();
    new_idx2->set_fieldid(102);
    new_idx2->set_indexid(1002);
    new_idx2->add_index_file_paths("/path/to/idx2");
    auto* new_param2 = new_idx2->add_index_params();
    new_param2->set_key("index_type");
    new_param2->set_value(milvus::index::INVERTED_INDEX_TYPE);

    auto* new_idx3 = new_proto.add_index_infos();
    new_idx3->set_fieldid(103);
    new_idx3->set_indexid(3001);
    new_idx3->add_index_file_paths("/path/to/idx3");
    auto* new_param3 = new_idx3->add_index_params();
    new_param3->set_key("index_type");
    new_param3->set_value(milvus::index::INVERTED_INDEX_TYPE);

    SegmentLoadInfo current_info(current_proto, schema_);
    SegmentLoadInfo new_info(new_proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);
    std::cout << "Index replace mixed diff: " << diff.ToString() << "\n";

    // Field 101: old index_id=1001 dropped, new index_id=2001 in replace
    EXPECT_EQ(diff.indexes_to_replace.size(), 1);
    EXPECT_TRUE(diff.indexes_to_replace.count(FieldId(101)) > 0);
    EXPECT_EQ(diff.indexes_to_replace[FieldId(101)][0].index_id, 2001);

    // Field 103: new index, should be in indexes_to_load
    EXPECT_EQ(diff.indexes_to_load.size(), 1);
    EXPECT_TRUE(diff.indexes_to_load.count(FieldId(103)) > 0);

    // Field 102: unchanged (same index_id), should not appear
    EXPECT_TRUE(diff.indexes_to_load.count(FieldId(102)) == 0);
    EXPECT_TRUE(diff.indexes_to_replace.count(FieldId(102)) == 0);

    // Old index_id=1001 for field 101 should be dropped
    EXPECT_EQ(diff.indexes_to_drop.size(), 1);
    EXPECT_TRUE(diff.indexes_to_drop.count(FieldId(101)) > 0);
}

TEST_F(SegmentLoadInfoTest, ComputeDiffBinlogReplace) {
    // When a field's binlog group changes, it goes to binlogs_to_replace

    // Current: field 101 in group 101 (legacy format)
    proto::segcore::SegmentLoadInfo current_proto;
    current_proto.set_segmentid(100);
    current_proto.set_num_of_rows(1000);
    auto* cur_binlog = current_proto.add_binlog_paths();
    cur_binlog->set_fieldid(101);
    auto* cur_log = cur_binlog->add_binlogs();
    cur_log->set_log_path("/path/to/binlog_old");
    cur_log->set_entries_num(500);

    // New: field 101 in a column group 200 with children 101, 102
    // Field 101 already exists -> replace; field 102 is new -> load
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_num_of_rows(1000);
    auto* new_binlog = new_proto.add_binlog_paths();
    new_binlog->set_fieldid(200);
    new_binlog->add_child_fields(101);
    new_binlog->add_child_fields(102);
    auto* new_log = new_binlog->add_binlogs();
    new_log->set_log_path("/path/to/group_binlog");
    new_log->set_entries_num(1000);

    SegmentLoadInfo current_info(current_proto, schema_);
    SegmentLoadInfo new_info(new_proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);
    std::cout << "Binlog replace diff: " << diff.ToString() << "\n";

    EXPECT_TRUE(diff.HasChanges());
    // Field 102 is new -> binlogs_to_load
    EXPECT_EQ(diff.binlogs_to_load.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first.size(), 1);
    EXPECT_EQ(diff.binlogs_to_load[0].first[0].get(), 102);

    // Field 101 already existed -> binlogs_to_replace
    EXPECT_EQ(diff.binlogs_to_replace.size(), 1);
    EXPECT_EQ(diff.binlogs_to_replace[0].first.size(), 1);
    EXPECT_EQ(diff.binlogs_to_replace[0].first[0].get(), 101);

    // Nothing dropped (field 101 still present, just in different group)
    EXPECT_TRUE(diff.field_data_to_drop.empty());
}

TEST_F(SegmentLoadInfoTest, ComputeDiffBinlogNoReplace) {
    // When binlogs don't change, no replacement should occur

    // Current and new: same field 101 in same group (legacy)
    proto::segcore::SegmentLoadInfo proto;
    proto.set_segmentid(100);
    proto.set_num_of_rows(1000);
    auto* binlog = proto.add_binlog_paths();
    binlog->set_fieldid(101);
    auto* log = binlog->add_binlogs();
    log->set_log_path("/path/to/binlog");
    log->set_entries_num(500);

    SegmentLoadInfo current_info(proto, schema_);
    // First GetLoadDiff to initialize default fields
    auto first_diff = current_info.GetLoadDiff();

    SegmentLoadInfo new_info(proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);

    EXPECT_FALSE(diff.HasChanges());
    EXPECT_TRUE(diff.binlogs_to_load.empty());
    EXPECT_TRUE(diff.binlogs_to_replace.empty());
    EXPECT_TRUE(diff.indexes_to_load.empty());
    EXPECT_TRUE(diff.indexes_to_replace.empty());
}

TEST_F(SegmentLoadInfoTest, LoadDiffHasChangesWithReplace) {
    // Test that HasChanges returns true for each replace field
    {
        LoadDiff diff;
        EXPECT_FALSE(diff.HasChanges());
        diff.indexes_to_replace[FieldId(101)].push_back(LoadIndexInfo{});
        EXPECT_TRUE(diff.HasChanges());
    }
    {
        LoadDiff diff;
        diff.binlogs_to_replace.emplace_back(std::vector<FieldId>{FieldId(101)},
                                             proto::segcore::FieldBinlog{});
        EXPECT_TRUE(diff.HasChanges());
    }
    {
        LoadDiff diff;
        diff.column_groups_to_replace.emplace_back(
            0, std::vector<FieldId>{FieldId(101)});
        EXPECT_TRUE(diff.HasChanges());
    }
    {
        LoadDiff diff;
        diff.column_groups_to_lazyreplace.emplace_back(
            0, std::vector<FieldId>{FieldId(101)});
        EXPECT_TRUE(diff.HasChanges());
    }
}

TEST_F(SegmentLoadInfoTest, LoadDiffToStringIncludesReplace) {
    // Test that ToString includes all replace fields
    LoadDiff diff;
    diff.indexes_to_replace[FieldId(101)].push_back(LoadIndexInfo{});
    diff.binlogs_to_replace.emplace_back(std::vector<FieldId>{FieldId(102)},
                                         proto::segcore::FieldBinlog{});
    diff.column_groups_to_replace.emplace_back(
        0, std::vector<FieldId>{FieldId(103)});
    diff.column_groups_to_lazyreplace.emplace_back(
        1, std::vector<FieldId>{FieldId(104)});

    std::string str = diff.ToString();
    EXPECT_TRUE(str.find("indexes_to_replace") != std::string::npos);
    EXPECT_TRUE(str.find("binlogs_to_replace") != std::string::npos);
    EXPECT_TRUE(str.find("column_groups_to_replace") != std::string::npos);
    EXPECT_TRUE(str.find("column_groups_to_lazyreplace") != std::string::npos);
}

TEST_F(SegmentLoadInfoTest, ComputeDiffDefaultFilledFieldBecomesReplace) {
    // When a field was previously filled with default values (no binlog)
    // and the new LoadInfo provides actual binlog data for it,
    // it should go to binlogs_to_replace (not binlogs_to_load)
    // because the segment already has data loaded for it.

    // Step 1: Initial load with only pk field (100).
    // Fields 101-110 will be filled with default values.
    proto::segcore::SegmentLoadInfo initial_proto;
    initial_proto.set_segmentid(100);
    initial_proto.set_num_of_rows(1000);
    auto* binlog1 = initial_proto.add_binlog_paths();
    binlog1->set_fieldid(100);
    auto* log1 = binlog1->add_binlogs();
    log1->set_log_path("/path/to/pk_binlog");
    log1->set_entries_num(1000);

    SegmentLoadInfo current_info(initial_proto, schema_);
    // GetLoadDiff populates fields_filled_with_default_ for fields 101-110
    auto initial_diff = current_info.GetLoadDiff();
    EXPECT_FALSE(initial_diff.fields_to_fill_default.empty());

    // Step 2: New LoadInfo adds binlog for field 101 (was default-filled)
    proto::segcore::SegmentLoadInfo new_proto;
    new_proto.set_segmentid(100);
    new_proto.set_num_of_rows(1000);
    auto* new_binlog1 = new_proto.add_binlog_paths();
    new_binlog1->set_fieldid(100);
    auto* new_log1 = new_binlog1->add_binlogs();
    new_log1->set_log_path("/path/to/pk_binlog");
    new_log1->set_entries_num(1000);

    auto* new_binlog2 = new_proto.add_binlog_paths();
    new_binlog2->set_fieldid(101);
    auto* new_log2 = new_binlog2->add_binlogs();
    new_log2->set_log_path("/path/to/vec_binlog");
    new_log2->set_entries_num(1000);

    SegmentLoadInfo new_info(new_proto, schema_);
    auto diff = current_info.ComputeDiff(new_info);
    std::cout << "Default-filled replace diff: " << diff.ToString() << "\n";

    // Field 101 was default-filled → should be in binlogs_to_replace
    EXPECT_FALSE(diff.binlogs_to_replace.empty());
    bool found_101_in_replace = false;
    for (const auto& [field_ids, binlog] : diff.binlogs_to_replace) {
        for (const auto& fid : field_ids) {
            if (fid.get() == 101) {
                found_101_in_replace = true;
            }
        }
    }
    EXPECT_TRUE(found_101_in_replace);

    // Field 101 should NOT be in binlogs_to_load
    bool found_101_in_load = false;
    for (const auto& [field_ids, binlog] : diff.binlogs_to_load) {
        for (const auto& fid : field_ids) {
            if (fid.get() == 101) {
                found_101_in_load = true;
            }
        }
    }
    EXPECT_FALSE(found_101_in_load);

    // Field 101 should NOT be in fields_to_fill_default (has data source now)
    for (const auto& fid : diff.fields_to_fill_default) {
        EXPECT_NE(fid.get(), 101);
    }
}
