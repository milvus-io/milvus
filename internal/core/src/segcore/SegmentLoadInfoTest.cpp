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

#include "segcore/SegmentLoadInfo.h"

using namespace milvus;
using namespace milvus::segcore;

class SegmentLoadInfoTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
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

        // Add index info
        auto* index_info = proto_.add_index_infos();
        index_info->set_fieldid(101);
        index_info->set_indexid(1001);
        index_info->set_buildid(2001);
        index_info->set_index_version(1);
        index_info->add_index_file_paths("/path/to/index1");
        index_info->add_index_file_paths("/path/to/index2");

        auto* index_info2 = proto_.add_index_infos();
        index_info2->set_fieldid(102);
        index_info2->set_indexid(1002);
        index_info2->add_index_file_paths("/path/to/index3");

        // Add binlog paths
        auto* binlog = proto_.add_binlog_paths();
        binlog->set_fieldid(101);
        auto* log1 = binlog->add_binlogs();
        log1->set_log_path("/path/to/binlog1");
        log1->set_entries_num(500);
        auto* log2 = binlog->add_binlogs();
        log2->set_log_path("/path/to/binlog2");
        log2->set_entries_num(500);

        // Add column group binlog
        auto* group_binlog = proto_.add_binlog_paths();
        group_binlog->set_fieldid(200);
        group_binlog->add_child_fields(201);
        group_binlog->add_child_fields(202);
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

    proto::segcore::SegmentLoadInfo proto_;
};

TEST_F(SegmentLoadInfoTest, DefaultConstructor) {
    SegmentLoadInfo info;
    EXPECT_TRUE(info.IsEmpty());
    EXPECT_EQ(info.GetSegmentID(), 0);
    EXPECT_EQ(info.GetNumOfRows(), 0);
}

TEST_F(SegmentLoadInfoTest, ConstructFromProto) {
    SegmentLoadInfo info(proto_);

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
    SegmentLoadInfo info1(proto_);
    SegmentLoadInfo info2(std::move(info1));

    EXPECT_EQ(info2.GetSegmentID(), 12345);
    EXPECT_EQ(info2.GetNumOfRows(), 10000);
}

TEST_F(SegmentLoadInfoTest, CopyAssignment) {
    SegmentLoadInfo info1(proto_);
    SegmentLoadInfo info2;
    info2 = info1;

    EXPECT_EQ(info2.GetSegmentID(), 12345);
    EXPECT_EQ(info2.GetNumOfRows(), 10000);
}

TEST_F(SegmentLoadInfoTest, SetMethod) {
    SegmentLoadInfo info;
    info.Set(proto_);

    EXPECT_EQ(info.GetSegmentID(), 12345);
    EXPECT_EQ(info.GetNumOfRows(), 10000);
}

TEST_F(SegmentLoadInfoTest, CompactionInfo) {
    SegmentLoadInfo info(proto_);

    EXPECT_TRUE(info.IsCompacted());
    EXPECT_EQ(info.GetCompactionFromCount(), 2);
    EXPECT_EQ(info.GetCompactionFrom()[0], 111);
    EXPECT_EQ(info.GetCompactionFrom()[1], 222);
}

TEST_F(SegmentLoadInfoTest, IndexInfo) {
    SegmentLoadInfo info(proto_);

    EXPECT_EQ(info.GetIndexInfoCount(), 2);
    EXPECT_TRUE(info.HasIndexInfo(FieldId(101)));
    EXPECT_TRUE(info.HasIndexInfo(FieldId(102)));
    EXPECT_FALSE(info.HasIndexInfo(FieldId(999)));

    auto index_infos = info.GetFieldIndexInfos(FieldId(101));
    EXPECT_EQ(index_infos.size(), 1);
    EXPECT_EQ(index_infos[0]->indexid(), 1001);

    auto first_index = info.GetFirstFieldIndexInfo(FieldId(101));
    EXPECT_NE(first_index, nullptr);
    EXPECT_EQ(first_index->buildid(), 2001);

    auto indexed_fields = info.GetIndexedFieldIds();
    EXPECT_EQ(indexed_fields.size(), 2);
    EXPECT_TRUE(indexed_fields.count(FieldId(101)) > 0);
    EXPECT_TRUE(indexed_fields.count(FieldId(102)) > 0);

    // Test non-existent field
    auto empty_infos = info.GetFieldIndexInfos(FieldId(999));
    EXPECT_TRUE(empty_infos.empty());

    auto null_index = info.GetFirstFieldIndexInfo(FieldId(999));
    EXPECT_EQ(null_index, nullptr);
}

TEST_F(SegmentLoadInfoTest, BinlogInfo) {
    SegmentLoadInfo info(proto_);

    EXPECT_EQ(info.GetBinlogPathCount(), 2);
    EXPECT_TRUE(info.HasBinlogPath(FieldId(101)));
    EXPECT_TRUE(info.HasBinlogPath(FieldId(200)));
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
    SegmentLoadInfo info(proto_);

    EXPECT_TRUE(info.IsColumnGroup(FieldId(200)));
    EXPECT_FALSE(info.IsColumnGroup(FieldId(101)));
    EXPECT_FALSE(info.IsColumnGroup(FieldId(999)));

    auto child_fields = info.GetChildFieldIds(FieldId(200));
    EXPECT_EQ(child_fields.size(), 2);
    EXPECT_EQ(child_fields[0].get(), 201);
    EXPECT_EQ(child_fields[1].get(), 202);

    auto empty_children = info.GetChildFieldIds(FieldId(101));
    EXPECT_TRUE(empty_children.empty());
}

TEST_F(SegmentLoadInfoTest, StatsAndDeltaLogs) {
    SegmentLoadInfo info(proto_);

    EXPECT_EQ(info.GetStatslogCount(), 1);
    EXPECT_EQ(info.GetDeltalogCount(), 1);

    const auto& statslog = info.GetStatslog(0);
    EXPECT_EQ(statslog.fieldid(), 101);

    const auto& deltalog = info.GetDeltalog(0);
    EXPECT_EQ(deltalog.fieldid(), 0);
}

TEST_F(SegmentLoadInfoTest, TextStats) {
    SegmentLoadInfo info(proto_);

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
    SegmentLoadInfo info(proto_);

    EXPECT_TRUE(info.HasJsonKeyStatsLog(102));
    EXPECT_FALSE(info.HasJsonKeyStatsLog(999));

    auto json_stats = info.GetJsonKeyStatsLog(102);
    EXPECT_NE(json_stats, nullptr);
    EXPECT_EQ(json_stats->fieldid(), 102);

    auto null_stats = info.GetJsonKeyStatsLog(999);
    EXPECT_EQ(null_stats, nullptr);
}

TEST_F(SegmentLoadInfoTest, Bm25Logs) {
    SegmentLoadInfo info(proto_);

    EXPECT_EQ(info.GetBm25logCount(), 1);
    const auto& bm25log = info.GetBm25log(0);
    EXPECT_EQ(bm25log.fieldid(), 103);
}

TEST_F(SegmentLoadInfoTest, UnderlyingProtoAccess) {
    SegmentLoadInfo info(proto_);

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

    SegmentLoadInfo info(empty_proto);
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

    SegmentLoadInfo info(test_proto);

    // Index without files should not be in cache
    EXPECT_FALSE(info.HasIndexInfo(FieldId(101)));
    EXPECT_EQ(info.GetIndexInfoCount(), 1);  // Proto still has it
}
