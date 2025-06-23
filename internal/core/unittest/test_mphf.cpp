#include <gtest/gtest.h>
#include <common/BooPHF.h>
#include <common/PrimaryIndex.h>
#include <vector>
#include <iostream>

#include "common/FieldData.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "index/PrimaryIndex.h"
#include "storage/ChunkManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"

using namespace milvus;
using namespace milvus::index;
using namespace milvus::storage;

struct Segment {
    int64_t segment_id;
    std::vector<std::string> keys;

    Segment(int64_t id) : segment_id(id) {
    }

    void
    add_key(const std::string& key) {
        keys.push_back(key);
    }
};

TEST(BooPHF, Basic) {
    // Configuration
    const int num_segments = 1000;
    const int keys_per_segment = 1000;

    // Create segments
    std::vector<Segment> segments;
    segments.reserve(num_segments);

    for (int i = 0; i < num_segments; ++i) {
        segments.emplace_back(i + 1);  // Segment IDs start from 1

        // Generate unique keys for this segment
        for (int j = 0; j < keys_per_segment; ++j) {
            std::string key =
                "seg" + std::to_string(i + 1) + "_key" + std::to_string(j + 1);
            segments[i].add_key(key);
        }
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    primaryIndex::PrimaryIndex<> index(10.0, 8);
    index.build(segments);

    int64_t segment_id = index.lookup("seg1_key1");
    EXPECT_EQ(segment_id, 1);
    segment_id = index.lookup("seg250_key25");
    EXPECT_EQ(segment_id, 250);

    segment_id = index.lookup("seg9999_key1");
    EXPECT_EQ(segment_id, -1);
}

TEST(PrimaryIndexTest, BasicQuery) {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 1000;
    int64_t index_version = 1;
    int64_t field_id = 100;
    proto::schema::FieldSchema field_schema;
    auto field_meta = storage::FieldDataMeta{
        collection_id, partition_id, segment_id, field_id, field_schema};
    auto index_meta =
        storage::IndexMeta{segment_id, field_id, index_build_id, index_version};

    std::string root_path = "/tmp/test-primary-index/";

    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);

    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager);
    std::vector<std::string> index_files;

    std::vector<SegmentData> primary_keys;
    primary_keys.push_back({1, {"seg1_key1", "seg1_key2", "seg1_key3"}, {}});
    primary_keys.push_back({2, {"seg2_key20", "seg2_key21", "seg2_key22"}, {}});
    primary_keys.push_back({9999, {"seg9999_key9999", "seg9999_key9998"}, {}});

    auto index = std::make_shared<PrimaryIndex>(ctx, false);
    index->BuildWithPrimaryKeys(primary_keys);

    auto create_index_result = index->Upload();
    auto memSize = create_index_result->GetMemSize();
    auto serializedSize = create_index_result->GetSerializedSize();
    ASSERT_GT(memSize, 0);
    ASSERT_GT(serializedSize, 0);
    index_files = create_index_result->GetIndexFiles();

    Config config;
    config[milvus::index::INDEX_FILES] = index_files;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;

    index = std::make_unique<PrimaryIndex>(ctx, true);
    index->Load(config);

    auto result = index->query("seg2_key20");
    ASSERT_EQ(result, 2);
    result = index->query("seg9999_key9999");
    ASSERT_EQ(result, 9999);
    result = index->query("seg3_key1");
    ASSERT_EQ(result, -1);
}
