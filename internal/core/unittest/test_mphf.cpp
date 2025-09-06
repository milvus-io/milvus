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

template <typename T>
struct Segment {
    int64_t segment_id;
    std::vector<T> keys;

    Segment(int64_t id) : segment_id(id) {
    }

    void
    add_key(const T& key) {
        keys.push_back(key);
    }
};

template <typename T>
T
generate_key(int segment_id, int key_id);

template <>
std::string
generate_key<std::string>(int segment_id, int key_id) {
    return "seg" + std::to_string(segment_id) + "_key" + std::to_string(key_id);
}

template <>
int64_t
generate_key<int64_t>(int segment_id, int key_id) {
    return static_cast<int64_t>(segment_id * 10000 + key_id);
}

template <typename T>
void
test_primary_index_basic() {
    // Configuration
    const int num_segments = 100;
    const int keys_per_segment = 100;

    // Create segments
    std::vector<Segment<T>> segments;
    segments.reserve(num_segments);

    for (int i = 0; i < num_segments; ++i) {
        segments.emplace_back(i + 1);  // Segment IDs start from 1

        // Generate unique keys for this segment
        for (int j = 0; j < keys_per_segment; ++j) {
            T key = generate_key<T>(i + 1, j + 1);
            segments[i].add_key(key);
        }
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    primaryIndex::PrimaryIndex<T> index(10.0, 8);
    index.build(segments);

    int64_t segment_id = index.lookup(generate_key<T>(1, 1));
    EXPECT_EQ(segment_id, 1);
    segment_id = index.lookup(generate_key<T>(50, 25));
    EXPECT_EQ(segment_id, 50);

    segment_id = index.lookup(generate_key<T>(9999, 1));
    EXPECT_EQ(segment_id, -1);
}

TEST(BooPHF, StringBasic) {
    test_primary_index_basic<std::string>();
}

TEST(BooPHF, Int64Basic) {
    test_primary_index_basic<int64_t>();
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

    std::vector<milvus::index::SegmentData<std::string>> primary_keys;
    primary_keys.push_back(SegmentData<std::string>(1));
    primary_keys.back().add_key("seg1_key1");
    primary_keys.back().add_key("seg1_key2");
    primary_keys.back().add_key("seg1_key3");
    primary_keys.push_back(SegmentData<std::string>(2));
    primary_keys.back().add_key("seg2_key20");
    primary_keys.back().add_key("seg2_key21");
    primary_keys.back().add_key("seg2_key22");
    primary_keys.push_back(SegmentData<std::string>(9999));
    primary_keys.back().add_key("seg9999_key9999");
    primary_keys.back().add_key("seg9999_key9998");

    auto index =
        std::make_shared<milvus::index::PrimaryIndex<std::string>>(ctx, false);
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

    index =
        std::make_unique<milvus::index::PrimaryIndex<std::string>>(ctx, true);
    index->Load(config);

    auto result = index->query("seg2_key20");
    ASSERT_EQ(result, 2);
    result = index->query("seg9999_key9999");
    ASSERT_EQ(result, 9999);
    result = index->query("seg3_key1");
    ASSERT_EQ(result, -1);
}
