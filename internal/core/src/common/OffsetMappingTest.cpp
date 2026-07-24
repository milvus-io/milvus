// Copyright (C) 2019-2026 Zilliz. All rights reserved.
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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "common/OffsetMapping.h"
#include "index/VectorIndexValidDataUtils.h"
#include "storage/MmapChunkManager.h"

namespace milvus {

namespace {
std::vector<bool>
MakeValid(std::initializer_list<int> bits) {
    std::vector<bool> v;
    v.reserve(bits.size());
    for (int b : bits) {
        v.push_back(b != 0);
    }
    return v;
}

std::vector<uint8_t>
ToBoolBytes(const std::vector<bool>& valid) {
    std::vector<uint8_t> bytes(valid.size());
    for (size_t i = 0; i < valid.size(); ++i) {
        bytes[i] = valid[i] ? 1 : 0;
    }
    return bytes;
}

std::filesystem::path
MakeMmapRoot(const std::string& test_name) {
    return std::filesystem::temp_directory_path() /
           ("milvus_offset_mapping_" + test_name + "_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
}

storage::MmapChunkManagerPtr
MakeMmapChunkManager(const std::filesystem::path& mmap_root) {
    return std::make_shared<storage::MmapChunkManager>(
        mmap_root.string(), 64 * 1024 * 1024, 4 * 1024);
}

OffsetMappingBuildOptions
MmapOptions(const storage::MmapChunkManagerPtr& mmap_chunk_manager,
            bool enable_i2o = true,
            bool enable_o2i = true) {
    OffsetMappingBuildOptions options;
    options.enable_mmap_i2o_map = enable_i2o;
    options.enable_mmap_o2i_map = enable_o2i;
    options.mmap_chunk_manager = mmap_chunk_manager;
    return options;
}

std::vector<std::filesystem::path>
MmapBlockFiles(const std::filesystem::path& mmap_root) {
    std::vector<std::filesystem::path> files;
    if (!std::filesystem::exists(mmap_root)) {
        return files;
    }
    for (const auto& entry : std::filesystem::directory_iterator(mmap_root)) {
        if (entry.is_regular_file()) {
            files.emplace_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

void
ExpectMmapBlockFiles(const std::filesystem::path& mmap_root,
                     std::vector<uint64_t> minimum_bytes) {
    auto mmap_files = MmapBlockFiles(mmap_root);
    ASSERT_EQ(mmap_files.size(), minimum_bytes.size());

    std::sort(minimum_bytes.begin(), minimum_bytes.end());
    std::vector<uint64_t> file_sizes;
    file_sizes.reserve(mmap_files.size());
    for (const auto& mmap_file : mmap_files) {
        file_sizes.emplace_back(std::filesystem::file_size(mmap_file));
    }
    std::sort(file_sizes.begin(), file_sizes.end());

    for (size_t i = 0; i < file_sizes.size(); ++i) {
        EXPECT_GE(file_sizes[i], minimum_bytes[i]);
    }
}

void
ExpectMappingValues(const SealedOffsetMapping& mapping,
                    const std::vector<int64_t>& expected_l2p,
                    const std::vector<int64_t>& expected_p2l) {
    EXPECT_TRUE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetTotalCount(),
              static_cast<int64_t>(expected_l2p.size()));
    EXPECT_EQ(mapping.GetValidCount(),
              static_cast<int64_t>(expected_p2l.size()));

    for (size_t i = 0; i < expected_l2p.size(); ++i) {
        EXPECT_EQ(mapping.GetPhysicalOffset(i), expected_l2p[i])
            << "logical offset: " << i;
    }
    for (size_t i = 0; i < expected_p2l.size(); ++i) {
        EXPECT_EQ(mapping.GetLogicalOffset(i), expected_p2l[i])
            << "physical offset: " << i;
    }
}

void
ExpectStorageMode(const SealedOffsetMapping& mapping,
                  bool i2o_mmap,
                  bool o2i_mmap,
                  bool i2o_map,
                  bool o2i_map) {
    EXPECT_EQ(mapping.IsI2OMmap(), i2o_mmap);
    EXPECT_EQ(mapping.IsO2IMmap(), o2i_mmap);
    EXPECT_EQ(mapping.IsMmap(), i2o_mmap || o2i_mmap);
    EXPECT_EQ(mapping.IsI2OUsingMap(), i2o_map);
    EXPECT_EQ(mapping.IsO2IUsingMap(), o2i_map);
    EXPECT_EQ(mapping.IsUsingMap(), i2o_map || o2i_map);
}

void
ExpectTransformOperations(const SealedOffsetMapping& mapping) {
    TargetBitmap bitset(5, false);
    bitset.set(2);
    TargetBitmap transformed;
    EXPECT_EQ(mapping.TransformBitset(bitset, transformed),
              OffsetMapping::BitsetTransformStatus::Transformed);
    ASSERT_EQ(transformed.size(), 3);
    EXPECT_FALSE(transformed[0]);
    EXPECT_TRUE(transformed[1]);
    EXPECT_FALSE(transformed[2]);

    std::vector<int64_t> physical_offsets{0, 1, 2, -1};
    mapping.TransformOffsets(physical_offsets);
    EXPECT_EQ(physical_offsets, (std::vector<int64_t>{0, 2, 3, -1}));

    std::vector<int64_t> logical_offsets{0, 1, 3, 4};
    mapping.TransformLogicalOffsets(logical_offsets);
    EXPECT_EQ(logical_offsets, (std::vector<int64_t>{0, -1, 2, -1}));

    const int64_t input_offsets[] = {0, 1, 2, 4};
    bool input_valid_data[4] = {};
    std::vector<int64_t> filtered_offsets;
    mapping.FilterValidLogicalOffsets(
        input_offsets, 4, input_valid_data, filtered_offsets);
    EXPECT_TRUE(input_valid_data[0]);
    EXPECT_FALSE(input_valid_data[1]);
    EXPECT_TRUE(input_valid_data[2]);
    EXPECT_FALSE(input_valid_data[3]);
    EXPECT_EQ(filtered_offsets, (std::vector<int64_t>{0, 1}));
}

class TestVectorIndex : public index::VectorIndex {
 public:
    TestVectorIndex() : index::VectorIndex("TEST", knowhere::metric::L2) {
    }

    BinarySet
    Serialize(const Config& config) override {
        (void)config;
        return {};
    }

    void
    Load(const BinarySet& binary_set, const Config& config) override {
        (void)binary_set;
        (void)config;
    }

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config) override {
        (void)ctx;
        (void)config;
    }

    void
    BuildWithDataset(const DatasetPtr& dataset, const Config& config) override {
        (void)dataset;
        (void)config;
    }

    void
    Build(const Config& config) override {
        (void)config;
    }

    int64_t
    Count() override {
        return 0;
    }

    index::IndexStatsPtr
    Upload(const Config& config) override {
        (void)config;
        return nullptr;
    }

    void
    Query(const DatasetPtr dataset,
          const SearchInfo& search_info,
          const BitsetView& bitset,
          milvus::OpContext* op_context,
          SearchResult& search_result) const override {
        (void)dataset;
        (void)search_info;
        (void)bitset;
        (void)op_context;
        (void)search_result;
    }

    const bool
    HasRawData() const override {
        return false;
    }

    bool
    IsIndexRefineEnabled() const override {
        return false;
    }

    std::vector<uint8_t>
    GetVector(const DatasetPtr dataset) const override {
        (void)dataset;
        return {};
    }

    std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
    GetSparseVector(const DatasetPtr dataset) const override {
        (void)dataset;
        return nullptr;
    }
};
}  // namespace

// ---------- Default (disabled) state ----------

TEST(OffsetMapping, DefaultIsDisabledAndPassThrough) {
    OffsetMapping mapping;
    EXPECT_FALSE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetValidCount(), 0);
    EXPECT_EQ(mapping.GetTotalCount(), 0);
    // When disabled, offset queries must pass through unchanged.
    EXPECT_EQ(mapping.GetPhysicalOffset(42), 42);
    EXPECT_EQ(mapping.GetLogicalOffset(42), 42);
}

// ---------- Build (eager) ----------

TEST(OffsetMapping, BuildBasicVecMode) {
    auto valid = ToBoolBytes(MakeValid({1, 0, 1, 1, 0}));
    const std::vector<int64_t> expected_l2p{0, -1, 1, 2, -1};
    const std::vector<int64_t> expected_p2l{0, 2, 3};

    {
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()), 5);
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, false, false, false, false);
    }

    {
        const auto mmap_root = MakeMmapRoot("basic_both");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      5,
                      MmapOptions(mmap_chunk_manager));
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, true, true, false, false);
        ExpectMmapBlockFiles(mmap_root,
                             {3 * sizeof(int32_t), 5 * sizeof(int32_t)});
    }
}

TEST(OffsetMapping, BuildBasicVecModeMapsDirectionsIndependently) {
    auto valid = ToBoolBytes(MakeValid({1, 0, 1, 1, 0}));
    const std::vector<int64_t> expected_l2p{0, -1, 1, 2, -1};
    const std::vector<int64_t> expected_p2l{0, 2, 3};

    {
        const auto mmap_root = MakeMmapRoot("basic_i2o");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      5,
                      MmapOptions(mmap_chunk_manager, true, false));
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, true, false, false, false);
        ExpectMmapBlockFiles(mmap_root, {3 * sizeof(int32_t)});
    }

    {
        const auto mmap_root = MakeMmapRoot("basic_o2i");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      5,
                      MmapOptions(mmap_chunk_manager, false, true));
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, false, true, false, false);
        ExpectMmapBlockFiles(mmap_root, {5 * sizeof(int32_t)});
    }
}

TEST(OffsetMapping, BuildMapModeOnSparse) {
    std::vector<uint8_t> valid(100, 0);
    valid[5] = 1;
    valid[50] = 1;
    std::vector<int64_t> expected_l2p(100, -1);
    expected_l2p[5] = 0;
    expected_l2p[50] = 1;
    const std::vector<int64_t> expected_p2l{5, 50};

    {
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()), 100);
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, false, false, true, true);
    }

    {
        const auto mmap_root = MakeMmapRoot("sparse_both");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      100,
                      MmapOptions(mmap_chunk_manager));
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, true, true, false, false);
        ExpectMmapBlockFiles(mmap_root,
                             {2 * sizeof(int32_t), 100 * sizeof(int32_t)});
    }

    {
        const auto mmap_root = MakeMmapRoot("sparse_i2o");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      100,
                      MmapOptions(mmap_chunk_manager, true, false));
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, true, false, false, true);
        ExpectMmapBlockFiles(mmap_root, {2 * sizeof(int32_t)});
    }

    {
        const auto mmap_root = MakeMmapRoot("sparse_o2i");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      100,
                      MmapOptions(mmap_chunk_manager, false, true));
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, false, true, true, false);
        ExpectMmapBlockFiles(mmap_root, {100 * sizeof(int32_t)});
    }
}

TEST(OffsetMapping, BuildAllValid) {
    std::vector<uint8_t> valid(4, 1);
    const std::vector<int64_t> expected{0, 1, 2, 3};

    {
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()), 4);
        ExpectMappingValues(mapping, expected, expected);
        ExpectStorageMode(mapping, false, false, false, false);
    }

    {
        const auto mmap_root = MakeMmapRoot("all_valid");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      4,
                      MmapOptions(mmap_chunk_manager));
        ExpectMappingValues(mapping, expected, expected);
        ExpectStorageMode(mapping, true, true, false, false);
        ExpectMmapBlockFiles(mmap_root,
                             {4 * sizeof(int32_t), 4 * sizeof(int32_t)});
    }
}

TEST(OffsetMapping, BuildAllNull) {
    std::vector<uint8_t> valid(4, 0);
    const std::vector<int64_t> expected_l2p(4, -1);
    const std::vector<int64_t> expected_p2l;

    {
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()), 4);
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, false, false, true, true);
        EXPECT_EQ(mapping.GetLogicalOffset(0), -1);
    }

    {
        const auto mmap_root = MakeMmapRoot("all_null");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      4,
                      MmapOptions(mmap_chunk_manager));
        ExpectMappingValues(mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(mapping, false, true, false, false);
        EXPECT_EQ(mapping.GetLogicalOffset(0), -1);
        ExpectMmapBlockFiles(mmap_root, {4 * sizeof(int32_t)});
    }
}

TEST(OffsetMapping, TransformOperationsMatchBuildMode) {
    auto valid = ToBoolBytes(MakeValid({1, 0, 1, 1, 0}));

    {
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()), 5);
        ExpectTransformOperations(mapping);
    }

    {
        const auto mmap_root = MakeMmapRoot("transform");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      5,
                      MmapOptions(mmap_chunk_manager));
        ExpectTransformOperations(mapping);
    }
}

TEST(OffsetMapping, BuildNoopOnNullOrZero) {
    {
        SealedOffsetMapping mapping;
        mapping.Build(nullptr, 100);
        EXPECT_FALSE(mapping.IsEnabled());

        std::vector<uint8_t> valid(1, 1);
        mapping.Build(reinterpret_cast<const bool*>(valid.data()), 0);
        EXPECT_FALSE(mapping.IsEnabled());
    }

    {
        const auto mmap_root = MakeMmapRoot("noop");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        auto valid = ToBoolBytes(MakeValid({1, 0, 1, 1, 0}));

        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      5,
                      MmapOptions(mmap_chunk_manager));
        EXPECT_TRUE(mapping.IsEnabled());
        EXPECT_TRUE(mapping.IsMmap());

        mapping.Build(nullptr, 100, MmapOptions(mmap_chunk_manager));
        EXPECT_FALSE(mapping.IsEnabled());
        EXPECT_FALSE(mapping.IsMmap());
        EXPECT_FALSE(mapping.IsUsingMap());
        EXPECT_EQ(mapping.GetPhysicalOffset(3), 3);

        mapping.Build(reinterpret_cast<const bool*>(valid.data()),
                      0,
                      MmapOptions(mmap_chunk_manager));
        EXPECT_FALSE(mapping.IsEnabled());
        EXPECT_FALSE(mapping.IsMmap());
        EXPECT_FALSE(mapping.IsUsingMap());
    }
}

TEST(OffsetMapping, BuildTwiceResetsState) {
    auto v1 = ToBoolBytes(MakeValid({1, 1, 0, 0, 1}));
    auto v2 = ToBoolBytes(MakeValid({1, 0, 0}));

    {
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(v1.data()), 5);
        EXPECT_EQ(mapping.GetValidCount(), 3);
        EXPECT_EQ(mapping.GetTotalCount(), 5);

        mapping.Build(reinterpret_cast<const bool*>(v2.data()), 3);
        EXPECT_EQ(mapping.GetValidCount(), 1);
        EXPECT_EQ(mapping.GetTotalCount(), 3);
        EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
        EXPECT_EQ(mapping.GetPhysicalOffset(1), -1);
        EXPECT_EQ(mapping.GetPhysicalOffset(2), -1);
        EXPECT_FALSE(mapping.IsMmap());
    }

    {
        const auto mmap_root = MakeMmapRoot("build_twice");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(v1.data()),
                      5,
                      MmapOptions(mmap_chunk_manager));
        EXPECT_TRUE(mapping.IsMmap());

        mapping.Build(reinterpret_cast<const bool*>(v2.data()), 3);
        EXPECT_EQ(mapping.GetValidCount(), 1);
        EXPECT_EQ(mapping.GetTotalCount(), 3);
        EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
        EXPECT_EQ(mapping.GetPhysicalOffset(1), -1);
        EXPECT_EQ(mapping.GetPhysicalOffset(2), -1);
        EXPECT_FALSE(mapping.IsMmap());
    }
}

TEST(OffsetMapping, MmapOptionsUseOnlyIdMappingConfigKeys) {
    Config config;
    config[index::ENABLE_MMAP] = true;

    auto options = index::GetOffsetMappingMmapOptions(config);
    EXPECT_FALSE(options.enable_mmap_i2o_map);
    EXPECT_FALSE(options.enable_mmap_o2i_map);

    config[index::ENABLE_MMAP_I2O_MAP] = true;
    config[index::ENABLE_MMAP_O2I_MAP] = false;

    options = index::GetOffsetMappingMmapOptions(config);
    EXPECT_TRUE(options.enable_mmap_i2o_map);
    EXPECT_FALSE(options.enable_mmap_o2i_map);

    config[index::ENABLE_MMAP_I2O_MAP] = "false";
    config[index::ENABLE_MMAP_O2I_MAP] = "true";

    options = index::GetOffsetMappingMmapOptions(config);
    EXPECT_FALSE(options.enable_mmap_i2o_map);
    EXPECT_TRUE(options.enable_mmap_o2i_map);
}

TEST(OffsetMapping, OffsetMappingMmapDirUsesDedicatedIndexSubdirectory) {
    const auto local_index_prefix = (std::filesystem::temp_directory_path() /
                                     "milvus_local_chunk/index_files/1_2_3_4")
                                        .string();
    const auto mmap_dir = index::GetOffsetMappingMmapDir(local_index_prefix);

    EXPECT_EQ(mmap_dir,
              (std::filesystem::path(local_index_prefix) /
               index::OFFSET_MAPPING_MMAP_DIR)
                  .string());
    EXPECT_EQ(std::filesystem::path(mmap_dir).parent_path(),
              std::filesystem::path(local_index_prefix));
}

TEST(OffsetMapping, NeedOffsetMappingMmapMatchesDirectionSizes) {
    OffsetMappingBuildOptions options;
    EXPECT_FALSE(index::NeedOffsetMappingMmap(options, 10, 5));

    options.enable_mmap_i2o_map = true;
    EXPECT_TRUE(index::NeedOffsetMappingMmap(options, 10, 5));
    EXPECT_FALSE(index::NeedOffsetMappingMmap(options, 10, 0));

    options.enable_mmap_i2o_map = false;
    options.enable_mmap_o2i_map = true;
    EXPECT_TRUE(index::NeedOffsetMappingMmap(options, 10, 0));
    EXPECT_FALSE(index::NeedOffsetMappingMmap(options, 0, 0));
}

TEST(OffsetMapping, ValidDataHelpersPreserveMappingValuesWithMmapOptions) {
    const std::vector<uint8_t> bitmap{0b00001101};
    const std::vector<int64_t> expected_l2p{0, -1, 1, 2, -1};
    const std::vector<int64_t> expected_p2l{0, 2, 3};

    {
        TestVectorIndex vector_index;
        index::BuildValidDataFromBitmap(&vector_index, 5, bitmap.data());
        const auto* mapping = dynamic_cast<const SealedOffsetMapping*>(
            &vector_index.GetOffsetMapping());
        ASSERT_NE(mapping, nullptr);
        ExpectMappingValues(*mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(*mapping, false, false, false, false);
    }

    {
        const auto mmap_root = MakeMmapRoot("bitmap_helper");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        TestVectorIndex vector_index;
        index::BuildValidDataFromBitmap(
            &vector_index, 5, bitmap.data(), MmapOptions(mmap_chunk_manager));
        const auto* mapping = dynamic_cast<const SealedOffsetMapping*>(
            &vector_index.GetOffsetMapping());
        ASSERT_NE(mapping, nullptr);
        ExpectMappingValues(*mapping, expected_l2p, expected_p2l);
        ExpectStorageMode(*mapping, true, true, false, false);
    }
}

TEST(OffsetMapping, LoadValidDataFromBinarySetUsesProvidedMmapOptions) {
    SealedOffsetMapping source_mapping;
    auto valid = ToBoolBytes(MakeValid({1, 0, 1, 1, 0}));
    source_mapping.Build(reinterpret_cast<const bool*>(valid.data()), 5);

    BinarySet binary_set;
    index::AppendValidDataToBinarySet(source_mapping, binary_set);

    {
        TestVectorIndex vector_index;
        ASSERT_TRUE(
            index::LoadValidDataFromBinarySet(binary_set, &vector_index));
        const auto* mapping = dynamic_cast<const SealedOffsetMapping*>(
            &vector_index.GetOffsetMapping());
        ASSERT_NE(mapping, nullptr);
        EXPECT_FALSE(mapping->IsMmap());
        EXPECT_EQ(mapping->GetPhysicalOffset(3), 2);
        EXPECT_EQ(mapping->GetLogicalOffset(2), 3);
    }

    {
        const auto mmap_root = MakeMmapRoot("binary_set_helper");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        TestVectorIndex vector_index;
        ASSERT_TRUE(index::LoadValidDataFromBinarySet(
            binary_set, &vector_index, MmapOptions(mmap_chunk_manager)));
        const auto* mapping = dynamic_cast<const SealedOffsetMapping*>(
            &vector_index.GetOffsetMapping());
        ASSERT_NE(mapping, nullptr);
        EXPECT_TRUE(mapping->IsMmap());
        EXPECT_EQ(mapping->GetPhysicalOffset(3), 2);
        EXPECT_EQ(mapping->GetLogicalOffset(2), 3);
    }
}

// ---------- Append ----------

TEST(OffsetMapping, AppendBasic) {
    GrowingOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1, 1}));
    mapping.Append(reinterpret_cast<const bool*>(v.data()), 4, 0, 0);

    EXPECT_TRUE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetValidCount(), 3);
    EXPECT_EQ(mapping.GetTotalCount(), 4);
    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(2), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(3), 2);
}

TEST(OffsetMapping, AppendMultipleBatches) {
    GrowingOffsetMapping mapping;
    auto b1 = ToBoolBytes(MakeValid({1, 0, 1}));
    mapping.Append(reinterpret_cast<const bool*>(b1.data()), 3, 0, 0);
    EXPECT_EQ(mapping.GetValidCount(), 2);
    EXPECT_EQ(mapping.GetTotalCount(), 3);

    auto b2 = ToBoolBytes(MakeValid({0, 1, 1}));
    mapping.Append(reinterpret_cast<const bool*>(b2.data()),
                   3,
                   mapping.GetTotalCount(),
                   mapping.GetValidCount());
    EXPECT_EQ(mapping.GetValidCount(), 4);
    EXPECT_EQ(mapping.GetTotalCount(), 6);

    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(2), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(4), 2);
    EXPECT_EQ(mapping.GetPhysicalOffset(5), 3);
    EXPECT_EQ(mapping.GetLogicalOffset(3), 5);
}

TEST(OffsetMapping, AppendNoopOnNullOrZero) {
    GrowingOffsetMapping mapping;
    mapping.Append(nullptr, 3, 0, 0);
    EXPECT_FALSE(mapping.IsEnabled());
    std::vector<uint8_t> v(1, 1);
    mapping.Append(reinterpret_cast<const bool*>(v.data()), 0, 0, 0);
    EXPECT_FALSE(mapping.IsEnabled());
}

// ---------- IsValid ----------

TEST(OffsetMapping, IsValidMatchesPhysicalOffsetSign) {
    auto v = ToBoolBytes(MakeValid({1, 0, 1, 0}));

    {
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(v.data()), 4);
        EXPECT_TRUE(mapping.IsValid(0));
        EXPECT_FALSE(mapping.IsValid(1));
        EXPECT_TRUE(mapping.IsValid(2));
        EXPECT_FALSE(mapping.IsValid(3));
    }

    {
        const auto mmap_root = MakeMmapRoot("is_valid");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(v.data()),
                      4,
                      MmapOptions(mmap_chunk_manager));
        EXPECT_TRUE(mapping.IsValid(0));
        EXPECT_FALSE(mapping.IsValid(1));
        EXPECT_TRUE(mapping.IsValid(2));
        EXPECT_FALSE(mapping.IsValid(3));
    }
}

// ---------- Out-of-bounds queries ----------

TEST(OffsetMapping, OutOfBoundsReturnsMinusOne) {
    auto v = ToBoolBytes(MakeValid({1, 0, 1}));

    {
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(v.data()), 3);
        EXPECT_EQ(mapping.GetPhysicalOffset(99), -1);
        EXPECT_EQ(mapping.GetLogicalOffset(99), -1);
    }

    {
        const auto mmap_root = MakeMmapRoot("out_of_bounds");
        auto mmap_chunk_manager = MakeMmapChunkManager(mmap_root);
        SealedOffsetMapping mapping;
        mapping.Build(reinterpret_cast<const bool*>(v.data()),
                      3,
                      MmapOptions(mmap_chunk_manager));
        EXPECT_EQ(mapping.GetPhysicalOffset(99), -1);
        EXPECT_EQ(mapping.GetLogicalOffset(99), -1);
    }
}

}  // namespace milvus
