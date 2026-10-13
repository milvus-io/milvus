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
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <string>
#include <vector>

#include "common/GrowingOffsetMapping.h"
#include "common/OffsetMapping.h"
#include "common/SealedOffsetMapping.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "knowhere/id_map.h"

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
    if (!expected_p2l.empty()) {
        auto ids = mapping.GetPhysicalToLogicalIds(0, expected_p2l.size());
        ASSERT_FALSE(ids.empty());
        ASSERT_EQ(ids.count, expected_p2l.size());
        for (size_t i = 0; i < expected_p2l.size(); ++i) {
            EXPECT_EQ(ids.data[i], expected_p2l[i])
                << "physical offset in p2l view: " << i;
        }
    }
}

void
ExpectFilterValidLogicalOffsets(const SealedOffsetMapping& mapping) {
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

void
ExpectIdSnapshotValues(const knowhere::IdArray& ids,
                       const std::vector<int32_t>& expected) {
    ASSERT_EQ(ids.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(ids[i], expected[i]) << "physical offset: " << i;
    }
}

void
ExpectIdMapValues(const knowhere::IdMap& id_map,
                   const std::vector<int64_t>& expected_l2p,
                   const std::vector<int32_t>& expected_p2l) {
    EXPECT_EQ(id_map.type(), knowhere::IdMap::Type::SEALED);
    EXPECT_EQ(id_map.OutCount(), expected_l2p.size());
    EXPECT_EQ(id_map.InCount(), expected_p2l.size());
    // InToOutIds returns an owning value, including for mmap-backed arrays.
    const auto ids = id_map.InToOutIds();
    ExpectIdSnapshotValues(ids, expected_p2l);
    for (size_t i = 0; i < expected_l2p.size(); ++i) {
        EXPECT_EQ(id_map.MapOutToIn(i), expected_l2p[i])
            << "logical offset: " << i;
        EXPECT_EQ(id_map.IsValidOutId(i), expected_l2p[i] >= 0);
    }
    for (size_t i = 0; i < expected_p2l.size(); ++i) {
        EXPECT_EQ(id_map.MapInToOut(i), expected_p2l[i])
            << "physical offset: " << i;
    }
}
}  // namespace

// ---------- Default (disabled) state ----------

TEST(OffsetMapping, DefaultIsDisabledAndPassThrough) {
    NoOpOffsetMapping mapping;
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

    SealedOffsetMapping mapping;
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 5);
    ExpectMappingValues(mapping, expected_l2p, expected_p2l);
}

TEST(OffsetMapping, BuildSparseUsesContiguousStorage) {
    std::vector<uint8_t> valid(100, 0);
    valid[5] = 1;
    valid[50] = 1;
    std::vector<int64_t> expected_l2p(100, -1);
    expected_l2p[5] = 0;
    expected_l2p[50] = 1;
    const std::vector<int64_t> expected_p2l{5, 50};

    SealedOffsetMapping mapping;
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 100);
    ExpectMappingValues(mapping, expected_l2p, expected_p2l);
}

TEST(OffsetMapping, BuildAllValid) {
    std::vector<uint8_t> valid(4, 1);
    const std::vector<int64_t> expected{0, 1, 2, 3};

    SealedOffsetMapping mapping;
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 4);
    ExpectMappingValues(mapping, expected, expected);
}

TEST(OffsetMapping, BuildAllNull) {
    std::vector<uint8_t> valid(4, 0);
    const std::vector<int64_t> expected_l2p(4, -1);
    const std::vector<int64_t> expected_p2l;

    SealedOffsetMapping mapping;
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 4);
    ExpectMappingValues(mapping, expected_l2p, expected_p2l);
    EXPECT_EQ(mapping.GetLogicalOffset(0), -1);
}

TEST(OffsetMapping, TransformOperationsMatchBuildMode) {
    auto valid = ToBoolBytes(MakeValid({1, 0, 1, 1, 0}));

    SealedOffsetMapping mapping;
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 5);
    ExpectFilterValidLogicalOffsets(mapping);
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
}

TEST(OffsetMapping, BuildTwiceResetsState) {
    auto v1 = ToBoolBytes(MakeValid({1, 1, 0, 0, 1}));
    auto v2 = ToBoolBytes(MakeValid({1, 0, 0}));

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
}

TEST(IdMapValidDataHelpers, ConfigureMmapUsesOnlyIdMappingConfigKeys) {
    const std::vector<uint8_t> bitmap{0b00001101};
    Config config;
    config[index::ENABLE_MMAP] = true;

    {
        const auto mmap_root = MakeMmapRoot("mmap_disabled");
        knowhere::IdMap id_map;
        id_map.SetType(knowhere::IdMap::Type::SEALED);
        index::ConfigureIdMapMmap(id_map, config, mmap_root.string());
        id_map.AddFromData(
            knowhere::IdMapData::FromValidBitmap(bitmap.data(), 5));
        id_map.FinalizeVectorIds();
        EXPECT_TRUE(MmapBlockFiles(mmap_root / index::ID_MAP_MMAP_DIR).empty());
    }

    {
        const auto mmap_root = MakeMmapRoot("mmap_i2o");
        knowhere::IdMap id_map;
        Config i2o_config = config;
        i2o_config[index::ENABLE_MMAP_I2O_MAP] = true;
        i2o_config[index::ENABLE_MMAP_O2I_MAP] = false;
        index::ConfigureIdMapMmap(id_map, i2o_config, mmap_root.string());
        id_map.AddFromData(
            knowhere::IdMapData::FromValidBitmap(bitmap.data(), 5));
        id_map.FinalizeVectorIds();
        ExpectMmapBlockFiles(mmap_root / index::ID_MAP_MMAP_DIR,
                             {3 * sizeof(int32_t)});
    }

    {
        const auto mmap_root = MakeMmapRoot("mmap_o2i");
        knowhere::IdMap id_map;
        Config o2i_config = config;
        o2i_config[index::ENABLE_MMAP_I2O_MAP] = "false";
        o2i_config[index::ENABLE_MMAP_O2I_MAP] = "true";
        index::ConfigureIdMapMmap(id_map, o2i_config, mmap_root.string());
        id_map.AddFromData(
            knowhere::IdMapData::FromValidBitmap(bitmap.data(), 5));
        id_map.FinalizeVectorIds();
        ExpectMmapBlockFiles(mmap_root / index::ID_MAP_MMAP_DIR,
                             {5 * sizeof(int32_t)});
    }
}

TEST(IdMapValidDataHelpers, ValidDataHelpersPreserveSnapshotValues) {
    const std::vector<uint8_t> bitmap{0b00001101};
    const std::vector<int64_t> expected_out_to_in{0, -1, 1, 2, -1};
    const std::vector<int32_t> expected_in_to_out{0, 2, 3};

    for (const bool enable_mmap : {false, true}) {
        SCOPED_TRACE(enable_mmap);
        const auto mmap_root = MakeMmapRoot("bitmap_helper");
        knowhere::IdArray ids_snapshot;
        knowhere::BitmapArray valid_snapshot;
        {
            knowhere::IdMap id_map;
            id_map.SetType(knowhere::IdMap::Type::SEALED);
            if (enable_mmap) {
                id_map.ConfigureMmap(
                    knowhere::IdMapMmapOptions{true, true, mmap_root.string()});
            }
            id_map.AddFromData(
                knowhere::IdMapData::FromValidBitmap(bitmap.data(), 5));
            EXPECT_TRUE(id_map.InToOutIds().empty());
            id_map.FinalizeVectorIds();
            ExpectIdMapValues(
                id_map, expected_out_to_in, expected_in_to_out);
            ids_snapshot = id_map.InToOutIds();
            valid_snapshot = id_map.ValidBitmap();
            if (enable_mmap) {
                ExpectMmapBlockFiles(mmap_root,
                                     {3 * sizeof(int32_t), 5 * sizeof(int32_t)});
            }
        }
        // The value snapshots retain their buffers after the IdMap is gone.
        ExpectIdSnapshotValues(ids_snapshot, expected_in_to_out);
        ASSERT_EQ(valid_snapshot.size(), expected_out_to_in.size());
        for (size_t i = 0; i < expected_out_to_in.size(); ++i) {
            EXPECT_EQ((valid_snapshot[i / 8] >> (i % 8)) & 1,
                      expected_out_to_in[i] >= 0);
        }
    }
}

TEST(IdMapValidDataHelpers, LoadIdMapDataFromBinarySetRestoresIdMapData) {
    const std::array<bool, 5> valid{{true, false, true, true, false}};
    const std::vector<int64_t> expected_out_to_in{0, -1, 1, 2, -1};
    const std::vector<int32_t> expected_in_to_out{0, 2, 3};
    BinarySet binary_set;
    {
        knowhere::IdMap source_id_map;
        source_id_map.SetType(knowhere::IdMap::Type::SEALED);
        source_id_map.AddFromData(
            knowhere::IdMapData::FromValidData(valid.data(), valid.size()));
        index::AppendValidDataToBinarySet(source_id_map, binary_set);
    }

    for (const bool enable_mmap : {false, true}) {
        SCOPED_TRACE(enable_mmap);
        const auto mmap_root = MakeMmapRoot("binary_set_helper");
        knowhere::IdMap id_map;
        if (enable_mmap) {
            id_map.SetType(knowhere::IdMap::Type::SEALED);
            id_map.ConfigureMmap(
                knowhere::IdMapMmapOptions{true, true, mmap_root.string()});
        } else {
            EXPECT_EQ(id_map.type(), knowhere::IdMap::Type::DISABLED);
        }
        const auto restored =
            index::RestoreIdMapFromBinarySet(binary_set, id_map);
        ASSERT_TRUE(restored.has_valid_data);
        EXPECT_FALSE(restored.IsAllNullNullable());
        EXPECT_EQ(id_map.OutCount(), valid.size());
        EXPECT_TRUE(id_map.InToOutIds().empty());
        id_map.FinalizeVectorIds();
        ExpectIdMapValues(id_map, expected_out_to_in, expected_in_to_out);
        if (enable_mmap) {
            ExpectMmapBlockFiles(mmap_root,
                                 {3 * sizeof(int32_t), 5 * sizeof(int32_t)});
        }
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
}

}  // namespace milvus
