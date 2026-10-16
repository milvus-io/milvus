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
#include "knowhere/index/index_factory.h"
#include "knowhere/version.h"

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

knowhere::Index<knowhere::IndexNode>
CreateValidityIndex() {
    auto result = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_FAISS_IDMAP,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    if (!result.has_value()) {
        ThrowInfo(KnowhereStatusToErrorCode(result.error()),
                  "create validity helper fixture: {}",
                  result.what());
    }
    return std::move(result.value());
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
    for (int flags = 0; flags < 4; ++flags) {
        const auto mmap_root =
            MakeMmapRoot("semantic_mmap_" + std::to_string(flags));
        Config config;
        // Payload mmap must not implicitly enable nullable mapping mmap.
        config[index::ENABLE_MMAP] = true;
        config[index::ENABLE_MMAP_I2O_MAP] = (flags & 1) ? "true" : "false";
        config[index::ENABLE_MMAP_O2I_MAP] = (flags & 2) != 0;
        {
            auto native = CreateValidityIndex();
            const auto input =
                index::MakeNullableLoadInput({true, 5, bitmap.data()},
                                             index::GetIdMapMmapFlags(config),
                                             mmap_root.string());
            ASSERT_TRUE(input.validity.has_value());
            ASSERT_EQ(native.ImportValidity(*input.validity, input.mmap),
                      knowhere::Status::success);
            EXPECT_EQ(native.LogicalCount().value(), 5);
            EXPECT_EQ(native.ValidCount().value(), 3);
            EXPECT_FALSE(native.IsRowValid(1).value());
            std::vector<uint64_t> expected;
            if (flags & 1) {
                expected.push_back(3 * sizeof(int32_t));
            }
            if (flags & 2) {
                expected.push_back(5 * sizeof(int32_t));
            }
            ExpectMmapBlockFiles(mmap_root / index::ID_MAP_MMAP_DIR, expected);
        }
        EXPECT_TRUE(MmapBlockFiles(mmap_root / index::ID_MAP_MMAP_DIR).empty());
        std::filesystem::remove_all(mmap_root);
    }
}

TEST(IdMapValidDataHelpers, ValidDataHelpersPreserveSnapshotValues) {
    const std::vector<uint8_t> bitmap{0b00001101};
    for (const bool enable_mmap : {false, true}) {
        SCOPED_TRACE(enable_mmap);
        const auto mmap_root = MakeMmapRoot("bitmap_helper");
        knowhere::ValidityData snapshot;
        {
            auto native = CreateValidityIndex();
            ASSERT_EQ(native.ImportValidity(
                          {bitmap.data(), bitmap.size(), 5},
                          {enable_mmap, enable_mmap, mmap_root.string()}),
                      knowhere::Status::success);
            auto exported = native.ExportValidity();
            ASSERT_TRUE(exported.has_value());
            ASSERT_TRUE(exported.value().has_value());
            snapshot = std::move(*exported.value());
            EXPECT_EQ(native.ValidCount().value(), 3);
            if (enable_mmap) {
                ExpectMmapBlockFiles(
                    mmap_root, {3 * sizeof(int32_t), 5 * sizeof(int32_t)});
            }
        }
        // The exported value owns its bytes after the native index is gone.
        EXPECT_EQ(snapshot.logical_row_count, 5);
        EXPECT_EQ(snapshot.packed_bits, bitmap);
        EXPECT_TRUE(MmapBlockFiles(mmap_root).empty());
        std::filesystem::remove_all(mmap_root);
    }
}

TEST(IdMapValidDataHelpers, BinarySetRoundTripPreservesWireData) {
    const std::vector<uint8_t> bitmap{0x0d};
    BinarySet binary_set;
    {
        auto native = CreateValidityIndex();
        ASSERT_EQ(native.ImportValidity({bitmap.data(), bitmap.size(), 5}),
                  knowhere::Status::success);
        index::AppendValidDataToBinarySet(native, binary_set);
    }
    ASSERT_EQ(binary_set.GetByName(index::VALID_DATA_COUNT_KEY)->size,
              sizeof(uint64_t));
    ASSERT_EQ(binary_set.GetByName(index::VALID_DATA_KEY)->size, 1);
    EXPECT_EQ(binary_set.GetByName(index::VALID_DATA_KEY)->data[0], 0x0d);
    for (const bool enable_mmap : {false, true}) {
        SCOPED_TRACE(enable_mmap);
        const auto mmap_root = MakeMmapRoot("binary_set_helper");
        {
            auto native = CreateValidityIndex();
            auto input = index::MakeNullableLoadInput(
                index::LoadValidDataViewFromBinarySet(binary_set));
            input.mmap = {enable_mmap, enable_mmap, mmap_root.string()};
            ASSERT_TRUE(input.validity.has_value());
            ASSERT_EQ(native.ImportValidity(*input.validity, input.mmap),
                      knowhere::Status::success);
            EXPECT_EQ(native.LogicalCount().value(), 5);
            EXPECT_EQ(native.ValidCount().value(), 3);
            for (int64_t row = 0; row < 5; ++row) {
                EXPECT_EQ(native.IsRowValid(row).value(),
                          ((bitmap[0] >> row) & 1) != 0);
            }
            if (enable_mmap) {
                ExpectMmapBlockFiles(
                    mmap_root, {3 * sizeof(int32_t), 5 * sizeof(int32_t)});
            }
        }
        EXPECT_TRUE(MmapBlockFiles(mmap_root).empty());
        std::filesystem::remove_all(mmap_root);
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
