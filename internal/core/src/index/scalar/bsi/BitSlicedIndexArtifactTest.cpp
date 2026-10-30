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
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <arrow/io/memory.h>

#include "common/EasyAssert.h"
#include "common/ValidityView.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/scalar/bsi/BitSlicedIndex.h"
#include "index/scalar/bsi/BitSlicedIndexArtifact.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/TestArtifactIO.h"
#include "storage/IndexEntryReader.h"
#include "storage/RemoteInputStream.h"

namespace milvus::index {
namespace {

using test::ExpectSegcoreError;
using test::TestArtifactData;
using test::TestArtifactSource;

constexpr int32_t kMin = std::numeric_limits<int32_t>::min();
constexpr int32_t kMax = std::numeric_limits<int32_t>::max();

using Rows = std::vector<std::optional<int32_t>>;

// ---------------------------------------------------------------------------
// Building, serializing and loading
// ---------------------------------------------------------------------------

BitSlicedIndexInt32
Build(const Rows& rows) {
    std::vector<int32_t> values(rows.size());
    auto validity = std::make_unique<bool[]>(rows.size());
    bool has_null = false;
    for (size_t i = 0; i < rows.size(); ++i) {
        // NULL rows carry a garbage value that must not be persisted.
        values[i] = rows[i].value_or(static_cast<int32_t>(0x5a5a5a5a));
        validity[i] = rows[i].has_value();
        has_null |= !rows[i].has_value();
    }
    const ScalarBuildBatch<int32_t> batch{
        std::span<const int32_t>(values),
        has_null ? ValidityView::FromExpanded(validity.get()) : ValidityView()};
    return BitSlicedIndexInt32(ScalarBuildInput<int32_t>{
        std::span<const ScalarBuildBatch<int32_t>>(&batch, 1)});
}

TestArtifactData
SerializeLegacy(const Rows& rows) {
    return test::SerializeV1V2(BitSlicedIndexArtifactInt32(Build(rows)));
}

BitSlicedIndexInt32
LoadLegacy(const TestArtifactData& data) {
    TestArtifactSource source(data, storage::Generation::V1V2);
    return LoadBitSlicedIndex<int32_t>(source);
}

// Through the real packed V3 envelope (directory, CRCs), not the test double.
BitSlicedIndexInt32
LoadPacked(const TestArtifactData& data) {
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            test::MakePackedArtifactBuffer(data)));
    const auto reader = storage::IndexEntryReader::Open(input, input->Size());
    return LoadBitSlicedIndex<int32_t>(*reader);
}

bsi_format::Header
HeaderOf(const TestArtifactData& data) {
    const auto& bytes = data.entries.at(std::string(bsi_format::kHeaderEntry));
    EXPECT_EQ(bytes.size(), sizeof(bsi_format::Header));
    bsi_format::Header header{};
    std::memcpy(&header, bytes.data(), sizeof(header));
    return header;
}

void
SetHeader(TestArtifactData& data, const bsi_format::Header& header) {
    auto& bytes = data.entries[std::string(bsi_format::kHeaderEntry)];
    bytes.resize(sizeof(header));
    std::memcpy(bytes.data(), &header, sizeof(header));
}

// Sets bit `row` of the `word_index`-th word of an entry.
void
SetBit(TestArtifactData& data,
       std::string_view entry,
       size_t word_index,
       size_t bit) {
    auto& bytes = data.entries.at(std::string(entry));
    uint64_t word = 0;
    std::memcpy(&word, bytes.data() + word_index * sizeof(word), sizeof(word));
    word |= uint64_t{1} << bit;
    std::memcpy(bytes.data() + word_index * sizeof(word), &word, sizeof(word));
}

// ---------------------------------------------------------------------------
// Query equivalence
// ---------------------------------------------------------------------------

::testing::AssertionResult
SameBits(const TargetBitmap& actual, const TargetBitmap& expected) {
    if (actual.size() != expected.size()) {
        return ::testing::AssertionFailure()
               << "bitmap size " << actual.size() << " != " << expected.size();
    }
    for (size_t i = 0; i < expected.size(); ++i) {
        if (actual[i] != expected[i]) {
            return ::testing::AssertionFailure()
                   << "first mismatch at row " << i;
        }
    }
    if (actual.count() != expected.count()) {
        return ::testing::AssertionFailure()
               << "count() " << actual.count() << " != " << expected.count();
    }
    return ::testing::AssertionSuccess();
}

// Constants that probe every boundary of the index: its bounds and their
// neighbours, the type extremes, zero, and a few stored values.
std::vector<int32_t>
Constants(const Rows& rows) {
    std::vector<int32_t> constants{kMin, -1, 0, 1, kMax};
    std::vector<int32_t> values;
    for (const auto& row : rows) {
        if (row) {
            values.push_back(*row);
        }
    }
    if (!values.empty()) {
        const auto [lo, hi] = std::minmax_element(values.begin(), values.end());
        for (const int64_t c : {int64_t{*lo} - 1,
                                int64_t{*lo},
                                int64_t{*lo} + 1,
                                int64_t{*hi} - 1,
                                int64_t{*hi},
                                int64_t{*hi} + 1,
                                int64_t{values[values.size() / 2]}}) {
            if (c >= kMin && c <= kMax) {
                constants.push_back(static_cast<int32_t>(c));
            }
        }
    }
    std::sort(constants.begin(), constants.end());
    constants.erase(std::unique(constants.begin(), constants.end()),
                    constants.end());
    return constants;
}

constexpr CompareOp kAllOps[] = {CompareOp::Equal,
                                 CompareOp::NotEqual,
                                 CompareOp::GreaterThan,
                                 CompareOp::GreaterEqual,
                                 CompareOp::LessThan,
                                 CompareOp::LessEqual};

// The loaded index must report the same shape and answer every query like
// the original.
void
ExpectSameIndex(const BitSlicedIndexInt32& loaded,
                const BitSlicedIndexInt32& original,
                const Rows& rows) {
    EXPECT_EQ(loaded.Count(), original.Count());
    EXPECT_EQ(loaded.ValidCount(), original.ValidCount());
    EXPECT_EQ(loaded.SliceCount(), original.SliceCount());
    EXPECT_EQ(loaded.Min(), original.Min());
    EXPECT_EQ(loaded.Max(), original.Max());
    EXPECT_EQ(loaded.MemoryUsage(), original.MemoryUsage());
    EXPECT_TRUE(SameBits(loaded.IsNull(), original.IsNull()));
    EXPECT_TRUE(SameBits(loaded.IsNotNull(), original.IsNotNull()));

    const auto constants = Constants(rows);
    for (const int32_t c : constants) {
        for (const auto op : kAllOps) {
            EXPECT_TRUE(SameBits(loaded.Range(c, op), original.Range(c, op)))
                << "op " << static_cast<int>(op) << " constant " << c;
        }
    }
    for (const int32_t lo : constants) {
        for (const int32_t hi : constants) {
            EXPECT_TRUE(SameBits(loaded.Range(lo, true, hi, false),
                                 original.Range(lo, true, hi, false)))
                << "[" << lo << ", " << hi << ")";
        }
    }
    EXPECT_TRUE(SameBits(loaded.In(constants.size(), constants.data()),
                         original.In(constants.size(), constants.data())));
    EXPECT_TRUE(SameBits(loaded.NotIn(constants.size(), constants.data()),
                         original.NotIn(constants.size(), constants.data())));
}

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

struct Dataset {
    std::string name;
    Rows rows;
};

Rows
RandomRows(
    size_t count, int32_t lo, int32_t hi, double null_rate, uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> value(lo, hi);
    std::bernoulli_distribution is_null(null_rate);
    Rows rows(count);
    for (auto& row : rows) {
        if (!is_null(rng)) {
            row = value(rng);
        }
    }
    return rows;
}

std::vector<Dataset>
Datasets() {
    Rows narrow;
    for (int32_t v = -50; v <= 50; ++v) {
        narrow.emplace_back(v);
        narrow.emplace_back(std::nullopt);
    }
    return {
        {"empty", {}},
        {"all_null", {std::nullopt, std::nullopt, std::nullopt}},
        {"single_value", {7, std::nullopt, 7, 7}},
        {"extremes", {kMin, kMax, 0, std::nullopt, -1, 1, kMin + 1, kMax - 1}},
        {"narrow_with_nulls", std::move(narrow)},
        {"exactly_one_word", RandomRows(64, 0, 1000, 0.0, 1)},
        {"one_word_and_a_bit", RandomRows(65, -1000, 1000, 0.2, 2)},
        {"random_narrow", RandomRows(1000, -5000, 5000, 0.1, 3)},
        {"random_full_range", RandomRows(300, kMin, kMax, 0.1, 4)},
    };
}

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

TEST(BitSlicedIndexArtifactTest, LegacyRoundTripPreservesResults) {
    for (const auto& dataset : Datasets()) {
        SCOPED_TRACE(dataset.name);
        const auto original = Build(dataset.rows);
        const auto loaded = LoadLegacy(SerializeLegacy(dataset.rows));
        ExpectSameIndex(loaded, original, dataset.rows);
    }
}

TEST(BitSlicedIndexArtifactTest, PackedRoundTripPreservesResults) {
    for (const auto& dataset : Datasets()) {
        SCOPED_TRACE(dataset.name);
        const auto original = Build(dataset.rows);
        const auto data =
            test::SerializeV3(BitSlicedIndexArtifactInt32(Build(dataset.rows)));
        const auto loaded = LoadPacked(data);
        ExpectSameIndex(loaded, original, dataset.rows);
    }
}

TEST(BitSlicedIndexArtifactTest, ArtifactKeepsTheIndexQueryable) {
    const Rows rows{3, std::nullopt, 5};
    const BitSlicedIndexArtifactInt32 artifact(Build(rows));
    EXPECT_EQ(artifact.Index().Count(), rows.size());
    EXPECT_TRUE(SameBits(artifact.Index().Range(5, CompareOp::Equal),
                         Build(rows).Range(5, CompareOp::Equal)));
}

// The layout is a contract with every artifact already written; pin it.
TEST(BitSlicedIndexArtifactTest, LayoutIsStable) {
    // 65 rows: two words, the second with one row. Values 0..64 need 7 slices.
    Rows rows;
    for (int32_t v = 0; v < 65; ++v) {
        rows.emplace_back(v);
    }
    rows[1] = std::nullopt;
    const auto data = SerializeLegacy(rows);

    ASSERT_EQ(data.entries.size(), 2U + 7U);
    const auto header = HeaderOf(data);
    EXPECT_EQ(header.version, 1U);
    EXPECT_EQ(header.value_kind, bsi_format::kSignedInteger);
    EXPECT_EQ(header.value_bits, 32U);
    EXPECT_EQ(header.slice_count, 7U);
    EXPECT_EQ(header.num_rows, 65U);
    EXPECT_EQ(header.min, 0);
    EXPECT_EQ(header.max, 64);
    // The version is the first field, so other versions stay recognizable.
    uint32_t first_word = 0;
    std::memcpy(&first_word,
                data.entries.at(std::string(bsi_format::kHeaderEntry)).data(),
                sizeof(first_word));
    EXPECT_EQ(first_word, bsi_format::kVersion);

    const auto& existence =
        data.entries.at(std::string(bsi_format::kExistenceEntry));
    ASSERT_EQ(existence.size(), 2 * sizeof(uint64_t));
    uint64_t words[2];
    std::memcpy(words, existence.data(), sizeof(words));
    EXPECT_EQ(words[0], ~uint64_t{2});  // every row but row 1
    EXPECT_EQ(words[1], uint64_t{1});   // row 64

    EXPECT_EQ(bsi_format::SliceEntryName(0), "bsi_slice_00");
    EXPECT_EQ(bsi_format::SliceEntryName(31), "bsi_slice_31");
    // Slice 6 (bit 6 of the offset) is set only for row 64 = 0b1000000.
    const auto& slice6 = data.entries.at(bsi_format::SliceEntryName(6));
    ASSERT_EQ(slice6.size(), 2 * sizeof(uint64_t));
    std::memcpy(words, slice6.data(), sizeof(words));
    EXPECT_EQ(words[0], uint64_t{0});
    EXPECT_EQ(words[1], uint64_t{1});
    // Slice 0 holds the odd rows, except NULL row 1.
    const auto& slice0 = data.entries.at(bsi_format::SliceEntryName(0));
    ASSERT_EQ(slice0.size(), 2 * sizeof(uint64_t));
    std::memcpy(words, slice0.data(), sizeof(words));
    EXPECT_EQ(words[0], 0xAAAAAAAAAAAAAAAAULL & ~uint64_t{2});
    EXPECT_EQ(words[1], uint64_t{0});
}

// ---------------------------------------------------------------------------
// Rejected artifacts
// ---------------------------------------------------------------------------

const Rows&
SampleRows() {
    static const Rows rows{0, 5, std::nullopt, 3, 1};  // span 5, 3 slices
    return rows;
}

// Every entry of the sample artifact, by name.
std::vector<std::string>
SampleEntries() {
    return {std::string(bsi_format::kHeaderEntry),
            std::string(bsi_format::kExistenceEntry),
            bsi_format::SliceEntryName(0),
            bsi_format::SliceEntryName(1),
            bsi_format::SliceEntryName(2)};
}

TEST(BitSlicedIndexArtifactTest, MissingEntryIsRejected) {
    for (const auto& entry : SampleEntries()) {
        SCOPED_TRACE(entry);
        auto data = SerializeLegacy(SampleRows());
        data.entries.erase(entry);
        ExpectSegcoreError(ErrorCode::DataFormatBroken,
                           [&] { (void)LoadLegacy(data); });
    }
}

TEST(BitSlicedIndexArtifactTest, TruncatedOrInflatedEntryIsRejected) {
    for (const auto& entry : SampleEntries()) {
        SCOPED_TRACE(entry);
        auto truncated = SerializeLegacy(SampleRows());
        truncated.entries.at(entry).pop_back();
        ExpectSegcoreError(ErrorCode::DataFormatBroken,
                           [&] { (void)LoadLegacy(truncated); });

        auto inflated = SerializeLegacy(SampleRows());
        inflated.entries.at(entry).push_back(0);
        ExpectSegcoreError(ErrorCode::DataFormatBroken,
                           [&] { (void)LoadLegacy(inflated); });
    }
}

TEST(BitSlicedIndexArtifactTest, UnsupportedVersionIsRejected) {
    auto data = SerializeLegacy(SampleRows());
    auto header = HeaderOf(data);
    header.version = bsi_format::kVersion + 1;
    SetHeader(data, header);
    ExpectSegcoreError(ErrorCode::Unsupported, [&] { (void)LoadLegacy(data); });

    // A later version may have a header of another size; it must still be
    // reported as another version, not as a corrupt header.
    auto& bytes = data.entries.at(std::string(bsi_format::kHeaderEntry));
    bytes.resize(bytes.size() + 8, 0);
    ExpectSegcoreError(ErrorCode::Unsupported, [&] { (void)LoadLegacy(data); });

    // Too short even for a version is corruption.
    bytes.resize(sizeof(uint32_t) - 1);
    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { (void)LoadLegacy(data); });
}

TEST(BitSlicedIndexArtifactTest, OtherValueKindOrWidthIsRejected) {
    auto width = SerializeLegacy(SampleRows());
    auto header = HeaderOf(width);
    header.value_bits = 64;
    SetHeader(width, header);
    ExpectSegcoreError(ErrorCode::Unsupported,
                       [&] { (void)LoadLegacy(width); });

    auto kind = SerializeLegacy(SampleRows());
    header = HeaderOf(kind);
    header.value_kind = bsi_format::kSignedInteger + 1;
    SetHeader(kind, header);
    ExpectSegcoreError(ErrorCode::Unsupported, [&] { (void)LoadLegacy(kind); });
}

TEST(BitSlicedIndexArtifactTest, InconsistentHeaderIsRejected) {
    const auto reject = [](auto mutate) {
        auto data = SerializeLegacy(SampleRows());
        auto header = HeaderOf(data);
        mutate(data, header);
        SetHeader(data, header);
        ExpectSegcoreError(ErrorCode::DataFormatBroken,
                           [&] { (void)LoadLegacy(data); });
    };
    reject([](TestArtifactData&, bsi_format::Header& h) {
        h.slice_count = BitSlicedIndexInt32::kMaxSlices + 1;
    });
    reject([](TestArtifactData&, bsi_format::Header& h) {
        h.min = int64_t{kMin} - 1;
    });
    reject([](TestArtifactData&, bsi_format::Header& h) {
        h.max = int64_t{kMax} + 1;
    });
    reject([](TestArtifactData&, bsi_format::Header& h) {
        std::swap(h.min, h.max);
    });
    // One slice too many for the value range, with a payload to match, so
    // that the slice count check rather than the size check rejects it.
    reject([](TestArtifactData& data, bsi_format::Header& h) {
        data.entries[bsi_format::SliceEntryName(h.slice_count)].assign(
            sizeof(uint64_t), 0);
        ++h.slice_count;
    });
    // A slice entry the header does not declare, even a well-formed one.
    reject([](TestArtifactData& data, bsi_format::Header& h) {
        data.entries[bsi_format::SliceEntryName(h.slice_count)] =
            data.entries.at(bsi_format::SliceEntryName(0));
    });
    // Looser bounds than the rows reach. bit_width(7) is still 3, so only
    // the "max is reached" check can catch it.
    reject([](TestArtifactData&, bsi_format::Header& h) { h.max = 7; });
    // A row count that needs more words than the payload holds.
    reject([](TestArtifactData&, bsi_format::Header& h) { h.num_rows += 64; });
}

// With no valid rows the state must be exactly what the constructor
// produces: min = max = 0 and no slices.
TEST(BitSlicedIndexArtifactTest, NoValidRowsAcceptsOnlyTheEmptyState) {
    auto data = SerializeLegacy(SampleRows());
    auto& existence = data.entries.at(std::string(bsi_format::kExistenceEntry));
    std::fill(existence.begin(), existence.end(), 0);
    auto header = HeaderOf(data);

    // Slices but no valid rows.
    auto slices = data;
    auto slices_header = header;
    slices_header.min = 0;
    slices_header.max = 0;
    SetHeader(slices, slices_header);
    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { (void)LoadLegacy(slices); });

    // Bounds but no valid rows.
    for (size_t s = 0; s < header.slice_count; ++s) {
        data.entries.erase(bsi_format::SliceEntryName(s));
    }
    header.slice_count = 0;
    SetHeader(data, header);
    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { (void)LoadLegacy(data); });

    // The empty state itself loads.
    header.min = 0;
    header.max = 0;
    SetHeader(data, header);
    const auto loaded = LoadLegacy(data);
    EXPECT_EQ(loaded.Count(), SampleRows().size());
    EXPECT_EQ(loaded.ValidCount(), 0U);
    EXPECT_EQ(loaded.Min(), std::nullopt);
}

TEST(BitSlicedIndexArtifactTest, SliceBitOnNullRowIsRejected) {
    auto data = SerializeLegacy(SampleRows());
    SetBit(data, bsi_format::SliceEntryName(0), 0, 2);  // NULL row 2
    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { (void)LoadLegacy(data); });
}

TEST(BitSlicedIndexArtifactTest, OffsetAboveMaxIsRejected) {
    auto data = SerializeLegacy(SampleRows());
    // Row 0 holds offset 0; set all three slice bits to make it 7 > span 5.
    for (size_t slice = 0; slice < 3; ++slice) {
        SetBit(data, bsi_format::SliceEntryName(slice), 0, 0);
    }
    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { (void)LoadLegacy(data); });
}

TEST(BitSlicedIndexArtifactTest, BitsPastTheLastRowAreRejected) {
    auto existence = SerializeLegacy(SampleRows());
    SetBit(existence, bsi_format::kExistenceEntry, 0, 63);
    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { (void)LoadLegacy(existence); });

    auto slice = SerializeLegacy(SampleRows());
    SetBit(slice, bsi_format::SliceEntryName(1), 0, 63);
    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { (void)LoadLegacy(slice); });
}

// FromState is the last line of defence and is reachable without an
// envelope; check the size invariants it owns directly.
TEST(BitSlicedIndexArtifactTest, FromStateRejectsMismatchedBitmapSizes) {
    BitSlicedIndexInt32::State short_existence{
        .num_rows = 3, .min = 0, .max = 0, .existence = TargetBitmap(2, true)};
    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        (void)BitSlicedIndexInt32::FromState(std::move(short_existence));
    });

    BitSlicedIndexInt32::State short_slice{
        .num_rows = 3, .min = 0, .max = 1, .existence = TargetBitmap(3, true)};
    short_slice.slices.emplace_back(2, false);
    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        (void)BitSlicedIndexInt32::FromState(std::move(short_slice));
    });
}

// Both bounds must be held by some row; a looser header is corruption even
// when the slice count still fits.
TEST(BitSlicedIndexArtifactTest, FromStateRejectsUnreachedBounds) {
    // Rows 0 and 1 valid, offsets 1 and 2: min 10 is held by no row.
    BitSlicedIndexInt32::State no_min{.num_rows = 2,
                                      .min = 10,
                                      .max = 12,
                                      .existence = TargetBitmap(2, true)};
    no_min.slices.emplace_back(2, false);
    no_min.slices.emplace_back(2, false);
    no_min.slices[0].set(0);  // row 0: offset 1
    no_min.slices[1].set(1);  // row 1: offset 2
    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        (void)BitSlicedIndexInt32::FromState(std::move(no_min));
    });

    // Offsets 0 and 1: max 12 (offset 2) is held by no row.
    BitSlicedIndexInt32::State no_max{.num_rows = 2,
                                      .min = 10,
                                      .max = 12,
                                      .existence = TargetBitmap(2, true)};
    no_max.slices.emplace_back(2, false);
    no_max.slices.emplace_back(2, false);
    no_max.slices[0].set(1);  // row 1: offset 1
    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        (void)BitSlicedIndexInt32::FromState(std::move(no_max));
    });
}

TEST(BitSlicedIndexArtifactTest, FromStateAcceptsAValidState) {
    // Rows 0 and 2 valid with values 4 and 6 (min 4, offsets 0 and 2).
    BitSlicedIndexInt32::State state{
        .num_rows = 3, .min = 4, .max = 6, .existence = TargetBitmap(3, false)};
    state.existence.set(0);
    state.existence.set(2);
    state.slices.emplace_back(3, false);
    state.slices.emplace_back(3, false);
    state.slices[1].set(2);
    const auto index = BitSlicedIndexInt32::FromState(std::move(state));
    EXPECT_EQ(index.ValidCount(), 2U);
    EXPECT_EQ(index.Min(), 4);
    EXPECT_EQ(index.Max(), 6);
    EXPECT_TRUE(
        SameBits(index.Range(6, CompareOp::Equal),
                 Build({4, std::nullopt, 6}).Range(6, CompareOp::Equal)));
}

}  // namespace
}  // namespace milvus::index
