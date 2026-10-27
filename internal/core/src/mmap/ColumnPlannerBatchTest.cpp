// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <vector>

#include "mmap/ChunkedColumnScanCommon.h"
#include "test_utils/OriginalCohereFixture.h"

namespace milvus {
namespace {

// These offsets and boundaries are read-API metadata over original fixture
// rows, not generated scalar values or a new performance dataset.
template <typename Offset>
void
CheckLocations(const ColumnPlanner& planner,
               const std::vector<Offset>& offsets,
               int64_t first,
               int64_t count) {
    std::vector<CellLocation> result(count + 2, CellLocation{-7, -11});
    planner.LocateBatch(OffsetView::From(offsets.data(), offsets.size()),
                        first,
                        count,
                        result.data() + 1);
    EXPECT_EQ(result.front().cell_id, -7);
    EXPECT_EQ(result.front().cell_offset, -11);
    EXPECT_EQ(result.back().cell_id, -7);
    EXPECT_EQ(result.back().cell_offset, -11);
    const auto& boundaries = planner.CellBoundaries();
    for (int64_t i = 0; i < count; ++i) {
        const auto row = offsets[first + i];
        int64_t expected_cell = -1;
        // Independent linear geometry oracle, including empty cells.
        for (size_t cell = 0; cell + 1 < boundaries.size(); ++cell) {
            if (row >= boundaries[cell] && row < boundaries[cell + 1]) {
                expected_cell = cell;
                break;
            }
        }
        ASSERT_GE(expected_cell, 0);
        const auto single = planner.Locate(row);
        const auto& actual = result[i + 1];
        EXPECT_EQ(actual.cell_id, expected_cell);
        EXPECT_EQ(actual.cell_offset, row - boundaries[expected_cell]);
        EXPECT_EQ(actual.cell_id, single.cell_id);
        EXPECT_EQ(actual.cell_offset, single.cell_offset);
    }
}

TEST(TakeContextFoundation, PlannerBatchUsesOriginalMappingForBothWidths) {
    const auto original = LoadOriginalCohereIds();
    const auto rows = static_cast<int64_t>(original.size());
    ASSERT_EQ(rows, 1024);
    ColumnPlanner single(std::vector<int64_t>{0, rows});
    ColumnPlanner irregular(
        std::vector<int64_t>{0, 0, rows / 4, rows / 2, rows / 2, rows, rows});
    std::vector<int64_t> offsets64;
    for (int64_t i = 0; i < rows; ++i) {
        offsets64.push_back((i * 73 + 11) % rows);
    }
    const std::vector<int32_t> offsets32(offsets64.begin(), offsets64.end());
    for (const auto* planner : {&single, &irregular}) {
        CheckLocations(*planner, offsets64, 0, rows);  // Not a 64-row API.
        CheckLocations(*planner, offsets32, 13, 257);
        CheckLocations(*planner, offsets64, rows - 17, 17);
        CheckLocations(*planner,
                       std::vector<int64_t>{0, rows - 1, 0, rows / 2, rows / 4},
                       0,
                       5);
        CheckLocations(*planner, offsets32, rows, 0);
        EXPECT_NO_THROW(planner->LocateBatch(OffsetView{}, 0, 0, nullptr));
    }
}

TEST(TakeContextFoundation, PlannerBatchRejectsInvalidSlicesAndRows) {
    const auto original = LoadOriginalCohereIds();
    const auto rows = static_cast<int64_t>(original.size());
    ColumnPlanner planner(std::vector<int64_t>{0, rows / 2, rows});
    const std::array<int64_t, 2> valid{0, rows - 1};
    const auto input = OffsetView::From(valid.data(), valid.size());
    std::array<CellLocation, 2> output;
    EXPECT_ANY_THROW(planner.LocateBatch(input, -1, 1, output.data()));
    EXPECT_ANY_THROW(planner.LocateBatch(input, 0, -1, output.data()));
    EXPECT_ANY_THROW(planner.LocateBatch(input, 3, 0, nullptr));
    EXPECT_ANY_THROW(planner.LocateBatch(input, 1, 2, output.data()));
    EXPECT_ANY_THROW(planner.LocateBatch(
        input, 1, std::numeric_limits<int64_t>::max(), output.data()));
    EXPECT_ANY_THROW(planner.LocateBatch(input, 0, 1, nullptr));
    EXPECT_ANY_THROW(planner.LocateBatch(
        OffsetView::From(static_cast<const int64_t*>(nullptr), 1),
        0,
        1,
        output.data()));
    EXPECT_ANY_THROW(
        planner.LocateBatch(OffsetView::From(valid.data(), -1), 0, 0, nullptr));
    for (auto invalid : {-int64_t{1}, rows}) {
        EXPECT_ANY_THROW(planner.LocateBatch(
            OffsetView::From(&invalid, 1), 0, 1, output.data()));
    }
    auto unsupported = input;
    unsupported.element_type = static_cast<OffsetElementType>(2);
    EXPECT_ANY_THROW(planner.LocateBatch(unsupported, 0, 1, output.data()));
}

TEST(TakeContextFoundation, PlannerAllOriginalOffsetsAcrossCellGeometries) {
    const auto original = LoadOriginalCohereIds();
    const auto rows = static_cast<int64_t>(original.size());
    ASSERT_EQ(rows, 1024);
    std::vector<int64_t> offsets64;
    for (int64_t row = 0; row < rows; ++row) {
        offsets64.push_back(row);
    }
    const std::vector<int32_t> offsets32(offsets64.begin(), offsets64.end());
    // These are alternate read-API geometries over the same original 1024
    // rows, not fabricated scalar values or inflated performance datasets.
    for (const int64_t cells : {1,  2,  3,  4,  5,  7,  8,   15,  16,  17,
                                31, 32, 33, 63, 64, 65, 127, 128, 129, 1024}) {
        for (const bool include_empty : {false, true}) {
            std::vector<int64_t> boundaries;
            for (int64_t cell = 0; cell <= cells; ++cell) {
                const auto boundary = rows * cell / cells;
                boundaries.push_back(boundary);
                if (include_empty && (cell % 3 == 0 || cell == cells)) {
                    boundaries.push_back(boundary);
                }
            }
            ColumnPlanner planner(std::move(boundaries));
            CheckLocations(planner, offsets64, 0, rows);
            CheckLocations(planner, offsets32, 0, rows);
            EXPECT_ANY_THROW(planner.Locate(-1));
            EXPECT_ANY_THROW(planner.Locate(rows));
        }
    }
    ColumnPlanner empty(std::vector<int64_t>{0, 0, 0, 0});
    EXPECT_ANY_THROW(empty.Locate(0));
    EXPECT_ANY_THROW(empty.Locate(-1));
}

}  // namespace
}  // namespace milvus
