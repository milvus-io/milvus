// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/Geometry.h"
#include "index/contracts/query/ISpatialReader.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
namespace {

constexpr auto kBackend = "SpatialRTreeHeap";
constexpr auto kDataset = "SpatialWkbNullable";
constexpr std::array<std::string_view, 4> kAllBackends = {
    "SpatialRTreeHeap",
    "SpatialRTreeHeapNonNull",
    "SpatialRTreeMmapRequested",
    "SpatialRTreeMmapRequestedNonNull",
};

struct ConcreteSpatialCase {
    std::string name;
    SpatialOp op;
    std::optional<std::string> query_wkt;
    std::vector<size_t> expected_offsets;
};

const IndexTestCases&
ConcreteRTreeCases() {
    static const auto cases = [] {
        IndexTestCases cases;
        const std::vector<ConcreteSpatialCase> table = {
            // Offsets 8 (GEOMETRYCOLLECTION EMPTY) and 9 (corrupt WKB) are
            // non-null rows with no computable envelope, so they carry the
            // deterministic placeholder MBR at the origin and this
            // origin-covering query pulls them into the candidate set. Exact
            // refinement discards them; dropping them from the index instead
            // would desynchronize the index row count from the segment row
            // count. Regression guard for the placeholder contract (#50951).
            {.name = "PointMbrIntersection",
             .op = SpatialOp::Equals,
             .query_wkt = "POINT(0 0)",
             .expected_offsets = {0, 2, 3, 6, 8, 9}},
            // The same two rows stay out of every candidate set whose box does
            // not cover the origin, which is what keeps the placeholder cheap.
            {.name = "PlaceholderRowsStayOutOfDistantMbr",
             .op = SpatialOp::Intersects,
             .query_wkt = "POLYGON((4 4,4 6,6 6,6 4,4 4))",
             .expected_offsets = {5, 7}},
            {.name = "OperatorIndependentMbrIntersection",
             .op = SpatialOp::Contains,
             .query_wkt = "POINT(1 1)",
             .expected_offsets = {2, 3, 4, 11}},
            {.name = "CallerExpandedDWithinMbr",
             .op = SpatialOp::DWithin,
             .query_wkt = "POLYGON((1.5 1.5,1.5 2.5,2.5 2.5,2.5 1.5,1.5 1.5))",
             .expected_offsets = {1, 2, 3, 4}},
            {.name = "InvalidQueryFallsBackToEveryNonNullRow",
             .op = SpatialOp::Intersects,
             .query_wkt = std::nullopt,
             .expected_offsets = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11}},
        };
        for (const auto& test_case : table) {
            cases.Add(IndexTestCase<std::string_view>{
                .name = test_case.name,
                .dataset = kDataset,
                .input_shape = BackendInputShape::SpatialWkb,
                .domain = Domain::Row,
                .logical_value_type = DataType::GEOMETRY,
                .input_lifetime = InputLifetime::ReleaseBeforeBody,
                .backends = {kBackend},
                .body =
                    Observe<std::string_view>{
                        .capability = &ReaderCaps::spatial,
                        .run =
                            [test_case](
                                const ReaderBackend&,
                                const ScalarTestData<std::string_view>& data,
                                IIndexReaderBasePtr& reader) {
                                const auto* spatial =
                                    dynamic_cast<const ISpatialReader*>(
                                        reader.get());
                                ASSERT_NE(spatial, nullptr);
                                auto actual = [&] {
                                    if (test_case.query_wkt.has_value()) {
                                        const Geometry query(
                                            GetThreadLocalGEOSContext(),
                                            test_case.query_wkt->c_str());
                                        return spatial->Candidates(test_case.op,
                                                                   query);
                                    }
                                    const Geometry query;
                                    return spatial->Candidates(test_case.op,
                                                               query);
                                }();
                                const auto expected =
                                    Hits(data.values.size(),
                                         test_case.expected_offsets);
                                ExpectBitmap(actual, expected);
                            },
                    },
            });
        }
        cases.Add(IndexTestCase<std::string_view>{
            .name = "UsesHeapResourceAccounting",
            .dataset = "SpatialWkbAllValid",
            .input_shape = BackendInputShape::SpatialWkb,
            .domain = Domain::Row,
            .logical_value_type = DataType::GEOMETRY,
            .input_lifetime = InputLifetime::ReleaseBeforeBody,
            .backends = std::vector<std::string>(kAllBackends.begin(),
                                                 kAllBackends.end()),
            .body =
                Observe<std::string_view>{
                    .capability = &ReaderCaps::spatial,
                    .run =
                        [](const ReaderBackend&,
                           const ScalarTestData<std::string_view>&,
                           IIndexReaderBasePtr& reader) {
                            EXPECT_EQ(reader->CellByteSize().file_bytes, 0);
                        },
                },
        });
        return cases;
    }();
    return cases;
}

std::vector<FilterParam>
RTreeEmptyBuildCases() {
    const auto* dataset =
        &ScalarDataSets().Get<std::string_view>("SpatialEmpty");
    std::vector<FilterParam> cases;
    cases.reserve(kAllBackends.size());
    for (const auto name : kAllBackends) {
        const auto backend =
            ScalarReaderBackends().Get<std::string_view>(std::string(name));
        cases.push_back({
            .name = backend.Name() + "_SpatialEmpty_BuildRejectsEmptyInput",
            .run =
                [backend, dataset] {
                    auto data = dataset->make_data();
                    const ScalarTestInput<std::string_view> input(data);
                    try {
                        static_cast<void>(backend.Build(
                            input.View(),
                            {.row_count = 0, .values = data.metadata}));
                        ADD_FAILURE()
                            << "R-Tree empty build unexpectedly succeeded";
                    } catch (const SegcoreError& error) {
                        EXPECT_EQ(error.get_error_code(),
                                  ErrorCode::DataIsEmpty);
                    } catch (const std::exception& error) {
                        ADD_FAILURE()
                            << "R-Tree empty build threw a non-SegcoreError: "
                            << error.what();
                    } catch (...) {
                        ADD_FAILURE()
                            << "R-Tree empty build threw a non-SegcoreError";
                    }
                },
        });
    }
    return cases;
}

class RTreeIndexReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(RTreeIndexReaderTest, ReturnsCurrentMbrCandidates) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(RTreeHeap,
                         RTreeIndexReaderTest,
                         ::testing::ValuesIn(ConcreteRTreeCases().All()),
                         FilterParamName);

class RTreeIndexBuildTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(RTreeIndexBuildTest, RejectsEmptyInput) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(RTreeProfiles,
                         RTreeIndexBuildTest,
                         ::testing::ValuesIn(RTreeEmptyBuildCases()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
