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

#include <cstddef>
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

struct SpatialCase {
    SpatialOp op;
    std::string query_wkt;
    // Exact geometry hits that every candidate superset must retain.
    std::vector<size_t> required_offsets;
};

void
AddSpatialCase(IndexTestCases& cases,
               std::string name,
               std::string dataset,
               SpatialCase test_case) {
    cases.Add(IndexTestCase<std::string_view>{
        .name = std::move(name),
        .dataset = std::move(dataset),
        .input_shape = BackendInputShape::SpatialWkb,
        .domain = Domain::Row,
        .logical_value_type = DataType::GEOMETRY,
        .input_lifetime = InputLifetime::ReleaseBeforeBody,
        .body =
            Observe<std::string_view>{
                .capability = &ReaderCaps::spatial,
                .run =
                    [test_case = std::move(test_case)](
                        const ReaderBackend&,
                        const ScalarTestData<std::string_view>& data,
                        IIndexReaderBasePtr& reader) {
                        ASSERT_TRUE(reader->Caps().spatial);
                        EXPECT_FALSE(reader->Caps().exact);
                        const auto* spatial =
                            dynamic_cast<const ISpatialReader*>(reader.get());
                        ASSERT_NE(spatial, nullptr);

                        const Geometry query(GetThreadLocalGEOSContext(),
                                             test_case.query_wkt.c_str());
                        const auto actual =
                            spatial->Candidates(test_case.op, query);
                        ASSERT_EQ(actual.size(), data.values.size());
                        for (const auto offset : test_case.required_offsets) {
                            ASSERT_LT(offset, actual.size());
                            EXPECT_TRUE(actual[offset])
                                << "spatial candidates dropped exact hit "
                                << offset;
                        }
                        ExpectNullState(data, *reader);
                    },
            },
    });
}

void
AddAllSpatialOps(IndexTestCases& cases) {
    constexpr auto dataset = "SpatialWkbNullable";
    AddSpatialCase(cases,
                   "EqualsRetainsPointHit",
                   dataset,
                   {.op = SpatialOp::Equals,
                    .query_wkt = "POINT(0 0)",
                    .required_offsets = {0}});
    AddSpatialCase(cases,
                   "TouchesRetainsBoundaryHits",
                   dataset,
                   {.op = SpatialOp::Touches,
                    .query_wkt = "POLYGON((2 2,2 4,4 4,4 2,2 2))",
                    .required_offsets = {1, 3, 5}});
    AddSpatialCase(
        cases,
        "OverlapsRetainsAreaHits",
        dataset,
        {.op = SpatialOp::Overlaps,
         .query_wkt = "POLYGON((0.5 0.5,0.5 2.5,2.5 2.5,2.5 0.5,0.5 0.5))",
         .required_offsets = {3, 4}});
    AddSpatialCase(cases,
                   "CrossesRetainsLineAreaHits",
                   dataset,
                   {.op = SpatialOp::Crosses,
                    .query_wkt = "LINESTRING(-1 1.5,4 1.5)",
                    .required_offsets = {2, 3, 4}});
    AddSpatialCase(cases,
                   "ContainsRetainsPolygonsAroundPoint",
                   dataset,
                   {.op = SpatialOp::Contains,
                    .query_wkt = "POINT(1 1)",
                    .required_offsets = {2, 3, 11}});
    AddSpatialCase(cases,
                   "IntersectsRetainsAllManualHits",
                   dataset,
                   {.op = SpatialOp::Intersects,
                    .query_wkt = "POLYGON((0 0,0 1,1 1,1 0,0 0))",
                    .required_offsets = {0, 2, 3, 4, 6, 11}});
    AddSpatialCase(cases,
                   "WithinRetainsContainedGeometries",
                   dataset,
                   {.op = SpatialOp::Within,
                    .query_wkt = "POLYGON((0 0,0 3,3 3,3 0,0 0))",
                    .required_offsets = {1, 2, 3, 4, 11}});
    AddSpatialCase(
        cases,
        "DWithinRetainsCenterOfCallerExpandedMbr",
        dataset,
        {.op = SpatialOp::DWithin,
         .query_wkt = "POLYGON((1.5 1.5,1.5 2.5,2.5 2.5,2.5 1.5,1.5 1.5))",
         .required_offsets = {1}});
}

void
AddNullAndCountCases(IndexTestCases& cases) {
    AddSpatialCase(cases,
                   "InvalidAndEmptyPayloadsDoNotChangeCount",
                   "SpatialWkbAllValid",
                   {.op = SpatialOp::Intersects,
                    .query_wkt = "POLYGON((0 0,0 2.5,2.5 2.5,2.5 0,0 0))",
                    .required_offsets = {0, 1}});
    AddSpatialCase(cases,
                   "AllNullReturnsCountSizedBitmap",
                   "SpatialAllNull",
                   {.op = SpatialOp::Intersects,
                    .query_wkt = "POLYGON((-1 -1,-1 3,3 3,3 -1,-1 -1))",
                    .required_offsets = {}});
}

const IndexTestCases&
SpatialCases() {
    static const auto cases = [] {
        IndexTestCases cases;
        AddAllSpatialOps(cases);
        AddNullAndCountCases(cases);
        return cases;
    }();
    return cases;
}

class SpatialReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(SpatialReaderTest, RetainsExactHitsInCandidateSuperset) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(SpatialBackends,
                         SpatialReaderTest,
                         ::testing::ValuesIn(SpatialCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
