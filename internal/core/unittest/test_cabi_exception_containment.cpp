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

#include <memory>
#include <string>

#include "common/EasyAssert.h"
#include "pb/plan.pb.h"
#include "query/Plan.h"
#include "segcore/plan_c.h"
#include "segcore/segment_c.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

std::string
EmptyPlaceholderGroupBlob() {
    // A protobuf with zero placeholders parses fine; the consumers are what
    // index into it.
    milvus::proto::common::PlaceholderGroup group;
    std::string blob;
    group.SerializeToString(&blob);
    return blob;
}

}  // namespace

// GetNumOfQueries is `group->at(0)` behind a plain int64_t return, so an empty
// placeholder group used to throw std::out_of_range across the C ABI and
// terminate the QueryNode. The invariant belongs to ParsePlaceholderGroup --
// which returns through a CStatus -- so the rejection happens there and every
// downstream consumer is covered at once.
TEST(CABIExceptionContainment, EmptyPlaceholderGroupIsRejectedAtParse) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::INT64);

    auto blob = EmptyPlaceholderGroupBlob();
    milvus::query::Plan plan(schema);
    try {
        milvus::query::ParsePlaceholderGroup(
            &plan, reinterpret_cast<const uint8_t*>(blob.data()), blob.size());
        FAIL() << "an empty placeholder group must be rejected at parse";
    } catch (const SegcoreError& e) {
        // Rejected here, inside a CStatus channel -- so GetNumOfQueries'
        // group->at(0) is never reached through a plain int64_t return.
        EXPECT_EQ(e.get_error_code(), ErrorCode::InvalidParameter);
    }
}

// HasRawData returns a plain bool, so it has no channel for an error -- yet the
// impls AssertInfo-throw for a field the published schema has not caught up
// with (normal schema-evolution timing) and for not-ready indexes. Reporting
// "no raw data" is the safe answer; terminating the process is not.
TEST(CABIExceptionContainment, HasRawDataReportsFalseInsteadOfThrowing) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    // The sealed impl is the one that looks the field up in the PUBLISHED
    // schema (HasRawDataFromState -> schema->operator[]), which AssertInfo-
    // throws for a field the snapshot has not caught up with yet. The growing
    // impl never consults the schema, so only this shape can throw.
    auto segment = CreateSealedSegment(schema);
    auto c_segment = static_cast<CSegmentInterface>(segment.get());

    EXPECT_NO_THROW({
        bool has = HasRawData(c_segment, 99999);
        EXPECT_FALSE(has);
    });
}
