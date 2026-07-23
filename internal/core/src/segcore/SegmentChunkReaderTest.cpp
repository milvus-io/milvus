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

#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "segcore/SegmentChunkReader.h"

namespace milvus::segcore {

TEST(SegmentChunkReader, NumericVariantMismatchIsSystemError) {
    const data_access_type value = int64_t{7};

    try {
        (void)get_from_variant<int32_t>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

TEST(SegmentChunkReader, StringVariantMismatchIsSystemError) {
    const data_access_type value = true;

    try {
        (void)get_from_variant<std::string>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

}  // namespace milvus::segcore
