// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/indexing/FieldIndexCapability.h"

#include <cstdint>
#include <unordered_set>

#include <gtest/gtest.h>

namespace milvus::segcore {

TEST(IndexIdentityTest, ClassifiesPreBuiltAndSegmentLocalIndexes) {
    EXPECT_EQ(IndexIdentity::PreBuiltIndex(-1).kind(),
              IndexIdentity::Kind::PreBuiltIndex);
    EXPECT_EQ(IndexIdentity::SegmentLocal(1).kind(),
              IndexIdentity::Kind::SegmentLocal);
}

TEST(IndexIdentityTest, IndexKeyIncludesKindAndField) {
    const auto prebuilt =
        IndexKey{FieldId(10), IndexIdentity::PreBuiltIndex(7)};
    const auto same_number_local =
        IndexKey{FieldId(10), IndexIdentity::SegmentLocal(7)};
    const auto same_identity_other_field =
        IndexKey{FieldId(11), IndexIdentity::PreBuiltIndex(7)};

    EXPECT_NE(prebuilt, same_number_local);
    EXPECT_NE(prebuilt, same_identity_other_field);

    std::unordered_set<IndexKey, IndexKeyHash> keys;
    keys.insert(prebuilt);
    keys.insert(same_number_local);
    keys.insert(same_identity_other_field);
    EXPECT_EQ(keys.size(), 3);
}

}  // namespace milvus::segcore
