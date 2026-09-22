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
#include <string>
#include <utility>

#include "common/Json.h"

namespace milvus {

TEST(JsonOwnershipTest, MoveAssignmentTransfersOwnedBuffer) {
    const std::string text = "{\"value\":\"" + std::string(1024, 'x') + "\"}";
    Json destination(simdjson::padded_string(std::string("{}")));
    const char* buffer;
    {
        Json source{simdjson::padded_string(text)};
        buffer = source.c_str();
        destination = std::move(source);
        EXPECT_EQ(destination.c_str(), buffer);
        // Reusing the moved-from object must not change the destination.
        source = Json(simdjson::padded_string(std::string("[]")));
    }
    EXPECT_EQ(destination.c_str(), buffer);
    EXPECT_EQ(destination.data(), text);
}

TEST(JsonOwnershipTest, MoveAssignmentPreservesBorrowedView) {
    simdjson::padded_string buffer(std::string("{\"value\":42}"));
    Json destination;
    {
        Json view(buffer.data(), buffer.size());
        destination = std::move(view);
    }
    EXPECT_EQ(destination.c_str(), buffer.data());
    EXPECT_EQ(destination.data(),
              std::string_view(buffer.data(), buffer.size()));
}

TEST(JsonOwnershipTest, MoveAssignmentPreservesAliasingView) {
    Json destination(simdjson::padded_string(std::string("{\"value\":42}")));
    const auto* buffer = destination.c_str();
    {
        Json view(destination.data());
        destination = std::move(view);
    }
    EXPECT_EQ(destination.c_str(), buffer);
    EXPECT_EQ(destination.data(), "{\"value\":42}");
}

TEST(JsonOwnershipTest, SelfMoveAndCopyAssignmentRetainValue) {
    const std::string text(1024, 'x');
    Json source{simdjson::padded_string(text)};
    const auto* buffer = source.c_str();
    auto* alias = &source;
    source = std::move(*alias);
    EXPECT_EQ(source.c_str(), buffer);
    EXPECT_EQ(source.data(), text);

    Json copy;
    copy = source;
    EXPECT_NE(copy.c_str(), source.c_str());
    EXPECT_EQ(copy.data(), text);
}

}  // namespace milvus
