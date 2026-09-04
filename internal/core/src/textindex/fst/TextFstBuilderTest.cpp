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

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "textindex/fst/text_fst.h"

namespace milvus::textindex {
namespace {

TEST(TextFstBuilderTest, BuildsSortedTerms) {
    const std::vector<std::string> terms{"book", "books", "fuzzy", "你好"};
    TextFst fst;
    std::size_t index = 0;
    fst.Build([&]() -> std::optional<std::string_view> {
        return index == terms.size()
                   ? std::nullopt
                   : std::optional<std::string_view>(terms[index++]);
    });

    EXPECT_TRUE(fst.VerifyChecksum());
    EXPECT_EQ(fst.TermCount(), terms.size());
    EXPECT_FALSE(fst.SerializedBytes().empty());
}

TEST(TextFstBuilderTest, SortedStreamRejectsInvalidEntries) {
    const auto expect_invalid = [](std::vector<std::string> terms) {
        TextFst fst;
        std::size_t index = 0;
        EXPECT_THROW(fst.Build([&]() -> std::optional<std::string_view> {
            return index == terms.size()
                       ? std::nullopt
                       : std::optional<std::string_view>(terms[index++]);
        }),
                     std::invalid_argument);
    };

    expect_invalid({"books", "book"});
    expect_invalid({"book", "book"});
    expect_invalid({""});
    expect_invalid({std::string(1, static_cast<char>(0xff))});
}

}  // namespace
}  // namespace milvus::textindex
