// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <gtest/gtest.h>

#include <cstddef>
#include <exception>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/scalar/ngram/JsonProjectedString.h"

namespace milvus::index::test {

template <typename T>
struct ScalarTestData;

inline TargetBitmap
Hits(size_t count, const std::vector<size_t>& offsets) {
    TargetBitmap result(count, false);
    for (const auto offset : offsets) {
        if (offset >= count) {
            throw std::logic_error("manual hit is outside the bitmap");
        }
        result.set(offset);
    }
    return result;
}

inline void
ExpectBitmap(TargetBitmap& actual, const TargetBitmap& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    EXPECT_TRUE(actual == expected);
}

inline void
ExpectBitmap(TargetBitmap&& actual, const TargetBitmap& expected) {
    ExpectBitmap(actual, expected);
}

// For readers that answer an operation with a candidate SUPERSET (see
// IPatternMatchReader::PatternMatchIsExact): every expected row must be
// present; extra rows are allowed because the consumer rechecks them.
inline void
ExpectBitmapSuperset(const TargetBitmap& actual,
                     const TargetBitmap& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        if (expected[i]) {
            EXPECT_TRUE(actual[i])
                << "candidate superset is missing expected row " << i;
        }
    }
}

inline void
ExpectHits(TargetBitmap& actual,
           size_t count,
           const std::vector<size_t>& offsets) {
    ASSERT_EQ(actual.size(), count);
    TargetBitmap expected(count, false);
    for (const auto offset : offsets) {
        ASSERT_LT(offset, count);
        expected.set(offset);
    }
    EXPECT_TRUE(actual == expected);
}

inline void
ExpectHits(TargetBitmap&& actual,
           size_t count,
           const std::vector<size_t>& offsets) {
    ExpectHits(actual, count, offsets);
}

template <typename T>
TargetBitmap
ExpectedNulls(const ScalarTestData<T>& data) {
    TargetBitmap result(data.values.size(), false);
    if constexpr (std::is_same_v<T, JsonProjectedString>) {
        for (size_t i = 0; i < data.values.size(); ++i) {
            if (data.values[i].state == JsonProjectedStringState::FieldNull) {
                result.set(i);
            }
        }
    } else {
        for (size_t i = 0; i < data.values.size(); ++i) {
            if (!data.validity[i]) {
                result.set(i);
            }
        }
    }
    return result;
}

template <typename T>
void
ExpectNullState(const ScalarTestData<T>& data, const IIndexReaderBase& reader) {
    const auto* null_reader = dynamic_cast<const INullReader*>(&reader);
    ASSERT_NE(null_reader, nullptr);

    const auto expected_null = ExpectedNulls(data);
    auto actual_null = null_reader->IsNull();
    ExpectBitmap(actual_null, expected_null);

    auto expected_not_null = ExpectedNulls(data);
    expected_not_null.flip();
    auto actual_not_null = null_reader->IsNotNull();
    ExpectBitmap(actual_not_null, expected_not_null);
}

template <typename F>
void
ExpectSegcoreError(ErrorCode expected, F&& operation) {
    try {
        std::forward<F>(operation)();
        ADD_FAILURE() << "operation expected a SegcoreError";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), expected);
    } catch (const std::exception& error) {
        ADD_FAILURE() << "operation threw a non-SegcoreError: " << error.what();
    } catch (...) {
        ADD_FAILURE() << "operation threw a non-SegcoreError";
    }
}

}  // namespace milvus::index::test
