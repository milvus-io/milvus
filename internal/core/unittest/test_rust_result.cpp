// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <stddef.h>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include "gtest/gtest.h"
#include "tantivy-binding.h"
#include "rust-array.h"

TEST(RustResultTest, TestResult) {
    auto arr = test_enum_with_array();
    auto len = arr.value.rust_array._0.len;
    for (size_t i = 0; i < len; i++) {
        EXPECT_EQ(i + 1, arr.value.rust_array._0.array[i]);
    }
    free_rust_result(arr);

    auto ptr = test_enum_with_ptr();
    EXPECT_EQ(1, *static_cast<uint32_t*>(ptr.value.ptr._0));
    free_rust_result(ptr);
    free_test_ptr(ptr.value.ptr._0);
}

using milvus::tantivy::RustArrayWrapper;
using milvus::tantivy::RustResultWrapper;

static_assert(std::is_nothrow_constructible_v<RustResultWrapper, RustResult>);
static_assert(std::is_nothrow_move_constructible_v<RustResultWrapper>);
static_assert(std::is_nothrow_move_assignable_v<RustResultWrapper>);

TEST(RustResultTest, WrapperMovesTransferOwnership) {
    RustResultWrapper source(test_enum_with_array());
    auto* original_array = source.result_->value.rust_array._0.array;

    RustResultWrapper moved(std::move(source));
    EXPECT_FALSE(source.result_);
    ASSERT_TRUE(moved.result_);
    EXPECT_EQ(moved.result_->value.rust_array._0.array, original_array);

    // Replacing an existing result must release it before adopting the source.
    RustResultWrapper destination(test_enum_with_array());
    destination = std::move(moved);
    EXPECT_FALSE(moved.result_);
    ASSERT_TRUE(destination.result_);
    EXPECT_EQ(destination.result_->value.rust_array._0.array, original_array);
    EXPECT_EQ(destination.result_->value.rust_array._0.len, 3);
    EXPECT_EQ(destination.result_->value.rust_array._0.array[2], 3);

    RustResultWrapper empty;
    destination = std::move(empty);
    EXPECT_FALSE(destination.result_);
    EXPECT_FALSE(empty.result_);
}

TEST(RustResultTest, WrapperCanTransferArrayOwnership) {
    RustResultWrapper result(test_enum_with_array());
    auto* original_array = result.result_->value.rust_array._0.array;
    RustArrayWrapper array(std::move(result.result_->value.rust_array._0));
    EXPECT_EQ(result.result_->value.rust_array._0.array, nullptr);
    EXPECT_EQ(array.array_.array, original_array);

    // Releasing the moved-from result must leave the transferred array valid.
    result = RustResultWrapper();
    EXPECT_EQ(array.array_.len, 3);
    EXPECT_EQ(array.array_.array[0], 1);
    EXPECT_EQ(array.array_.array[2], 3);
}
