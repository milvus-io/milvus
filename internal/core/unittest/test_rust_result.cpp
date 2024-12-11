
#include <cstdint>
#include "gtest/gtest.h"
#include "tantivy-binding.h"
TEST(RustResultTest, TestResult) {
    auto arr = test_enum_with_array();
    auto len = arr.value.rust_array._0.len;
    for (size_t i = 0; i < len; i++) {
        EXPECT_EQ(i + 1, arr.value.rust_array._0.array[i]);
    }

    auto ptr = test_enum_with_ptr();
    EXPECT_EQ(1, *static_cast<uint32_t*>(ptr.value.ptr._0));
}