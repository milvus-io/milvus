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
#include "common/ValueOp.h"
#include "common/Vector.h"

using namespace milvus;
using namespace milvus::common;

// Helper function to create a ColumnVector with given data and valid bitmaps
ColumnVectorPtr
CreateColumnVector(const std::vector<bool>& data,
                   const std::vector<bool>& valid) {
    size_t size = data.size();
    TargetBitmap data_bitmap(size);
    TargetBitmap valid_bitmap(size);
    for (size_t i = 0; i < size; ++i) {
        if (data[i]) {
            data_bitmap.set(i);
        }
        if (valid[i]) {
            valid_bitmap.set(i);
        }
    }
    return std::make_shared<ColumnVector>(std::move(data_bitmap),
                                          std::move(valid_bitmap));
}

// Helper function to extract results from ColumnVector
void
ExtractResults(const ColumnVectorPtr& vec,
               std::vector<bool>& data,
               std::vector<bool>& valid) {
    size_t size = vec->size();
    data.resize(size);
    valid.resize(size);
    TargetBitmapView data_view(vec->GetRawData(), size);
    TargetBitmapView valid_view(vec->GetValidRawData(), size);
    for (size_t i = 0; i < size; ++i) {
        data[i] = data_view[i];
        valid[i] = valid_view[i];
    }
}

// ============================================================================
// NOT Operation Tests
// ============================================================================

// Three-valued logic NOT truth table:
// | A     | NOT A |
// |-------|-------|
// | T     | F     |
// | F     | T     |
// | NULL  | NULL  |

TEST(ThreeValuedLogicOpTest, NotTrueReturnsFalse) {
    // NOT TRUE = FALSE
    auto vec = CreateColumnVector({true}, {true});
    ThreeValuedLogicOp::Not(vec);

    std::vector<bool> data, valid;
    ExtractResults(vec, data, valid);

    EXPECT_FALSE(data[0]);  // NOT TRUE = FALSE
    EXPECT_TRUE(valid[0]);  // Result is valid (determined)
}

TEST(ThreeValuedLogicOpTest, NotFalseReturnsTrue) {
    // NOT FALSE = TRUE
    auto vec = CreateColumnVector({false}, {true});
    ThreeValuedLogicOp::Not(vec);

    std::vector<bool> data, valid;
    ExtractResults(vec, data, valid);

    EXPECT_TRUE(data[0]);   // NOT FALSE = TRUE
    EXPECT_TRUE(valid[0]);  // Result is valid (determined)
}

TEST(ThreeValuedLogicOpTest, NotNullReturnsNull) {
    // NOT NULL = NULL
    auto vec = CreateColumnVector({false}, {false});
    ThreeValuedLogicOp::Not(vec);

    std::vector<bool> data, valid;
    ExtractResults(vec, data, valid);

    EXPECT_FALSE(data[0]);   // Data is false (but meaningless since invalid)
    EXPECT_FALSE(valid[0]);  // Result is NULL (undetermined)
}

TEST(ThreeValuedLogicOpTest, NotMultipleValues) {
    // Test multiple values at once
    // Input:  [T,    F,    NULL]
    // Output: [F,    T,    NULL]
    auto vec = CreateColumnVector({true, false, false}, {true, true, false});
    ThreeValuedLogicOp::Not(vec);

    std::vector<bool> data, valid;
    ExtractResults(vec, data, valid);

    EXPECT_FALSE(data[0]);   // NOT TRUE = FALSE
    EXPECT_TRUE(valid[0]);   // Valid
    EXPECT_TRUE(data[1]);    // NOT FALSE = TRUE
    EXPECT_TRUE(valid[1]);   // Valid
    EXPECT_FALSE(data[2]);   // NOT NULL -> data is false
    EXPECT_FALSE(valid[2]);  // NULL
}

// ============================================================================
// AND Operation Tests
// ============================================================================

// Three-valued logic AND truth table:
// | A     | B     | A AND B |
// |-------|-------|---------|
// | T     | T     | T       |
// | T     | F     | F       |
// | T     | NULL  | NULL    |
// | F     | T     | F       |
// | F     | F     | F       |
// | F     | NULL  | F       |  <- F AND NULL = F (determined)
// | NULL  | T     | NULL    |
// | NULL  | F     | F       |  <- NULL AND F = F (determined)
// | NULL  | NULL  | NULL    |

TEST(ThreeValuedLogicOpTest, AndTrueTrue) {
    auto left = CreateColumnVector({true}, {true});
    auto right = CreateColumnVector({true}, {true});
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_TRUE(data[0]);   // T AND T = T
    EXPECT_TRUE(valid[0]);  // Valid
}

TEST(ThreeValuedLogicOpTest, AndTrueFalse) {
    auto left = CreateColumnVector({true}, {true});
    auto right = CreateColumnVector({false}, {true});
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);  // T AND F = F
    EXPECT_TRUE(valid[0]);  // Valid
}

TEST(ThreeValuedLogicOpTest, AndTrueNull) {
    auto left = CreateColumnVector({true}, {true});
    auto right = CreateColumnVector({false}, {false});  // NULL
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);   // T AND NULL -> data is false
    EXPECT_FALSE(valid[0]);  // NULL (undetermined)
}

TEST(ThreeValuedLogicOpTest, AndFalseTrue) {
    auto left = CreateColumnVector({false}, {true});
    auto right = CreateColumnVector({true}, {true});
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);  // F AND T = F
    EXPECT_TRUE(valid[0]);  // Valid
}

TEST(ThreeValuedLogicOpTest, AndFalseFalse) {
    auto left = CreateColumnVector({false}, {true});
    auto right = CreateColumnVector({false}, {true});
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);  // F AND F = F
    EXPECT_TRUE(valid[0]);  // Valid
}

TEST(ThreeValuedLogicOpTest, AndFalseNull) {
    // Key test: F AND NULL = F (determined, not NULL!)
    auto left = CreateColumnVector({false}, {true});
    auto right = CreateColumnVector({false}, {false});  // NULL
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);  // F AND NULL = F
    EXPECT_TRUE(valid[0]);  // Valid! (determined because F AND anything = F)
}

TEST(ThreeValuedLogicOpTest, AndNullTrue) {
    auto left = CreateColumnVector({false}, {false});  // NULL
    auto right = CreateColumnVector({true}, {true});
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);   // NULL AND T -> data is false
    EXPECT_FALSE(valid[0]);  // NULL (undetermined)
}

TEST(ThreeValuedLogicOpTest, AndNullFalse) {
    // Key test: NULL AND F = F (determined, not NULL!)
    auto left = CreateColumnVector({false}, {false});  // NULL
    auto right = CreateColumnVector({false}, {true});
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);  // NULL AND F = F
    EXPECT_TRUE(valid[0]);  // Valid! (determined because anything AND F = F)
}

TEST(ThreeValuedLogicOpTest, AndNullNull) {
    auto left = CreateColumnVector({false}, {false});   // NULL
    auto right = CreateColumnVector({false}, {false});  // NULL
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);   // NULL AND NULL -> data is false
    EXPECT_FALSE(valid[0]);  // NULL
}

TEST(ThreeValuedLogicOpTest, AndMultipleValues) {
    // Test all 9 combinations in one test
    // Left:  [T,    T,    T,    F,    F,    F,    NULL, NULL, NULL]
    // Right: [T,    F,    NULL, T,    F,    NULL, T,    F,    NULL]
    // Exp:   [T,    F,    NULL, F,    F,    F,    NULL, F,    NULL]
    auto left = CreateColumnVector(
        {true, true, true, false, false, false, false, false, false},
        {true, true, true, true, true, true, false, false, false});
    auto right = CreateColumnVector(
        {true, false, false, true, false, false, true, false, false},
        {true, true, false, true, true, false, true, true, false});
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    // T AND T = T
    EXPECT_TRUE(data[0]);
    EXPECT_TRUE(valid[0]);
    // T AND F = F
    EXPECT_FALSE(data[1]);
    EXPECT_TRUE(valid[1]);
    // T AND NULL = NULL
    EXPECT_FALSE(data[2]);
    EXPECT_FALSE(valid[2]);
    // F AND T = F
    EXPECT_FALSE(data[3]);
    EXPECT_TRUE(valid[3]);
    // F AND F = F
    EXPECT_FALSE(data[4]);
    EXPECT_TRUE(valid[4]);
    // F AND NULL = F (determined!)
    EXPECT_FALSE(data[5]);
    EXPECT_TRUE(valid[5]);
    // NULL AND T = NULL
    EXPECT_FALSE(data[6]);
    EXPECT_FALSE(valid[6]);
    // NULL AND F = F (determined!)
    EXPECT_FALSE(data[7]);
    EXPECT_TRUE(valid[7]);
    // NULL AND NULL = NULL
    EXPECT_FALSE(data[8]);
    EXPECT_FALSE(valid[8]);
}

TEST(ThreeValuedLogicOpTest, AndDoesNotModifyRight) {
    auto left = CreateColumnVector({true}, {true});
    auto right = CreateColumnVector({false}, {false});  // NULL

    // Store original right values
    std::vector<bool> orig_right_data, orig_right_valid;
    ExtractResults(right, orig_right_data, orig_right_valid);

    ThreeValuedLogicOp::And(left, right);

    // Verify right was not modified
    std::vector<bool> new_right_data, new_right_valid;
    ExtractResults(right, new_right_data, new_right_valid);

    EXPECT_EQ(orig_right_data, new_right_data);
    EXPECT_EQ(orig_right_valid, new_right_valid);
}

// ============================================================================
// OR Operation Tests
// ============================================================================

// Three-valued logic OR truth table:
// | A     | B     | A OR B |
// |-------|-------|--------|
// | T     | T     | T      |
// | T     | F     | T      |
// | T     | NULL  | T      |  <- T OR NULL = T (determined)
// | F     | T     | T      |
// | F     | F     | F      |
// | F     | NULL  | NULL   |
// | NULL  | T     | T      |  <- NULL OR T = T (determined)
// | NULL  | F     | NULL   |
// | NULL  | NULL  | NULL   |

TEST(ThreeValuedLogicOpTest, OrTrueTrue) {
    auto left = CreateColumnVector({true}, {true});
    auto right = CreateColumnVector({true}, {true});
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_TRUE(data[0]);   // T OR T = T
    EXPECT_TRUE(valid[0]);  // Valid
}

TEST(ThreeValuedLogicOpTest, OrTrueFalse) {
    auto left = CreateColumnVector({true}, {true});
    auto right = CreateColumnVector({false}, {true});
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_TRUE(data[0]);   // T OR F = T
    EXPECT_TRUE(valid[0]);  // Valid
}

TEST(ThreeValuedLogicOpTest, OrTrueNull) {
    // Key test: T OR NULL = T (determined, not NULL!)
    auto left = CreateColumnVector({true}, {true});
    auto right = CreateColumnVector({false}, {false});  // NULL
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_TRUE(data[0]);   // T OR NULL = T
    EXPECT_TRUE(valid[0]);  // Valid! (determined because T OR anything = T)
}

TEST(ThreeValuedLogicOpTest, OrFalseTrue) {
    auto left = CreateColumnVector({false}, {true});
    auto right = CreateColumnVector({true}, {true});
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_TRUE(data[0]);   // F OR T = T
    EXPECT_TRUE(valid[0]);  // Valid
}

TEST(ThreeValuedLogicOpTest, OrFalseFalse) {
    auto left = CreateColumnVector({false}, {true});
    auto right = CreateColumnVector({false}, {true});
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);  // F OR F = F
    EXPECT_TRUE(valid[0]);  // Valid
}

TEST(ThreeValuedLogicOpTest, OrFalseNull) {
    auto left = CreateColumnVector({false}, {true});
    auto right = CreateColumnVector({false}, {false});  // NULL
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);   // F OR NULL -> data is false
    EXPECT_FALSE(valid[0]);  // NULL (undetermined)
}

TEST(ThreeValuedLogicOpTest, OrNullTrue) {
    // Key test: NULL OR T = T (determined, not NULL!)
    auto left = CreateColumnVector({false}, {false});  // NULL
    auto right = CreateColumnVector({true}, {true});
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_TRUE(data[0]);   // NULL OR T = T
    EXPECT_TRUE(valid[0]);  // Valid! (determined because anything OR T = T)
}

TEST(ThreeValuedLogicOpTest, OrNullFalse) {
    auto left = CreateColumnVector({false}, {false});  // NULL
    auto right = CreateColumnVector({false}, {true});
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);   // NULL OR F -> data is false
    EXPECT_FALSE(valid[0]);  // NULL (undetermined)
}

TEST(ThreeValuedLogicOpTest, OrNullNull) {
    auto left = CreateColumnVector({false}, {false});   // NULL
    auto right = CreateColumnVector({false}, {false});  // NULL
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    EXPECT_FALSE(data[0]);   // NULL OR NULL -> data is false
    EXPECT_FALSE(valid[0]);  // NULL
}

TEST(ThreeValuedLogicOpTest, OrMultipleValues) {
    // Test all 9 combinations in one test
    // Left:  [T,    T,    T,    F,    F,    F,    NULL, NULL, NULL]
    // Right: [T,    F,    NULL, T,    F,    NULL, T,    F,    NULL]
    // Exp:   [T,    T,    T,    T,    F,    NULL, T,    NULL, NULL]
    auto left = CreateColumnVector(
        {true, true, true, false, false, false, false, false, false},
        {true, true, true, true, true, true, false, false, false});
    auto right = CreateColumnVector(
        {true, false, false, true, false, false, true, false, false},
        {true, true, false, true, true, false, true, true, false});
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> data, valid;
    ExtractResults(left, data, valid);

    // T OR T = T
    EXPECT_TRUE(data[0]);
    EXPECT_TRUE(valid[0]);
    // T OR F = T
    EXPECT_TRUE(data[1]);
    EXPECT_TRUE(valid[1]);
    // T OR NULL = T (determined!)
    EXPECT_TRUE(data[2]);
    EXPECT_TRUE(valid[2]);
    // F OR T = T
    EXPECT_TRUE(data[3]);
    EXPECT_TRUE(valid[3]);
    // F OR F = F
    EXPECT_FALSE(data[4]);
    EXPECT_TRUE(valid[4]);
    // F OR NULL = NULL
    EXPECT_FALSE(data[5]);
    EXPECT_FALSE(valid[5]);
    // NULL OR T = T (determined!)
    EXPECT_TRUE(data[6]);
    EXPECT_TRUE(valid[6]);
    // NULL OR F = NULL
    EXPECT_FALSE(data[7]);
    EXPECT_FALSE(valid[7]);
    // NULL OR NULL = NULL
    EXPECT_FALSE(data[8]);
    EXPECT_FALSE(valid[8]);
}

TEST(ThreeValuedLogicOpTest, OrDoesNotModifyRight) {
    auto left = CreateColumnVector({false}, {true});
    auto right = CreateColumnVector({true}, {false});  // NULL with data=true

    // Store original right values
    std::vector<bool> orig_right_data, orig_right_valid;
    ExtractResults(right, orig_right_data, orig_right_valid);

    ThreeValuedLogicOp::Or(left, right);

    // Verify right was not modified
    std::vector<bool> new_right_data, new_right_valid;
    ExtractResults(right, new_right_data, new_right_valid);

    EXPECT_EQ(orig_right_data, new_right_data);
    EXPECT_EQ(orig_right_valid, new_right_valid);
}

// ============================================================================
// De Morgan's Law Tests
// ============================================================================

// De Morgan's Law: NOT (A OR B) = NOT A AND NOT B
// De Morgan's Law: NOT (A AND B) = NOT A OR NOT B
// These should hold even with NULL values in three-valued logic

TEST(ThreeValuedLogicOpTest, DeMorganOrToAnd) {
    // Test: NOT (A OR B) == NOT A AND NOT B
    // With all 9 combinations of A, B in {T, F, NULL}

    std::vector<bool> a_data = {
        true, true, true, false, false, false, false, false, false};
    std::vector<bool> a_valid = {
        true, true, true, true, true, true, false, false, false};
    std::vector<bool> b_data = {
        true, false, false, true, false, false, true, false, false};
    std::vector<bool> b_valid = {
        true, true, false, true, true, false, true, true, false};

    // Method 1: NOT (A OR B)
    auto left1 = CreateColumnVector(a_data, a_valid);
    auto right1 = CreateColumnVector(b_data, b_valid);
    ThreeValuedLogicOp::Or(left1, right1);
    ThreeValuedLogicOp::Not(left1);

    // Method 2: NOT A AND NOT B
    auto left2 = CreateColumnVector(a_data, a_valid);
    auto right2 = CreateColumnVector(b_data, b_valid);
    ThreeValuedLogicOp::Not(left2);
    ThreeValuedLogicOp::Not(right2);
    ThreeValuedLogicOp::And(left2, right2);

    std::vector<bool> result1_data, result1_valid;
    std::vector<bool> result2_data, result2_valid;
    ExtractResults(left1, result1_data, result1_valid);
    ExtractResults(left2, result2_data, result2_valid);

    // Results should be identical
    EXPECT_EQ(result1_data, result2_data);
    EXPECT_EQ(result1_valid, result2_valid);
}

TEST(ThreeValuedLogicOpTest, DeMorganAndToOr) {
    // Test: NOT (A AND B) == NOT A OR NOT B
    // With all 9 combinations of A, B in {T, F, NULL}

    std::vector<bool> a_data = {
        true, true, true, false, false, false, false, false, false};
    std::vector<bool> a_valid = {
        true, true, true, true, true, true, false, false, false};
    std::vector<bool> b_data = {
        true, false, false, true, false, false, true, false, false};
    std::vector<bool> b_valid = {
        true, true, false, true, true, false, true, true, false};

    // Method 1: NOT (A AND B)
    auto left1 = CreateColumnVector(a_data, a_valid);
    auto right1 = CreateColumnVector(b_data, b_valid);
    ThreeValuedLogicOp::And(left1, right1);
    ThreeValuedLogicOp::Not(left1);

    // Method 2: NOT A OR NOT B
    auto left2 = CreateColumnVector(a_data, a_valid);
    auto right2 = CreateColumnVector(b_data, b_valid);
    ThreeValuedLogicOp::Not(left2);
    ThreeValuedLogicOp::Not(right2);
    ThreeValuedLogicOp::Or(left2, right2);

    std::vector<bool> result1_data, result1_valid;
    std::vector<bool> result2_data, result2_valid;
    ExtractResults(left1, result1_data, result1_valid);
    ExtractResults(left2, result2_data, result2_valid);

    // Results should be identical
    EXPECT_EQ(result1_data, result2_data);
    EXPECT_EQ(result1_valid, result2_valid);
}

// ============================================================================
// Large Scale Tests
// ============================================================================

TEST(ThreeValuedLogicOpTest, LargeScaleAnd) {
    const size_t size = 10000;
    std::vector<bool> left_data(size), left_valid(size);
    std::vector<bool> right_data(size), right_valid(size);

    // Generate test data
    for (size_t i = 0; i < size; ++i) {
        left_data[i] = (i % 3) == 0;
        left_valid[i] = (i % 5) != 0;
        right_data[i] = (i % 2) == 0;
        right_valid[i] = (i % 7) != 0;
    }

    auto left = CreateColumnVector(left_data, left_valid);
    auto right = CreateColumnVector(right_data, right_valid);
    ThreeValuedLogicOp::And(left, right);

    std::vector<bool> result_data, result_valid;
    ExtractResults(left, result_data, result_valid);

    // Verify results match expected three-valued logic
    for (size_t i = 0; i < size; ++i) {
        bool l_data = left_data[i];
        bool l_valid = left_valid[i];
        bool r_data = right_data[i];
        bool r_valid = right_valid[i];

        // Expected data: left AND right
        bool exp_data = l_data && r_data;

        // Expected valid for AND:
        // Valid if: (left is definitely false) OR (right is definitely false)
        // OR (both are valid)
        bool left_definitely_false = l_valid && !l_data;
        bool right_definitely_false = r_valid && !r_data;
        bool exp_valid = left_definitely_false || right_definitely_false ||
                         (l_valid && r_valid);

        EXPECT_EQ(result_data[i], exp_data) << "Data mismatch at index " << i;
        EXPECT_EQ(result_valid[i], exp_valid)
            << "Valid mismatch at index " << i;
    }
}

TEST(ThreeValuedLogicOpTest, LargeScaleOr) {
    const size_t size = 10000;
    std::vector<bool> left_data(size), left_valid(size);
    std::vector<bool> right_data(size), right_valid(size);

    // Generate test data
    for (size_t i = 0; i < size; ++i) {
        left_data[i] = (i % 3) == 0;
        left_valid[i] = (i % 5) != 0;
        right_data[i] = (i % 2) == 0;
        right_valid[i] = (i % 7) != 0;
    }

    auto left = CreateColumnVector(left_data, left_valid);
    auto right = CreateColumnVector(right_data, right_valid);
    ThreeValuedLogicOp::Or(left, right);

    std::vector<bool> result_data, result_valid;
    ExtractResults(left, result_data, result_valid);

    // Verify results match expected three-valued logic
    for (size_t i = 0; i < size; ++i) {
        bool l_data = left_data[i];
        bool l_valid = left_valid[i];
        bool r_data = right_data[i];
        bool r_valid = right_valid[i];

        // Expected data: left OR right
        bool exp_data = l_data || r_data;

        // Expected valid for OR:
        // Valid if: (left is definitely true) OR (right is definitely true)
        // OR (both are valid)
        bool left_definitely_true = l_valid && l_data;
        bool right_definitely_true = r_valid && r_data;
        bool exp_valid = left_definitely_true || right_definitely_true ||
                         (l_valid && r_valid);

        EXPECT_EQ(result_data[i], exp_data) << "Data mismatch at index " << i;
        EXPECT_EQ(result_valid[i], exp_valid)
            << "Valid mismatch at index " << i;
    }
}
