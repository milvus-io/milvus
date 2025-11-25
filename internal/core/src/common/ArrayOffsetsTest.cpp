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
#include <thread>
#include <vector>

#include "common/ArrayOffsets.h"

using namespace milvus;

class ArrayOffsetsTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }
};

TEST_F(ArrayOffsetsTest, SealedBasic) {
    // Create a simple ArrayOffsetsSealed manually
    ArrayOffsetsSealed offsets;
    offsets.doc_count = 3;
    offsets.doc_to_element_start_.resize(4);  // doc_count + 1

    // doc 0: 2 elements (elem 0, 1)
    // doc 1: 3 elements (elem 2, 3, 4)
    // doc 2: 1 element  (elem 5)
    offsets.doc_to_element_start_[0] = 0;
    offsets.doc_to_element_start_[1] = 2;
    offsets.doc_to_element_start_[2] = 5;
    offsets.doc_to_element_start_[3] = 6;  // total element count

    offsets.element_doc_ids = {0, 0, 1, 1, 1, 2};

    // Test GetDocCount
    EXPECT_EQ(offsets.GetDocCount(), 3);

    // Test GetTotalElementCount
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

    // Test ElementIDToDoc
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(0);
        EXPECT_EQ(doc_id, 0);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(1);
        EXPECT_EQ(doc_id, 0);
        EXPECT_EQ(elem_idx, 1);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(2);
        EXPECT_EQ(doc_id, 1);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(4);
        EXPECT_EQ(doc_id, 1);
        EXPECT_EQ(elem_idx, 2);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(5);
        EXPECT_EQ(doc_id, 2);
        EXPECT_EQ(elem_idx, 0);
    }

    // Test DocIDToElementID
    {
        auto [start, end] = offsets.DocIDToElementID(0);
        EXPECT_EQ(start, 0);
        EXPECT_EQ(end, 2);
    }
    {
        auto [start, end] = offsets.DocIDToElementID(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 5);
    }
    {
        auto [start, end] = offsets.DocIDToElementID(2);
        EXPECT_EQ(start, 5);
        EXPECT_EQ(end, 6);
    }
    // When doc_id == doc_count, return (total_elements, total_elements)
    {
        auto [start, end] = offsets.DocIDToElementID(3);
        EXPECT_EQ(start, 6);
        EXPECT_EQ(end, 6);
    }
}

TEST_F(ArrayOffsetsTest, SealedDocBitsetToElementBitset) {
    ArrayOffsetsSealed offsets;
    offsets.doc_count = 3;
    offsets.doc_to_element_start_ = {0, 2, 5, 6};
    offsets.element_doc_ids = {0, 0, 1, 1, 1, 2};

    // doc_bitset: doc 0 = true, doc 1 = false, doc 2 = true
    TargetBitmap doc_bitset(3);
    doc_bitset[0] = true;
    doc_bitset[1] = false;
    doc_bitset[2] = true;

    TargetBitmap valid_doc_bitset(3, true);

    TargetBitmapView doc_view(doc_bitset.data(), doc_bitset.size());
    TargetBitmapView valid_view(valid_doc_bitset.data(), valid_doc_bitset.size());

    auto [elem_bitset, valid_elem_bitset] =
        offsets.DocBitsetToElementBitset(doc_view, valid_view);

    EXPECT_EQ(elem_bitset.size(), 6);
    // Elements of doc 0 (elem 0, 1) should be true
    EXPECT_TRUE(elem_bitset[0]);
    EXPECT_TRUE(elem_bitset[1]);
    // Elements of doc 1 (elem 2, 3, 4) should be false
    EXPECT_FALSE(elem_bitset[2]);
    EXPECT_FALSE(elem_bitset[3]);
    EXPECT_FALSE(elem_bitset[4]);
    // Elements of doc 2 (elem 5) should be true
    EXPECT_TRUE(elem_bitset[5]);
}

TEST_F(ArrayOffsetsTest, SealedEmptyArrays) {
    // Test with some docs having empty arrays
    ArrayOffsetsSealed offsets;
    offsets.doc_count = 4;
    offsets.doc_to_element_start_ = {0, 2, 2, 5, 5};  // doc 1 and doc 3 are empty

    offsets.element_doc_ids = {0, 0, 2, 2, 2};

    EXPECT_EQ(offsets.GetDocCount(), 4);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);

    // Doc 1 has no elements
    {
        auto [start, end] = offsets.DocIDToElementID(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 2);  // empty range
    }
    // Doc 3 has no elements
    {
        auto [start, end] = offsets.DocIDToElementID(3);
        EXPECT_EQ(start, 5);
        EXPECT_EQ(end, 5);  // empty range
    }
}

TEST_F(ArrayOffsetsTest, GrowingBasicInsert) {
    ArrayOffsetsGrowing offsets;

    // Insert docs in order
    std::vector<int32_t> lens1 = {2};  // doc 0: 2 elements
    offsets.Insert(0, lens1.data(), 1);

    std::vector<int32_t> lens2 = {3};  // doc 1: 3 elements
    offsets.Insert(1, lens2.data(), 1);

    std::vector<int32_t> lens3 = {1};  // doc 2: 1 element
    offsets.Insert(2, lens3.data(), 1);

    EXPECT_EQ(offsets.GetDocCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

    // Test ElementIDToDoc
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(0);
        EXPECT_EQ(doc_id, 0);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(2);
        EXPECT_EQ(doc_id, 1);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(5);
        EXPECT_EQ(doc_id, 2);
        EXPECT_EQ(elem_idx, 0);
    }

    // Test DocIDToElementID
    {
        auto [start, end] = offsets.DocIDToElementID(0);
        EXPECT_EQ(start, 0);
        EXPECT_EQ(end, 2);
    }
    {
        auto [start, end] = offsets.DocIDToElementID(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 5);
    }
    // When doc_id == doc_count, return (total_elements, total_elements)
    {
        auto [start, end] = offsets.DocIDToElementID(3);
        EXPECT_EQ(start, 6);
        EXPECT_EQ(end, 6);
    }
}

TEST_F(ArrayOffsetsTest, GrowingBatchInsert) {
    ArrayOffsetsGrowing offsets;

    // Insert multiple docs at once
    std::vector<int32_t> lens = {2, 3, 1};  // doc 0, 1, 2
    offsets.Insert(0, lens.data(), 3);

    EXPECT_EQ(offsets.GetDocCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

    {
        auto [start, end] = offsets.DocIDToElementID(0);
        EXPECT_EQ(start, 0);
        EXPECT_EQ(end, 2);
    }
    {
        auto [start, end] = offsets.DocIDToElementID(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 5);
    }
    {
        auto [start, end] = offsets.DocIDToElementID(2);
        EXPECT_EQ(start, 5);
        EXPECT_EQ(end, 6);
    }
}

TEST_F(ArrayOffsetsTest, GrowingOutOfOrderInsert) {
    ArrayOffsetsGrowing offsets;

    // Insert out of order - doc 2 arrives before doc 1
    std::vector<int32_t> lens0 = {2};
    offsets.Insert(0, lens0.data(), 1);  // doc 0

    std::vector<int32_t> lens2 = {1};
    offsets.Insert(2, lens2.data(), 1);  // doc 2 (pending)

    // doc 1 not inserted yet, so only doc 0 should be committed
    EXPECT_EQ(offsets.GetDocCount(), 1);
    EXPECT_EQ(offsets.GetTotalElementCount(), 2);

    // Now insert doc 1, which should drain pending doc 2
    std::vector<int32_t> lens1 = {3};
    offsets.Insert(1, lens1.data(), 1);  // doc 1

    // Now all 3 docs should be committed
    EXPECT_EQ(offsets.GetDocCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

    // Verify order is correct
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(0);
        EXPECT_EQ(doc_id, 0);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(2);
        EXPECT_EQ(doc_id, 1);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(5);
        EXPECT_EQ(doc_id, 2);
    }
}

TEST_F(ArrayOffsetsTest, GrowingEmptyArrays) {
    ArrayOffsetsGrowing offsets;

    // Insert docs with some empty arrays
    std::vector<int32_t> lens = {2, 0, 3, 0};  // doc 1 and doc 3 are empty
    offsets.Insert(0, lens.data(), 4);

    EXPECT_EQ(offsets.GetDocCount(), 4);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);

    // Doc 1 has no elements
    {
        auto [start, end] = offsets.DocIDToElementID(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 2);
    }
    // Doc 3 has no elements
    {
        auto [start, end] = offsets.DocIDToElementID(3);
        EXPECT_EQ(start, 5);
        EXPECT_EQ(end, 5);
    }
}

TEST_F(ArrayOffsetsTest, GrowingDocBitsetToElementBitset) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {2, 3, 1};
    offsets.Insert(0, lens.data(), 3);

    TargetBitmap doc_bitset(3);
    doc_bitset[0] = true;
    doc_bitset[1] = false;
    doc_bitset[2] = true;

    TargetBitmap valid_doc_bitset(3, true);

    TargetBitmapView doc_view(doc_bitset.data(), doc_bitset.size());
    TargetBitmapView valid_view(valid_doc_bitset.data(), valid_doc_bitset.size());

    auto [elem_bitset, valid_elem_bitset] =
        offsets.DocBitsetToElementBitset(doc_view, valid_view);

    EXPECT_EQ(elem_bitset.size(), 6);
    EXPECT_TRUE(elem_bitset[0]);
    EXPECT_TRUE(elem_bitset[1]);
    EXPECT_FALSE(elem_bitset[2]);
    EXPECT_FALSE(elem_bitset[3]);
    EXPECT_FALSE(elem_bitset[4]);
    EXPECT_TRUE(elem_bitset[5]);
}

TEST_F(ArrayOffsetsTest, GrowingConcurrentRead) {
    ArrayOffsetsGrowing offsets;

    // Insert initial data
    std::vector<int32_t> lens = {2, 3, 1};
    offsets.Insert(0, lens.data(), 3);

    // Concurrent reads should be safe
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&offsets]() {
            for (int i = 0; i < 1000; ++i) {
                auto doc_count = offsets.GetDocCount();
                auto elem_count = offsets.GetTotalElementCount();
                EXPECT_GE(doc_count, 0);
                EXPECT_GE(elem_count, 0);

                if (doc_count > 0) {
                    auto [start, end] = offsets.DocIDToElementID(0);
                    EXPECT_GE(start, 0);
                    EXPECT_GE(end, start);
                }

                if (elem_count > 0) {
                    auto [doc_id, elem_idx] = offsets.ElementIDToDoc(0);
                    EXPECT_GE(doc_id, 0);
                    EXPECT_GE(elem_idx, 0);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}

TEST_F(ArrayOffsetsTest, SingleDoc) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {5};
    offsets.Insert(0, lens.data(), 1);

    EXPECT_EQ(offsets.GetDocCount(), 1);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);

    for (int i = 0; i < 5; ++i) {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(i);
        EXPECT_EQ(doc_id, 0);
        EXPECT_EQ(elem_idx, i);
    }

    auto [start, end] = offsets.DocIDToElementID(0);
    EXPECT_EQ(start, 0);
    EXPECT_EQ(end, 5);
}

TEST_F(ArrayOffsetsTest, SingleElementPerDoc) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {1, 1, 1, 1, 1};
    offsets.Insert(0, lens.data(), 5);

    EXPECT_EQ(offsets.GetDocCount(), 5);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);

    for (int i = 0; i < 5; ++i) {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(i);
        EXPECT_EQ(doc_id, i);
        EXPECT_EQ(elem_idx, 0);

        auto [start, end] = offsets.DocIDToElementID(i);
        EXPECT_EQ(start, i);
        EXPECT_EQ(end, i + 1);
    }
}

TEST_F(ArrayOffsetsTest, LargeArrayLength) {
    ArrayOffsetsGrowing offsets;

    // Single doc with many elements
    std::vector<int32_t> lens = {10000};
    offsets.Insert(0, lens.data(), 1);

    EXPECT_EQ(offsets.GetDocCount(), 1);
    EXPECT_EQ(offsets.GetTotalElementCount(), 10000);

    // Test first, middle, and last elements
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(0);
        EXPECT_EQ(doc_id, 0);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(5000);
        EXPECT_EQ(doc_id, 0);
        EXPECT_EQ(elem_idx, 5000);
    }
    {
        auto [doc_id, elem_idx] = offsets.ElementIDToDoc(9999);
        EXPECT_EQ(doc_id, 0);
        EXPECT_EQ(elem_idx, 9999);
    }
}
