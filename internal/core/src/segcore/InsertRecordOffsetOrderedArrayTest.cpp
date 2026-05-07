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

#include <gtest/gtest.h>
#include <stdint.h>
#include <random>
#include <string>
#include <vector>

#include "common/Types.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "segcore/InsertRecord.h"
#include "segcore/TimestampIndex.h"

using namespace milvus;
using namespace milvus::segcore;

template <typename T>
class TypedOffsetOrderedArrayTest : public testing::Test {
 public:
    void
    SetUp() override {
        er = std::default_random_engine(42);
    }

    void
    TearDown() override {
    }

 protected:
    void
    insert(T pk) {
        map_.insert(pk, offset_++);
        data_.push_back(pk);
        std::sort(data_.begin(), data_.end());
    }

    void
    seal() {
        map_.seal();
    }

    std::vector<T>
    random_generate(int num) {
        std::vector<T> res;
        for (int i = 0; i < num; i++) {
            if constexpr (std::is_same_v<std::string, T>) {
                res.push_back(std::to_string(er()));
            } else {
                res.push_back(static_cast<T>(er()));
            }
        }
        return res;
    }

 protected:
    int64_t offset_ = 0;
    std::vector<T> data_;
    milvus::segcore::OffsetOrderedArray<T> map_;
    std::default_random_engine er;
};

using TypeOfPks = testing::Types<int64_t, std::string>;
TYPED_TEST_SUITE_P(TypedOffsetOrderedArrayTest);

TYPED_TEST_P(TypedOffsetOrderedArrayTest, find_first_n) {
    // not sealed.
    ASSERT_ANY_THROW(this->map_.find_first_n(Unlimited, {}));

    // insert 10 entities.
    int num = 10;
    auto data = this->random_generate(num);
    for (const auto& x : data) {
        this->insert(x);
    }

    // seal.
    this->seal();

    // all is satisfied.
    {
        BitsetType all(num);
        BitsetTypeView all_view(all.data(), num);
        {
            auto [offsets, has_more_res] =
                this->map_.find_first_n(num / 2, all_view);
            ASSERT_EQ(num / 2, offsets.size());
            ASSERT_TRUE(has_more_res);
            for (int i = 1; i < offsets.size(); i++) {
                ASSERT_TRUE(data[offsets[i - 1]] <= data[offsets[i]]);
            }
        }
        {
            auto [offsets, has_more_res] =
                this->map_.find_first_n(Unlimited, all_view);
            ASSERT_EQ(num, offsets.size());
            ASSERT_FALSE(has_more_res);
            for (int i = 1; i < offsets.size(); i++) {
                ASSERT_TRUE(data[offsets[i - 1]] <= data[offsets[i]]);
            }
        }
    }
    {
        // corner case, segment offset exceeds the size of bitset.
        BitsetType all_minus_1(num - 1);
        BitsetTypeView all_minus_1_view(all_minus_1.data(), num - 1);
        {
            auto [offsets, has_more_res] =
                this->map_.find_first_n(num / 2, all_minus_1_view);
            ASSERT_EQ(num / 2, offsets.size());
            ASSERT_TRUE(has_more_res);
            for (int i = 1; i < offsets.size(); i++) {
                ASSERT_TRUE(data[offsets[i - 1]] <= data[offsets[i]]);
            }
        }
        {
            auto [offsets, has_more_res] =
                this->map_.find_first_n(Unlimited, all_minus_1_view);
            ASSERT_EQ(all_minus_1.size(), offsets.size());
            ASSERT_FALSE(has_more_res);
            for (int i = 1; i < offsets.size(); i++) {
                ASSERT_TRUE(data[offsets[i - 1]] <= data[offsets[i]]);
            }
        }
    }
    {
        // none is satisfied.
        BitsetType none(num);
        none.set();
        BitsetTypeView none_view(none.data(), num);
        auto result_pair = this->map_.find_first_n(num / 2, none_view);
        ASSERT_EQ(0, result_pair.first.size());
        ASSERT_FALSE(result_pair.second);
        result_pair = this->map_.find_first_n(NoLimit, none_view);
        ASSERT_EQ(0, result_pair.first.size());
        ASSERT_FALSE(result_pair.second);
    }
}

TYPED_TEST_P(TypedOffsetOrderedArrayTest, find_first_n_element) {
    // Setup: insert 5 docs with sequential PKs
    int num = 5;
    int array_len = 3;
    auto data = this->random_generate(num);
    for (const auto& x : data) {
        this->insert(x);
    }
    this->seal();

    // Build ArrayOffsets: each doc has array_len elements
    std::vector<int32_t> row_to_element_start = {0};
    for (int doc = 0; doc < num; doc++) {
        row_to_element_start.push_back(
            static_cast<int32_t>((doc + 1) * array_len));
    }
    auto array_offsets =
        std::make_shared<ArrayOffsetsSealed>(std::move(row_to_element_start));

    int total_elements = num * array_len;  // 15

    // Case 1: all elements pass filter
    {
        BitsetType all(total_elements);
        all.reset();
        BitsetTypeView view(all.data(), total_elements);
        auto [doc_offsets, elem_indices, has_more] =
            this->map_.find_first_n_element(
                total_elements, view, array_offsets.get(), std::nullopt);
        ASSERT_EQ(doc_offsets.size(), num);
        for (size_t i = 0; i < doc_offsets.size(); i++) {
            ASSERT_EQ(elem_indices[i].size(), array_len);
        }
        ASSERT_FALSE(has_more);
    }

    // Case 2: limit counts elements, not docs
    {
        BitsetType all(total_elements);
        all.reset();
        BitsetTypeView view(all.data(), total_elements);
        auto [doc_offsets, elem_indices, has_more] =
            this->map_.find_first_n_element(
                4, view, array_offsets.get(), std::nullopt);
        int total = 0;
        for (auto& indices : elem_indices) {
            total += indices.size();
        }
        ASSERT_EQ(total, 4);
        ASSERT_EQ(doc_offsets.size(), 2);
        ASSERT_EQ(elem_indices[0].size(), 3);
        ASSERT_EQ(elem_indices[1].size(), 1);
        ASSERT_TRUE(has_more);
    }

    // Case 3: partial elements pass (only elem_idx=1 per doc)
    {
        BitsetType partial(total_elements);
        partial.set();
        for (int doc = 0; doc < num; doc++) {
            partial.reset(doc * array_len + 1);
        }
        BitsetTypeView view(partial.data(), total_elements);
        auto [doc_offsets, elem_indices, has_more] =
            this->map_.find_first_n_element(
                total_elements, view, array_offsets.get(), std::nullopt);
        ASSERT_EQ(doc_offsets.size(), num);
        for (size_t i = 0; i < doc_offsets.size(); i++) {
            ASSERT_EQ(elem_indices[i].size(), 1);
            ASSERT_EQ(elem_indices[i][0], 1);
        }
    }

    // Case 4: no elements pass
    {
        BitsetType none(total_elements);
        none.set();
        BitsetTypeView view(none.data(), total_elements);
        auto [doc_offsets, elem_indices, has_more] =
            this->map_.find_first_n_element(
                total_elements, view, array_offsets.get(), std::nullopt);
        ASSERT_EQ(doc_offsets.size(), 0);
        ASSERT_EQ(elem_indices.size(), 0);
        ASSERT_FALSE(has_more);
    }

    // Case 5: element bitset smaller than array_offsets
    {
        int smaller_size = total_elements - array_len;
        BitsetType small(smaller_size);
        small.reset();
        BitsetTypeView view(small.data(), smaller_size);
        auto [doc_offsets, elem_indices, has_more] =
            this->map_.find_first_n_element(
                total_elements, view, array_offsets.get(), std::nullopt);
        int total = 0;
        for (auto& indices : elem_indices) {
            total += indices.size();
        }
        ASSERT_EQ(total, smaller_size);
    }
}

TYPED_TEST_P(TypedOffsetOrderedArrayTest, find_first_n_element_has_more) {
    // Test has_more correctness when limit exactly equals total matching
    // elements. Previously, has_more used (it != end) || (hit_num >= limit)
    // which incorrectly returned true when all data was exhausted.

    int num = 3;
    int array_len = 2;
    // Insert docs with deterministic PKs for reproducibility
    std::vector<TypeParam> data;
    for (int i = 0; i < num; i++) {
        TypeParam pk;
        if constexpr (std::is_same_v<std::string, TypeParam>) {
            pk = std::to_string(i);
        } else {
            pk = static_cast<TypeParam>(i);
        }
        this->insert(pk);
        data.push_back(pk);
    }
    this->seal();

    // Build ArrayOffsets: each doc has array_len elements
    // doc0: [elem0, elem1], doc1: [elem2, elem3], doc2: [elem4, elem5]
    std::vector<int32_t> row_to_element_start = {0};
    for (int doc = 0; doc < num; doc++) {
        row_to_element_start.push_back(
            static_cast<int32_t>((doc + 1) * array_len));
    }
    auto array_offsets =
        std::make_shared<ArrayOffsetsSealed>(std::move(row_to_element_start));

    int total_elements = num * array_len;  // 6

    // Case 1: limit == total matching elements (aligned on doc boundary)
    // All 6 elements pass, limit=6 → should return all, has_more=false
    {
        BitsetType all(total_elements);
        all.reset();
        BitsetTypeView view(all.data(), total_elements);
        auto [doc_offsets, elem_indices, has_more] =
            this->map_.find_first_n_element(
                total_elements, view, array_offsets.get(), std::nullopt);
        int collected = 0;
        for (auto& indices : elem_indices) {
            collected += indices.size();
        }
        ASSERT_EQ(collected, total_elements);
        ASSERT_FALSE(has_more) << "has_more should be false when limit equals "
                                  "total matching elements";
    }

    // Case 2: limit == total matching elements (NOT aligned on doc boundary)
    // Only elem_idx=0 per doc passes → 3 matching elements, limit=3
    {
        BitsetType partial(total_elements);
        partial.set();
        for (int doc = 0; doc < num; doc++) {
            partial.reset(doc *
                          array_len);  // only first element per doc passes
        }
        BitsetTypeView view(partial.data(), total_elements);
        auto [doc_offsets, elem_indices, has_more] =
            this->map_.find_first_n_element(
                num, view, array_offsets.get(), std::nullopt);
        int collected = 0;
        for (auto& indices : elem_indices) {
            collected += indices.size();
        }
        ASSERT_EQ(collected, num);
        ASSERT_FALSE(has_more) << "has_more should be false when limit equals "
                                  "total matching elements (non-aligned)";
    }

    // Case 3: limit < total matching elements → has_more=true
    {
        BitsetType all(total_elements);
        all.reset();
        BitsetTypeView view(all.data(), total_elements);
        auto [doc_offsets, elem_indices, has_more] =
            this->map_.find_first_n_element(
                3, view, array_offsets.get(), std::nullopt);
        int collected = 0;
        for (auto& indices : elem_indices) {
            collected += indices.size();
        }
        ASSERT_EQ(collected, 3);
        ASSERT_TRUE(has_more)
            << "has_more should be true when more elements remain";
    }
}

TYPED_TEST_P(TypedOffsetOrderedArrayTest,
             find_first_n_element_with_iterator_cursor) {
    auto make_pk = [](int i) {
        if constexpr (std::is_same_v<std::string, TypeParam>) {
            return std::to_string(i);
        } else {
            return static_cast<TypeParam>(i);
        }
    };

    int num = 3;
    int array_len = 4;
    for (int i = 0; i < num; i++) {
        this->insert(make_pk(i));
    }
    this->seal();

    std::vector<int32_t> row_to_element_start = {0};
    for (int doc = 0; doc < num; doc++) {
        row_to_element_start.push_back(
            static_cast<int32_t>((doc + 1) * array_len));
    }
    auto array_offsets =
        std::make_shared<ArrayOffsetsSealed>(std::move(row_to_element_start));

    BitsetType bitset(num * array_len);
    bitset.reset();
    for (int e = 0; e < array_len; e++) {
        bitset.set(e);  // Simulate query expr pk >= 1.
    }
    BitsetTypeView view(bitset.data(), bitset.size());

    QueryIteratorCursor cursor;
    cursor.last_pk = make_pk(1);
    cursor.last_element_offset = 1;

    auto [doc_offsets, elem_indices, has_more] =
        this->map_.find_first_n_element(10, view, array_offsets.get(), cursor);
    ASSERT_EQ(doc_offsets, std::vector<int64_t>({1, 2}));
    ASSERT_EQ(elem_indices[0], std::vector<int32_t>({2, 3}));
    ASSERT_EQ(elem_indices[1], std::vector<int32_t>({0, 1, 2, 3}));
    ASSERT_FALSE(has_more);

    auto [limited_docs, limited_indices, limited_has_more] =
        this->map_.find_first_n_element(3, view, array_offsets.get(), cursor);
    ASSERT_EQ(limited_docs, std::vector<int64_t>({1, 2}));
    ASSERT_EQ(limited_indices[0], std::vector<int32_t>({2, 3}));
    ASSERT_EQ(limited_indices[1], std::vector<int32_t>({0}));
    ASSERT_TRUE(limited_has_more);
}

REGISTER_TYPED_TEST_SUITE_P(TypedOffsetOrderedArrayTest,
                            find_first_n,
                            find_first_n_element,
                            find_first_n_element_has_more,
                            find_first_n_element_with_iterator_cursor);
INSTANTIATE_TYPED_TEST_SUITE_P(Prefix, TypedOffsetOrderedArrayTest, TypeOfPks);

// =====================================================================
// VirtualPKOffsetMap tests
// =====================================================================

class VirtualPKOffsetMapTest : public testing::Test {
 protected:
    static constexpr int64_t kSegmentID = 0x123456789ABCDEF0LL;
    static constexpr int64_t kTruncatedSegID = kSegmentID & 0xFFFFFFFF;
    static constexpr int64_t kNumRows = 100;

    VirtualPKOffsetMap map_{kSegmentID, kNumRows};

    // Build a virtual PK from offset
    int64_t
    vpk(int64_t offset) const {
        return (kTruncatedSegID << 32) | (offset & 0xFFFFFFFF);
    }
};

TEST_F(VirtualPKOffsetMapTest, ContainAndFind) {
    // Valid PKs
    EXPECT_TRUE(map_.contain(PkType(vpk(0))));
    EXPECT_TRUE(map_.contain(PkType(vpk(50))));
    EXPECT_TRUE(map_.contain(PkType(vpk(99))));

    // Out of range
    EXPECT_FALSE(map_.contain(PkType(vpk(100))));
    EXPECT_FALSE(map_.contain(PkType(vpk(1000))));

    // Wrong segment ID
    int64_t wrong_seg = ((kTruncatedSegID + 1) << 32) | 0;
    EXPECT_FALSE(map_.contain(PkType(wrong_seg)));

    // Find returns correct offset
    auto result = map_.find(PkType(vpk(42)));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], 42);

    // Find out-of-range returns empty
    result = map_.find(PkType(vpk(100)));
    EXPECT_TRUE(result.empty());
}

TEST_F(VirtualPKOffsetMapTest, FindRange) {
    int64_t num = kNumRows;
    // Equal
    {
        BitsetType bitset(num);
        BitsetTypeView view(bitset.data(), num);
        map_.find_range(
            PkType(vpk(50)), proto::plan::OpType::Equal, view, [](int64_t) {
                return true;
            });
        EXPECT_TRUE(view[50]);
        // Only offset 50 should be set
        int count = 0;
        for (int64_t i = 0; i < num; i++) {
            if (view[i])
                count++;
        }
        EXPECT_EQ(count, 1);
    }
    // GreaterEqual
    {
        BitsetType bitset(num);
        BitsetTypeView view(bitset.data(), num);
        map_.find_range(PkType(vpk(95)),
                        proto::plan::OpType::GreaterEqual,
                        view,
                        [](int64_t) { return true; });
        for (int64_t i = 95; i < num; i++) {
            EXPECT_TRUE(view[i]) << "offset " << i;
        }
        for (int64_t i = 0; i < 95; i++) {
            EXPECT_FALSE(view[i]) << "offset " << i;
        }
    }
    // LessThan
    {
        BitsetType bitset(num);
        BitsetTypeView view(bitset.data(), num);
        map_.find_range(
            PkType(vpk(5)), proto::plan::OpType::LessThan, view, [](int64_t) {
                return true;
            });
        for (int64_t i = 0; i < 5; i++) {
            EXPECT_TRUE(view[i]) << "offset " << i;
        }
        for (int64_t i = 5; i < num; i++) {
            EXPECT_FALSE(view[i]) << "offset " << i;
        }
    }
}

TEST_F(VirtualPKOffsetMapTest, FindFirstN) {
    int64_t num = kNumRows;
    // All pass
    {
        BitsetType bitset(num);
        bitset.reset();  // 0 = pass
        BitsetTypeView view(bitset.data(), num);
        auto [offsets, has_more] = map_.find_first_n(10, view);
        ASSERT_EQ(offsets.size(), 10);
        // Should be sorted by PK order = offset order for virtual PKs
        for (int64_t i = 0; i < 10; i++) {
            EXPECT_EQ(offsets[i], i);
        }
        EXPECT_TRUE(has_more);
    }
    // None pass
    {
        BitsetType bitset(num);
        bitset.set();  // 1 = filtered out
        BitsetTypeView view(bitset.data(), num);
        auto [offsets, has_more] = map_.find_first_n(10, view);
        EXPECT_EQ(offsets.size(), 0);
        EXPECT_FALSE(has_more);
    }
}

TEST_F(VirtualPKOffsetMapTest, EmptyAndSeal) {
    EXPECT_FALSE(map_.empty());
    EXPECT_EQ(map_.memory_size(), sizeof(VirtualPKOffsetMap));

    // seal and insert are no-ops
    map_.seal();
    map_.insert(PkType(vpk(0)), 0);
    EXPECT_FALSE(map_.empty());

    // zero-row map
    VirtualPKOffsetMap empty_map(kSegmentID, 0);
    EXPECT_TRUE(empty_map.empty());
}
