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

#include <string_view>

#include "index/contracts/query/IPatternMatchReader.h"
#include "index/scalar/marisa/MarisaIndexReader.h"
#include "index/test_utils/ScalarReaderFactory.h"

namespace milvus::index::test {
namespace {

TEST(MarisaIndexOwnershipTest, ReadersRetainStorageAndOwnedValues) {
    auto trie = std::make_shared<marisa::Trie>();
    marisa::Keyset keys;
    keys.push_back("apple");
    trie->build(keys);
    auto storage = std::make_shared<MarisaIndexStorage>();
    storage->trie = std::move(trie);
    storage->str_ids_owner = std::make_shared<const std::vector<int64_t>>(
        std::initializer_list<int64_t>{0, -1});
    storage->csr_index_owner = std::make_shared<const std::vector<uint32_t>>(
        std::initializer_list<uint32_t>{0, 1});
    storage->csr_offsets_owner = std::make_shared<const std::vector<uint32_t>>(
        std::initializer_list<uint32_t>{0});
    storage->str_ids = storage->str_ids_owner->data();
    storage->str_ids_size = 2;
    storage->csr_index = storage->csr_index_owner->data();
    storage->csr_offsets = storage->csr_offsets_owner->data();
    storage->csr_num_keys = 1;
    std::weak_ptr<const MarisaIndexStorage> lifetime = storage;
    auto index = std::make_shared<MarisaIndex>(std::move(storage));
    auto first = index->CreateReader(nullptr);
    auto second = index->CreateReader(nullptr);
    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(index->Metadata().count, 2);
    index.reset();
    const auto* values =
        dynamic_cast<const IScalarValueReader<std::string_view>*>(first.get());
    ASSERT_NE(values, nullptr);
    auto owned = values->Lookup(0);
    first.reset();
    EXPECT_FALSE(lifetime.expired());
    const auto* nulls = dynamic_cast<const INullReader*>(second.get());
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(nulls->IsNull().count(), 1);
    second.reset();
    EXPECT_TRUE(lifetime.expired());
    EXPECT_EQ(owned, "apple");
}

TEST(MarisaIndexReaderTest, AdvertisesVarcharQueries) {
    for (const auto name : {"MarisaVarchar",
                            "MarisaVarcharMmap",
                            "MarisaVarcharNonNull",
                            "MarisaVarcharNonNullMmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        const auto caps = backend.DeriveCaps();
        EXPECT_TRUE(caps.predicate) << name;
        EXPECT_TRUE(caps.pattern_match) << name;
        EXPECT_TRUE(caps.value_lookup) << name;
        EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PrefixMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PostfixMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::InnerMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::RegexMatch),
                  PatternQueryPolicy::UseAndRun);
    }
}

}  // namespace
}  // namespace milvus::index::test
