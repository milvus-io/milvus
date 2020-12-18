// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <iostream>
#include <memory>

#include "db/metax/MetaAdapter.h"
#include "db/metax/MetaFactory.h"
#include "db/metax/MetaProxy.h"
#include "db/metax/MetaQueryHelper.h"
#include "db/metax/MetaQueryTraits.h"
#include "db/metax/MetaResField.h"
#include "db/metax/query/MetaCondition.h"

#include "db/snapshot/Resources.h"

// unittest folder
#include "db/utils.h"

template <typename F>
using MetaResField = milvus::engine::metax::MetaResField<F>;

template <typename Base, typename Equal, typename Derived>
using is_decay_base_of_and_equal_of = milvus::engine::metax::is_decay_base_of_and_equal_of<Base, Equal, Derived>;

using EngineType = milvus::engine::metax::EngineType;
using RangeType = milvus::engine::metax::RangeType;

//template <typename ... Args>
using milvus::engine::metax::Cols_;
using milvus::engine::metax::Col_;
using milvus::engine::metax::From_;
using milvus::engine::metax::Where_;
using milvus::engine::metax::Tab_;
using milvus::engine::metax::In_;
using milvus::engine::metax::Range_;
using milvus::engine::metax::One_;
using milvus::engine::metax::And_;

using milvus::engine::metax::is_columns_v;

TEST_F(MetaxTest, HelperTest) {
    auto proxy = std::make_shared<milvus::engine::metax::MetaProxy>(EngineType::mysql_);

    auto adapter = std::make_shared<milvus::engine::metax::MetaAdapter>(proxy);

    auto collection = std::make_shared<Collection>("metax_test_c1");
    int64_t id;
    auto status = adapter->Insert(collection, id);
    ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(MetaxTest, SelectTest) {
    meta_->Select(Cols_(Col_<Collection, IdField>(), Col_<Collection, NameField>()),
                  From_(Tab_<Collection>()),
                  Where_(And_(In_(Col_<Collection, IdField>(), {1, 2, 3}), Range_(Col_<Collection, IdField>(), 1, RangeType::eq_)))
                  );
    auto cols = Cols_(Col_<Collection, IdField>(), Col_<Collection, NameField>());
    std::cout << std::boolalpha << is_columns_v<decltype(cols)> << std::endl;
}

TEST_F(MetaxTest, TraitsTest) {
    auto ff = MetaResField<IdField>();

    std::cout << is_decay_base_of_and_equal_of<MetaResField<IdField>::FType, IdField, Collection>::value << std::endl;
    std::cout << is_decay_base_of_and_equal_of<MetaResField<MappingsField>::FType, MappingsField, Collection>::value << std::endl;
//    auto collection = std::make_shared<Collection>("a");
//    std::string b = bool(milvus::engine::meta::is_shared_ptr<decltype(collection)>::value) ? "True" : "False";
//    std::cout << "std::make_shared<Collection>(\"a\") is shared_ptr: "
//              << b << std::endl;
}

TEST_F(MetaxTest, ConditionTest) {
    auto where = milvus::engine::metax::Where<int, int>();
    std::cout << std::boolalpha << milvus::engine::metax::is_cond_clause_where<decltype(where)>::value << std::endl;

    auto tc1 = milvus::engine::metax::Column<Collection, IdField>();
    auto tc2 = std::vector<int>(1);

    std::cout << "tc1: " << std::boolalpha << milvus::engine::metax::is_column_v<decltype(tc1)> << std::endl;
    std::cout << "tc2: " << std::boolalpha << milvus::engine::metax::is_column_v<decltype(tc2)> << std::endl;

    auto w1 = milvus::engine::metax::Where<int, int, int>();
    std::cout << "w1: " << std::boolalpha << milvus::engine::metax::is_cond_clause<decltype(tc2)>::value << std::endl;
}
